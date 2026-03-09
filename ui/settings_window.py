from __future__ import annotations

import os
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import Qt, QSignalBlocker, QTimer, QThread, QUrl, Slot
from PySide6.QtGui import QColor, QDesktopServices
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSlider,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
    QColorDialog,
    QPlainTextEdit,
    QProgressBar,
)
try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:  # pragma: no cover
    def _qt_is_valid(obj: object) -> bool:
        return obj is not None

from ui.styles import StyleManager

log = logging.getLogger("settings_window")


@dataclass
class PillButtonStyle:
    padding_x: int = 10
    padding_y: int = 4
    radius: int = 6
    color: Optional[str] = None

    @classmethod
    def parse(cls, text: str) -> "PillButtonStyle":
        style = cls()
        if not isinstance(text, str):
            return style
        for chunk in text.split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            lower = chunk.lower()
            if lower.startswith("padding"):
                numbers = [int(token) for token in re.findall(r"\d+", chunk)]
                if len(numbers) >= 2:
                    style.padding_y = numbers[0]
                    style.padding_x = numbers[1]
                elif len(numbers) == 1:
                    style.padding_x = style.padding_y = numbers[0]
            elif lower.startswith("border-radius"):
                match = re.search(r"\d+", chunk)
                if match:
                    style.radius = int(match.group())
            elif "background-color" in lower:
                _, _, value = chunk.partition(":")
                color = value.strip()
                if color:
                    style.color = color
        return style

    def to_string(self) -> str:
        parts = [
            f"padding:{self.padding_y}px {self.padding_x}px",
            f"border-radius:{self.radius}px",
        ]
        if self.color:
            parts.append(f"background-color:{self.color}")
        return "; ".join(parts) + ";"


class SettingsWindow(QDialog):
    """Настройки приложения с отдельной вкладкой визуального оформления."""

    def __init__(
        self,
        *,
        state: Dict[str, bool],
        callbacks: Dict[str, Callable[..., Any]],
        ai_state: Optional[Dict[str, Any]] = None,
        ai_callbacks: Optional[Dict[str, Callable[[Dict[str, Any]], None]]] = None,
        tool_state: Optional[Dict[str, Any]] = None,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Настройки")
        self.resize(560, 460)
        self._callbacks = callbacks or {}
        self._ai_state = ai_state or {}
        self._ai_callbacks = ai_callbacks or {}
        self._tool_state = {
            str(key): dict(value)
            for key, value in dict(tool_state or {}).items()
            if isinstance(value, dict)
        }
        self._ai_pull_thread: Optional[QThread] = None
        self._ai_pull_worker: Optional[object] = None
        self._ai_pull_model: str = ""
        self._ai_tags_thread: Optional[QThread] = None
        self._ai_tags_worker: Optional[object] = None
        self._style_tab_widget: Optional[StyleEditorTab] = None
        self._style_tab_index: Optional[int] = None
        try:
            app = QApplication.instance()
            if app is not None:
                app.aboutToQuit.connect(self._on_app_about_to_quit)
        except Exception:
            pass

        root = QVBoxLayout(self)

        tabs = QTabWidget()
        StyleManager.instance().bind_stylesheet(tabs, "settings.tabs")
        tabs.addTab(self._build_general_tab(state, callbacks), "Общее")
        tabs.addTab(self._build_ai_tab(self._ai_state, self._ai_callbacks), "AI")
        placeholder = QWidget()
        ph_layout = QVBoxLayout(placeholder)
        ph_layout.setContentsMargins(12, 12, 12, 12)
        ph_layout.addWidget(QLabel("Открывайте вкладку «Оформление» при необходимости — загружается лениво."))
        ph_layout.addStretch(1)
        self._style_tab_index = tabs.addTab(placeholder, "Оформление")
        tabs.addTab(self._build_tools_tab(), "Tools")
        tabs.addTab(self._build_bug_report_tab(), "Баг-репорт")
        tabs.currentChanged.connect(lambda idx: self._maybe_init_style_tab(tabs, idx))
        root.addWidget(tabs, 1)

        box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        box.rejected.connect(self.reject)
        root.addWidget(box)

    def _maybe_init_style_tab(self, tabs: QTabWidget, idx: int) -> None:
        target = self._style_tab_index
        if target is None or idx != target:
            return
        if self._style_tab_widget is not None:
            return
        try:
            self._style_tab_widget = StyleEditorTab(self)
            tabs.removeTab(target)
            self._style_tab_index = tabs.insertTab(target, self._style_tab_widget, "Оформление")
            tabs.setCurrentIndex(self._style_tab_index)
        except Exception:
            log.exception("Failed to initialize style tab")
            self._style_tab_widget = None

    def reject(self) -> None:  # type: ignore[override]
        thread = getattr(self, "_ai_pull_thread", None)
        if self._thread_is_running(thread):
            QMessageBox.information(self, "Скачивание модели", "Дождитесь завершения скачивания модели.")
            return
        self._stop_ai_tags_thread()
        super().reject()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        thread = getattr(self, "_ai_pull_thread", None)
        if self._thread_is_running(thread):
            try:
                event.ignore()
            except Exception:
                pass
            QMessageBox.information(self, "Скачивание модели", "Дождитесь завершения скачивания модели.")
            return
        self._stop_ai_tags_thread()
        super().closeEvent(event)

    @Slot()
    def _on_app_about_to_quit(self) -> None:
        self._stop_ai_tags_thread()

    @staticmethod
    def _thread_is_running(thread: Optional[QThread]) -> bool:
        if thread is None:
            return False
        try:
            if not _qt_is_valid(thread):
                return False
        except Exception:
            return False

    def _stop_ai_tags_thread(self) -> None:
        thread = getattr(self, "_ai_tags_thread", None)
        worker = getattr(self, "_ai_tags_worker", None)
        if thread is None:
            return
        try:
            if worker is not None and hasattr(worker, "stop"):
                worker.stop()
        except Exception:
            pass
        try:
            if _qt_is_valid(thread) and thread.isRunning():
                thread.quit()
                thread.wait(1200)
        except Exception:
            pass
        self._ai_tags_thread = None
        self._ai_tags_worker = None
        try:
            return bool(thread.isRunning())
        except Exception:
            return False

    def _build_general_tab(self, state: Dict[str, bool], callbacks: Dict[str, Callable[..., Any]]) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(16)

        general = QGroupBox("Основные параметры")
        gen_form = QFormLayout(general)
        self.chk_auto_download = self._make_checkbox(
            text="Автоматически загружать медиа",
            checked=state.get("auto_download", False),
            callback=callbacks.get("auto_download"),
        )
        gen_form.addRow("Автоматически загружать медиа:", self.chk_auto_download)

        self.chk_ghost = self._make_checkbox(
            text="Режим «призрак»",
            checked=state.get("ghost_mode", False),
            callback=callbacks.get("ghost_mode"),
        )
        gen_form.addRow("Скрывать статус «прочитано»:", self.chk_ghost)

        self.chk_streamer = self._make_checkbox(
            text="Режим стримера",
            checked=state.get("streamer_mode", False),
            callback=callbacks.get("streamer_mode"),
        )
        gen_form.addRow("Режим стримера:", self.chk_streamer)

        self.chk_wave = self._make_checkbox(
            text="Анимация волны для голосовых",
            checked=state.get("voice_waveform", True),
            callback=callbacks.get("voice_waveform"),
        )
        gen_form.addRow("Показывать волну у голосовых:", self.chk_wave)

        self.chk_show_my_avatar = self._make_checkbox(
            text="Показывать мой аватар у моих сообщений",
            checked=state.get("show_my_avatar", True),
            callback=callbacks.get("show_my_avatar"),
        )
        gen_form.addRow("Мой аватар в чатах:", self.chk_show_my_avatar)

        self.chk_hide_hidden = self._make_checkbox(
            text="Скрывать скрытые чаты",
            checked=state.get("hide_hidden_chats", True),
            callback=callbacks.get("hide_hidden_chats"),
        )
        gen_form.addRow("Скрывать скрытые чаты в списке:", self.chk_hide_hidden)

        self.chk_keep_deleted = self._make_checkbox(
            text="Сохранять удалённые сообщения",
            checked=state.get("keep_deleted_messages", True),
            callback=callbacks.get("keep_deleted_messages"),
        )
        gen_form.addRow("Сохранять удалённые сообщения:", self.chk_keep_deleted)

        volume_row = QHBoxLayout()
        self.media_volume_slider = QSlider(Qt.Orientation.Horizontal)
        self.media_volume_slider.setRange(0, 100)
        try:
            volume_value = int(state.get("media_volume", 100) or 100)
        except Exception:
            volume_value = 100
        volume_value = max(0, min(100, volume_value))
        self.media_volume_slider.setValue(volume_value)
        self.media_volume_label = QLabel(f"{volume_value}%")
        volume_row.addWidget(self.media_volume_slider, 1)
        volume_row.addWidget(self.media_volume_label, 0)
        cb_volume = callbacks.get("media_volume")
        if callable(cb_volume):
            def _on_volume_changed(value: int) -> None:
                self.media_volume_label.setText(f"{int(value)}%")
                cb_volume(int(value))
            self.media_volume_slider.valueChanged.connect(_on_volume_changed)
        else:
            self.media_volume_slider.setEnabled(False)
        gen_form.addRow("Громкость медиа:", volume_row)
        layout.addWidget(general)

        media_box = QGroupBox("Медиа-инструменты")
        media_layout = QVBoxLayout(media_box)
        ffmpeg_hint = QLabel(
            "ffmpeg нужен для конвертации голосовых и кружков. "
            "Кнопка установит его в папку данных Telegram."
        )
        ffmpeg_hint.setWordWrap(True)
        media_layout.addWidget(ffmpeg_hint)
        self.btn_install_ffmpeg = QPushButton("Установить ffmpeg")
        ffmpeg_cb = callbacks.get("install_ffmpeg")
        if callable(ffmpeg_cb):
            self.btn_install_ffmpeg.clicked.connect(lambda: self._trigger_ffmpeg_install(ffmpeg_cb))
        else:
            self.btn_install_ffmpeg.setEnabled(False)
        media_layout.addWidget(self.btn_install_ffmpeg, alignment=Qt.AlignmentFlag.AlignLeft)
        self.ffmpeg_status = QLabel("")
        self.ffmpeg_status.setWordWrap(True)
        media_layout.addWidget(self.ffmpeg_status)
        self.ffmpeg_progress = QProgressBar()
        self.ffmpeg_progress.setRange(0, 100)
        self.ffmpeg_progress.setValue(0)
        self.ffmpeg_progress.hide()
        media_layout.addWidget(self.ffmpeg_progress)

        self.btn_install_voice_deps = QPushButton("Установить зависимости голосовых")
        voice_deps_cb = callbacks.get("install_voice_deps")
        if callable(voice_deps_cb):
            self.btn_install_voice_deps.clicked.connect(lambda: self._trigger_voice_deps_install(voice_deps_cb))
        else:
            self.btn_install_voice_deps.setEnabled(False)
        media_layout.addWidget(self.btn_install_voice_deps, alignment=Qt.AlignmentFlag.AlignLeft)
        self.voice_deps_status = QLabel("")
        self.voice_deps_status.setWordWrap(True)
        media_layout.addWidget(self.voice_deps_status)
        self.voice_deps_progress = QProgressBar()
        self.voice_deps_progress.setRange(0, 0)
        self.voice_deps_progress.hide()
        media_layout.addWidget(self.voice_deps_progress)
        layout.addWidget(media_box)

        theme_box = QGroupBox("Тема оформления")
        theme_layout = QVBoxLayout(theme_box)
        theme_layout.addWidget(QLabel("Режим темы интерфейса:"))
        theme_row = QHBoxLayout()
        self.rb_day = QRadioButton("Светлая тема")
        self.rb_night = QRadioButton("Тёмная тема")
        night_enabled = state.get("night_mode", True)
        self.rb_day.setChecked(not night_enabled)
        self.rb_night.setChecked(night_enabled)
        cb_theme = callbacks.get("night_mode")
        if cb_theme:
            self.rb_night.toggled.connect(lambda checked: cb_theme(bool(checked)))
        self.rb_day.setEnabled(bool(cb_theme))
        self.rb_night.setEnabled(bool(cb_theme))
        theme_row.addWidget(self.rb_day)
        theme_row.addWidget(self.rb_night)
        theme_row.addStretch(1)
        theme_layout.addLayout(theme_row)
        layout.addWidget(theme_box)

        privacy_box = QGroupBox("Приватность и AI")
        privacy_layout = QVBoxLayout(privacy_box)
        self.chk_ai = self._make_checkbox(
            text="Автоматически отвечать с помощью AI",
            checked=state.get("auto_ai", False),
            callback=callbacks.get("auto_ai"),
        )
        privacy_layout.addWidget(self.chk_ai)
        layout.addWidget(privacy_box)

        layout.addStretch(1)
        return tab

    def _trigger_ffmpeg_install(self, callback: Callable[[], Any]) -> None:
        try:
            self.ffmpeg_status.setText("Запуск установки ffmpeg…")
            self.ffmpeg_progress.setRange(0, 0)
            self.ffmpeg_progress.show()
        except Exception:
            pass
        callback()

    def _trigger_voice_deps_install(self, callback: Callable[[], Any]) -> None:
        try:
            self.voice_deps_status.setText("Запуск установки зависимостей голосовых…")
            self.voice_deps_progress.setRange(0, 0)
            self.voice_deps_progress.show()
        except Exception:
            pass
        callback()

    def set_ffmpeg_install_progress(self, status: str, done: int, total: int) -> None:
        if not hasattr(self, "ffmpeg_progress"):
            return
        text = str(status or "").strip()
        if text:
            self.ffmpeg_status.setText(text)
        if int(total or 0) > 0:
            self.ffmpeg_progress.setRange(0, 100)
            try:
                pct = int(max(0.0, min(1.0, float(done) / float(total))) * 100.0)
            except Exception:
                pct = 0
            self.ffmpeg_progress.setValue(pct)
        else:
            self.ffmpeg_progress.setRange(0, 0)
        self.ffmpeg_progress.show()

    def set_ffmpeg_install_finished(self, *, ok: bool, message: str) -> None:
        if not hasattr(self, "ffmpeg_progress"):
            return
        self.ffmpeg_progress.hide()
        self.ffmpeg_progress.setRange(0, 100)
        self.ffmpeg_progress.setValue(0)
        self.ffmpeg_status.setText(str(message or ("ffmpeg установлен" if ok else "Установка ffmpeg завершилась с ошибкой")))

    def set_voice_deps_install_progress(self, text: str) -> None:
        if not hasattr(self, "voice_deps_progress"):
            return
        status = str(text or "").strip()
        if status:
            self.voice_deps_status.setText(status)
        self.voice_deps_progress.setRange(0, 0)
        self.voice_deps_progress.show()

    def set_voice_deps_install_finished(self, *, ok: bool, message: str) -> None:
        if not hasattr(self, "voice_deps_progress"):
            return
        self.voice_deps_progress.hide()
        self.voice_deps_progress.setRange(0, 0)
        self.voice_deps_status.setText(str(message or ("Зависимости голосовых установлены" if ok else "Не удалось установить зависимости голосовых")))

    @staticmethod
    def _make_checkbox(text: str, *, checked: bool, callback: Optional[Callable[[bool], None]]) -> QCheckBox:
        cb = QCheckBox(text)
        cb.setChecked(bool(checked))
        if callback:
            cb.toggled.connect(callback)
        else:
            cb.setEnabled(False)
        return cb

    def _build_bug_report_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        hint = QLabel("Опишите проблему. К отчёту автоматически добавятся последние строки логов.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self.bug_report_edit = QPlainTextEdit()
        self.bug_report_edit.setPlaceholderText("Шаги воспроизведения, что ожидалось и что произошло…")
        self.bug_report_edit.setMinimumHeight(180)
        layout.addWidget(self.bug_report_edit, 1)

        self.bug_report_status = QLabel("")
        self.bug_report_status.setWordWrap(True)
        layout.addWidget(self.bug_report_status)

        btn_row = QHBoxLayout()
        btn_row.addStretch(1)
        self.btn_send_bug_report = QPushButton("Отправить на GitHub")
        self.btn_send_bug_report.clicked.connect(self._send_bug_report)
        btn_row.addWidget(self.btn_send_bug_report)
        layout.addLayout(btn_row)
        return tab

    def _build_tools_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        hint = QLabel("Инструменты обслуживания и принудительных фоновых операций.")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        self.btn_refresh_avatars = QPushButton("Принудительно подгрузить все аватарки")
        self.btn_refresh_avatars.clicked.connect(
            lambda: self._run_tool_action(self._callbacks.get("refresh_all_avatars"))
        )
        layout.addWidget(self.btn_refresh_avatars, 0, Qt.AlignmentFlag.AlignLeft)

        self.btn_scan_all = QPushButton("Анализ всех чатов/групп")
        self.btn_scan_all.clicked.connect(
            lambda: self._run_tool_action(self._callbacks.get("scan_all_chats"))
        )
        layout.addWidget(self.btn_scan_all, 0, Qt.AlignmentFlag.AlignLeft)

        self.tools_progress_label = QLabel("")
        self.tools_progress_label.setWordWrap(True)
        self.tools_progress_label.setStyleSheet("color:#b2c7de;")
        self.tools_progress_label.hide()
        layout.addWidget(self.tools_progress_label)

        self.tools_progress = QProgressBar()
        self.tools_progress.setRange(0, 100)
        self.tools_progress.setValue(0)
        self.tools_progress.hide()
        layout.addWidget(self.tools_progress)

        self.tools_avatar_state = QLabel("")
        self.tools_avatar_state.setWordWrap(True)
        self.tools_avatar_state.setStyleSheet("color:#8da8c4;")
        layout.addWidget(self.tools_avatar_state)

        self.tools_scan_state = QLabel("")
        self.tools_scan_state.setWordWrap(True)
        self.tools_scan_state.setStyleSheet("color:#8da8c4;")
        layout.addWidget(self.tools_scan_state)

        self.tools_status = QLabel("")
        self.tools_status.setWordWrap(True)
        self.tools_status.setStyleSheet("color:#8da8c4;")
        layout.addWidget(self.tools_status)
        self._refresh_tool_state_labels()
        layout.addStretch(1)
        return tab

    def _run_tool_action(self, callback: Optional[Callable[..., Any]]) -> None:
        if not callable(callback):
            self.set_tools_status("Инструмент недоступен в этой сборке.")
            return
        try:
            result = callback()
        except Exception as exc:
            log.exception("Settings tool action failed")
            self.set_tools_status(str(exc) or "Не удалось выполнить инструмент.")
            return
        if isinstance(result, tuple) and len(result) >= 2:
            self.set_tools_status(str(result[1] or ""))
        elif isinstance(result, str):
            self.set_tools_status(result)

    def set_tools_status(self, message: str) -> None:
        label = getattr(self, "tools_status", None)
        if label is not None:
            label.setText(str(message or ""))

    def set_tools_progress(self, message: str, done: int, total: int) -> None:
        label = getattr(self, "tools_progress_label", None)
        bar = getattr(self, "tools_progress", None)
        if label is not None:
            label.setText(str(message or ""))
            label.setVisible(bool(message))
        if bar is None:
            return
        total_value = max(0, int(total or 0))
        done_value = max(0, int(done or 0))
        if total_value > 0:
            bar.setRange(0, total_value)
            bar.setValue(min(done_value, total_value))
        else:
            bar.setRange(0, 0)
        bar.show()

    def clear_tools_progress(self) -> None:
        label = getattr(self, "tools_progress_label", None)
        bar = getattr(self, "tools_progress", None)
        if label is not None:
            label.clear()
            label.hide()
        if bar is not None:
            bar.reset()
            bar.hide()

    def set_tools_busy(self, busy: bool) -> None:
        enabled = not bool(busy)
        for name in ("btn_refresh_avatars", "btn_scan_all"):
            widget = getattr(self, name, None)
            if widget is not None:
                widget.setEnabled(enabled)

    def set_tool_state(self, key: str, state: Dict[str, Any]) -> None:
        if not key:
            return
        self._tool_state[str(key)] = dict(state or {})
        self._refresh_tool_state_labels()

    def _refresh_tool_state_labels(self) -> None:
        label_map = {
            "refresh_all_avatars": ("tools_avatar_state", "Подгрузка аватарок"),
            "scan_all_chats": ("tools_scan_state", "Скан чатов"),
        }
        for key, meta in label_map.items():
            attr_name, title = meta
            label = getattr(self, attr_name, None)
            if label is None:
                continue
            label.setText(self._format_tool_state_text(title, self._tool_state.get(key)))

    @staticmethod
    def _format_tool_state_text(title: str, state: Optional[Dict[str, Any]]) -> str:
        data = dict(state or {})
        if not bool(data.get("has_run")):
            return f"{title}: ещё не запускалось."
        timestamp = 0
        try:
            timestamp = int(data.get("last_run_at") or 0)
        except Exception:
            timestamp = 0
        if timestamp > 0:
            stamp = datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y %H:%M")
        else:
            stamp = "без времени"
        message = str(data.get("last_message") or "").strip()
        if not message:
            done = int(data.get("last_done") or 0)
            total = int(data.get("last_total") or 0)
            failed = int(data.get("last_failed") or 0)
            message = f"{done}/{total}, ошибок {failed}"
        return f"{title}: {stamp} • {message}"

    def _send_bug_report(self) -> None:
        callback = self._callbacks.get("send_bug_report") if isinstance(self._callbacks, dict) else None
        if not callable(callback):
            self.bug_report_status.setText("Отправка баг-репорта недоступна в этой сборке.")
            return
        comment = ""
        if hasattr(self, "bug_report_edit"):
            try:
                comment = str(self.bug_report_edit.toPlainText() or "").strip()
            except Exception:
                comment = ""
        self.btn_send_bug_report.setEnabled(False)
        self.bug_report_status.setText("Отправка баг-репорта…")
        ok = False
        message = ""
        try:
            result = callback(comment)
            if isinstance(result, tuple) and len(result) >= 2:
                ok = bool(result[0])
                message = str(result[1] or "")
            else:
                ok = bool(result)
        except Exception as exc:
            log.exception("Bug report action failed")
            ok = False
            message = str(exc)
        self.btn_send_bug_report.setEnabled(True)
        if ok:
            self.bug_report_status.setText(message or "Баг-репорт отправлен.")
            try:
                self.bug_report_edit.clear()
            except Exception:
                pass
        else:
            self.bug_report_status.setText(message or "Не удалось отправить баг-репорт.")


    def _build_ai_tab(self, state: Dict[str, Any], callbacks: Dict[str, Callable[[Dict[str, Any]], None]]) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        self._ai_callbacks = callbacks or {}
        self._ai_installed_models: set[str] = set()
        self._ai_curated_models: List[str] = [
            "gemma2",
            "gemma2:2b",
            "llama3.2",
            "llama3.2:1b",
            "llama3.2:3b",
            "llama3.1",
            "llama3",
            "mistral",
            "mixtral",
            "qwen2.5",
            "qwen2.5:7b",
            "qwen2.5:14b",
            "qwen2.5:32b",
            "qwen2.5-coder",
            "qwen2.5-coder:7b",
            "qwen2.5-coder:14b",
            "phi3.5",
            "phi3",
            "deepseek-r1",
            "deepseek-coder-v2",
            "codellama",
            "codestral",
            "starcoder2",
            "llava",
            "nomic-embed-text",
        ]
        self._ai_context_options: List[Optional[int]] = [2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144, None]

        model_box = QGroupBox("Модель")
        model_layout = QVBoxLayout(model_box)
        model_row = QHBoxLayout()
        model_row.addWidget(QLabel("Модель:"), 0)
        self.ai_model_combo = QComboBox()
        self.ai_model_combo.setEditable(True)
        self.ai_model_combo.currentIndexChanged.connect(self._on_ai_model_selected)
        try:
            line = self.ai_model_combo.lineEdit()
            if line is not None:
                line.editingFinished.connect(self._on_ai_model_selected)
        except Exception:
            pass
        model_row.addWidget(self.ai_model_combo, 1)
        self.ai_model_action = QPushButton("Скачать")
        self.ai_model_action.clicked.connect(self._on_ai_model_download)
        model_row.addWidget(self.ai_model_action)
        model_layout.addLayout(model_row)
        self.ai_model_status = QLabel("")
        model_layout.addWidget(self.ai_model_status)
        self.ai_model_progress = QProgressBar()
        self.ai_model_progress.setRange(0, 100)
        self.ai_model_progress.setValue(0)
        self.ai_model_progress.hide()
        model_layout.addWidget(self.ai_model_progress)
        layout.addWidget(model_box)

        ollama_box = QGroupBox("Ollama")
        ollama_layout = QVBoxLayout(ollama_box)
        ollama_hint = QLabel("Чтоб установить и работать с моделями нужно установить и запустить ollama.")
        ollama_hint.setWordWrap(True)
        ollama_layout.addWidget(ollama_hint)
        self.btn_ollama_download = QPushButton("Скачать Ollama")
        self.btn_ollama_download.clicked.connect(self._open_ollama_download_page)
        ollama_layout.addWidget(self.btn_ollama_download, alignment=Qt.AlignmentFlag.AlignLeft)
        layout.addWidget(ollama_box)

        ctx_box = QGroupBox("Контекст")
        ctx_layout = QVBoxLayout(ctx_box)
        ctx_row = QHBoxLayout()
        ctx_row.addWidget(QLabel("Размер контекста:"))
        self.ai_ctx_slider = QSlider(Qt.Orientation.Horizontal)
        self.ai_ctx_slider.setRange(0, len(self._ai_context_options) - 1)
        self.ai_ctx_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self.ai_ctx_slider.valueChanged.connect(self._on_ai_context_changed)
        ctx_row.addWidget(self.ai_ctx_slider, 1)
        self.ai_ctx_label = QLabel("")
        ctx_row.addWidget(self.ai_ctx_label)
        ctx_layout.addLayout(ctx_row)
        layout.addWidget(ctx_box)

        prompt_box = QGroupBox("Системный промпт")
        prompt_layout = QVBoxLayout(prompt_box)
        self.ai_prompt_edit = QPlainTextEdit()
        self.ai_prompt_edit.setMinimumHeight(180)
        self.ai_prompt_edit.setPlainText(str(state.get("prompt") or ""))
        prompt_layout.addWidget(self.ai_prompt_edit)
        prompt_save = QPushButton("Сохранить промпт")
        prompt_save.clicked.connect(self._on_ai_prompt_save)
        prompt_layout.addWidget(prompt_save, alignment=Qt.AlignmentFlag.AlignRight)
        layout.addWidget(prompt_box)

        self.ai_cuda_checkbox = QCheckBox("Использовать CUDA")
        self.ai_cuda_checkbox.setChecked(bool(state.get("use_cuda", True)))
        self.ai_cuda_checkbox.toggled.connect(self._on_ai_cuda_toggled)
        layout.addWidget(self.ai_cuda_checkbox)

        cross_box = QGroupBox("Память между чатами")
        cross_layout = QVBoxLayout(cross_box)
        self.ai_cross_chat_checkbox = QCheckBox("Подтягивать релевантный контекст из других AI-чатов")
        self.ai_cross_chat_checkbox.setChecked(bool(state.get("cross_chat_context", True)))
        self.ai_cross_chat_checkbox.toggled.connect(self._on_ai_cross_chat_toggled)
        cross_layout.addWidget(self.ai_cross_chat_checkbox)

        cross_limit_row = QHBoxLayout()
        cross_limit_row.addWidget(QLabel("Лимит фрагментов:"))
        self.ai_cross_chat_limit_spin = QSpinBox()
        self.ai_cross_chat_limit_spin.setRange(0, 20)
        try:
            cross_limit = int(state.get("cross_chat_limit", 6) or 6)
        except Exception:
            cross_limit = 6
        self.ai_cross_chat_limit_spin.setValue(max(0, min(cross_limit, 20)))
        self.ai_cross_chat_limit_spin.valueChanged.connect(self._on_ai_cross_chat_limit_changed)
        self.ai_cross_chat_limit_spin.setEnabled(bool(self.ai_cross_chat_checkbox.isChecked()))
        cross_limit_row.addWidget(self.ai_cross_chat_limit_spin)
        cross_limit_row.addStretch(1)
        cross_layout.addLayout(cross_limit_row)

        cross_hint = QLabel("0 — полностью отключить межчатовый контекст.")
        cross_hint.setWordWrap(True)
        cross_layout.addWidget(cross_hint)
        layout.addWidget(cross_box)

        layout.addStretch(1)

        self._refresh_ai_models(state)
        self._set_ai_context_value(state.get("context"))
        QTimer.singleShot(0, self._start_ai_tags_refresh)

        return tab
    def _emit_ai_change(self, payload: Dict[str, Any]) -> None:
        cb = self._ai_callbacks.get("on_change") if hasattr(self, "_ai_callbacks") else None
        if cb:
            try:
                cb(payload)
            except Exception:
                pass

    def _refresh_ai_models(self, state: Dict[str, Any], *, local_models: Optional[List[str]] = None) -> None:
        current_raw = str(state.get("model") or "")
        current = self._canonicalize_model_name(current_raw)
        local_models_raw = list(local_models) if isinstance(local_models, list) else list(getattr(self, "_ai_installed_models", set()) or [])
        installed = {self._canonicalize_model_name(name) for name in local_models_raw if name}
        installed.discard("")
        self._ai_installed_models = set(installed)

        names: List[str] = []
        seen: set[str] = set()

        def _add(name: str) -> None:
            canon = self._canonicalize_model_name(name)
            if not canon or canon in seen:
                return
            seen.add(canon)
            names.append(canon)

        for name in sorted(installed, key=lambda v: v.lower()):
            _add(name)
        for name in list(getattr(self, "_ai_curated_models", []) or []):
            _add(name)
        if current:
            _add(current)

        if not names:
            names = ["gemma2", "llama3.2", "mistral"]

        blocker = QSignalBlocker(self.ai_model_combo)
        self.ai_model_combo.clear()
        for name in names:
            is_installed = bool(name and name in self._ai_installed_models)
            prefix = "✅" if is_installed else "⬇️"
            self.ai_model_combo.addItem(f"{prefix} {name}", name)
        del blocker

        target = current or (names[0] if names else "")
        if target:
            idx = self.ai_model_combo.findData(target)
            if idx >= 0:
                self.ai_model_combo.setCurrentIndex(idx)
            else:
                self.ai_model_combo.setEditText(target)

        self._update_ai_model_controls()

    def _start_ai_tags_refresh(self) -> None:
        thread = getattr(self, "_ai_tags_thread", None)
        if self._thread_is_running(thread):
            return
        base_url = (os.getenv("DRAGO_OLLAMA_URL") or "http://localhost:11434").rstrip("/")
        try:
            from ui.ollama_workers import OllamaTagsWorker
        except Exception:
            return
        thread = QThread(self)
        worker = OllamaTagsWorker(base_url=base_url)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(self._on_ai_tags_loaded)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(self._on_ai_tags_thread_finished)
        thread.finished.connect(thread.deleteLater)
        self._ai_tags_thread = thread
        self._ai_tags_worker = worker
        thread.start()

    @Slot()
    def _on_ai_tags_thread_finished(self) -> None:
        self._ai_tags_thread = None
        self._ai_tags_worker = None

    def _on_ai_tags_loaded(self, models: list) -> None:
        self._ai_tags_worker = None
        try:
            local = [self._canonicalize_model_name(str(m)) for m in (models or []) if m]
        except Exception:
            local = []
        self._refresh_ai_models({"model": self._current_ai_model_name()}, local_models=local)

    def _fetch_local_models(self) -> List[str]:
        return []

    def _fetch_remote_models(self) -> List[str]:
        return []

    def _current_ai_model_name(self) -> str:
        name = str(self.ai_model_combo.currentData() or "").strip()
        if not name:
            name = str(self.ai_model_combo.currentText() or "").strip()
            if name.startswith(("✅", "⬇", "✓")):
                name = name[1:].lstrip()
        return self._canonicalize_model_name(name)

    @staticmethod
    def _canonicalize_model_name(name: str) -> str:
        raw = str(name or "").strip()
        if not raw:
            return ""
        if raw.startswith(("✅", "⬇", "✓")):
            raw = raw[1:].lstrip()
        if ":" in raw:
            base, tag = raw.split(":", 1)
            if tag.strip().lower() == "latest":
                return base.strip()
        return raw

    def _update_ai_model_controls(self) -> None:
        name = self._current_ai_model_name()
        installed = bool(name and name in self._ai_installed_models)
        if installed:
            self.ai_model_action.setText("✅ Скачано")
            self.ai_model_action.setEnabled(False)
            self.ai_model_status.setText("")
        else:
            self.ai_model_action.setText("⬇️ Скачать")
            self.ai_model_action.setEnabled(bool(name))

    def _on_ai_model_selected(self) -> None:
        self._update_ai_model_controls()
        name = self._current_ai_model_name()
        if name:
            self._emit_ai_change({"model": name})

    def _on_ai_model_download(self) -> None:
        name = self._current_ai_model_name()
        if not name or name in self._ai_installed_models:
            return
        self.ai_model_action.setEnabled(False)
        self.ai_model_combo.setEnabled(False)
        self.ai_model_action.setText("Скачиваем...")
        self.ai_model_status.setText("")
        self.ai_model_progress.setValue(0)
        self.ai_model_progress.setRange(0, 0)
        self.ai_model_progress.show()

        base_url = (os.getenv("DRAGO_OLLAMA_URL") or "http://localhost:11434").rstrip("/")
        self._ai_pull_model = name

        try:
            from ui.ollama_workers import OllamaPullWorker
        except Exception as exc:
            self.ai_model_progress.hide()
            self.ai_model_combo.setEnabled(True)
            self.ai_model_action.setText("Скачать")
            self.ai_model_action.setEnabled(True)
            self.ai_model_status.setText("")
            try:
                QMessageBox.warning(
                    self,
                    "Скачивание модели",
                    f"Не удалось запустить загрузку:\n{self._humanize_ollama_error(str(exc))}",
                )
            except Exception:
                pass
            return

        thread = QThread(self)
        worker = OllamaPullWorker(name, base_url=base_url)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.progress.connect(self._on_ai_pull_progress)
        worker.finished.connect(self._on_ai_pull_finished)
        worker.finished.connect(thread.quit)
        worker.finished.connect(worker.deleteLater)
        thread.finished.connect(self._on_ai_pull_thread_finished)
        thread.finished.connect(thread.deleteLater)
        self._ai_pull_thread = thread
        self._ai_pull_worker = worker
        thread.start()

    @Slot()
    def _on_ai_pull_thread_finished(self) -> None:
        self._ai_pull_thread = None
        self._ai_pull_worker = None

    def _on_ai_pull_progress(self, status: str, completed: object, total: object) -> None:
        if not hasattr(self, "ai_model_progress"):
            return
        try:
            total_int = int(total or 0)
        except Exception:
            total_int = 0
        try:
            done_int = int(completed or 0)
        except Exception:
            done_int = 0
        if total_int > 0:
            percent = int(max(0.0, min(1.0, done_int / float(total_int))) * 100.0)
            self.ai_model_progress.setRange(0, 100)
            self.ai_model_progress.setValue(percent)
        else:
            self.ai_model_progress.setRange(0, 0)
        if status:
            self.ai_model_status.setText(str(status))

    def _on_ai_pull_finished(self, success: bool, message: str) -> None:
        name = getattr(self, "_ai_pull_model", "") or ""
        self.ai_model_progress.hide()
        self.ai_model_combo.setEnabled(True)
        self.ai_model_action.setText("Скачать")
        self.ai_model_action.setEnabled(True)
        if success:
            self.ai_model_status.setText(message or f"{name} скачана")
            self._start_ai_tags_refresh()
            self._emit_ai_change({"model": name})
        else:
            self.ai_model_status.setText("")
            error_text = self._humanize_ollama_error(str(message or ""))
            try:
                QMessageBox.warning(self, "Скачивание модели", f"Не удалось скачать {name}:\n{error_text}")
            except Exception:
                pass
        self._ai_pull_model = ""

    @staticmethod
    def _humanize_ollama_error(raw: str) -> str:
        msg = str(raw or "").strip()
        lower = msg.lower()
        if "failed to establish a new connection" in lower or "connection refused" in lower:
            return (
                "Не удалось подключиться к Ollama (http://localhost:11434).\n"
                "Установите и запустите Ollama, затем повторите загрузку модели."
            )
        if "timed out" in lower or "timeout" in lower:
            return "Ollama не ответил вовремя. Проверьте, что сервис запущен и сеть не блокирует порт 11434."
        return msg or "Неизвестная ошибка Ollama."

    def _set_ai_context_value(self, ctx: Optional[int]) -> None:
        if not hasattr(self, "ai_ctx_slider"):
            return
        options = self._ai_context_options
        if ctx is None:
            idx = len(options) - 1
        else:
            try:
                ctx_int = int(ctx)
            except Exception:
                ctx_int = options[0]
            idx = 0
            for i, value in enumerate(options):
                if value is None:
                    idx = i
                    break
                if ctx_int <= value:
                    idx = i
                    break
        blocker = QSignalBlocker(self.ai_ctx_slider)
        self.ai_ctx_slider.setValue(idx)
        del blocker
        self._update_ai_context_label(idx)

    def _update_ai_context_label(self, idx: int) -> None:
        options = self._ai_context_options
        if not options:
            return
        idx = max(0, min(idx, len(options) - 1))
        value = options[idx]
        if value is None:
            label = "∞"
        else:
            label = f"{int(value) // 1024}K"
        self.ai_ctx_label.setText(label)

    def _on_ai_context_changed(self, value: int) -> None:
        idx = max(0, min(value, len(self._ai_context_options) - 1))
        ctx_value = self._ai_context_options[idx]
        self._update_ai_context_label(idx)
        self._emit_ai_change({"context": ctx_value})

    def _on_ai_prompt_save(self) -> None:
        text = self.ai_prompt_edit.toPlainText()
        self._emit_ai_change({"prompt": text})

    def _on_ai_cuda_toggled(self, checked: bool) -> None:
        self._emit_ai_change({"use_cuda": bool(checked)})

    def _on_ai_cross_chat_toggled(self, checked: bool) -> None:
        self._emit_ai_change({"cross_chat_context": bool(checked)})
        if hasattr(self, "ai_cross_chat_limit_spin"):
            self.ai_cross_chat_limit_spin.setEnabled(bool(checked))

    def _on_ai_cross_chat_limit_changed(self, value: int) -> None:
        self._emit_ai_change({"cross_chat_limit": int(value)})

    def _open_ollama_download_page(self) -> None:
        QDesktopServices.openUrl(QUrl("https://ollama.com/download"))


class StyleEditorTab(QWidget):
    """Visual editor for palette/padding sourced from styles.json."""

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._manager = StyleManager.instance()
        self._profiles: List[Dict[str, str]] = []
        self._color_buttons: Dict[str, List[QPushButton]] = defaultdict(list)
        self._value_bindings: Dict[str, List[Callable[[Any], None]]] = defaultdict(list)
        self._list_layouts: Dict[str, QHBoxLayout] = {}
        self._list_controls: Dict[str, Tuple[QPushButton, QPushButton]] = {}
        self._style_editors: Dict[str, QPlainTextEdit] = {}
        self._symmetry_rules = self._build_symmetry_rules()
        self._symmetry_enabled = True
        self._pill_style = PillButtonStyle()
        self._pending_updates: Dict[str, Tuple[Any, ...]] = {}
        self._update_timer = QTimer(self)
        self._update_timer.setInterval(120)
        self._update_timer.setSingleShot(True)
        self._update_timer.timeout.connect(self._flush_pending_updates)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        profile_row = QHBoxLayout()
        profile_row.addWidget(QLabel("Профиль:"))
        self.profile_combo = QComboBox()
        profile_row.addWidget(self.profile_combo, 1)
        self.btn_delete = QPushButton("Delete")
        self.btn_delete.clicked.connect(self._delete_preset)
        profile_row.addWidget(self.btn_delete)
        btn_reload = QPushButton("Перезагрузить styles.json")
        btn_reload.clicked.connect(self._reload_styles)
        profile_row.addWidget(btn_reload)
        layout.addLayout(profile_row)

        preset_row = QHBoxLayout()
        self.preset_edit = QLineEdit()
        self.preset_edit.setPlaceholderText("Имя пресета")
        preset_row.addWidget(self.preset_edit, 1)
        btn_save = QPushButton("Сохранить пресет")
        btn_save.clicked.connect(self._save_preset)
        preset_row.addWidget(btn_save)
        layout.addLayout(preset_row)

        symmetry_row = QHBoxLayout()
        self.symmetry_toggle = QCheckBox("Symmetry mode")
        self.symmetry_toggle.setChecked(True)
        self.symmetry_toggle.toggled.connect(self._toggle_symmetry)
        symmetry_row.addWidget(self.symmetry_toggle)
        self.symmetry_hint = QLabel("")
        symmetry_row.addWidget(self.symmetry_hint, 1)
        layout.addLayout(symmetry_row)

        self.quick_group = QGroupBox("Быстрая палитра")
        self._manager.bind_stylesheet(self.quick_group, "style_editor.group")
        quick_layout = QGridLayout(self.quick_group)
        quick_layout.setContentsMargins(12, 12, 12, 12)
        quick_layout.setHorizontalSpacing(16)
        quick_layout.setVerticalSpacing(10)
        quick_fields = [
            ("App background", "palette.Window"),
            ("Panels & cards", "palette.Button"),
            ("Accent", "palette.Highlight"),
            ("Interface text", "palette.Text"),
        ]
        for idx, (label, path) in enumerate(quick_fields):
            self._add_quick_picker(quick_layout, idx // 2, idx % 2, label, path)

        self.manual_container = QWidget()
        manual_layout = QVBoxLayout(self.manual_container)
        manual_layout.setContentsMargins(0, 0, 0, 0)
        manual_layout.setSpacing(12)

        palette_box = QGroupBox("Палитра интерфейса")
        self._manager.bind_stylesheet(palette_box, "style_editor.group")
        palette_form = QFormLayout(palette_box)
        palette_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        for label, path in [
            ("Window", "palette.Window"),
            ("Base", "palette.Base"),
            ("Alternate base", "palette.AlternateBase"),
            ("Buttons", "palette.Button"),
            ("Button text", "palette.ButtonText"),
            ("Body text", "palette.Text"),
            ("Window text", "palette.WindowText"),
            ("Placeholder", "palette.PlaceholderText"),
            ("Highlight", "palette.Highlight"),
            ("Bright text", "palette.BrightText"),
            ("Selected text", "palette.HighlightedText"),
        ]:
            self._add_color_button(palette_form, label, path)
        manual_layout.addWidget(palette_box)

        self.section_tabs = QTabWidget()
        layout.addWidget(self.section_tabs, 1)
        self.section_tabs.addTab(self._build_palette_tab(), "Palette")
        self.section_tabs.addTab(self._build_messages_tab(), "Messages")
        self.section_tabs.addTab(self._build_interface_tab(), "Interface")
        self.section_tabs.addTab(self._build_styles_tab(), "Components")

        self.profile_combo.currentIndexChanged.connect(self._on_profile_changed)
        self._manager.style_changed.connect(lambda _: self._refresh_all())
        self._manager.profile_changed.connect(lambda name: self._refresh_profiles(select=name))

        self._refresh_profiles()
        self._refresh_all()
        self._toggle_symmetry(self.symmetry_toggle.isChecked())

    def _build_palette_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self.quick_group)
        layout.addWidget(self.manual_container)
        layout.addWidget(self._build_disabled_palette_box())
        layout.addStretch(1)
        return tab

    def _build_disabled_palette_box(self) -> QGroupBox:
        box = QGroupBox("Палитра неактивного состояния")
        self._manager.bind_stylesheet(box, "style_editor.group")
        form = QFormLayout(box)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        for label, path in [
            ("Disabled text", "palette.Disabled.Text"),
            ("Button text (disabled)", "palette.Disabled.ButtonText"),
            ("Window text (disabled)", "palette.Disabled.WindowText"),
        ]:
            self._add_color_button(form, label, path)
        return box

    def _build_messages_tab(self) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        layout.addWidget(self._build_message_colors_box())
        legacy = self._build_legacy_bubbles_box()
        if legacy:
            layout.addWidget(legacy)
        layout.addWidget(self._build_username_palette_box())
        layout.addWidget(self._build_metrics_box())
        layout.addWidget(self._build_quote_box())
        layout.addStretch(1)
        scroll.setWidget(tab)
        return scroll

    def _build_message_colors_box(self) -> QGroupBox:
        box = QGroupBox("Цвета сообщений")
        self._manager.bind_stylesheet(box, "style_editor.group")
        form = QFormLayout(box)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        for role, label in [
            ("me", "Outgoing"),
            ("assistant", "Assistant"),
            ("other", "Incoming"),
        ]:
            self._add_color_button(form, f"{label}: background", f"message_widgets.bubbles.{role}.bg")
            self._add_color_button(form, f"{label}: text", f"message_widgets.bubbles.{role}.text")
            self._add_color_button(form, f"{label}: border", f"message_widgets.bubbles.{role}.border")
        self._add_color_button(form, "Links", "message_widgets.link_color")
        self._add_color_button(form, "Deleted: background", "message_widgets.deleted_bubble.bg")
        self._add_color_button(form, "Deleted: border", "message_widgets.deleted_bubble.border")
        self._add_color_button(form, "Deleted: text", "message_widgets.deleted_bubble.text")
        return box

    def _build_legacy_bubbles_box(self) -> Optional[QGroupBox]:
        bubbles = self._manager.value("bubbles", {})
        if not isinstance(bubbles, dict) or not bubbles:
            return None
        box = QGroupBox("Палитра пузырей (устаревшая)")
        self._manager.bind_stylesheet(box, "style_editor.group")
        form = QFormLayout(box)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
        for role in sorted(bubbles.keys()):
            caption = self._humanize_label(role)
            self._add_color_button(form, f"{caption}: background", f"bubbles.{role}.bg")
            self._add_color_button(form, f"{caption}: text", f"bubbles.{role}.text")
            self._add_color_button(form, f"{caption}: border", f"bubbles.{role}.border")
            self._add_color_button(form, f"{caption}: link", f"bubbles.{role}.link")
        return box

    def _build_username_palette_box(self) -> QGroupBox:
        box = QGroupBox("Палитра имён")
        self._manager.bind_stylesheet(box, "style_editor.group")
        wrapper = QVBoxLayout(box)
        hint = QLabel("Used to color random usernames in feeds. Tap a chip to recolor.")
        hint.setWordWrap(True)
        wrapper.addWidget(hint)
        row = QHBoxLayout()
        row.setSpacing(8)
        wrapper.addLayout(row)
        path = "message_feed.username_colors"
        self._list_layouts[path] = row
        controls = QHBoxLayout()
        add_btn = QPushButton("Add color")
        add_btn.clicked.connect(lambda: self._append_list_color(path))
        remove_btn = QPushButton("Remove last")
        remove_btn.clicked.connect(lambda: self._remove_list_color(path))
        controls.addWidget(add_btn)
        controls.addWidget(remove_btn)
        controls.addStretch(1)
        wrapper.addLayout(controls)
        self._list_controls[path] = (add_btn, remove_btn)
        self._register_binding(path, lambda colors: self._render_color_list(path, colors))
        return box

    def _build_metrics_box(self) -> QGroupBox:
        box = QGroupBox("Макет пузырей")
        self._manager.bind_stylesheet(box, "style_editor.group")
        layout = QHBoxLayout(box)
        layout.addWidget(QLabel("Radius:"))
        self.radius_slider = QSlider(Qt.Orientation.Horizontal)
        self.radius_slider.setRange(6, 32)
        self.radius_slider.setSingleStep(1)
        self.radius_slider.valueChanged.connect(self._on_radius_changed)
        layout.addWidget(self.radius_slider, 1)
        self.radius_label = QLabel("")
        self.radius_label.setObjectName("StyleMetricLabel")
        layout.addWidget(self.radius_label)
        return box

    def _build_quote_box(self) -> QGroupBox:
        box = QGroupBox("Стиль блока цитаты")
        self._manager.bind_stylesheet(box, "style_editor.group")
        vbox = QVBoxLayout(box)
        editor = QPlainTextEdit()
        editor.setPlaceholderText("CSS snippet applied to quote blocks.")
        editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        editor.setFixedHeight(110)
        vbox.addWidget(editor)
        self._bind_plain_text(editor, "message_widgets.quote_block_style")
        return box

    def _build_interface_tab(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        meta_box = QGroupBox("Паспорт темы")
        self._manager.bind_stylesheet(meta_box, "style_editor.group")
        meta_form = QFormLayout(meta_box)
        self.label_edit = QLineEdit()
        self.label_edit.setPlaceholderText("Shown name in menus")
        meta_form.addRow("Display name", self.label_edit)
        layout.addWidget(meta_box)
        self._bind_line_edit(self.label_edit, "label")

        colors_dict = self._manager.mapping("colors")
        if colors_dict:
            colors_box = QGroupBox("Акцентные иконки и цвета")
            self._manager.bind_stylesheet(colors_box, "style_editor.group")
            colors_form = QFormLayout(colors_box)
            colors_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
            for key in sorted(colors_dict.keys()):
                self._add_color_button(colors_form, self._humanize_label(key), None, mapping=("colors", key))
            layout.addWidget(colors_box)

        animation_colors = self._manager.value("animation_demo.colors", {})
        if isinstance(animation_colors, dict) and animation_colors:
            animation_box = QGroupBox("Превью анимации")
            self._manager.bind_stylesheet(animation_box, "style_editor.group")
            animation_form = QFormLayout(animation_box)
            animation_form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
            for key in sorted(animation_colors.keys()):
                self._add_color_button(animation_form, self._humanize_label(key), f"animation_demo.colors.{key}")
            layout.addWidget(animation_box)

        layout.addWidget(self._build_pill_button_box())

        stylesheet_box = QGroupBox("Таблица стилей приложения")
        self._manager.bind_stylesheet(stylesheet_box, "style_editor.group")
        box_layout = QVBoxLayout(stylesheet_box)
        info = QLabel("Blocks are applied to QApplication style sheet. Separate rules with blank lines.")
        info.setWordWrap(True)
        box_layout.addWidget(info)
        self.app_stylesheet_edit = QPlainTextEdit()
        self.app_stylesheet_edit.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
        self.app_stylesheet_edit.setPlaceholderText("Enter QSS blocks...")
        self.app_stylesheet_edit.setFixedHeight(180)
        box_layout.addWidget(self.app_stylesheet_edit)
        self._bind_plain_text_list(self.app_stylesheet_edit, "app_stylesheet")
        layout.addWidget(stylesheet_box)

        layout.addStretch(1)
        return tab

    def _build_pill_button_box(self) -> QGroupBox:
        box = QGroupBox("Кнопка-пилюля")
        self._manager.bind_stylesheet(box, "style_editor.group")
        form = QFormLayout(box)
        form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)

        padding_row = QHBoxLayout()
        padding_row.setSpacing(8)
        padding_row.addWidget(QLabel("X:"))
        self.pill_padding_x = QSpinBox()
        self.pill_padding_x.setRange(0, 80)
        self.pill_padding_x.valueChanged.connect(lambda value: self._update_pill_style(padding_x=int(value)))
        padding_row.addWidget(self.pill_padding_x)
        padding_row.addWidget(QLabel("Y:"))
        self.pill_padding_y = QSpinBox()
        self.pill_padding_y.setRange(0, 80)
        self.pill_padding_y.valueChanged.connect(lambda value: self._update_pill_style(padding_y=int(value)))
        padding_row.addWidget(self.pill_padding_y)
        padding_row.addStretch(1)
        form.addRow("Padding:", padding_row)

        self.pill_radius = QSpinBox()
        self.pill_radius.setRange(0, 80)
        self.pill_radius.valueChanged.connect(lambda value: self._update_pill_style(radius=int(value)))
        form.addRow("Radius:", self.pill_radius)

        color_row = QHBoxLayout()
        color_row.setSpacing(6)
        self.pill_color_btn = self._make_color_button()
        self.pill_color_btn.clicked.connect(self._edit_pill_color)
        color_row.addWidget(self.pill_color_btn)
        self.pill_color_clear = QPushButton("Reset")
        self.pill_color_clear.clicked.connect(self._clear_pill_color)
        color_row.addWidget(self.pill_color_clear)
        color_row.addStretch(1)
        form.addRow("Color:", color_row)
        return box

    def _build_styles_tab(self) -> QWidget:
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        styles = self._manager.mapping("styles")
        self._style_editors = {}
        if not styles:
            placeholder = QLabel("Current theme does not define component styles.")
            placeholder.setWordWrap(True)
            layout.addWidget(placeholder)
        else:
            grouped: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
            for full_key in sorted(styles.keys()):
                if full_key == "settings.pill_button":
                    continue
                prefix, _, suffix = full_key.partition(".")
                grouped[prefix or "General"].append((suffix, full_key))
            for prefix, entries in grouped.items():
                box = QGroupBox(self._humanize_label(prefix))
                self._manager.bind_stylesheet(box, "style_editor.group")
                form = QFormLayout(box)
                form.setLabelAlignment(Qt.AlignmentFlag.AlignLeft)
                for suffix, full_key in entries:
                    editor = QPlainTextEdit()
                    editor.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
                    editor.setPlaceholderText("QSS snippet")
                    editor.setFixedHeight(120)
                    editor.textChanged.connect(
                        lambda fk=full_key, widget=editor: self._queue_mapping_update("styles", fk, widget.toPlainText())
                    )
                    form.addRow(self._humanize_label(suffix or prefix), editor)
                    self._style_editors[full_key] = editor
                layout.addWidget(box)
        layout.addStretch(1)
        scroll.setWidget(container)
        return scroll

    def _bind_line_edit(self, widget: QLineEdit, path: str) -> None:
        def _update(value: Any) -> None:
            text = "" if value is None else str(value)
            if widget.text() != text:
                blocker = QSignalBlocker(widget)
                widget.setText(text)

        self._register_binding(path, _update)
        widget.textEdited.connect(lambda text: self._queue_value_update(path, text))

    def _bind_plain_text(self, widget: QPlainTextEdit, path: str) -> None:
        def _update(value: Any) -> None:
            text = "" if value is None else str(value)
            if widget.toPlainText() != text:
                blocker = QSignalBlocker(widget)
                widget.setPlainText(text)

        self._register_binding(path, _update)
        widget.textChanged.connect(lambda: self._queue_value_update(path, widget.toPlainText()))

    def _bind_plain_text_list(self, widget: QPlainTextEdit, path: str) -> None:
        def _update(value: Any) -> None:
            if isinstance(value, list):
                text = "\n\n".join(value)
            elif isinstance(value, str):
                text = value
            else:
                text = ""
            if widget.toPlainText() != text:
                blocker = QSignalBlocker(widget)
                widget.setPlainText(text)

        self._register_binding(path, _update)
        widget.textChanged.connect(lambda: self._queue_value_update(path, self._split_stylesheet(widget.toPlainText())))

    def _register_binding(self, path: str, callback: Callable[[Any], None]) -> None:
        self._value_bindings[path].append(callback)

    @staticmethod
    def _humanize_label(raw: str) -> str:
        if not raw:
            return "General"
        parts = [part.strip().replace("_", " ") for part in raw.split(".") if part]
        return " ? ".join(part.title() for part in parts) if parts else raw.title()

    @staticmethod
    def _split_stylesheet(text: str) -> List[str]:
        if not text.strip():
            return []
        blocks = [chunk for chunk in text.split("\n\n") if chunk.strip()]
        return blocks or [text]

    def _color_binding_key(self, *, path: Optional[str] = None, mapping: Optional[Tuple[str, str]] = None) -> str:
        if mapping:
            return f"mapping::{mapping[0]}::{mapping[1]}"
        if path:
            return path
        raise ValueError("Either path or mapping must be provided")

    def _register_color_button(self, key: str, button: QPushButton) -> None:
        self._color_buttons[key].append(button)

    def _make_color_button(self) -> QPushButton:
        btn = QPushButton()
        btn.setObjectName("StyleColorButton")
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        return btn

    def _update_pill_style(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self._pill_style, key, int(value))
        self._emit_pill_style()

    def _emit_pill_style(self) -> None:
        text = self._pill_style.to_string()
        self._queue_mapping_update("styles", "settings.pill_button", text)

    def _sync_pill_button_controls(self) -> None:
        if not hasattr(self, "pill_padding_x"):
            return
        styles = self._manager.mapping("styles")
        raw = styles.get("settings.pill_button", "")
        self._pill_style = PillButtonStyle.parse(raw)
        for widget, value in [
            (self.pill_padding_x, self._pill_style.padding_x),
            (self.pill_padding_y, self._pill_style.padding_y),
            (self.pill_radius, self._pill_style.radius),
        ]:
            blocker = QSignalBlocker(widget)
            widget.setValue(int(value))
        self._update_pill_color_button(self._pill_style.color)

    def _update_pill_color_button(self, color: Optional[str]) -> None:
        if not hasattr(self, "pill_color_btn"):
            return
        base = self._manager.stylesheet("style_editor.color_button")
        if color and QColor(color).isValid():
            text_color = self._auto_text_color(QColor(color))
            text = color.upper()
            style = f"{base} background-color:{color}; color:{text_color};"
        else:
            text = "Auto"
            style = base
        self.pill_color_btn.setText(text)
        self.pill_color_btn.setStyleSheet(style)

    def _edit_pill_color(self) -> None:
        current = self._pill_style.color or "#59b7e9"
        color = QColorDialog.getColor(QColor(current), self, "Выберите цвет")
        if color.isValid():
            self._pill_style.color = color.name()
            self._update_pill_color_button(self._pill_style.color)
            self._emit_pill_style()

    def _clear_pill_color(self) -> None:
        if self._pill_style.color is None:
            return
        self._pill_style.color = None
        self._update_pill_color_button(None)
        self._emit_pill_style()

    def _refresh_profiles(self, select: Optional[str] = None) -> None:
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self._profiles = self._manager.profile_list()
        active = select or self._manager.active_profile_name()
        target_idx = 0
        for idx, meta in enumerate(self._profiles):
            self.profile_combo.addItem(meta["label"], meta)
            if meta["name"] == active:
                target_idx = idx
        self.profile_combo.setCurrentIndex(target_idx)
        self.profile_combo.blockSignals(False)
        self._update_profile_actions()

    def _update_profile_actions(self) -> None:
        meta = self.profile_combo.currentData()
        self.btn_delete.setEnabled(bool(meta and meta.get("type") == "preset"))

    def _on_profile_changed(self, index: int) -> None:
        self._flush_pending_updates()
        meta = self.profile_combo.itemData(index)
        if not meta:
            return
        self._manager.set_active_profile(meta["name"])
        self._update_profile_actions()

    def _refresh_all(self) -> None:
        self._refresh_controls()

    def _refresh_controls(self) -> None:
        button_base = self._manager.stylesheet("style_editor.color_button")
        for key, buttons in self._color_buttons.items():
            current = self._color_value_for_key(key, "#000000")
            text_color = self._auto_text_color(QColor(current))
            for btn in buttons:
                btn.setText(current.upper())
                btn.setStyleSheet(f"{button_base} background-color:{current}; color:{text_color};")
        for path, callbacks in self._value_bindings.items():
            value = self._manager.value(path)
            for callback in callbacks:
                callback(value)
        radius = int(self._manager.metric("message_widgets.metrics.body_radius", 14) or 14)
        self.radius_slider.blockSignals(True)
        self.radius_slider.setValue(radius)
        self.radius_slider.blockSignals(False)
        self.radius_label.setText(f"{radius}px")
        self._refresh_style_editors()
        self._sync_pill_button_controls()

    def _refresh_style_editors(self) -> None:
        styles = self._manager.mapping("styles")
        for key, editor in self._style_editors.items():
            text = styles.get(key, "")
            if editor.toPlainText() != text:
                blocker = QSignalBlocker(editor)
                editor.setPlainText(text)

    def _add_color_button(
        self,
        form: QFormLayout,
        label: str,
        path: Optional[str] = None,
        *,
        mapping: Optional[Tuple[str, str]] = None,
        anchor: Optional[str] = None,
    ) -> None:
        btn = self._make_color_button()
        key = self._color_binding_key(path=path, mapping=mapping)
        btn.clicked.connect(lambda _, k=key, a=anchor: self._select_color(k, anchor=a))
        form.addRow(label, btn)
        self._register_color_button(key, btn)

    def _add_quick_picker(self, grid: QGridLayout, row: int, column: int, label: str, path: str) -> None:
        cell = QWidget()
        box = QVBoxLayout(cell)
        box.setContentsMargins(0, 0, 0, 0)
        box.setSpacing(4)
        caption = QLabel(label)
        caption.setObjectName("StyleQuickLabel")
        box.addWidget(caption)
        btn = self._make_color_button()
        key = self._color_binding_key(path=path)
        btn.clicked.connect(lambda _, k=key, anchor=path: self._select_color(k, anchor=anchor))
        box.addWidget(btn)
        grid.addWidget(cell, row, column)
        self._register_color_button(key, btn)

    def _select_color(self, key: str, anchor: Optional[str] = None) -> None:
        current = self._color_value_for_key(key, "#ffffff")
        label = anchor or key
        color = QColorDialog.getColor(QColor(current), self, f"Выберите цвет")
        if color.isValid():
            if anchor and self._symmetry_enabled and anchor in self._symmetry_rules:
                self._apply_symmetry(anchor, color)
            else:
                self._apply_color_value(key, color.name())

    def _color_value_for_key(self, key: str, default: str) -> str:
        if key.startswith("mapping::"):
            _, mapping, entry = key.split("::", 2)
            mapping_values = self._manager.value(mapping, {})
            if isinstance(mapping_values, dict):
                return str(mapping_values.get(entry, default))
            return default
        return str(self._manager.value(key, default))

    def _apply_color_value(self, key: str, value: str) -> None:
        if key.startswith("mapping::"):
            _, mapping, entry = key.split("::", 2)
            self._queue_mapping_update(mapping, entry, value)
        else:
            self._queue_value_update(key, value)

    def _render_color_list(self, path: str, colors: Optional[List[str]]) -> None:
        layout = self._list_layouts.get(path)
        if not layout:
            return
        palette = list(colors or [])
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        button_base = self._manager.stylesheet("style_editor.color_button")
        for idx, value in enumerate(palette):
            color_hex = str(value)
            btn = self._make_color_button()
            btn.setFixedHeight(32)
            btn.clicked.connect(lambda _, i=idx, p=path: self._edit_color_list_entry(p, i))
            text_color = self._auto_text_color(QColor(color_hex))
            btn.setText(color_hex.upper())
            btn.setStyleSheet(f"{button_base} background-color:{color_hex}; color:{text_color};")
            layout.addWidget(btn)
        layout.addStretch(1)
        controls = self._list_controls.get(path)
        if controls:
            controls[1].setEnabled(len(palette) > 1)

    def _edit_color_list_entry(self, path: str, index: int) -> None:
        colors = list(self._manager.value(path, []) or [])
        if not (0 <= index < len(colors)):
            return
        current = str(colors[index])
        color = QColorDialog.getColor(QColor(current), self, f"Выберите цвет")
        if color.isValid():
            colors[index] = color.name()
            self._queue_value_update(path, colors)

    def _append_list_color(self, path: str) -> None:
        colors = list(self._manager.value(path, []) or [])
        base = colors[-1] if colors else "#59b7e9"
        colors.append(base)
        self._queue_value_update(path, colors)

    def _remove_list_color(self, path: str) -> None:
        colors = list(self._manager.value(path, []) or [])
        if not colors:
            return
        colors.pop()
        self._queue_value_update(path, colors)

    def _queue_value_update(self, path: str, value: Any) -> None:
        self._pending_updates[f"value|{path}"] = ("value", path, value)
        self._schedule_flush()

    def _queue_mapping_update(self, mapping: str, key: str, value: Any) -> None:
        self._pending_updates[f"mapping|{mapping}|{key}"] = ("mapping", mapping, key, value)
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        self._update_timer.start(self._update_timer.interval())

    def _flush_pending_updates(self) -> None:
        if not self._pending_updates:
            return
        self._update_timer.stop()
        value_updates: Dict[str, Any] = {}
        mapping_updates: Dict[str, Dict[str, Any]] = defaultdict(dict)
        for payload in self._pending_updates.values():
            if payload[0] == "value":
                _, path, value = payload
                value_updates[path] = value
            else:
                _, mapping, key, value = payload
                mapping_updates[mapping][key] = value
        self._pending_updates.clear()
        if value_updates:
            self._manager.update_values(value_updates)
        for mapping, updates in mapping_updates.items():
            self._manager.update_mapping_entries(mapping, updates)

    def _save_preset(self) -> None:
        self._flush_pending_updates()
        name = self.preset_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Имя пресета", "Введите имя пресета перед сохранением.")
            return
        label = name.title()
        self._manager.save_preset(name, label=label)
        self.preset_edit.clear()
        self._refresh_profiles(select=name)

    def _delete_preset(self) -> None:
        self._flush_pending_updates()
        meta = self.profile_combo.currentData()
        if not meta or meta.get("type") != "preset":
            QMessageBox.information(self, "Удалить пресет", "Можно удалять только пользовательские пресеты.")
            return
        name = meta["name"]
        if QMessageBox.question(
            self,
            "Удалить пресет",
            f"Удалить выбранный пресет?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        ) == QMessageBox.StandardButton.Yes:
            self._manager.delete_preset(name)
            self._refresh_profiles()

    def _reload_styles(self) -> None:
        self._flush_pending_updates()
        self._manager.reload()
        self._refresh_profiles()
        self._refresh_all()

    def _on_radius_changed(self, value: int) -> None:
        self.radius_label.setText(f"{value}px")
        self._queue_value_update("message_widgets.metrics.body_radius", int(value))

    def _toggle_symmetry(self, enabled: bool) -> None:
        self._symmetry_enabled = bool(enabled)
        self.quick_group.setVisible(self._symmetry_enabled)
        self.manual_container.setVisible(not self._symmetry_enabled)
        if self._symmetry_enabled:
            self.symmetry_hint.setText("Основные цвета связаны для сбалансированного вида.")
        else:
            self.symmetry_hint.setText("Symmetry off: tune every parameter manually.")

    def _build_symmetry_rules(self) -> Dict[str, List[tuple[str, Callable[[QColor], str]]]]:
        def wrap(path: str, func: Callable[[QColor], Any]) -> tuple[str, Callable[[QColor], str]]:
            def _inner(color: QColor) -> str:
                result = func(QColor(color))
                if isinstance(result, QColor):
                    return self._color_to_hex(result)
                return str(result)

            return path, _inner

        def shift(value_delta: float = 0.0, saturation_delta: float = 0.0) -> Callable[[QColor], QColor]:
            return lambda color: self._shift_color(color, value_delta=value_delta, saturation_delta=saturation_delta)

        def auto_text() -> Callable[[QColor], str]:
            return lambda color: self._auto_text_color(color)

        return {
            "palette.Window": [
                wrap("palette.Window", lambda c: c),
                wrap("palette.Base", shift(-0.04, -0.02)),
                wrap("palette.AlternateBase", shift(-0.08, -0.04)),
                wrap("palette.PlaceholderText", shift(0.25, -0.2)),
                wrap("message_widgets.bubbles.other.bg", shift(0.02, -0.05)),
                wrap("message_widgets.bubbles.other.border", shift(-0.08, -0.08)),
                wrap("message_widgets.bubbles.assistant.bg", shift(0.05, -0.04)),
                wrap("message_widgets.bubbles.assistant.border", shift(-0.05, -0.06)),
                wrap("message_widgets.deleted_bubble.bg", shift(-0.12, -0.05)),
                wrap("message_widgets.deleted_bubble.border", shift(-0.18, -0.08)),
            ],
            "palette.Button": [
                wrap("palette.Button", lambda c: c),
                wrap("palette.ButtonText", auto_text()),
                wrap("palette.Disabled.ButtonText", shift(0.35, -0.35)),
                wrap("palette.Disabled.WindowText", shift(0.35, -0.35)),
            ],
            "palette.Highlight": [
                wrap("palette.Highlight", lambda c: c),
                wrap("palette.BrightText", shift(0.18, -0.02)),
                wrap("palette.HighlightedText", auto_text()),
                wrap("message_widgets.link_color", lambda c: c),
                wrap("message_widgets.bubbles.me.bg", shift(-0.05, -0.08)),
                wrap("message_widgets.bubbles.me.border", shift(-0.14, -0.05)),
                wrap(
                    "message_widgets.bubbles.me.text",
                    lambda c: self._auto_text_color(self._shift_color(c, value_delta=-0.05, saturation_delta=-0.08)),
                ),
            ],
            "palette.Text": [
                wrap("palette.Text", lambda c: c),
                wrap("palette.WindowText", lambda c: c),
                wrap("palette.Disabled.Text", shift(-0.08, -0.05)),
                wrap("message_widgets.bubbles.other.text", lambda c: c),
                wrap("message_widgets.bubbles.assistant.text", lambda c: c),
                wrap("message_widgets.deleted_bubble.text", shift(-0.12, -0.05)),
            ],
        }

    @staticmethod
    def _shift_color(color: QColor, *, value_delta: float = 0.0, saturation_delta: float = 0.0) -> QColor:
        base = QColor(color)
        h, s, v, a = base.getHsvF()
        if h < 0:
            gray = StyleEditorTab._clamp((base.redF() + base.greenF() + base.blueF()) / 3.0 + value_delta)
            return QColor.fromRgbF(gray, gray, gray, a)
        s = StyleEditorTab._clamp(s + saturation_delta)
        v = StyleEditorTab._clamp(v + value_delta)
        return QColor.fromHsvF(h, s, v, a)

    @staticmethod
    def _auto_text_color(bg: QColor) -> str:
        return "#0b111a" if StyleEditorTab._luminance(bg) > 0.6 else "#f7f9ff"

    @staticmethod
    def _luminance(color: QColor) -> float:
        c = QColor(color)
        r, g, b, _ = c.getRgbF()
        return 0.299 * r + 0.587 * g + 0.114 * b

    @staticmethod
    def _color_to_hex(color: QColor) -> str:
        return QColor(color).name()

    @staticmethod
    def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
        return max(low, min(high, value))







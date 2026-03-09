from __future__ import annotations

from datetime import datetime
import os
import threading
from functools import partial
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QObject, QPoint, QSize, Qt, Signal, Slot, QTimer, QUrl
from PySide6.QtGui import QDesktopServices, QImage, QImageReader, QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QMenu,
    QProgressBar,
    QPushButton,
    QLineEdit,
    QScrollArea,
    QSizePolicy,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ui.components.avatar import AvatarWidget


def format_chat_subtitle(info: Dict[str, Any]) -> str:
    if not isinstance(info, dict):
        return ""
    parts: List[str] = []
    ctype = str(info.get("type") or "").strip().lower()
    type_map = {
        "private": "Личные сообщения",
        "bot": "Бот",
        "user": "Пользователь",
        "group": "Группа",
        "supergroup": "Супергруппа",
        "channel": "Канал",
    }
    if ctype:
        parts.append(type_map.get(ctype, ctype.title()))
    members_count = info.get("members_count")
    try:
        members = int(members_count) if members_count is not None else 0
    except Exception:
        members = 0
    if members > 0:
        parts.append(f"{members:,}".replace(",", " ") + " участников")
    username = str(info.get("username") or "").strip()
    if username:
        parts.append(f"@{username}")
    return " • ".join([part for part in parts if part])


class _ClickableFrame(QFrame):
    clicked = Signal()

    def mouseReleaseEvent(self, event) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mouseReleaseEvent(event)


class ChatHeaderBar(QFrame):
    infoRequested = Signal()
    menuRequested = Signal(QPoint)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("chatHeaderBar")
        self.setStyleSheet(
            "QFrame#chatHeaderBar{background-color:#102033;border-bottom:1px solid rgba(255,255,255,0.06);}"
            "QLabel#chatHeaderTitle{color:#f4f7ff;font-size:17px;font-weight:700;}"
            "QLabel#chatHeaderSubtitle{color:#8da8c4;font-size:12px;}"
            "QPushButton{background-color:rgba(255,255,255,0.04);color:#dfe7f5;border:none;border-radius:14px;padding:6px 10px;}"
            "QPushButton:hover{background-color:rgba(255,255,255,0.09);}"
        )
        layout = QHBoxLayout(self)
        layout.setContentsMargins(14, 10, 14, 10)
        layout.setSpacing(10)

        self._click_area = _ClickableFrame(self)
        self._click_area.setFrameShape(QFrame.Shape.NoFrame)
        self._click_area.setCursor(Qt.CursorShape.PointingHandCursor)
        click_layout = QHBoxLayout(self._click_area)
        click_layout.setContentsMargins(0, 0, 0, 0)
        click_layout.setSpacing(10)

        self.avatar = AvatarWidget(size=42, parent=self._click_area)
        click_layout.addWidget(self.avatar, 0, Qt.AlignmentFlag.AlignVCenter)

        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(2)
        self.title_label = QLabel("Выберите чат")
        self.title_label.setObjectName("chatHeaderTitle")
        self.subtitle_label = QLabel("")
        self.subtitle_label.setObjectName("chatHeaderSubtitle")
        self.subtitle_label.hide()
        text_col.addWidget(self.title_label)
        text_col.addWidget(self.subtitle_label)
        click_layout.addLayout(text_col, 1)
        layout.addWidget(self._click_area, 1)

        self.btn_more = QPushButton("⋯", self)
        self.btn_more.setFixedWidth(42)
        layout.addWidget(self.btn_more, 0)

        self._click_area.clicked.connect(self.infoRequested.emit)
        self.btn_more.clicked.connect(self._emit_menu_requested)

    def set_chat(self, *, title: str, subtitle: str = "", avatar: Optional[QPixmap] = None) -> None:
        self.title_label.setText(str(title or "Чат"))
        subtitle_text = str(subtitle or "").strip()
        self.subtitle_label.setText(subtitle_text)
        self.subtitle_label.setVisible(bool(subtitle_text))
        if avatar is not None and not avatar.isNull():
            self.avatar.set_pixmap(avatar)

    def _emit_menu_requested(self) -> None:
        try:
            point = self.btn_more.mapToGlobal(self.btn_more.rect().bottomRight())
        except Exception:
            point = QPoint()
        self.menuRequested.emit(point)


class _StatsSection(QWidget):
    def __init__(self, title: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        header = QLabel(title)
        header.setStyleSheet("color:#f4f7ff;font-size:15px;font-weight:700;")
        layout.addWidget(header)
        self.body = QVBoxLayout()
        self.body.setContentsMargins(0, 0, 0, 0)
        self.body.setSpacing(6)
        layout.addLayout(self.body)


class _ChatInfoAsyncBus(QObject):
    sectionLoaded = Signal(str, object, str)
    previewLoaded = Signal(str, int, str, object)


class ChatInfoDialog(QDialog):
    def __init__(
        self,
        info: Dict[str, Any],
        *,
        avatar: Optional[QPixmap] = None,
        sections: Optional[Dict[str, Any]] = None,
        callbacks: Optional[Dict[str, Any]] = None,
        embedded: bool = False,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self._info = dict(info or {})
        self._sections = dict(sections or {})
        self._callbacks = dict(callbacks or {})
        self._embedded = bool(embedded)
        self._tabs: Optional[QTabWidget] = None
        self._tab_layouts: Dict[int, QVBoxLayout] = {}
        self._tab_builders: Dict[int, Any] = {}
        self._tab_loaded: set[int] = set()
        self._tab_titles: Dict[int, str] = {}
        self._tab_sections: Dict[int, str] = {1: "media", 2: "files", 3: "links", 4: "members"}
        self._section_loaded: set[str] = {
            key for key in ("media", "files", "links", "members") if key in self._sections
        }
        self._section_loading: set[str] = set()
        self._section_errors: Dict[str, str] = {}
        self._section_threads: Dict[str, threading.Thread] = {}
        self._async_bus = _ChatInfoAsyncBus(self)
        self._async_bus.sectionLoaded.connect(self._on_section_loaded)
        self._async_bus.previewLoaded.connect(self._on_preview_loaded)
        self._media_preview_generation: int = 0
        self._media_preview_request_seq: int = 0
        self._media_preview_queue: List[Dict[str, Any]] = []
        self._media_preview_targets: Dict[str, QLabel] = {}
        self._media_preview_inflight: Dict[int, int] = {}
        self._media_preview_cache: Dict[str, QPixmap] = {}
        self._media_preview_batch_size: int = 5

        self.setWindowTitle("Профиль чата")
        if not self._embedded:
            self.resize(680, 680)
        else:
            self.setMinimumSize(0, 0)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QLabel{color:#dfe7f5;background-color:transparent;border:none;}"
            "QTabWidget::pane{border:1px solid rgba(255,255,255,0.08);border-radius:10px;top:-1px;background:#102033;}"
            "QTabBar::tab{background:rgba(255,255,255,0.04);color:#b9cce3;padding:8px 12px;margin-right:4px;border-top-left-radius:8px;border-top-right-radius:8px;}"
            "QTabBar::tab:selected{background:rgba(89,183,255,0.22);color:#f4f7ff;}"
            "QLineEdit{background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.10);border-radius:9px;padding:7px 10px;color:#e6f0ff;}"
            "QLineEdit:focus{border:1px solid rgba(89,183,255,0.58);}"
            "QPushButton{background:rgba(255,255,255,0.05);color:#dfe7f5;border:none;border-radius:8px;padding:7px 10px;}"
            "QPushButton:hover{background:rgba(255,255,255,0.10);}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        header = QHBoxLayout()
        header.setSpacing(12)
        avatar_widget = AvatarWidget(size=64, parent=self)
        if avatar is not None and not avatar.isNull():
            avatar_widget.set_pixmap(avatar)
        header.addWidget(avatar_widget, 0, Qt.AlignmentFlag.AlignTop)

        text_col = QVBoxLayout()
        text_col.setSpacing(4)
        title_label = QLabel(str(self._info.get("title") or self._info.get("id") or "Чат"))
        title_label.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        text_col.addWidget(title_label)
        subtitle = format_chat_subtitle(self._info)
        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setStyleSheet("color:#8da8c4;font-size:12px;")
            subtitle_label.setWordWrap(True)
            text_col.addWidget(subtitle_label)
        header.addLayout(text_col, 1)
        root.addLayout(header)

        tabs = QTabWidget(self)
        self._tabs = tabs
        root.addWidget(tabs, 1)

        self._tab_builders = {
            0: lambda: self._wrap_scroll(self._build_overview_tab()),
            1: self._build_media_tab,
            2: self._build_files_tab,
            3: self._build_links_tab,
            4: lambda: self._wrap_scroll(self._build_members_tab()),
            5: lambda: self._wrap_scroll(self._build_actions_tab()),
        }
        titles = ["Обзор", "Медиа", "Файлы", "Ссылки", "Участники", "Действия"]
        for idx, title in enumerate(titles):
            self._tab_titles[idx] = title
            container = QWidget(self)
            container_layout = QVBoxLayout(container)
            container_layout.setContentsMargins(0, 0, 0, 0)
            container_layout.setSpacing(0)
            self._tab_layouts[idx] = container_layout
            tabs.addTab(container, title)

        self._set_lazy_tab_widget(0, self._wrap_scroll(self._build_overview_tab()))
        self._tab_loaded.add(0)
        tabs.currentChanged.connect(self._ensure_lazy_tab_loaded)

        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            buttons.accepted.connect(self.accept)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        try:
            QTimer.singleShot(0, lambda: self._ensure_lazy_tab_loaded(int(tabs.currentIndex())))
        except Exception:
            pass

    def _wrap_scroll(self, widget: QWidget) -> QScrollArea:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(widget)
        return scroll

    def _set_lazy_tab_widget(self, index: int, widget: QWidget) -> None:
        layout = self._tab_layouts.get(int(index))
        if layout is None:
            return
        self._clear_layout(layout)
        widget.setParent(self)
        layout.addWidget(widget, 1)

    def _ensure_lazy_tab_loaded(self, index: int) -> None:
        idx = int(index)
        section = self._tab_sections.get(idx, "")
        if idx in self._tab_loaded:
            if section:
                self._request_section(section)
            return
        builder = self._tab_builders.get(idx)
        if not callable(builder):
            return
        try:
            widget = builder()
        except Exception:
            widget = self._make_empty_tab("Не удалось загрузить вкладку.")
        self._set_lazy_tab_widget(idx, widget)
        self._tab_loaded.add(idx)
        if section:
            self._request_section(section)

    @staticmethod
    def _format_ts(ts: Any) -> str:
        try:
            value = int(ts or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "неизвестно"
        try:
            return datetime.fromtimestamp(value).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return str(value)

    @staticmethod
    def _format_size(size: Any) -> str:
        try:
            value = int(size or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "n/a"
        units = ["B", "KB", "MB", "GB", "TB"]
        amount = float(value)
        unit_idx = 0
        while amount >= 1024.0 and unit_idx < len(units) - 1:
            amount /= 1024.0
            unit_idx += 1
        if unit_idx == 0:
            return f"{int(amount)} {units[unit_idx]}"
        return f"{amount:.1f} {units[unit_idx]}"

    def _call(self, key: str) -> None:
        cb = self._callbacks.get(key)
        if callable(cb):
            try:
                cb()
            except Exception:
                pass

    def _make_empty_tab(self, text: str) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        lbl = QLabel(text)
        lbl.setWordWrap(True)
        lbl.setStyleSheet("color:#8da8c4;")
        layout.addWidget(lbl)
        layout.addStretch(1)
        return tab

    def _make_loading_tab(self, text: str) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        title = QLabel(text)
        title.setWordWrap(True)
        title.setStyleSheet("color:#dfe7f5;font-weight:600;")
        note = QLabel("Данные подгружаются в фоне. Интерфейс останется доступным.")
        note.setWordWrap(True)
        note.setStyleSheet("color:#8da8c4;")
        layout.addWidget(title)
        layout.addWidget(note)
        layout.addStretch(1)
        return tab

    def _make_error_tab(self, text: str, *, section: Optional[str] = None) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        title = QLabel(text)
        title.setWordWrap(True)
        title.setStyleSheet("color:#ffb6bd;font-weight:600;")
        layout.addWidget(title)
        if section:
            retry_btn = QPushButton("Повторить", tab)
            retry_btn.clicked.connect(lambda: self._retry_section(section))
            layout.addWidget(retry_btn, 0)
        layout.addStretch(1)
        return tab

    def _retry_section(self, section: str) -> None:
        key = str(section or "").strip().lower()
        if not key:
            return
        self._section_errors.pop(key, None)
        self._section_loaded.discard(key)
        self._request_section(key, force=True)

    def _request_section(self, section: str, *, force: bool = False) -> None:
        key = str(section or "").strip().lower()
        if not key:
            return
        if not force and (key in self._section_loaded or key in self._section_loading):
            return
        loader = self._callbacks.get("load_profile_section")
        if not callable(loader):
            self._section_loaded.add(key)
            return
        self._section_loading.add(key)
        self._section_errors.pop(key, None)
        self._refresh_section_tab(key)

        def _run() -> None:
            payload: List[Dict[str, Any]] = []
            error = ""
            try:
                raw = loader(key)
                payload = [dict(row) for row in list(raw or []) if isinstance(row, dict)]
            except Exception as exc:
                error = str(exc or "Не удалось загрузить данные")
            try:
                self._async_bus.sectionLoaded.emit(key, payload, error)
            except Exception:
                return

        worker = threading.Thread(target=_run, daemon=True, name=f"profile-section-{key}")
        self._section_threads[key] = worker
        worker.start()

    def _on_section_loaded(self, section: str, payload: object, error: str) -> None:
        key = str(section or "").strip().lower()
        self._section_loading.discard(key)
        self._section_threads.pop(key, None)
        if error:
            self._section_errors[key] = str(error)
            self._refresh_section_tab(key)
            return
        rows = [dict(row) for row in list(payload or []) if isinstance(row, dict)]
        self._sections[key] = rows
        self._section_loaded.add(key)
        self._section_errors.pop(key, None)
        self._refresh_section_tab(key)

    def _refresh_section_tab(self, section: str) -> None:
        key = str(section or "").strip().lower()
        tab_index = next((idx for idx, value in self._tab_sections.items() if value == key), None)
        if tab_index is None or int(tab_index) not in self._tab_loaded:
            return
        builder = self._tab_builders.get(int(tab_index))
        if not callable(builder):
            return
        try:
            widget = builder()
        except Exception:
            widget = self._make_error_tab("Не удалось обновить вкладку.", section=key)
        self._set_lazy_tab_widget(int(tab_index), widget)

    def _build_overview_tab(self) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(12)

        about = str(self._info.get("about") or "").strip()
        if about:
            about_label = QLabel(about)
            about_label.setWordWrap(True)
            about_label.setStyleSheet("padding:2px 0 10px 0;color:#c7d4e7;")
            layout.addWidget(about_label)

        details = QGridLayout()
        details.setHorizontalSpacing(10)
        details.setVerticalSpacing(8)
        rows = [
            ("ID", self._info.get("id")),
            ("Юзернейм", f"@{self._info.get('username')}" if self._info.get("username") else ""),
            ("Телефон", self._info.get("phone")),
            ("Тип", str(self._info.get("type") or "").strip()),
            ("Участники", self._info.get("members_count")),
            ("Проверка", "Да" if self._info.get("is_verified") else "Нет"),
            ("Премиум", "Да" if self._info.get("is_premium") else "Нет"),
        ]
        row_idx = 0
        for label, value in rows:
            value_str = str(value or "").strip()
            if not value_str:
                continue
            left = QLabel(label)
            left.setStyleSheet("color:#8da8c4;")
            right = QLabel(value_str)
            right.setWordWrap(True)
            details.addWidget(left, row_idx, 0, Qt.AlignmentFlag.AlignTop)
            details.addWidget(right, row_idx, 1)
            row_idx += 1
        if row_idx:
            layout.addLayout(details)

        reactions = list(self._info.get("available_reactions") or [])
        if reactions:
            section = _StatsSection("Доступные реакции", tab)
            chips = QLabel(" ".join(str(item) for item in reactions[:80]))
            chips.setWordWrap(True)
            chips.setStyleSheet("color:#59b7ff;font-size:18px;")
            section.body.addWidget(chips)
            layout.addWidget(section)

        layout.addStretch(1)
        return tab

    @staticmethod
    def _month_group_label(ts: Any) -> str:
        try:
            value = int(ts or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "Без даты"
        try:
            dt = datetime.fromtimestamp(value)
            months = (
                "январь",
                "февраль",
                "март",
                "апрель",
                "май",
                "июнь",
                "июль",
                "август",
                "сентябрь",
                "октябрь",
                "ноябрь",
                "декабрь",
            )
            month = months[max(1, min(12, int(dt.month))) - 1]
            return f"{month.capitalize()} {int(dt.year)}"
        except Exception:
            return "Без даты"

    @staticmethod
    def _drop_duplicates(rows: List[Dict[str, Any]], *, mode: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for row in rows:
            if not isinstance(row, dict):
                continue
            if mode == "links":
                url = str(row.get("url") or "").strip().lower()
                key = f"url:{url}" if url else f"mid:{int(row.get('id') or 0)}"
            else:
                path = str(row.get("file_path") or "").strip().lower()
                file_name = str(row.get("file_name") or "").strip().lower()
                file_size = int(row.get("file_size") or 0)
                if path:
                    key = f"path:{path}"
                elif file_name:
                    key = f"name:{file_name}|{file_size}"
                else:
                    key = f"mid:{int(row.get('id') or 0)}"
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(row))
        return out

    @staticmethod
    def _clear_layout(layout: QVBoxLayout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            child = item.widget()
            if child is not None:
                child.setParent(None)
                child.deleteLater()

    def _section_row_matches(self, row: Dict[str, Any], mode: str, query: str) -> bool:
        q = str(query or "").strip().lower()
        if not q:
            return True
        fields: List[str] = []
        if mode == "links":
            fields.extend([str(row.get("url") or ""), str(row.get("context") or "")])
        else:
            fields.extend(
                [
                    str(row.get("file_name") or ""),
                    str(row.get("text") or ""),
                    str(row.get("type") or ""),
                    str(row.get("mime") or ""),
                    str(row.get("file_path") or ""),
                ]
            )
        haystack = " ".join(fields).lower()
        return q in haystack

    def _build_section_preview(self, row: Dict[str, Any], *, mode: str, parent: QWidget) -> QLabel:
        preview = QLabel(parent)
        preview.setFixedSize(56, 56)
        preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        preview.setStyleSheet(
            "background-color:rgba(255,255,255,0.06);"
            "border:1px solid rgba(255,255,255,0.08);"
            "border-radius:8px;"
            "color:#9fc0de;"
            "font-size:12px;"
            "font-weight:700;"
        )
        if mode == "links":
            preview.setText("URL")
            return preview
        kind = str(row.get("type") or "").strip().lower()
        fpath = str(row.get("file_path") or "").strip()
        if mode == "media" and fpath and os.path.isfile(fpath):
            pix = QPixmap(fpath)
            if not pix.isNull():
                preview.setPixmap(
                    pix.scaled(
                        preview.size(),
                        Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                        Qt.TransformationMode.SmoothTransformation,
                    )
                )
                preview.setStyleSheet("border-radius:8px;")
                return preview
        if mode == "files":
            ext = os.path.splitext(str(row.get("file_name") or "").strip())[1].lstrip(".").upper()
            preview.setText(ext[:4] if ext else "FILE")
            return preview
        label = "MEDIA"
        if kind in {"video", "video_note"}:
            label = "VIDEO"
        elif kind in {"animation", "gif"}:
            label = "GIF"
        elif kind in {"image", "photo"}:
            label = "IMG"
        elif kind == "sticker":
            label = "STK"
        preview.setText(label)
        return preview

    def _add_section_item(self, layout: QVBoxLayout, row: Dict[str, Any], *, mode: str) -> None:
        card = QFrame(self)
        card.setStyleSheet(
            "QFrame{background:transparent;border:none;border-bottom:1px solid rgba(255,255,255,0.06);}"
            "QPushButton{padding:5px 8px;border-radius:8px;}"
        )
        root = QVBoxLayout(card)
        root.setContentsMargins(0, 6, 0, 8)
        root.setSpacing(6)

        head = QHBoxLayout()
        head.setContentsMargins(0, 0, 0, 0)
        head.setSpacing(10)
        head.addWidget(self._build_section_preview(row, mode=mode, parent=card), 0)

        content = QVBoxLayout()
        content.setContentsMargins(0, 0, 0, 0)
        content.setSpacing(3)

        if mode == "links":
            title_text = str(row.get("url") or "Ссылка")
            subtitle_text = str(row.get("context") or "").strip()
        else:
            title_text = str(row.get("file_name") or row.get("text") or "").strip()
            if not title_text:
                title_text = f"Сообщение #{int(row.get('id') or 0)}"
            subtitle_text = str(row.get("text") or "").strip()
        title = QLabel(title_text)
        title.setWordWrap(True)
        title.setStyleSheet("color:#dff0ff;font-size:13px;font-weight:600;background:transparent;")
        content.addWidget(title)

        if subtitle_text and mode != "links":
            subtitle = QLabel(subtitle_text[:180])
            subtitle.setWordWrap(True)
            subtitle.setStyleSheet("color:#9eb7d4;font-size:12px;background:transparent;")
            content.addWidget(subtitle)
        elif subtitle_text and mode == "links":
            subtitle = QLabel(subtitle_text[:180])
            subtitle.setWordWrap(True)
            subtitle.setStyleSheet("color:#8ca8c8;font-size:11px;background:transparent;")
            content.addWidget(subtitle)

        meta_parts = [self._format_ts(row.get("date")), f"#{int(row.get('id') or 0)}"]
        if mode != "links":
            kind = str(row.get("type") or "").strip()
            if kind:
                meta_parts.append(kind)
            size_text = self._format_size(row.get("file_size"))
            if size_text != "n/a":
                meta_parts.append(size_text)
        meta = QLabel(" • ".join([part for part in meta_parts if part]))
        meta.setStyleSheet("color:#7f99b7;font-size:11px;background:transparent;")
        content.addWidget(meta)
        head.addLayout(content, 1)
        root.addLayout(head)

        actions = QHBoxLayout()
        actions.setContentsMargins(66, 0, 0, 0)
        actions.setSpacing(6)
        if mode == "links":
            url = str(row.get("url") or "").strip()
            open_btn = QPushButton("Открыть ссылку", card)
            open_btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl(url)))
            actions.addWidget(open_btn, 0)
        else:
            fpath = str(row.get("file_path") or "").strip()
            if fpath:
                open_btn = QPushButton("Открыть файл", card)
                open_btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl.fromLocalFile(fpath)))
                actions.addWidget(open_btn, 0)
        jump_btn = QPushButton("К сообщению", card)
        try:
            msg_id = int(row.get("id") or 0)
        except Exception:
            msg_id = 0
        jump_btn.setEnabled(msg_id > 0)
        if msg_id > 0:
            jump_btn.clicked.connect(partial(self._call_with_message, "jump_to_message", msg_id))
        actions.addWidget(jump_btn, 0)
        actions.addStretch(1)
        root.addLayout(actions)
        layout.addWidget(card, 0)

    def _call_with_message(self, key: str, message_id: int) -> None:
        cb = self._callbacks.get(key)
        if not callable(cb):
            return
        try:
            cb(int(message_id))
        except Exception:
            pass

    def _build_media_tile(self, row: Dict[str, Any], parent: QWidget) -> QWidget:
        tile = QFrame(parent)
        tile.setStyleSheet(
            "QFrame{background-color:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.08);border-radius:10px;}"
            "QPushButton{padding:4px 7px;border-radius:7px;font-size:11px;}"
        )
        layout = QVBoxLayout(tile)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(5)

        preview = QLabel(tile)
        preview.setFixedSize(96, 96)
        preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        preview.setStyleSheet("background:#0f1f30;border-radius:8px;")
        fpath = str(row.get("file_path") or "").strip()
        if fpath and os.path.isfile(fpath):
            ext = os.path.splitext(fpath)[1].lower()
            if ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif"}:
                preview.setText("IMG")
                self._queue_media_preview(preview, fpath)
            else:
                preview.setText("MEDIA")
        else:
            kind = str(row.get("type") or "media").strip().upper()
            preview.setText(kind[:5])
        layout.addWidget(preview, 0, Qt.AlignmentFlag.AlignCenter)

        stamp = QLabel(self._format_ts(row.get("date")))
        stamp.setStyleSheet("color:#7d97b5;font-size:11px;")
        stamp.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(stamp, 0)

        actions = QHBoxLayout()
        actions.setContentsMargins(0, 0, 0, 0)
        actions.setSpacing(4)
        if fpath:
            open_btn = QPushButton("Откр.", tile)
            open_btn.clicked.connect(partial(QDesktopServices.openUrl, QUrl.fromLocalFile(fpath)))
            actions.addWidget(open_btn, 0)
        try:
            msg_id = int(row.get("id") or 0)
        except Exception:
            msg_id = 0
        jump_btn = QPushButton("К сообщ.", tile)
        jump_btn.setEnabled(msg_id > 0)
        if msg_id > 0:
            jump_btn.clicked.connect(partial(self._call_with_message, "jump_to_message", msg_id))
        actions.addWidget(jump_btn, 0)
        layout.addLayout(actions)
        return tile

    @staticmethod
    def _decode_preview_image(path: str, size: QSize) -> Optional[QImage]:
        try:
            if not path or not os.path.isfile(path):
                return None
            width = max(48, int(size.width() or 0))
            height = max(48, int(size.height() or 0))
            reader = QImageReader(path)
            reader.setAutoTransform(True)
            reader.setScaledSize(QSize(width, height))
            img = reader.read()
            if img.isNull():
                return None
            return img
        except Exception:
            return None

    def _start_media_preview_pass(self) -> None:
        self._media_preview_generation += 1
        self._media_preview_queue = []
        self._media_preview_targets = {}
        self._media_preview_inflight.setdefault(self._media_preview_generation, 0)

    def _queue_media_preview(self, preview: QLabel, fpath: str) -> None:
        if preview is None or not fpath:
            return
        size = preview.size()
        cache_key = f"{fpath}|{int(size.width())}x{int(size.height())}"
        cached = self._media_preview_cache.get(cache_key)
        if cached is not None and not cached.isNull():
            preview.setPixmap(cached)
            preview.setText("")
            return
        generation = int(self._media_preview_generation)
        self._media_preview_request_seq += 1
        token = f"{generation}:{self._media_preview_request_seq}"
        preview.setProperty("media_preview_token", token)
        self._media_preview_targets[token] = preview
        self._media_preview_queue.append(
            {
                "token": token,
                "generation": generation,
                "path": fpath,
                "cache_key": cache_key,
                "size": QSize(size),
            }
        )

    def _pump_media_preview_queue(self) -> None:
        current_generation = int(self._media_preview_generation)
        inflight = int(self._media_preview_inflight.get(current_generation, 0) or 0)
        while self._media_preview_queue and inflight < int(self._media_preview_batch_size):
            job = dict(self._media_preview_queue.pop(0))
            generation = int(job.get("generation") or 0)
            if generation != current_generation:
                continue
            token = str(job.get("token") or "")
            fpath = str(job.get("path") or "")
            cache_key = str(job.get("cache_key") or "")
            size = job.get("size")
            if not token or not fpath or not isinstance(size, QSize):
                continue
            self._media_preview_inflight[current_generation] = inflight + 1
            inflight += 1

            def _run(token_value: str, generation_value: int, path_value: str, cache_key_value: str, size_value: QSize) -> None:
                image = self._decode_preview_image(path_value, size_value)
                try:
                    self._async_bus.previewLoaded.emit(token_value, generation_value, cache_key_value, image)
                except Exception:
                    return

            threading.Thread(
                target=_run,
                args=(token, generation, fpath, cache_key, QSize(size)),
                daemon=True,
                name=f"profile-preview-{generation}-{token.split(':')[-1]}",
            ).start()

    @Slot(str, int, str, object)
    def _on_preview_loaded(self, token: str, generation: int, cache_key: str, image: object) -> None:
        gen = int(generation or 0)
        inflight = max(0, int(self._media_preview_inflight.get(gen, 0) or 0) - 1)
        if inflight:
            self._media_preview_inflight[gen] = inflight
        else:
            self._media_preview_inflight.pop(gen, None)

        preview = self._media_preview_targets.pop(str(token or ""), None)
        if isinstance(image, QImage) and not image.isNull():
            pix = QPixmap.fromImage(image)
            if not pix.isNull():
                self._media_preview_cache[str(cache_key or "")] = pix
                if preview is not None and preview.parent() is not None:
                    current_token = str(preview.property("media_preview_token") or "")
                    if current_token == str(token or ""):
                        preview.setPixmap(
                            pix.scaled(
                                preview.size(),
                                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                                Qt.TransformationMode.SmoothTransformation,
                            )
                        )
                        preview.setText("")
        if gen == int(self._media_preview_generation):
            QTimer.singleShot(0, self._pump_media_preview_queue)

    def _create_cards_tab(self, rows: List[Dict[str, Any]], *, mode: str) -> QWidget:
        deduped = self._drop_duplicates(rows, mode=mode)
        deduped.sort(key=lambda item: (int(item.get("date") or 0), int(item.get("id") or 0)), reverse=True)
        initial_limit = 15 if mode == "media" else 36
        show_more_step = 15 if mode == "media" else 72
        state = {"limit": initial_limit}

        tab = QWidget(self)
        root = QVBoxLayout(tab)
        root.setContentsMargins(10, 10, 10, 10)
        root.setSpacing(8)

        search = QLineEdit(tab)
        if mode == "media":
            search.setPlaceholderText("Поиск по медиа...")
        elif mode == "files":
            search.setPlaceholderText("Поиск по файлам...")
        else:
            search.setPlaceholderText("Поиск по ссылкам...")
        root.addWidget(search, 0)

        scroll = QScrollArea(tab)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        root.addWidget(scroll, 1)

        container = QWidget(scroll)
        items_layout = QVBoxLayout(container)
        items_layout.setContentsMargins(0, 0, 0, 0)
        items_layout.setSpacing(2)
        scroll.setWidget(container)

        def _render(query: str = "") -> None:
            self._clear_layout(items_layout)
            if not deduped:
                items_layout.addWidget(self._make_empty_tab("Пока нет данных."), 0)
                return
            filtered = [row for row in deduped if self._section_row_matches(row, mode, query)]
            if not filtered:
                empty = QLabel("Ничего не найдено")
                empty.setStyleSheet("color:#8da8c4;padding:8px 4px;")
                items_layout.addWidget(empty, 0)
                items_layout.addStretch(1)
                return
            visible_limit = max(6, int(state.get("limit", initial_limit) or initial_limit))
            visible_rows = filtered[:visible_limit]
            if mode == "media":
                self._start_media_preview_pass()
                grouped: Dict[str, List[Dict[str, Any]]] = {}
                for row in visible_rows:
                    grouped.setdefault(self._month_group_label(row.get("date")), []).append(row)
                for group_name, group_rows in grouped.items():
                    header = QLabel(group_name)
                    header.setStyleSheet("color:#dfefff;font-weight:700;padding:8px 2px 2px 2px;")
                    items_layout.addWidget(header, 0)
                    grid_wrap = QWidget(container)
                    grid = QGridLayout(grid_wrap)
                    grid.setContentsMargins(0, 0, 0, 0)
                    grid.setHorizontalSpacing(8)
                    grid.setVerticalSpacing(8)
                    columns = 3
                    for idx, row in enumerate(group_rows):
                        r = idx // columns
                        c = idx % columns
                        grid.addWidget(self._build_media_tile(row, grid_wrap), r, c)
                    items_layout.addWidget(grid_wrap, 0)
                if len(filtered) > len(visible_rows):
                    more_btn = QPushButton(f"Показать ещё ({len(filtered) - len(visible_rows)})", container)
                    more_btn.clicked.connect(lambda: _show_more(search.text()))
                    items_layout.addWidget(more_btn, 0)
                items_layout.addStretch(1)
                QTimer.singleShot(0, self._pump_media_preview_queue)
                return
            prev_group = ""
            for row in visible_rows:
                group = self._month_group_label(row.get("date"))
                if group != prev_group:
                    prev_group = group
                    header = QLabel(group)
                    header.setStyleSheet("color:#dfefff;font-weight:700;padding:8px 2px 2px 2px;")
                    items_layout.addWidget(header, 0)
                self._add_section_item(items_layout, row, mode=mode)
            if len(filtered) > len(visible_rows):
                more_btn = QPushButton(f"Показать ещё ({len(filtered) - len(visible_rows)})", container)
                more_btn.clicked.connect(lambda: _show_more(search.text()))
                items_layout.addWidget(more_btn, 0)
            items_layout.addStretch(1)

        def _show_more(query: str) -> None:
            state["limit"] = int(state.get("limit", initial_limit) or initial_limit) + show_more_step
            _render(query)

        def _on_search_changed(text: str) -> None:
            state["limit"] = initial_limit
            _render(text)

        search.textChanged.connect(_on_search_changed)
        _render("")
        return tab

    def _build_media_tab(self) -> QWidget:
        if "media" in self._section_errors:
            return self._make_error_tab(str(self._section_errors.get("media") or "Не удалось загрузить медиа."), section="media")
        if "media" in self._section_loading or "media" not in self._section_loaded:
            return self._make_loading_tab("Загружаю медиа…")
        rows = [row for row in list(self._sections.get("media") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="media")

    def _build_files_tab(self) -> QWidget:
        if "files" in self._section_errors:
            return self._make_error_tab(str(self._section_errors.get("files") or "Не удалось загрузить файлы."), section="files")
        if "files" in self._section_loading or "files" not in self._section_loaded:
            return self._make_loading_tab("Загружаю файлы…")
        rows = [row for row in list(self._sections.get("files") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="files")

    def _build_links_tab(self) -> QWidget:
        if "links" in self._section_errors:
            return self._make_error_tab(str(self._section_errors.get("links") or "Не удалось загрузить ссылки."), section="links")
        if "links" in self._section_loading or "links" not in self._section_loaded:
            return self._make_loading_tab("Загружаю ссылки…")
        rows = [row for row in list(self._sections.get("links") or []) if isinstance(row, dict)]
        return self._create_cards_tab(rows, mode="links")

    def _build_members_tab(self) -> QWidget:
        if "members" in self._section_errors:
            return self._make_error_tab(str(self._section_errors.get("members") or "Не удалось загрузить участников."), section="members")
        if "members" in self._section_loading or "members" not in self._section_loaded:
            return self._make_loading_tab("Загружаю участников…")
        members = [row for row in list(self._sections.get("members") or []) if isinstance(row, dict)]
        if not members:
            return self._make_empty_tab("Список участников появится после синхронизации чата.")
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)
        for row in members:
            line = QFrame(tab)
            line.setStyleSheet("QFrame{background:transparent;border:none;border-bottom:1px solid rgba(255,255,255,0.06);}")
            line_layout = QHBoxLayout(line)
            line_layout.setContentsMargins(0, 6, 0, 10)
            line_layout.setSpacing(10)
            left = QVBoxLayout()
            name = str(row.get("name") or row.get("id") or "unknown")
            username = str(row.get("username") or "").strip()
            status = str(row.get("status") or "").strip()
            kind = str(row.get("type") or "").strip()
            head = name
            if username:
                head += f" (@{username})"
            if status:
                head += f" • {status}"
            if kind:
                head += f" [{kind}]"
            title = QLabel(head)
            title.setWordWrap(True)
            title.setStyleSheet("color:#f4f7ff;font-weight:600;")
            left.addWidget(title)
            msgs = int(row.get("messages") or 0)
            deleted = int(row.get("deleted_messages") or 0)
            last_date = self._format_ts(row.get("last_date"))
            details = QLabel(f"Сообщений: {msgs} • удалено: {deleted} • активен: {last_date}")
            details.setStyleSheet("color:#8da8c4;font-size:11px;")
            left.addWidget(details)
            line_layout.addLayout(left, 1)
            layout.addWidget(line)
        layout.addStretch(1)
        return tab

    def _build_actions_tab(self) -> QWidget:
        tab = QWidget(self)
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        title = QLabel("Быстрые действия")
        title.setStyleSheet("font-size:16px;font-weight:700;color:#f4f7ff;")
        layout.addWidget(title)

        stats_btn = QPushButton("Открыть статистику чата", tab)
        stats_btn.clicked.connect(partial(self._call, "show_stats"))
        layout.addWidget(stats_btn, 0)

        read_btn = QPushButton("Пометить чат прочитанным", tab)
        read_btn.clicked.connect(partial(self._call, "mark_read"))
        layout.addWidget(read_btn, 0)

        leave_btn = QPushButton("Покинуть чат / канал", tab)
        leave_btn.setStyleSheet(
            "QPushButton{background:rgba(255,96,96,0.18);color:#ffd7d7;border:none;border-radius:8px;padding:7px 10px;}"
            "QPushButton:hover{background:rgba(255,96,96,0.30);}"
        )
        leave_btn.clicked.connect(partial(self._call, "leave_chat"))
        layout.addWidget(leave_btn, 0)

        note = QLabel("Часть действий может быть недоступна в приватных диалогах или без прав администратора.")
        note.setWordWrap(True)
        note.setStyleSheet("color:#8da8c4;")
        layout.addWidget(note)
        layout.addStretch(1)
        return tab


class ChatStatisticsDialog(QDialog):
    def __init__(
        self,
        title: str,
        data: Dict[str, Any],
        parent: Optional[QWidget] = None,
        *,
        embedded: bool = False,
        callbacks: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(parent)
        self._embedded = bool(embedded)
        self._callbacks = dict(callbacks or {})
        self.setWindowTitle("Статистика чата")
        if not self._embedded:
            self.resize(520, 640)
        else:
            self.setMinimumSize(0, 0)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QProgressBar{background-color:rgba(255,255,255,0.05);border:none;border-radius:6px;height:10px;text-align:center;}"
            "QProgressBar::chunk{background-color:#59b7ff;border-radius:6px;}"
            "QTabWidget::pane{border:1px solid rgba(255,255,255,0.08);border-radius:10px;background:#102033;}"
            "QTabBar::tab{background:rgba(255,255,255,0.04);color:#b9cce3;padding:8px 12px;margin-right:4px;border-top-left-radius:8px;border-top-right-radius:8px;}"
            "QTabBar::tab:selected{background:rgba(89,183,255,0.22);color:#f4f7ff;}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        heading_row = QHBoxLayout()
        heading_row.setSpacing(8)
        heading = QLabel(str(title or "Чат"))
        heading.setStyleSheet("font-size:20px;font-weight:700;color:#f4f7ff;")
        heading_row.addWidget(heading, 1)
        btn_scan = QPushButton("Скан", self)
        btn_scan.clicked.connect(self._scan_clicked)
        heading_row.addWidget(btn_scan, 0)
        root.addLayout(heading_row)

        tabs = QTabWidget(self)
        tabs.setDocumentMode(True)
        tabs.setUsesScrollButtons(True)
        root.addWidget(tabs, 1)

        summary_tab, summary_layout = self._stats_tab()
        activity_tab, activity_layout = self._stats_tab()
        reactions_tab, reactions_layout = self._stats_tab()
        senders_tab, senders_layout = self._stats_tab()
        polls_tab, polls_layout = self._stats_tab()
        risk_tab, risk_layout = self._stats_tab()

        tabs.addTab(summary_tab, "Сводка")
        tabs.addTab(activity_tab, "Активность")
        tabs.addTab(reactions_tab, "Реакции")
        tabs.addTab(senders_tab, "Отправители")
        tabs.addTab(polls_tab, "Опросы")
        tabs.addTab(risk_tab, "Риски")

        summary = _StatsSection("Сводка", self)
        metrics = [
            ("Сообщений", int(data.get("total_messages") or 0)),
            ("Медиа", int(data.get("media_messages") or 0)),
            ("Удалено", int(data.get("deleted_messages") or 0)),
            ("Просмотры", int(data.get("total_views") or 0)),
            ("Пересылки", int(data.get("total_forwards") or 0)),
            ("Реакции", int(data.get("total_reactions") or 0)),
        ]
        for label, value in metrics:
            row = QLabel(f"{label}: {value}")
            row.setStyleSheet("color:#dfe7f5;font-size:13px;")
            summary.body.addWidget(row)
        snapshot_meta = data.get("snapshot_meta") if isinstance(data.get("snapshot_meta"), dict) else {}
        latest_snapshot = snapshot_meta.get("latest") if isinstance(snapshot_meta.get("latest"), dict) else None
        previous_snapshot = snapshot_meta.get("previous") if isinstance(snapshot_meta.get("previous"), dict) else None
        delta_snapshot = snapshot_meta.get("delta") if isinstance(snapshot_meta.get("delta"), dict) else {}
        if latest_snapshot:
            latest_ts = int(latest_snapshot.get("scanned_at") or 0)
            summary.body.addWidget(QLabel(f"Последний скан: {self._format_ts(latest_ts)}"))
        if previous_snapshot:
            prev_ts = int(previous_snapshot.get("scanned_at") or 0)
            summary.body.addWidget(QLabel(f"Предыдущий скан: {self._format_ts(prev_ts)}"))
        if delta_snapshot:
            delta_section = _StatsSection("Сравнение со прошлым сканом", self)
            label_map = {
                "total_messages": "Сообщений",
                "media_messages": "Медиа",
                "deleted_messages": "Удалено",
                "total_views": "Просмотры",
                "total_forwards": "Пересылки",
                "total_reactions": "Реакции",
            }
            for key, label in label_map.items():
                if key not in delta_snapshot:
                    continue
                try:
                    value = int(delta_snapshot.get(key) or 0)
                except Exception:
                    value = 0
                sign = "+" if value > 0 else ""
                item = QLabel(f"{label}: {sign}{value}")
                item.setStyleSheet("color:#8fd1ff;font-size:12px;")
                delta_section.body.addWidget(item)
            summary_layout.addWidget(delta_section)
        summary_layout.addWidget(summary)

        engagement = data.get("engagement") if isinstance(data.get("engagement"), dict) else {}
        if engagement:
            section = _StatsSection("Вовлечённость", self)
            section.body.addWidget(QLabel(f"Просмотров на сообщение: {float(engagement.get('views_per_message') or 0):.2f}"))
            section.body.addWidget(QLabel(f"Реакций на 100 сообщений: {float(engagement.get('reactions_per_100_messages') or 0):.2f}"))
            section.body.addWidget(QLabel(f"Пересылок на 100 сообщений: {float(engagement.get('forwards_per_100_messages') or 0):.2f}"))
            summary_layout.addWidget(section)

        hourly = [row for row in list(data.get("hourly_activity") or []) if isinstance(row, dict)]
        if hourly:
            section = _StatsSection("Часы пик", self)
            ranked = sorted(hourly, key=lambda row: int(row.get("count") or 0), reverse=True)
            top_hours = sorted(ranked[:12], key=lambda row: int(row.get("hour") or 0))
            max_count = max([int(row.get("count") or 0) for row in top_hours] or [1])
            for row in top_hours:
                hour = int(row.get("hour") or 0)
                count = int(row.get("count") or 0)
                line = QHBoxLayout()
                label = QLabel(f"{hour:02d}:00")
                label.setFixedWidth(54)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 0)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            activity_layout.addWidget(section)

        daily = [row for row in list(data.get("daily_activity") or []) if isinstance(row, dict)]
        if daily:
            section = _StatsSection("Дни", self)
            max_count = max([int(row.get("count") or 0) for row in daily[-14:]] or [1])
            for row in daily[-14:]:
                day = str(row.get("day") or "")
                count = int(row.get("count") or 0)
                line = QHBoxLayout()
                label = QLabel(day or "день")
                label.setMinimumWidth(100)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 0)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            activity_layout.addWidget(section)

        reactions = list(data.get("top_reactions") or [])
        if reactions:
            section = _StatsSection("Топ реакций", self)
            max_count = max(int(item.get("count") or 0) for item in reactions) or 1
            for item in reactions:
                line = QHBoxLayout()
                symbol = QLabel(str(item.get("emoji") or item.get("title") or "?"))
                symbol.setFixedWidth(42)
                count = int(item.get("count") or 0)
                text = QLabel(str(count))
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(symbol, 0)
                line.addWidget(bar, 1)
                line.addWidget(text, 0)
                section.body.addLayout(line)
            reactions_layout.addWidget(section)

        senders = list(data.get("top_senders") or [])
        if senders:
            section = _StatsSection("Активность отправителей", self)
            max_count = max(int(item.get("count") or 0) for item in senders) or 1
            for item in senders:
                name = str(item.get("name") or item.get("sender_id") or "unknown")
                username = str(item.get("username") or "").strip()
                sender_type = str(item.get("type") or "").strip().lower()
                count = int(item.get("count") or 0)
                label_text = name
                if username:
                    label_text += f" (@{username})"
                if sender_type:
                    label_text += f" [{sender_type}]"
                line = QHBoxLayout()
                label = QLabel(label_text)
                label.setMinimumWidth(180)
                label.setWordWrap(True)
                bar = QProgressBar(self)
                bar.setRange(0, max_count)
                bar.setValue(count)
                line.addWidget(label, 1)
                line.addWidget(bar, 1)
                line.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(line)
            senders_layout.addWidget(section)

        reason_labels = {
            "bot_like_sender": "похож на бота",
            "high_message_share": "высокая доля сообщений",
            "dominant_sender": "доминирует в потоке сообщений",
        }
        suspicious = list(data.get("suspicious_senders") or [])
        if suspicious:
            section = _StatsSection("Подозрительные отправители", self)
            for row in suspicious:
                name = str(row.get("name") or row.get("sender_id") or "unknown")
                username = str(row.get("username") or "").strip()
                reason_items = [
                    reason_labels.get(str(x).strip(), str(x).strip())
                    for x in list(row.get("reasons") or [])
                    if str(x).strip()
                ]
                reasons = ", ".join(reason_items)
                count = int(row.get("count") or 0)
                share = float(row.get("share") or 0.0)
                details = name
                if username:
                    details += f" (@{username})"
                details += f" • сообщений: {count} • доля: {share * 100.0:.1f}%"
                if reasons:
                    details += f"\nПризнаки: {reasons}"
                lbl = QLabel(details)
                lbl.setWordWrap(True)
                lbl.setStyleSheet("color:#ffd78c;")
                section.body.addWidget(lbl)
            risk_layout.addWidget(section)

        anomaly_labels = {
            "reactions_exceed_views": "Реакций существенно больше просмотров.",
            "forwards_exceed_views": "Пересылок существенно больше просмотров.",
            "multiple_suspicious_senders": "Обнаружено несколько подозрительных отправителей.",
        }
        anomaly_flags = [str(x) for x in list(data.get("anomaly_flags") or []) if str(x).strip()]
        if anomaly_flags:
            section = _StatsSection("Аномалии", self)
            for flag in anomaly_flags:
                section.body.addWidget(QLabel(f"• {anomaly_labels.get(flag, flag)}"))
            risk_layout.addWidget(section)

        polls_summary = data.get("polls_summary") if isinstance(data.get("polls_summary"), dict) else {}
        if polls_summary:
            section = _StatsSection("Сводка по опросам", self)
            section.body.addWidget(QLabel(f"Опросов: {int(polls_summary.get('total_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Открытых: {int(polls_summary.get('open_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Закрытых: {int(polls_summary.get('closed_polls') or 0)}"))
            section.body.addWidget(QLabel(f"Сумма голосов: {int(polls_summary.get('total_voters') or 0)}"))
            polls_layout.addWidget(section)

        polls = list(data.get("polls") or [])
        if polls:
            for poll in polls[:10]:
                block = _StatsSection(str(poll.get("question") or "Опрос"), self)
                total_voters = int(poll.get("total_voter_count") or 0)
                block.body.addWidget(QLabel(f"Голосов: {total_voters}"))
                max_votes = max([int(opt.get("voter_count") or 0) for opt in list(poll.get("options") or [])] or [1])
                for option in list(poll.get("options") or []):
                    row = QHBoxLayout()
                    label = QLabel(str(option.get("text") or ""))
                    label.setWordWrap(True)
                    label.setMinimumWidth(160)
                    value = int(option.get("voter_count") or 0)
                    bar = QProgressBar(self)
                    bar.setRange(0, max_votes)
                    bar.setValue(value)
                    row.addWidget(label, 1)
                    row.addWidget(bar, 1)
                    row.addWidget(QLabel(str(value)), 0)
                    block.body.addLayout(row)
                polls_layout.addWidget(block)

        for layout in (summary_layout, activity_layout, reactions_layout, senders_layout, polls_layout, risk_layout):
            layout.addStretch(1)

        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)

    @staticmethod
    def _format_ts(ts: Any) -> str:
        try:
            value = int(ts or 0)
        except Exception:
            value = 0
        if value <= 0:
            return "неизвестно"
        try:
            return datetime.fromtimestamp(value).strftime("%d.%m.%Y %H:%M")
        except Exception:
            return str(value)

    def _stats_tab(self) -> tuple[QScrollArea, QVBoxLayout]:
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget(scroll)
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)
        scroll.setWidget(container)
        return scroll, layout

    def _scan_clicked(self) -> None:
        callback = self._callbacks.get("scan") if isinstance(self._callbacks, dict) else None
        if callable(callback):
            try:
                callback()
            except Exception:
                pass


class MessageStatisticsDialog(QDialog):
    def __init__(self, data: Dict[str, Any], parent: Optional[QWidget] = None, *, embedded: bool = False) -> None:
        super().__init__(parent)
        self._embedded = bool(embedded)
        self.setWindowTitle("Статистика сообщения")
        self.resize(500, 540)
        self.setStyleSheet(
            "QDialog{background-color:#0f1b27;color:#dfe7f5;}"
            "QProgressBar{background-color:rgba(255,255,255,0.05);border:none;border-radius:6px;height:10px;text-align:center;}"
            "QProgressBar::chunk{background-color:#59b7ff;border-radius:6px;}"
        )
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 16, 16, 16)
        root.setSpacing(12)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget(scroll)
        body = QVBoxLayout(container)
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(12)
        scroll.setWidget(container)
        root.addWidget(scroll, 1)

        message = dict(data.get("message") or {})
        deleted_snapshot = data.get("deleted_snapshot") if isinstance(data.get("deleted_snapshot"), dict) else None

        preview = QLabel(str(message.get("text") or deleted_snapshot.get("snapshot_text") if deleted_snapshot else ""))
        preview.setWordWrap(True)
        preview.setStyleSheet("padding:0;color:#dfe7f5;")
        body.addWidget(preview)

        summary = _StatsSection("Показатели", self)
        rows = [
            ("Тип", message.get("type") or "text"),
            ("Просмотры", int(message.get("views") or 0)),
            ("Пересылки", int(message.get("forwards") or 0)),
            ("Удалено", "Да" if message.get("is_deleted") else "Нет"),
        ]
        for label, value in rows:
            summary.body.addWidget(QLabel(f"{label}: {value}"))
        body.addWidget(summary)

        sender_profile = data.get("sender_profile") if isinstance(data.get("sender_profile"), dict) else None
        if sender_profile:
            section = _StatsSection("Отправитель", self)
            sender_name = str(sender_profile.get("name") or sender_profile.get("id") or "unknown")
            sender_username = str(sender_profile.get("username") or "")
            sender_type = str(sender_profile.get("type") or "")
            section.body.addWidget(QLabel(f"Имя: {sender_name}"))
            if sender_username:
                section.body.addWidget(QLabel(f"Username: @{sender_username}"))
            if sender_type:
                section.body.addWidget(QLabel(f"Тип: {sender_type}"))
            body.addWidget(section)

        reactions = list(message.get("reactions") or [])
        if reactions:
            section = _StatsSection("Реакции", self)
            max_count = max(int(item.get("count") or 0) for item in reactions) or 1
            for item in reactions:
                row = QHBoxLayout()
                row.addWidget(QLabel(str(item.get("emoji") or item.get("title") or "?")), 0)
                bar = QProgressBar(self)
                count = int(item.get("count") or 0)
                bar.setRange(0, max_count)
                bar.setValue(count)
                row.addWidget(bar, 1)
                row.addWidget(QLabel(str(count)), 0)
                section.body.addLayout(row)
            body.addWidget(section)

        poll = message.get("poll") if isinstance(message.get("poll"), dict) else None
        if poll:
            section = _StatsSection(str(poll.get("question") or "Опрос"), self)
            max_votes = max([int(opt.get("voter_count") or 0) for opt in list(poll.get("options") or [])] or [1])
            for option in list(poll.get("options") or []):
                row = QHBoxLayout()
                row.addWidget(QLabel(str(option.get("text") or "")), 1)
                bar = QProgressBar(self)
                value = int(option.get("voter_count") or 0)
                bar.setRange(0, max_votes)
                bar.setValue(value)
                row.addWidget(bar, 1)
                row.addWidget(QLabel(str(value)), 0)
                section.body.addLayout(row)
            body.addWidget(section)

        if deleted_snapshot:
            section = _StatsSection("Удаление", self)
            section.body.addWidget(QLabel(f"Источник: {deleted_snapshot.get('source') or 'telegram'}"))
            deleted_at = int(deleted_snapshot.get("deleted_at") or 0)
            if deleted_at > 0:
                section.body.addWidget(QLabel(f"Время: {deleted_at}"))
            snapshot_text = str(deleted_snapshot.get("snapshot_text") or "").strip()
            if snapshot_text:
                label = QLabel(snapshot_text)
                label.setWordWrap(True)
                label.setStyleSheet("color:#9fb3cc;")
                section.body.addWidget(label)
            body.addWidget(section)

        risk_labels = {
            "reactions_exceed_views": "Реакций больше ожидаемого относительно просмотров.",
            "forwards_exceed_views": "Пересылок больше ожидаемого относительно просмотров.",
            "sender_bot_like": "Отправитель похож на бот-аккаунт.",
        }
        risk_flags = [str(x) for x in list(data.get("risk_flags") or []) if str(x).strip()]
        if risk_flags:
            section = _StatsSection("Анти-накрутка: риск-флаги", self)
            for flag in risk_flags:
                lbl = QLabel(f"• {risk_labels.get(flag, flag)}")
                lbl.setStyleSheet("color:#ffd78c;")
                section.body.addWidget(lbl)
            body.addWidget(section)

        body.addStretch(1)
        if not self._embedded:
            buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, parent=self)
            buttons.rejected.connect(self.reject)
            root.addWidget(buttons)
        else:
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)


def build_header_menu(parent: QWidget) -> QMenu:
    menu = QMenu(parent)
    menu.setStyleSheet(
        "QMenu{background-color:#102033;color:#dfe7f5;border:1px solid rgba(255,255,255,0.08);padding:6px;}"
        "QMenu::item{padding:7px 24px 7px 12px;border-radius:8px;}"
        "QMenu::item:selected{background-color:rgba(255,255,255,0.08);}"
    )
    return menu

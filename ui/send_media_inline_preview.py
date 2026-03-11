from __future__ import annotations

import hashlib
import os
import time
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QUrl, Signal, Slot, QRectF
from PySide6.QtGui import QColor, QPainter, QRegion
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSizePolicy,
    QSlider,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from ui.common import HAVE_QTMULTIMEDIA, MediaPlaybackCoordinator, log
from ui.styles import StyleManager

if HAVE_QTMULTIMEDIA:
    from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
    from PySide6.QtMultimediaWidgets import QVideoWidget
else:  # pragma: no cover
    QAudioOutput = object  # type: ignore[assignment]
    QMediaPlayer = object  # type: ignore[assignment]
    QVideoWidget = QWidget  # type: ignore[assignment]


def _fmt_time(ms: int) -> str:
    total = max(0, int(ms or 0)) // 1000
    m, s = divmod(total, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _media_volume_ratio(default: float = 1.0) -> float:
    raw = str(os.getenv("DRAGO_MEDIA_VOLUME", "") or "").strip()
    if not raw:
        return max(0.0, min(1.0, float(default)))
    try:
        if "." in raw:
            val = float(raw)
            if val <= 1.0:
                return max(0.0, min(1.0, val))
            return max(0.0, min(1.0, val / 100.0))
        val_i = int(raw)
        return max(0.0, min(1.0, float(val_i) / 100.0))
    except Exception:
        return max(0.0, min(1.0, float(default)))


def _fmt_size(path: str) -> str:
    try:
        size = int(os.path.getsize(path))
    except Exception:
        return ""
    units = ("B", "KB", "MB", "GB")
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.1f} {unit}"


class _VoicePreviewWaveform(QWidget):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._bars = [0.45 for _ in range(52)]
        self._progress = 0.0
        self._active = False
        self.setMinimumHeight(28)
        self.setMaximumHeight(28)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)

    def set_seed(self, seed: str) -> None:
        raw = hashlib.sha256(str(seed or "").encode("utf-8", "ignore")).digest()
        bars = []
        for idx in range(52):
            val = raw[idx % len(raw)] / 255.0
            bars.append(0.22 + (val * 0.78))
        self._bars = bars
        self.update()

    def set_progress(self, position_ms: int, duration_ms: int) -> None:
        total = max(0, int(duration_ms or 0))
        current = max(0, min(int(position_ms or 0), total)) if total else 0
        self._progress = (float(current) / float(total)) if total > 0 else 0.0
        self.update()

    def set_active(self, active: bool) -> None:
        active_bool = bool(active)
        if self._active == active_bool:
            return
        self._active = active_bool
        self.update()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        rect = self.rect().adjusted(0, 2, 0, -2)
        if rect.width() <= 8 or rect.height() <= 4:
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        bars_count = min(len(self._bars), max(12, rect.width() // 5))
        step = rect.width() / max(1, bars_count)
        played = int(round(self._progress * bars_count))
        accent = QColor(111, 201, 255, 230 if self._active else 205)
        idle = QColor(116, 142, 171, 105)
        for idx in range(bars_count):
            value = self._bars[idx % len(self._bars)]
            width = max(2.0, step - 2.0)
            height = max(4.0, rect.height() * value)
            x = rect.left() + (idx * step) + ((step - width) / 2.0)
            y = rect.center().y() - (height / 2.0)
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(accent if idx < played else idle)
            radius = min(1.8, width / 2.0)
            painter.drawRoundedRect(QRectF(x, y, width, height), radius, radius)
        painter.end()


class InlineMediaPreviewBar(QFrame):
    """Inline media preview shown inside chat (no separate dialog)."""

    sendRequested = Signal()
    cancelRequested = Signal()

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("mediaPreviewBar")
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        StyleManager.instance().bind_stylesheet(self, "main.media_preview_bar")

        self._kind: str = ""
        self.file_path: str = ""

        self._player: Optional[QMediaPlayer] = None
        self._audio: Optional[QAudioOutput] = None
        self._video_widget: Optional[QVideoWidget] = None
        self._voice_wave: Optional[_VoicePreviewWaveform] = None

        self._slider_dragging = False
        self._source_pending = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._hdr = QLabel("", self)
        self._hdr.setWordWrap(True)
        StyleManager.instance().bind_stylesheet(self._hdr, "main.media_preview_header")
        layout.addWidget(self._hdr)

        self._meta = QLabel("", self)
        self._meta.setWordWrap(True)
        self._meta.setStyleSheet("color:#8ea8c2;font-size:11px;")
        layout.addWidget(self._meta)

        self._media_container = QVBoxLayout()
        self._media_container.setContentsMargins(0, 0, 0, 0)
        self._media_container.setSpacing(6)
        layout.addLayout(self._media_container)

        controls_frame = QFrame(self)
        controls_frame.setObjectName("previewControls")
        controls_frame.setStyleSheet(
            "QFrame#previewControls{background-color:rgba(255,255,255,0.03);"
            "border:1px solid rgba(255,255,255,0.05);border-radius:14px;}"
        )
        controls = QHBoxLayout(controls_frame)
        controls.setContentsMargins(10, 8, 10, 8)
        controls.setSpacing(8)

        self.btn_play = QToolButton(self)
        self.btn_play.setObjectName("previewPlayButton")
        self.btn_play.setText("▶")
        self.btn_play.setFixedSize(44, 44)
        self.btn_play.setStyleSheet(
            "QToolButton{background-color:rgba(89,183,255,0.18);border:1px solid rgba(89,183,255,0.28);"
            "border-radius:22px;color:#e9f7ff;font-size:18px;font-weight:700;}"
            "QToolButton:hover{background-color:rgba(89,183,255,0.28);}"
        )
        self.btn_play.clicked.connect(self._toggle_play)

        self.slider = QSlider(Qt.Orientation.Horizontal, self)
        self.slider.setRange(0, 0)
        self.slider.setStyleSheet(
            "QSlider::groove:horizontal{background-color:rgba(255,255,255,0.08);height:4px;border-radius:2px;}"
            "QSlider::sub-page:horizontal{background-color:rgba(89,183,255,0.78);border-radius:2px;}"
            "QSlider::handle:horizontal{background-color:#dff3ff;border:1px solid rgba(89,183,255,0.65);"
            "width:14px;height:14px;margin:-6px 0;border-radius:7px;}"
        )
        self.slider.sliderPressed.connect(self._on_slider_pressed)
        self.slider.sliderReleased.connect(self._on_slider_released)

        self.lbl_time = QLabel("00:00 / 00:00", self)
        self.lbl_time.setMinimumWidth(92)
        self.lbl_time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        StyleManager.instance().bind_stylesheet(self.lbl_time, "main.media_preview_time")

        self.volume_slider = QSlider(Qt.Orientation.Horizontal, self)
        self.volume_slider.setRange(0, 100)
        self.volume_slider.setValue(int(round(_media_volume_ratio() * 100.0)))
        self.volume_slider.setFixedWidth(110)
        self.volume_slider.setToolTip("Громкость")
        self.volume_slider.setStyleSheet(
            "QSlider::groove:horizontal{background-color:rgba(255,255,255,0.08);height:4px;border-radius:2px;}"
            "QSlider::sub-page:horizontal{background-color:rgba(141,213,255,0.72);border-radius:2px;}"
            "QSlider::handle:horizontal{background-color:#eef8ff;border:1px solid rgba(141,213,255,0.70);"
            "width:12px;height:12px;margin:-5px 0;border-radius:6px;}"
        )
        self.volume_slider.valueChanged.connect(self._on_volume_changed)

        controls.addWidget(self.btn_play)
        controls.addWidget(self.slider, 1)
        controls.addWidget(self.lbl_time)
        controls.addWidget(self.volume_slider, 0)
        layout.addWidget(controls_frame)

        actions = QHBoxLayout()
        actions.addStretch(1)

        self.btn_cancel = QPushButton("Отмена", self)
        self.btn_cancel.setObjectName("previewCancelButton")
        self.btn_cancel.setStyleSheet(
            "QPushButton{background-color:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.08);"
            "border-radius:12px;color:#d7e4f3;padding:8px 14px;font-weight:600;}"
            "QPushButton:hover{background-color:rgba(255,255,255,0.10);}"
        )
        self.btn_cancel.clicked.connect(self.cancelRequested.emit)

        self.btn_send = QPushButton("Отправить", self)
        self.btn_send.setObjectName("previewSendButton")
        self.btn_send.setStyleSheet(
            "QPushButton{background-color:rgba(89,183,255,0.22);border:1px solid rgba(89,183,255,0.32);"
            "border-radius:12px;color:#e7f5ff;padding:8px 14px;font-weight:700;}"
            "QPushButton:hover{background-color:rgba(89,183,255,0.30);}"
        )
        self.btn_send.clicked.connect(self.sendRequested.emit)

        actions.addWidget(self.btn_cancel)
        actions.addWidget(self.btn_send)
        layout.addLayout(actions)

        self._status_label = QLabel("", self)
        self._status_label.setWordWrap(True)
        StyleManager.instance().bind_stylesheet(self._status_label, "main.media_preview_status")
        layout.addWidget(self._status_label)
        self._status_label.hide()

        self.setVisible(False)

    def set_media(self, kind: str, file_path: str, source_name: str = "") -> None:
        self._kind = str(kind or "").strip().lower()
        self.file_path = str(file_path or "")

        name = os.path.basename(source_name) if source_name else os.path.basename(self.file_path)
        size_text = _fmt_size(self.file_path)
        if self._kind == "voice":
            self._hdr.setText("Голосовое сообщение")
        else:
            self._hdr.setText("Видео-кружок")
        self._meta.setText(" • ".join([part for part in (name, size_text, "Локальный предпросмотр перед отправкой") if part]))

        self._reset_media_container()
        self._set_status_text("")
        self.btn_send.setEnabled(True)
        try:
            self.slider.setValue(0)
        except Exception:
            pass

        if not os.path.isfile(self.file_path):
            self._set_status_text("Файл предпросмотра не найден.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            self.btn_send.setEnabled(False)
            return
        if not HAVE_QTMULTIMEDIA:
            self._set_status_text("QtMultimedia недоступен: воспроизведение выключено.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            return

        self.btn_play.setEnabled(True)
        self.slider.setEnabled(True)

        player = self._ensure_player()
        if not player:
            self._set_status_text("Не удалось инициализировать плеер.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            self.btn_send.setEnabled(False)
            return

        # Video output only for video notes.
        if self._kind == "video_note":
            if self._video_widget is None:
                self._video_widget = QVideoWidget(self)
                self._video_widget.setFixedSize(240, 240)
                try:
                    self._video_widget.setMask(QRegion(0, 0, 240, 240, QRegion.RegionType.Ellipse))
                except Exception:
                    pass
                try:
                    self._video_widget.setStyleSheet("background:#111;")
                except Exception:
                    pass
                shell = QFrame(self)
                shell.setStyleSheet(
                    "QFrame{background-color:rgba(255,255,255,0.035);border:1px solid rgba(255,255,255,0.06);border-radius:24px;}"
                )
                wrap = QHBoxLayout(shell)
                wrap.setContentsMargins(14, 14, 14, 14)
                wrap.addStretch(1)
                wrap.addWidget(self._video_widget)
                wrap.addStretch(1)
                badge = QLabel("VIDEO NOTE", shell)
                badge.setStyleSheet(
                    "background-color:rgba(89,183,255,0.18);color:#dff3ff;border-radius:10px;padding:4px 8px;font-size:10px;font-weight:700;"
                )
                wrap.addWidget(badge, 0, Qt.AlignmentFlag.AlignTop)
                self._media_container.addWidget(shell)
            try:
                player.setVideoOutput(self._video_widget)
            except Exception:
                pass
        else:
            card = QFrame(self)
            card.setStyleSheet(
                "QFrame{background-color:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.06);border-radius:16px;}"
                "QLabel{background:transparent;}"
            )
            card_layout = QHBoxLayout(card)
            card_layout.setContentsMargins(12, 12, 12, 12)
            card_layout.setSpacing(12)
            icon = QLabel("🎙", card)
            icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
            icon.setFixedSize(48, 48)
            icon.setStyleSheet(
                "background-color:rgba(89,183,255,0.18);color:#dff2ff;border-radius:24px;font-size:24px;font-weight:700;"
            )
            card_layout.addWidget(icon, 0)
            text_col = QVBoxLayout()
            text_col.setContentsMargins(0, 0, 0, 0)
            text_col.setSpacing(4)
            title_lbl = QLabel(name or "Голосовое сообщение", card)
            title_lbl.setStyleSheet("color:#eef7ff;font-size:13px;font-weight:700;")
            subtitle_lbl = QLabel("Проверьте звучание и длительность перед отправкой", card)
            subtitle_lbl.setStyleSheet("color:#8da8c4;font-size:11px;")
            wave = _VoicePreviewWaveform(card)
            wave.set_seed(name or self.file_path)
            self._voice_wave = wave
            text_col.addWidget(title_lbl)
            text_col.addWidget(subtitle_lbl)
            text_col.addWidget(wave)
            card_layout.addLayout(text_col, 1)
            self._media_container.addWidget(card)
            try:
                player.setVideoOutput(None)  # type: ignore[arg-type]
            except Exception:
                pass

        self._source_pending = True
        self._set_status_text("Подготовка предпросмотра…")
        QTimer.singleShot(0, self._load_source_if_needed)

    def stop(self) -> None:
        self._stop_player()

    def _reset_media_container(self) -> None:
        while self._media_container.count():
            item = self._media_container.takeAt(0)
            w = item.widget()
            if w is not None:
                w.setParent(None)
                w.deleteLater()
        self._video_widget = None
        self._voice_wave = None

    def _ensure_player(self) -> Optional[QMediaPlayer]:
        if not HAVE_QTMULTIMEDIA:
            return None
        if self._player is None:
            self._audio = QAudioOutput(self)
            self._audio.setVolume(_media_volume_ratio())
            self._player = QMediaPlayer(self)
            self._player.setAudioOutput(self._audio)
            self._player.durationChanged.connect(self._on_duration_changed)
            self._player.positionChanged.connect(self._on_position_changed)
            self._player.playbackStateChanged.connect(self._on_state_changed)
            try:
                self._player.errorOccurred.connect(self._on_player_error)  # type: ignore[attr-defined]
                self._player.mediaStatusChanged.connect(self._on_media_status_changed)  # type: ignore[attr-defined]
            except Exception:
                pass
            MediaPlaybackCoordinator.register(self._player)
        return self._player

    def _load_source_if_needed(self) -> None:
        player = self._player
        if not player or not self._source_pending:
            return
        self._source_pending = False
        try:
            player.setSource(QUrl.fromLocalFile(self.file_path))
            self._set_status_text("")
        except Exception:
            self._set_status_text("Не удалось открыть файл предпросмотра.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            self.btn_send.setEnabled(False)

    @Slot()
    def _toggle_play(self) -> None:
        player = self._player
        if not player:
            return
        if self._source_pending:
            self._load_source_if_needed()
        if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            player.pause()
        else:
            MediaPlaybackCoordinator.pause_others(player)
            try:
                dur = int(player.duration() or 0)
                pos = int(player.position() or 0)
                if dur > 0 and pos >= max(0, dur - 200):
                    player.setPosition(0)
            except Exception:
                pass
            player.play()

    @Slot()
    def _on_slider_pressed(self) -> None:
        self._slider_dragging = True

    @Slot()
    def _on_slider_released(self) -> None:
        self._slider_dragging = False
        player = self._player
        if not player:
            return
        try:
            player.setPosition(int(self.slider.value()))
        except Exception:
            pass

    @Slot(int)
    def _on_duration_changed(self, duration: int) -> None:
        duration = max(0, int(duration or 0))
        self.slider.setRange(0, duration)
        if self._voice_wave is not None:
            self._voice_wave.set_progress(self.slider.value(), duration)
        self._update_time_label(self.slider.value(), duration)

    @Slot(int)
    def _on_position_changed(self, position: int) -> None:
        player = self._player
        duration = player.duration() if player else 0
        if not self._slider_dragging:
            self.slider.blockSignals(True)
            self.slider.setValue(max(0, int(position or 0)))
            self.slider.blockSignals(False)
        if self._voice_wave is not None:
            self._voice_wave.set_progress(position, duration)
        self._update_time_label(position, duration)

    @Slot(object)
    def _on_state_changed(self, state: object) -> None:
        if not HAVE_QTMULTIMEDIA:
            self.btn_play.setText("▶")
            return
        playing = state == QMediaPlayer.PlaybackState.PlayingState
        self.btn_play.setText("⏸" if playing else "▶")
        if self._voice_wave is not None:
            self._voice_wave.set_active(playing)

    def _update_time_label(self, pos_ms: int, dur_ms: int) -> None:
        self.lbl_time.setText(f"{_fmt_time(pos_ms)} / {_fmt_time(dur_ms)}")

    @Slot(int)
    def _on_volume_changed(self, value: int) -> None:
        audio = self._audio
        if audio is None:
            return
        try:
            audio.setVolume(max(0.0, min(1.0, float(value) / 100.0)))
        except Exception:
            pass

    @Slot(object, str)
    def _on_player_error(self, error: object, error_str: str) -> None:
        try:
            log.warning("[GUI] Preview player error %s: %s", str(error), str(error_str or ""))
        except Exception:
            pass
        msg = str(error_str or "").strip() or "Ошибка воспроизведения"
        try:
            self._set_status_text(msg)
        except Exception:
            pass

    @Slot(object)
    def _on_media_status_changed(self, status: object) -> None:
        # Provide a small hint if the backend cannot load the file.
        try:
            if HAVE_QTMULTIMEDIA and status == QMediaPlayer.MediaStatus.InvalidMedia:
                self._set_status_text("Файл не поддерживается (InvalidMedia).")
        except Exception:
            pass

    def _set_status_text(self, text: str) -> None:
        message = str(text or "").strip()
        self._status_label.setText(message)
        self._status_label.setVisible(bool(message))

    def _stop_player(self) -> None:
        player = self._player
        if not player:
            return
        try:
            player.pause()
        except Exception:
            pass
        try:
            player.stop()
        except Exception:
            pass
        try:
            player.setSource(QUrl())
        except Exception:
            pass

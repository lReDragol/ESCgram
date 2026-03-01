from __future__ import annotations

import os
import time
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QUrl, Signal, Slot
from PySide6.QtGui import QRegion
from PySide6.QtWidgets import (
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
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

        self._slider_dragging = False
        self._source_pending = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._hdr = QLabel("", self)
        self._hdr.setWordWrap(True)
        StyleManager.instance().bind_stylesheet(self._hdr, "main.media_preview_header")
        layout.addWidget(self._hdr)

        self._media_container = QVBoxLayout()
        self._media_container.setContentsMargins(0, 0, 0, 0)
        self._media_container.setSpacing(6)
        layout.addLayout(self._media_container)

        controls = QHBoxLayout()
        controls.setSpacing(8)

        self.btn_play = QToolButton(self)
        self.btn_play.setText("▶")
        self.btn_play.setFixedWidth(40)
        self.btn_play.clicked.connect(self._toggle_play)

        self.slider = QSlider(Qt.Orientation.Horizontal, self)
        self.slider.setRange(0, 0)
        self.slider.sliderPressed.connect(self._on_slider_pressed)
        self.slider.sliderReleased.connect(self._on_slider_released)

        self.lbl_time = QLabel("00:00 / 00:00", self)
        self.lbl_time.setMinimumWidth(92)
        self.lbl_time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        StyleManager.instance().bind_stylesheet(self.lbl_time, "main.media_preview_time")

        controls.addWidget(self.btn_play)
        controls.addWidget(self.slider, 1)
        controls.addWidget(self.lbl_time)
        layout.addLayout(controls)

        actions = QHBoxLayout()
        actions.addStretch(1)

        self.btn_cancel = QPushButton("Отмена", self)
        self.btn_cancel.clicked.connect(self.cancelRequested.emit)

        self.btn_send = QPushButton("Отправить", self)
        self.btn_send.clicked.connect(self.sendRequested.emit)

        actions.addWidget(self.btn_cancel)
        actions.addWidget(self.btn_send)
        layout.addLayout(actions)

        self._status_label = QLabel("", self)
        self._status_label.setWordWrap(True)
        StyleManager.instance().bind_stylesheet(self._status_label, "main.media_preview_status")
        layout.addWidget(self._status_label)

        self.setVisible(False)

    def set_media(self, kind: str, file_path: str, source_name: str = "") -> None:
        self._kind = str(kind or "").strip().lower()
        self.file_path = str(file_path or "")

        name = os.path.basename(source_name) if source_name else os.path.basename(self.file_path)
        if self._kind == "voice":
            self._hdr.setText(f"Проверьте голосовое перед отправкой: {name}")
        else:
            self._hdr.setText(f"Проверьте видео-кружок перед отправкой: {name}")

        self._reset_media_container()
        self._status_label.clear()
        self.btn_send.setEnabled(True)
        try:
            self.slider.setValue(0)
        except Exception:
            pass

        if not os.path.isfile(self.file_path):
            self._status_label.setText("Файл предпросмотра не найден.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            self.btn_send.setEnabled(False)
            return
        if not HAVE_QTMULTIMEDIA:
            self._status_label.setText("QtMultimedia недоступен: воспроизведение выключено.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            return

        self.btn_play.setEnabled(True)
        self.slider.setEnabled(True)

        player = self._ensure_player()
        if not player:
            self._status_label.setText("Не удалось инициализировать плеер.")
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
                wrap = QHBoxLayout()
                wrap.addStretch(1)
                wrap.addWidget(self._video_widget)
                wrap.addStretch(1)
                holder = QWidget(self)
                holder.setLayout(wrap)
                self._media_container.addWidget(holder)
            try:
                player.setVideoOutput(self._video_widget)
            except Exception:
                pass
        else:
            icon = QLabel("Голосовое сообщение", self)
            icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
            icon.setStyleSheet("font-size:13px; padding:6px 0;")
            self._media_container.addWidget(icon)
            try:
                player.setVideoOutput(None)  # type: ignore[arg-type]
            except Exception:
                pass

        self._source_pending = True
        self._status_label.setText("Подготовка предпросмотра…")
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

    def _ensure_player(self) -> Optional[QMediaPlayer]:
        if not HAVE_QTMULTIMEDIA:
            return None
        if self._player is None:
            self._audio = QAudioOutput(self)
            self._audio.setVolume(1.0)
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
            self._status_label.clear()
        except Exception:
            self._status_label.setText("Не удалось открыть файл предпросмотра.")
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
        self._update_time_label(self.slider.value(), duration)

    @Slot(int)
    def _on_position_changed(self, position: int) -> None:
        player = self._player
        duration = player.duration() if player else 0
        if not self._slider_dragging:
            self.slider.blockSignals(True)
            self.slider.setValue(max(0, int(position or 0)))
            self.slider.blockSignals(False)
        self._update_time_label(position, duration)

    @Slot(object)
    def _on_state_changed(self, state: object) -> None:
        if not HAVE_QTMULTIMEDIA:
            self.btn_play.setText("▶")
            return
        playing = state == QMediaPlayer.PlaybackState.PlayingState
        self.btn_play.setText("⏸" if playing else "▶")

    def _update_time_label(self, pos_ms: int, dur_ms: int) -> None:
        self.lbl_time.setText(f"{_fmt_time(pos_ms)} / {_fmt_time(dur_ms)}")

    @Slot(object, str)
    def _on_player_error(self, error: object, error_str: str) -> None:
        try:
            log.warning("[GUI] Preview player error %s: %s", str(error), str(error_str or ""))
        except Exception:
            pass
        msg = str(error_str or "").strip() or "Ошибка воспроизведения"
        try:
            self._status_label.setText(msg)
        except Exception:
            pass

    @Slot(object)
    def _on_media_status_changed(self, status: object) -> None:
        # Provide a small hint if the backend cannot load the file.
        try:
            if HAVE_QTMULTIMEDIA and status == QMediaPlayer.MediaStatus.InvalidMedia:
                self._status_label.setText("Файл не поддерживается (InvalidMedia).")
        except Exception:
            pass

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

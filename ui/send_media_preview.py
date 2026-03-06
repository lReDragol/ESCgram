from __future__ import annotations

import os
from typing import Optional

from PySide6.QtCore import Qt, QTimer, QUrl, Slot
from PySide6.QtGui import QRegion
from PySide6.QtWidgets import (
    QDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSlider,
    QVBoxLayout,
    QWidget,
)

from ui.common import HAVE_QTMULTIMEDIA

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


class _BasePreviewDialog(QDialog):
    def __init__(self, file_path: str, title: str, header: str, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.file_path = file_path
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(460, 420)

        self._player: Optional[QMediaPlayer] = None
        self._audio: Optional[QAudioOutput] = None
        self._slider_dragging = False
        self._source_pending = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(10)

        hdr = QLabel(header, self)
        hdr.setWordWrap(True)
        hdr.setStyleSheet("font-size:13px; color:#dfe8f4;")
        layout.addWidget(hdr)

        self.media_container = QVBoxLayout()
        self.media_container.setContentsMargins(0, 0, 0, 0)
        self.media_container.setSpacing(8)
        layout.addLayout(self.media_container)

        controls = QHBoxLayout()
        controls.setSpacing(8)

        self.btn_play = QPushButton("▶", self)
        self.btn_play.setFixedWidth(52)
        self.btn_play.clicked.connect(self._toggle_play)

        self.slider = QSlider(Qt.Orientation.Horizontal, self)
        self.slider.setRange(0, 0)
        self.slider.sliderPressed.connect(self._on_slider_pressed)
        self.slider.sliderReleased.connect(self._on_slider_released)

        self.lbl_time = QLabel("00:00 / 00:00", self)
        self.lbl_time.setMinimumWidth(92)
        self.lbl_time.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.lbl_time.setStyleSheet("color:#9fb0c5; font-size:11px;")

        controls.addWidget(self.btn_play)
        controls.addWidget(self.slider, 1)
        controls.addWidget(self.lbl_time)
        layout.addLayout(controls)

        buttons = QHBoxLayout()
        buttons.addStretch(1)

        self.btn_cancel = QPushButton("Отмена", self)
        self.btn_cancel.clicked.connect(self.reject)

        self.btn_send = QPushButton("Отправить", self)
        self.btn_send.clicked.connect(self.accept)

        buttons.addWidget(self.btn_cancel)
        buttons.addWidget(self.btn_send)
        layout.addLayout(buttons)

        self._status_label: Optional[QLabel] = None
        self._init_preview()

    def _init_preview(self) -> None:
        if not os.path.isfile(self.file_path):
            self._show_status("Файл предпросмотра не найден.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            return
        if not HAVE_QTMULTIMEDIA:
            self._show_status("QtMultimedia недоступен: воспроизведение выключено.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)
            return

        self._audio = QAudioOutput(self)
        self._audio.setVolume(_media_volume_ratio())
        self._player = QMediaPlayer(self)
        self._player.setAudioOutput(self._audio)

        self._attach_video_output(self._player)

        self._player.durationChanged.connect(self._on_duration_changed)
        self._player.positionChanged.connect(self._on_position_changed)
        self._player.playbackStateChanged.connect(self._on_state_changed)
        self._source_pending = True
        self._show_status("Подготовка предпросмотра…")

    def _attach_video_output(self, player: QMediaPlayer) -> None:
        _ = player

    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        if self._source_pending:
            QTimer.singleShot(0, self._load_source_if_needed)

    def _load_source_if_needed(self) -> None:
        player = self._player
        if not player or not self._source_pending:
            return
        self._source_pending = False
        try:
            player.setSource(QUrl.fromLocalFile(self.file_path))
            if self._status_label is not None:
                self._status_label.clear()
        except Exception:
            self._show_status("Не удалось открыть файл предпросмотра.")
            self.btn_play.setEnabled(False)
            self.slider.setEnabled(False)

    def _show_status(self, text: str) -> None:
        if self._status_label is None:
            self._status_label = QLabel(self)
            self._status_label.setWordWrap(True)
            self._status_label.setStyleSheet("color:#9fb0c5; font-size:11px;")
            self.media_container.addWidget(self._status_label)
        self._status_label.setText(text)

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

    def _stop_player(self) -> None:
        player = self._player
        if not player:
            return
        try:
            player.stop()
        except Exception:
            pass
        try:
            player.setSource(QUrl())
        except Exception:
            pass

    def accept(self) -> None:  # type: ignore[override]
        self._stop_player()
        super().accept()

    def reject(self) -> None:  # type: ignore[override]
        self._stop_player()
        super().reject()


class VoicePreviewDialog(_BasePreviewDialog):
    def __init__(self, file_path: str, source_name: str, parent: Optional[QWidget] = None):
        name = os.path.basename(source_name) or os.path.basename(file_path)
        super().__init__(
            file_path,
            "Предпрослушивание голосового",
            f"Проверьте голосовое перед отправкой: {name}",
            parent,
        )

        icon = QLabel("🎙️ Голосовое сообщение", self)
        icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon.setStyleSheet("font-size:15px; color:#dbe6f3; padding:8px 0;")
        self.media_container.insertWidget(0, icon)


class VideoNotePreviewDialog(_BasePreviewDialog):
    def __init__(self, file_path: str, source_name: str, parent: Optional[QWidget] = None):
        name = os.path.basename(source_name) or os.path.basename(file_path)
        super().__init__(
            file_path,
            "Предпросмотр кружка",
            f"Проверьте видео-кружок перед отправкой: {name}",
            parent,
        )

    def _attach_video_output(self, player: QMediaPlayer) -> None:  # type: ignore[override]
        frame = QFrame(self)
        frame.setFixedSize(300, 300)
        frame.setFrameShape(QFrame.Shape.NoFrame)
        frame.setStyleSheet("background:#111;")

        video = QVideoWidget(frame)
        video.setFixedSize(300, 300)
        video.setMask(QRegion(0, 0, 300, 300, QRegion.RegionType.Ellipse))
        video.setStyleSheet("background:#111;")

        holder = QHBoxLayout(frame)
        holder.setContentsMargins(0, 0, 0, 0)
        holder.setSpacing(0)
        holder.addWidget(video, 0, Qt.AlignmentFlag.AlignCenter)

        wrap = QHBoxLayout()
        wrap.addStretch(1)
        wrap.addWidget(frame)
        wrap.addStretch(1)

        container = QWidget(self)
        container.setLayout(wrap)

        hint = QLabel("Круглый кадр соответствует формату отправки", self)
        hint.setAlignment(Qt.AlignmentFlag.AlignCenter)
        hint.setStyleSheet("color:#9fb0c5; font-size:11px;")

        self.media_container.addWidget(container)
        self.media_container.addWidget(hint)
        player.setVideoOutput(video)

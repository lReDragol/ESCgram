from __future__ import annotations

import os
from typing import Optional

from PySide6.QtCore import Qt, QUrl, QSize
from PySide6.QtGui import QKeyEvent, QPixmap, QMovie
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QSizePolicy,
    QSlider,
    QToolButton,
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
    value = max(0, int(ms or 0))
    sec = value // 1000
    mm, ss = divmod(sec, 60)
    hh, mm = divmod(mm, 60)
    if hh:
        return f"{hh}:{mm:02d}:{ss:02d}"
    return f"{mm}:{ss:02d}"


class MediaViewerDialog(QDialog):
    def __init__(self, *, media_path: str, kind: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._path = str(media_path or "")
        self._kind = str(kind or "").lower()
        self._pixmap: Optional[QPixmap] = None
        self._movie: Optional[QMovie] = None
        self._slider_dragging = False
        self._player: Optional[QMediaPlayer] = None
        self._audio_output: Optional[QAudioOutput] = None

        self.setWindowTitle("Просмотр медиа")
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        if parent is not None:
            self.setWindowFlags(Qt.WindowType.Widget | Qt.WindowType.FramelessWindowHint)
        else:
            self.setWindowFlag(Qt.WindowType.FramelessWindowHint, True)
        self.setStyleSheet("background-color:#050b12; color:#dce8f8;")

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 12, 14, 12)
        root.setSpacing(8)

        top = QHBoxLayout()
        top.addStretch(1)
        btn_close = QToolButton(self)
        btn_close.setText("✕")
        btn_close.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_close.setStyleSheet(
            "QToolButton{color:#e8f2ff;font-size:20px;background:rgba(34,52,76,0.75);"
            "border:none;border-radius:16px;padding:6px 10px;}"
            "QToolButton:hover{background:rgba(61,90,128,0.9);}"
        )
        btn_close.clicked.connect(self.close)
        top.addWidget(btn_close, 0, Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight)
        root.addLayout(top)

        self._content = QWidget(self)
        self._content_layout = QVBoxLayout(self._content)
        self._content_layout.setContentsMargins(0, 0, 0, 0)
        self._content_layout.setSpacing(8)
        root.addWidget(self._content, 1)

        if self._kind in {"video", "video_note"}:
            if HAVE_QTMULTIMEDIA:
                self._build_video()
            else:
                self._build_error("QtMultimedia недоступен.")
        else:
            self._build_image_like()

    def _build_error(self, text: str) -> None:
        lbl = QLabel(text, self._content)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl.setStyleSheet("color:#ff9aa0; font-size:14px;")
        self._content_layout.addWidget(lbl, 1)

    def _build_image_like(self) -> None:
        self._image_label = QLabel(self._content)
        self._image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._image_label.setStyleSheet("background-color: transparent;")
        self._image_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._image_label.setMinimumSize(320, 180)
        self._content_layout.addWidget(self._image_label, 1)

        if not self._path or not os.path.isfile(self._path):
            self._image_label.setText("Файл не найден")
            return

        lower = self._path.lower()
        if self._kind == "animation" and lower.endswith(".gif"):
            movie = QMovie(self._path)
            if movie.isValid():
                self._movie = movie
                frame_size = movie.frameRect().size()
                target = self._fit_size(frame_size if frame_size.isValid() else QSize(640, 360))
                if target.isValid():
                    movie.setScaledSize(target)
                self._image_label.setMovie(movie)
                movie.start()
                return

        pix = QPixmap(self._path)
        if pix.isNull():
            self._image_label.setText("Не удалось открыть изображение")
            return
        self._pixmap = pix
        self._rescale_pixmap()

    def _build_video(self) -> None:
        if not self._path or not os.path.isfile(self._path):
            self._build_error("Видеофайл не найден")
            return

        video = QVideoWidget(self._content)
        video.setStyleSheet("background:#000;")
        video.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        try:
            video.setAspectRatioMode(Qt.AspectRatioMode.KeepAspectRatio)
        except Exception:
            pass
        self._content_layout.addWidget(video, 1)

        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(8)

        self._btn_play = QToolButton(self._content)
        self._btn_play.setText("⏸")
        self._btn_play.setCursor(Qt.CursorShape.PointingHandCursor)
        self._btn_play.clicked.connect(self._toggle_play)
        controls.addWidget(self._btn_play, 0)

        self._slider = QSlider(Qt.Orientation.Horizontal, self._content)
        self._slider.setRange(0, 0)
        self._slider.sliderPressed.connect(self._on_slider_pressed)
        self._slider.sliderReleased.connect(self._on_slider_released)
        self._slider.sliderMoved.connect(self._on_slider_moved)
        controls.addWidget(self._slider, 1)

        self._time_label = QLabel("0:00 / 0:00", self._content)
        self._time_label.setStyleSheet("color:#9fb0c8; font-size:12px;")
        controls.addWidget(self._time_label, 0)
        self._content_layout.addLayout(controls)

        player = QMediaPlayer(self)
        audio = QAudioOutput(self)
        player.setAudioOutput(audio)
        player.setVideoOutput(video)
        player.durationChanged.connect(self._on_duration_changed)
        player.positionChanged.connect(self._on_position_changed)
        player.playbackStateChanged.connect(self._on_state_changed)
        player.setSource(QUrl.fromLocalFile(self._path))
        player.play()
        self._player = player
        self._audio_output = audio

    def _rescale_pixmap(self) -> None:
        lbl = getattr(self, "_image_label", None)
        pix = self._pixmap
        if lbl is None or pix is None or pix.isNull():
            return
        area = self._content.size()
        if area.width() <= 8 or area.height() <= 8:
            return
        target = pix.scaled(
            area,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        lbl.setPixmap(target)

    def _fit_size(self, src: QSize) -> QSize:
        if not src.isValid():
            return QSize()
        area = self._content.size()
        if area.width() <= 8 or area.height() <= 8:
            return QSize()
        return src.scaled(area, Qt.AspectRatioMode.KeepAspectRatio)

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        self._rescale_pixmap()
        if self._movie is not None:
            try:
                frame_size = self._movie.frameRect().size()
                target = self._fit_size(frame_size if frame_size.isValid() else QSize(640, 360))
                if target.isValid():
                    self._movie.setScaledSize(target)
            except Exception:
                pass

    def showEvent(self, event) -> None:  # type: ignore[override]
        super().showEvent(event)
        self._rescale_pixmap()
        if self._movie is not None:
            try:
                frame_size = self._movie.frameRect().size()
                target = self._fit_size(frame_size if frame_size.isValid() else QSize(640, 360))
                if target.isValid():
                    self._movie.setScaledSize(target)
            except Exception:
                pass

    def keyPressEvent(self, event: QKeyEvent) -> None:  # type: ignore[override]
        if event.key() in {Qt.Key.Key_Escape, Qt.Key.Key_Backspace}:
            self.close()
            return
        super().keyPressEvent(event)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            if self._movie:
                self._movie.stop()
        except Exception:
            pass
        try:
            if self._player:
                self._player.stop()
        except Exception:
            pass
        super().closeEvent(event)

    def _toggle_play(self) -> None:
        player = self._player
        if not player:
            return
        if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            player.pause()
        else:
            player.play()

    def _on_slider_pressed(self) -> None:
        self._slider_dragging = True

    def _on_slider_released(self) -> None:
        self._slider_dragging = False
        if self._player:
            self._player.setPosition(int(self._slider.value()))

    def _on_slider_moved(self, value: int) -> None:
        player = self._player
        if not player:
            return
        self._time_label.setText(f"{_fmt_time(value)} / {_fmt_time(player.duration())}")

    def _on_duration_changed(self, duration: int) -> None:
        self._slider.setRange(0, max(0, int(duration)))
        self._time_label.setText(f"{_fmt_time(0)} / {_fmt_time(duration)}")

    def _on_position_changed(self, position: int) -> None:
        player = self._player
        if player is None:
            return
        if not self._slider_dragging:
            self._slider.blockSignals(True)
            self._slider.setValue(max(0, int(position)))
            self._slider.blockSignals(False)
        self._time_label.setText(f"{_fmt_time(position)} / {_fmt_time(player.duration())}")

    def _on_state_changed(self, state: QMediaPlayer.PlaybackState) -> None:
        self._btn_play.setText("⏸" if state == QMediaPlayer.PlaybackState.PlayingState else "▶")

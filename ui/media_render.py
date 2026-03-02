from __future__ import annotations

import math
import os
import time
from typing import Any, Callable, Optional, cast

from PySide6.QtCore import (
    Qt, QSize, QUrl, QThread, QPointF, QRectF, Signal, QObject, QEvent, QTimer
)
from PySide6.QtGui import (
    QMovie, QPixmap, QRegion, QPainter, QPen, QColor, QMouseEvent, QPaintEvent, QImage, QPainterPath
)
from PySide6.QtWidgets import QLabel, QVBoxLayout, QWidget, QHBoxLayout, QSlider

from ui.common import HAVE_QTMULTIMEDIA, log, MediaPlaybackCoordinator

if HAVE_QTMULTIMEDIA:
    from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer, QVideoSink, QVideoFrame
    from PySide6.QtMultimediaWidgets import QVideoWidget
else:  # pragma: no cover
    QAudioOutput = object  # type: ignore[assignment]
    QMediaPlayer = object  # type: ignore[assignment]
    QVideoSink = object  # type: ignore[assignment]
    QVideoFrame = object  # type: ignore[assignment]
    QVideoWidget = object  # type: ignore[assignment]
from ui.media_workers import ThumbWorker
from ui.styles import StyleManager


# ------------------------ утилиты -------------------------

def _fmt_time(ms: int) -> str:
    if ms < 0:
        ms = 0
    s = ms // 1000
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:d}:{s:02d}"


# FIX: ограничитель параллельных превью-игроков
class _ThumbLimiter:
    active: int = 0
    max_active: int = 2      # не более двух параллельно
    backoff_ms: int = 180    # повтор через 180 мс


class _ClickLabel(QLabel):
    clicked = Signal(QPointF)
    def mousePressEvent(self, e: QMouseEvent) -> None:
        self.clicked.emit(QPointF(e.position()))
        super().mousePressEvent(e)


class _VideoCircleLabel(QLabel):
    def __init__(self, size_px: int, parent: QWidget | None = None):
        super().__init__(parent)
        self.setFixedSize(size_px, size_px)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setMask(QRegion(0, 0, size_px, size_px, QRegion.RegionType.Ellipse))
        self._img: Optional[QImage] = None
        self._bg = QColor(0x1F, 0x1F, 0x1F)
    def set_frame(self, img: QImage) -> None:
        if not img or img.isNull():
            return
        # Pre-scale/crop once per frame; paintEvent stays cheap.
        w, h = self.width(), self.height()
        try:
            scaled = img.scaled(w, h, Qt.AspectRatioMode.KeepAspectRatioByExpanding, Qt.TransformationMode.SmoothTransformation)
            x = max(0, (scaled.width() - w) // 2)
            y = max(0, (scaled.height() - h) // 2)
            self._img = scaled.copy(x, y, w, h)
        except Exception:
            self._img = img
        self.update()
    def paintEvent(self, e: QPaintEvent) -> None:
        qp = QPainter(self)
        qp.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        qp.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        qp.fillRect(self.rect(), self._bg)
        if self._img and not self._img.isNull():
            qp.drawImage(0, 0, self._img)
        qp.end()


class _CircleSeekOverlay(QWidget):
    def __init__(self, host: "MediaRenderingMixin", parent: QWidget):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)
        self.setAttribute(Qt.WidgetAttribute.WA_NoSystemBackground, True)
        self.setMouseTracking(True)
        self._host = host
        self._dragging = False
        self._press_pos = QPointF(-1, -1)
        self._click_slop2 = 9.0
        self.margin = 10
        self._base_thickness = 8
        self._thin_scale = 0.7       # FIX: тоньше ~30%
        self.alpha_bg = 120
        self.alpha_fg = 200
        self._ratio = 0.0
    def thickness(self) -> int:
        return max(2, int(round(self._base_thickness * self._thin_scale)))
    def _center(self) -> QPointF:
        return QPointF(self.width() / 2.0, self.height() / 2.0)
    def _radii(self) -> tuple[float, float, float]:
        D = float(min(self.width(), self.height()))
        R_out = D / 2.0 - self.margin
        th = float(self.thickness())
        R_in = max(0.0, R_out - th)
        return D, R_out, R_in
    @staticmethod
    def _ratio_from_point(c: QPointF, p: QPointF) -> float:
        import math as _m
        theta = _m.degrees(_m.atan2(c.y() - p.y(), p.x() - c.x()))
        ratio = ((90.0 - theta) % 360.0) / 360.0
        return max(0.0, min(1.0, ratio))
    def mousePressEvent(self, e: QMouseEvent) -> None:
        self._press_pos = QPointF(e.position())
        _, R_out, R_in = self._radii()
        c = self._center()
        r = ((e.position().x() - c.x()) ** 2 + (e.position().y() - c.y()) ** 2) ** 0.5
        if R_in <= r <= R_out:
            self._dragging = True
            self._seek_to_point(e.position())
        else:
            self._dragging = False
        e.accept()
    def mouseMoveEvent(self, e: QMouseEvent) -> None:
        if self._dragging:
            self._seek_to_point(e.position()); e.accept()
        else:
            e.ignore()
    def mouseReleaseEvent(self, e: QMouseEvent) -> None:
        c = self._center()
        _, R_out, R_in = self._radii()
        r = ((e.position().x() - c.x()) ** 2 + (e.position().y() - c.y()) ** 2) ** 0.5
        moved2 = (e.position().x() - self._press_pos.x()) ** 2 + (e.position().y() - self._press_pos.y()) ** 2
        if not self._dragging and r < R_in and moved2 <= self._click_slop2:
            self._toggle(); e.accept(); return
        if self._dragging and R_in <= r <= R_out:
            self._seek_to_point(e.position()); self._dragging = False; e.accept(); return
        self._dragging = False; e.ignore()
    def _toggle(self) -> None:
        pl = self._host.player
        if not pl: return
        if pl.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            pl.pause()
        else:
            pl.play()
    def _seek_to_point(self, p: QPointF) -> None:
        pl = self._host.player
        if not pl: return
        dur = max(0, pl.duration())
        if dur <= 0: return
        ratio = self._ratio_from_point(self._center(), p)
        pl.setPosition(int(dur * ratio))
    def paintEvent(self, ev: QPaintEvent) -> None:
        qp = QPainter(self)
        qp.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        qp.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)
        _, R_out, _ = self._radii()
        side = int(2 * R_out)
        tlx = int(self.width() / 2 - R_out)
        tly = int(self.height() / 2 - R_out)
        rect = QRectF(tlx, tly, side, side)
        th = self.thickness()
        pen_bg = QPen(QColor(180, 180, 180, self.alpha_bg), th,
                      Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
        qp.setPen(pen_bg); qp.drawArc(rect, 90 * 16, -360 * 16)
        ratio = self._ratio
        if ratio > 0.0:
            pen_fg = QPen(QColor(60, 145, 255, self.alpha_fg), th,
                          Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
            qp.setPen(pen_fg)
            span = -int(ratio * 360.0 * 16.0)
            qp.drawArc(rect, 90 * 16, span)
        qp.end()
    def set_ratio(self, r: float) -> None:
        r = 0.0 if not (r >= 0.0) else (1.0 if r > 1.0 else r)
        if abs(self._ratio - r) > 1e-4:
            self._ratio = r
            self.update()


class _VideoEventFilter(QObject):
    def __init__(self, host: "MediaRenderingMixin"):
        super().__init__(host if isinstance(host, QObject) else None)
        self._host = host
    def eventFilter(self, obj: QObject, ev: QEvent) -> bool:
        if obj is self._host.video_w:
            et = ev.type()
            if et == QEvent.Type.Resize and self._host._circle_overlay:
                self._host._circle_overlay.setGeometry(self._host.video_w.rect())  # type: ignore[arg-type]
                return False
            if et == QEvent.Type.MouseButtonPress:
                # Toggle play/pause on click inside video widget.
                self._host._toggle_play(circular=bool(getattr(self._host, "_video_is_circular", False)))
                return True
        return False


class MediaRenderingMixin:
    chat_id: Optional[str]
    msg_id: Optional[int]
    kind: Optional[str]
    file_path: Optional[str]
    thumb_path: Optional[str]
    server: Any

    lbl_img: Optional[QLabel]
    lbl_anim: Optional[QLabel]
    preview: Optional[_ClickLabel]
    video_w: Optional[QWidget]
    player: Optional[QMediaPlayer]
    _audio_output: Optional[QAudioOutput]
    _container_layout: Optional[QVBoxLayout]
    _thumb_cb: Optional[Callable[[str], None]]
    _video_is_circular: bool
    _frame_size: Optional[QSize]

    _controls_bar: Optional[QHBoxLayout]
    _time_lbl: Optional[QLabel]
    _seek: Optional[QSlider]
    _slider_dragging: bool
    _circle_overlay: Optional[_CircleSeekOverlay]

    _video_sink: Optional[QVideoSink]
    _circle_img_label: Optional[_VideoCircleLabel]
    _evt_filter: Optional[_VideoEventFilter]

    # FIX: состояние одноразового превью
    _thumb_player: Optional[QMediaPlayer]
    _thumb_sink: Optional[QVideoSink]
    _thumb_audio: Optional[QAudioOutput]
    _thumb_started: bool
    _thumb_finished: bool
    on_media_activate: Optional[Callable[[dict], bool]]

    FRAME_LANDSCAPE = QSize(520, 292)
    FRAME_PORTRAIT  = QSize(360, 520)
    MAX_IMAGE_LANDSCAPE = QSize(450, 350)
    MAX_IMAGE_PORTRAIT  = QSize(350, 450)
    MIN_IMAGE_DIM = 32
    IMAGE_PLACEHOLDER = QSize(320, 220)
    VIDEO_NOTE_SIZE = 300

    # -------------------- размеры/масштаб --------------------
    @staticmethod
    def _enum_name(value: Any) -> str:
        """Return human-readable enum/int label for logging."""
        try:
            return str(value.name)  # type: ignore[attr-defined]
        except Exception:
            try:
                return str(int(value))
            except Exception:
                return str(value)

    def _media_debug_context(self, *, source: str) -> str:
        """Build short context string for media-player diagnostics."""
        chat = getattr(self, "chat_id", None) or "-"
        msg = getattr(self, "msg_id", None) or "-"
        kind = getattr(self, "kind", None) or "-"
        file_path = getattr(self, "file_path", None) or getattr(self, "thumb_path", None) or "-"
        return f"{source} chat={chat} msg={msg} kind={kind} file={file_path}"

    def _on_player_error(self, error: QMediaPlayer.Error, error_str: str) -> None:
        context = self._media_debug_context(source="player")
        log.warning(
            "[GUI] QMediaPlayer error %s: %s (%s)",
            self._enum_name(error),
            error_str or "no message",
            context,
        )
        preview = getattr(self, "preview", None)
        if preview and hasattr(preview, "setText"):
            try:
                preview.setText("Ошибка воспроизведения")
            except Exception:
                pass
        # Audio messages have no "preview" label; show a minimal hint if possible.
        status = getattr(self, "status_label", None)
        if status is not None and hasattr(status, "setText"):
            try:
                status.setText(error_str or "Ошибка воспроизведения")
                if hasattr(status, "show"):
                    status.show()
            except Exception:
                pass

    def _on_media_status_changed(self, status: QMediaPlayer.MediaStatus) -> None:
        warn_statuses = {
            getattr(QMediaPlayer.MediaStatus, "InvalidMedia", None),
            getattr(QMediaPlayer.MediaStatus, "NoMedia", None),
        }
        unknown_status = getattr(QMediaPlayer.MediaStatus, "UnknownMediaStatus", None)
        if unknown_status is not None:
            warn_statuses.add(unknown_status)
        if status in warn_statuses:
            log.warning(
                "[GUI] QMediaPlayer status=%s (%s)",
                self._enum_name(status),
                self._media_debug_context(source="player"),
            )

    def _on_thumb_error(self, error: QMediaPlayer.Error, error_str: str) -> None:
        log.warning(
            "[GUI] Thumb player error %s: %s (%s)",
            self._enum_name(error),
            error_str or "no message",
            self._media_debug_context(source="thumb"),
        )
        preview = getattr(self, "preview", None)
        if preview and hasattr(preview, "setText") and not self._thumb_finished:
            try:
                preview.setText("Не удалось подготовить превью")
            except Exception:
                pass

    def _on_thumb_status(self, status: QMediaPlayer.MediaStatus) -> None:
        if status == QMediaPlayer.MediaStatus.InvalidMedia:
            log.warning(
                "[GUI] Thumb player status=%s (%s)",
                self._enum_name(status),
                self._media_debug_context(source="thumb"),
            )
    @staticmethod
    def _is_vertical(w: int, h: int) -> bool:
        return h > w
    @classmethod
    def _frame_for_size(cls, w: int, h: int) -> QSize:
        if w <= 0 or h <= 0:
            return cls.FRAME_LANDSCAPE
        return cls.FRAME_PORTRAIT if cls._is_vertical(w, h) else cls.FRAME_LANDSCAPE
    def _target_image_size(self, width: int, height: int) -> QSize:
        if width <= 0 or height <= 0:
            return QSize(self.MIN_IMAGE_DIM, self.MIN_IMAGE_DIM)

        scale = 1.0
        if width < self.MIN_IMAGE_DIM or height < self.MIN_IMAGE_DIM:
            scale = max(self.MIN_IMAGE_DIM / max(width, 1), self.MIN_IMAGE_DIM / max(height, 1))

        limit = self.MAX_IMAGE_LANDSCAPE if width >= height else self.MAX_IMAGE_PORTRAIT
        limit_w, limit_h = limit.width(), limit.height()
        if width * scale > limit_w or height * scale > limit_h:
            scale = min(limit_w / width, limit_h / height)

        target_w = max(int(round(width * scale)), self.MIN_IMAGE_DIM)
        target_h = max(int(round(height * scale)), self.MIN_IMAGE_DIM)
        return QSize(target_w, target_h)

    @staticmethod
    def _fit_in_frame(orig_w: int, orig_h: int, frame: QSize) -> QSize:
        if orig_w <= 0 or orig_h <= 0:
            return frame
        fw, fh = frame.width(), frame.height()
        k = min(fw / float(orig_w), fh / float(orig_h))
        return QSize(max(1, int(orig_w * k)), max(1, int(orig_h * k)))
    def _apply_pix_to_label(self, lbl: QLabel, pix: QPixmap) -> None:
        target = self._target_image_size(pix.width(), pix.height())
        scaled = pix.scaled(
            target,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        rounded = QPixmap(target)
        rounded.fill(Qt.GlobalColor.transparent)
        painter = QPainter(rounded)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        path = QPainterPath()
        path.addRoundedRect(QRectF(0, 0, float(target.width()), float(target.height())), 12.0, 12.0)
        painter.setClipPath(path)
        x = (target.width() - scaled.width()) // 2
        y = (target.height() - scaled.height()) // 2
        painter.drawPixmap(x, y, scaled)
        painter.end()
        lbl.setFixedSize(target)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl.setText("")
        lbl.setPixmap(rounded)
        self._frame_size = target

    def _request_media_activate(self, *, kind: Optional[str] = None, path: Optional[str] = None) -> bool:
        handler = getattr(self, "on_media_activate", None)
        if not callable(handler):
            return False
        candidate = str(path or "").strip()
        file_path = str(getattr(self, "file_path", "") or "").strip()
        thumb_path = str(getattr(self, "thumb_path", "") or "").strip()
        if not candidate:
            if file_path and os.path.isfile(file_path):
                candidate = file_path
            elif thumb_path and os.path.isfile(thumb_path):
                candidate = thumb_path
        payload = {
            "kind": str(kind or getattr(self, "kind", "") or "").lower(),
            "path": candidate,
            "file_path": file_path,
            "thumb_path": thumb_path,
            "chat_id": str(getattr(self, "chat_id", "") or ""),
            "msg_id": int(getattr(self, "msg_id", 0) or 0),
        }
        try:
            return bool(handler(payload))
        except Exception:
            return False

    def _on_image_surface_clicked(self, _pos: QPointF, *, kind: str) -> None:
        path = str(getattr(self, "file_path", "") or "").strip()
        if not path or not os.path.isfile(path):
            path = str(getattr(self, "thumb_path", "") or "").strip()
        self._request_media_activate(kind=kind, path=path)

    # -------------------- рендер контента --------------------
    def _render_image(self, lay: QVBoxLayout) -> None:
        self._container_layout = lay
        self.lbl_img = _ClickLabel("Файл не загружен", parent=cast(QWidget, self))
        self.lbl_img.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_img.setCursor(Qt.CursorShape.PointingHandCursor)
        cast(Any, self.lbl_img.clicked).connect(lambda p: self._on_image_surface_clicked(p, kind="image"))
        placeholder = QSize(self.IMAGE_PLACEHOLDER)
        self.lbl_img.setFixedSize(placeholder)
        self._frame_size = placeholder
        lay.addWidget(self.lbl_img, 0, Qt.AlignmentFlag.AlignHCenter)
        if self.file_path and os.path.isfile(self.file_path):
            self._set_pix(self.lbl_img, self.file_path)
        elif self.thumb_path and os.path.isfile(self.thumb_path):
            self._set_pix(self.lbl_img, self.thumb_path)
        elif self.server and self.chat_id and self.msg_id is not None:
            self._start_thumb(lambda p: self._set_pix(self.lbl_img, p))

    def _render_animation(self, lay: QVBoxLayout) -> None:
        self._container_layout = lay
        self.lbl_anim = _ClickLabel("Файл не загружен", parent=cast(QWidget, self))
        self.lbl_anim.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_anim.setCursor(Qt.CursorShape.PointingHandCursor)
        cast(Any, self.lbl_anim.clicked).connect(lambda p: self._on_image_surface_clicked(p, kind="animation"))
        placeholder = QSize(self.IMAGE_PLACEHOLDER)
        self.lbl_anim.setFixedSize(placeholder)
        self._frame_size = placeholder
        lay.addWidget(self.lbl_anim, 0, Qt.AlignmentFlag.AlignHCenter)
        if self.file_path and os.path.isfile(self.file_path):
            self._show_animation(self.file_path)
        elif self.thumb_path and os.path.isfile(self.thumb_path):
            self._set_pix(self.lbl_anim, self.thumb_path)

    def _render_video(self, lay: QVBoxLayout, *, circular: bool = False) -> None:
        self._container_layout = lay
        self.preview = _ClickLabel("Медиа не загружено", parent=cast(QWidget, self))
        self.preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.preview.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        style_mgr = StyleManager.instance()
        if circular:
            size = self.VIDEO_NOTE_SIZE
            self.preview.setFixedSize(size, size)
            css = style_mgr.stylesheet("media.preview_circular", {"radius": size // 2})
            if not css:
                css = f"border-radius:{size // 2}px; background-color:#1f1f1f; color:#fff;"
            self.preview.setStyleSheet(css)
            self.preview.setMask(QRegion(0, 0, size, size, QRegion.RegionType.Ellipse))
            cast(Any, self.preview.clicked).connect(lambda p: self._on_preview_clicked(p, True))
        else:
            self.preview.setFixedSize(self.FRAME_LANDSCAPE)
            cast(Any, self.preview.clicked).connect(lambda _p: self._on_preview_clicked(_p, False))
        lay.addWidget(self.preview, 0, Qt.AlignmentFlag.AlignHCenter)

        self._controls_bar = None
        self._time_lbl = None
        self._seek = None
        if not circular:
            # Keep timeline controls over the preview surface (Telegram-like),
            # so geometry stays stable and does not jump when opening media.
            overlay = QWidget(self.preview)
            overlay.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
            overlay.setStyleSheet("background-color: rgba(10, 18, 26, 150); border-radius: 8px;")
            overlay_layout = QHBoxLayout(overlay)
            overlay_layout.setContentsMargins(8, 2, 8, 2)
            overlay_layout.setSpacing(6)
            self._seek = QSlider(Qt.Orientation.Horizontal, parent=cast(QWidget, self))
            self._seek.setRange(0, 0)
            cast(Any, self._seek.sliderPressed).connect(self._on_slider_pressed)
            cast(Any, self._seek.sliderReleased).connect(self._on_slider_released)
            cast(Any, self._seek.sliderMoved).connect(self._on_slider_moved)
            self._seek.setEnabled(False)
            self._time_lbl = QLabel("0:00 / 0:00", parent=cast(QWidget, self))
            label_css = style_mgr.stylesheet("media.time_label")
            self._time_lbl.setStyleSheet(label_css or "color:#9fa6b1; font-size:11px;")
            overlay_layout.addWidget(self._seek, 1)
            overlay_layout.addWidget(self._time_lbl, 0)
            holder = QVBoxLayout(self.preview)
            holder.setContentsMargins(8, 8, 8, 8)
            holder.setSpacing(0)
            holder.addWidget(overlay, 0, Qt.AlignmentFlag.AlignTop)
            holder.addStretch(1)
            self._controls_bar = overlay_layout

        # FIX: сброс флагов превью
        self._thumb_player = None
        self._thumb_sink = None
        self._thumb_audio = None
        self._thumb_started = False
        self._thumb_finished = False

        if self.file_path and os.path.isfile(self.file_path):
            self.preview.setText("Готово: нажмите, чтобы открыть" if not circular else "Готово: нажмите по центру")
            # Планируем генерацию превью после отрисовки виджета, чтобы не блокировать UI на создании элемента.
            delay_ms = 220 if bool(getattr(self, "_loading_history", False)) else 0
            QTimer.singleShot(delay_ms, lambda c=circular: self._spawn_local_video_thumb(circular=c))
        else:
            self.preview.setText("Медиа не загружено")

        self._video_is_circular = circular
        if not circular:
            if self.thumb_path and os.path.isfile(self.thumb_path):
                self._apply_preview_pix(self.thumb_path, circular=False)
            elif self.server and self.chat_id and self.msg_id is not None:
                self._start_thumb(lambda p: self._apply_preview_pix(p, circular=False))

        self.video_w = None
        self.player = None
        self._audio_output = None
        self._frame_size = None
        self._slider_dragging = False
        self._circle_overlay = None
        self._video_sink = None
        self._circle_img_label = None
        self._evt_filter = None

    def _render_video_note(self, lay: QVBoxLayout) -> None:
        self._render_video(lay, circular=True)

    # -------------------- воспроизведение --------------------
    def _ensure_player(self) -> Optional[QMediaPlayer]:
        if not HAVE_QTMULTIMEDIA:
            return None
        if not self.player:
            self.player = QMediaPlayer(parent=cast(QWidget, self))
            self._audio_output = QAudioOutput(parent=cast(QWidget, self))
            try:
                self._audio_output.setVolume(1.0)
            except Exception:
                pass
            self.player.setAudioOutput(self._audio_output)
            cast(Any, self.player.durationChanged).connect(self._on_duration_changed)
            cast(Any, self.player.positionChanged).connect(self._on_position_changed)
            cast(Any, self.player.playbackStateChanged).connect(self._on_state_changed)
            cast(Any, self.player.errorOccurred).connect(self._on_player_error)
            cast(Any, self.player.mediaStatusChanged).connect(self._on_media_status_changed)
            MediaPlaybackCoordinator.register(self.player)
        return self.player

    def _ensure_video_widget(self, circular: bool) -> Optional[QWidget]:
        if not HAVE_QTMULTIMEDIA:
            return None
        recreate = False
        if not self.video_w:
            recreate = True
        elif getattr(self, "_video_is_circular", False) != circular:
            self.video_w.setParent(None); self.video_w.deleteLater(); self.video_w = None; recreate = True
        if recreate:
            if self._circle_overlay:
                try:
                    self._circle_overlay.setParent(None)
                    self._circle_overlay.deleteLater()
                except Exception:
                    pass
                self._circle_overlay = None
            if circular:
                size = self.VIDEO_NOTE_SIZE
                # For circles use QVideoSink -> QLabel rendering path.
                # This avoids platform-specific blank video output with masked QVideoWidget.
                self.video_w = _VideoCircleLabel(size, parent=cast(QWidget, self))
                self._circle_img_label = cast(_VideoCircleLabel, self.video_w)
                try:
                    self.video_w.setStyleSheet("background:#111;")
                except Exception:
                    pass
                if self._container_layout:
                    self._container_layout.addWidget(self.video_w, 0, Qt.AlignmentFlag.AlignHCenter)
                try:
                    self._circle_overlay = _CircleSeekOverlay(self, cast(QWidget, self.video_w))
                    self._circle_overlay.setGeometry(cast(QWidget, self.video_w).rect())
                    self._circle_overlay.raise_()
                    self._circle_overlay.show()
                except Exception:
                    self._circle_overlay = None
            else:
                self._circle_img_label = None
                self.video_w = QVideoWidget(parent=cast(QWidget, self))
                frame = self._frame_size or self.FRAME_LANDSCAPE
                self.video_w.setFixedSize(frame)
                if self._container_layout:
                    self._container_layout.addWidget(self.video_w, 0, Qt.AlignmentFlag.AlignHCenter)
                if self._evt_filter is None:
                    self._evt_filter = _VideoEventFilter(self)
                self.video_w.installEventFilter(self._evt_filter)
        elif circular:
            if isinstance(self.video_w, _VideoCircleLabel):
                self._circle_img_label = self.video_w
            if self._circle_overlay:
                self._circle_overlay.setGeometry(cast(QWidget, self.video_w).rect())
                self._circle_overlay.raise_()
        self._video_is_circular = circular
        return self.video_w

    def _ensure_video_sink(self) -> Optional[QVideoSink]:
        if not HAVE_QTMULTIMEDIA:
            return None
        if not self._video_sink:
            self._video_sink = QVideoSink(parent=cast(QWidget, self))
            cast(Any, self._video_sink.videoFrameChanged).connect(self._on_video_frame)
        return self._video_sink

    def _toggle_play(self, *, circular: bool = False) -> None:
        if not self.file_path or not os.path.isfile(self.file_path):
            if self.preview: self.preview.setText("Файл ещё не загружен")
            return
        player = self._ensure_player()
        vw = self._ensure_video_widget(circular)
        if not player or not vw:
            if self.preview: self.preview.setText("QtMultimedia недоступен")
            return
        if circular:
            sink = self._ensure_video_sink()
            if not sink:
                if self.preview:
                    self.preview.setText("QtMultimedia недоступен")
                return
            player.setVideoOutput(sink)
        else:
            player.setVideoOutput(cast(QVideoWidget, vw))
        if player.source().isEmpty():
            player.setSource(QUrl.fromLocalFile(self.file_path))
        if self.preview and self.preview.isVisible():
            self.preview.hide()
            seek = getattr(self, "_seek", None)
            if seek:
                seek.setEnabled(True)
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

    # ------- кадры круглого видео -------
    def _on_video_frame(self, frame: QVideoFrame) -> None:
        if not self._circle_img_label:
            return
        # Throttle CPU-heavy frame -> QImage conversions. Without it, playing multiple
        # circular videos (or just a high-FPS clip) can stall the UI on some machines.
        try:
            now = time.monotonic()
            last = float(getattr(self, "_circle_last_frame_ts", 0.0) or 0.0)
            if now - last < 0.05:  # ~20 FPS cap
                return
            setattr(self, "_circle_last_frame_ts", now)
        except Exception:
            pass
        try:
            img = frame.toImage()
        except Exception:
            return
        if img.isNull():
            return
        # Defensive copy: QVideoFrame-backed images can reference ephemeral buffers.
        try:
            img = img.copy()
        except Exception:
            pass
        self._circle_img_label.set_frame(img)

    # ------- сигналы -------
    def _on_duration_changed(self, dur: int) -> None:
        seek = getattr(self, "_seek", None)
        if seek:
            seek.setRange(0, max(0, int(dur)))
        time_lbl = getattr(self, "_time_lbl", None)
        if time_lbl:
            total = dur if dur > 0 else getattr(self, "_voice_duration_ms", None) or 0
            time_lbl.setText(f"{_fmt_time(0)} / {_fmt_time(total)}")
        circle_overlay = getattr(self, "_circle_overlay", None)
        if circle_overlay:
            circle_overlay.set_ratio(0.0)
    def _on_position_changed(self, pos: int) -> None:
        dur = self.player.duration() if self.player else 0
        if dur <= 0:
            dur = getattr(self, "_voice_duration_ms", None) or 0
        time_lbl = getattr(self, "_time_lbl", None)
        if time_lbl:
            time_lbl.setText(f"{_fmt_time(pos)} / {_fmt_time(dur)}")
        seek = getattr(self, "_seek", None)
        if seek and not getattr(self, "_slider_dragging", False):
            seek.blockSignals(True); seek.setValue(int(pos)); seek.blockSignals(False)
        circle_overlay = getattr(self, "_circle_overlay", None)
        if circle_overlay and dur > 0:
            circle_overlay.set_ratio(max(0.0, min(1.0, pos / float(dur))))
        if getattr(self, "_voice_wave_widget", None) and dur > 0:
            try:
                ratio = max(0.0, min(1.0, pos / float(dur)))
                self._voice_wave_widget.set_progress(ratio)  # type: ignore[attr-defined]
            except Exception:
                pass
    def _on_state_changed(self, _st: QMediaPlayer.PlaybackState) -> None:
        pass

    # ------- слайдер -------
    def _on_slider_pressed(self) -> None:
        self._slider_dragging = True
    def _on_slider_released(self) -> None:
        seek = getattr(self, "_seek", None)
        if seek and self.player:
            self.player.setPosition(int(seek.value()))
        self._slider_dragging = False
    def _on_slider_moved(self, v: int) -> None:
        time_lbl = getattr(self, "_time_lbl", None)
        if time_lbl and self.player:
            time_lbl.setText(f"{_fmt_time(v)} / {_fmt_time(self.player.duration())}")

    # -------------------- превью/картинки --------------------
    def _start_thumb(self, on_done: Callable[[str], None]) -> None:
        if not (self.server and self.chat_id and self.msg_id is not None):
            return
        self._thumb_cb = on_done
        th = QThread()
        worker = ThumbWorker(self.server, str(self.chat_id), int(self.msg_id))
        worker.moveToThread(th)
        cast(Any, worker.done).connect(th.quit)
        cast(Any, th.finished).connect(worker.deleteLater)
        cast(Any, th.finished).connect(th.deleteLater)
        cast(Any, worker.done).connect(self._on_thumb_done)
        if hasattr(self, "_bg_threads"):
            try: self._bg_threads.append(th)
            except Exception: pass
            cast(Any, th.finished).connect(
                lambda: self._bg_threads.remove(th)
                if hasattr(self, "_bg_threads") and th in getattr(self, "_bg_threads", []) else None
            )
        cast(Any, th.started).connect(worker.run)
        th.start()

    def _on_thumb_done(self, _msg_id: int, path: str) -> None:
        cb = self._thumb_cb; self._thumb_cb = None
        if callable(cb) and path:
            try: cb(path)
            except Exception: pass

    def _apply_preview_pix(self, path: str, *, circular: bool = False) -> None:
        if not path or not os.path.isfile(path) or not self.preview:
            return
        pix = QPixmap(path)
        if circular:
            size = self.VIDEO_NOTE_SIZE
            pm = pix.scaled(size, size,
                            Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                            Qt.TransformationMode.SmoothTransformation)
            self.preview.setPixmap(pm)
            self.preview.setFixedSize(size, size)
            self.preview.setMask(QRegion(0, 0, size, size, QRegion.RegionType.Ellipse))
            return
        self._apply_pix_to_label(self.preview, pix)

    # FIX: лёгкий одноразовый генератор превью с ограничением конкурентности
    def _spawn_local_video_thumb(self, *, circular: bool) -> None:
        if not (self.file_path and os.path.isfile(self.file_path) and HAVE_QTMULTIMEDIA):
            return
        if self._thumb_finished:
            return
        if self.thumb_path and os.path.isfile(self.thumb_path):
            self._thumb_finished = True
            self._apply_preview_pix(self.thumb_path, circular=circular)
            return
        if self._thumb_started:
            return
        # глобальный лимит
        if _ThumbLimiter.active >= _ThumbLimiter.max_active:
            QTimer.singleShot(_ThumbLimiter.backoff_ms, lambda: self._spawn_local_video_thumb(circular=circular))
            return

        # старт
        self._thumb_started = True
        _ThumbLimiter.active += 1

        try:
            # без аудио: только QVideoSink (получаем кадр)  :contentReference[oaicite:5]{index=5}
            self._thumb_player = QMediaPlayer(cast(QWidget, self))
            self._thumb_sink = QVideoSink(cast(QWidget, self))
            # ВАЖНО: не подключаем QAudioOutput — декодер тише и легче
            self._thumb_player.setVideoOutput(self._thumb_sink)
            cast(Any, self._thumb_player.errorOccurred).connect(self._on_thumb_error)
            cast(Any, self._thumb_player.mediaStatusChanged).connect(self._on_thumb_status)

            handled = {"done": False}

            def _on_frame(frame: QVideoFrame) -> None:
                if handled["done"]:
                    return
                try:
                    img: QImage = frame.toImage()
                except Exception:
                    return
                if img.isNull():
                    return
                try:
                    img = img.copy()
                except Exception:
                    pass
                handled["done"] = True
                # сохраняем рядом
                dst = self.file_path + ".thumb.jpg"
                img.save(dst, "JPG", 80)
                self.thumb_path = dst
                self._thumb_finished = True
                self._apply_preview_pix(dst, circular=circular)

                # мягкая остановка и очистка
                try:
                    self._thumb_player.stop()
                except Exception:
                    pass
                # Не вызываем disconnect() — PySide может ругаться в лог,
                # удаление объекта корректно отцепляет слоты  :contentReference[oaicite:6]{index=6}
                for obj in (self._thumb_player, self._thumb_sink):
                    try:
                        obj.deleteLater()
                    except Exception:
                        pass
                self._thumb_player = None
                self._thumb_sink = None
                _ThumbLimiter.active = max(0, _ThumbLimiter.active - 1)

            # подключаем обработчик кадра
            cast(Any, self._thumb_sink.videoFrameChanged).connect(_on_frame)
            self._thumb_player.setSource(QUrl.fromLocalFile(self.file_path))
            self._thumb_player.play()

            # страховка: если за 2 секунды кадр так и не пришёл — отменяем
            def _abort_if_needed() -> None:
                if not self._thumb_finished:
                    try:
                        if self._thumb_player:
                            self._thumb_player.stop()
                    except Exception:
                        pass
                    for obj in (self._thumb_player, self._thumb_sink):
                        try:
                            if obj: obj.deleteLater()
                        except Exception:
                            pass
                    self._thumb_player = None
                    self._thumb_sink = None
                    _ThumbLimiter.active = max(0, _ThumbLimiter.active - 1)
            QTimer.singleShot(2000, _abort_if_needed)

        except Exception:
            # аварийная развязка лимитера
            _ThumbLimiter.active = max(0, _ThumbLimiter.active - 1)

    def _show_animation(self, path: str) -> None:
        if not self.lbl_anim:
            return
        if not path:
            self.lbl_anim.setText("Файл недоступен"); return
        if path.lower().endswith(".gif"):
            movie = QMovie(path)
            sz = movie.frameRect().size()
            if sz.isValid():
                target = self._target_image_size(sz.width(), sz.height())
                movie.setScaledSize(target)
                self.lbl_anim.setFixedSize(target)
            else:
                self.lbl_anim.setFixedSize(self.MAX_IMAGE_LANDSCAPE)
            self.lbl_anim.setMovie(movie)
            movie.start()
        else:
            self._set_pix(self.lbl_anim, path)

    # -------------------- коллбэки загрузчика --------------------
    def show_downloaded_media(self, kind: str, path: str) -> None:
        self.file_path = path
        k = (kind or "").lower()
        if k in {"image", "photo"} and self.lbl_img:
            self._set_pix(self.lbl_img, path)
        elif k in {"animation", "gif"}:
            self._show_animation(path)
        elif k == "video":
            if os.path.isfile(path):
                self._spawn_local_video_thumb(circular=False)
            if self.preview:
                self.preview.setText("Готово: нажмите, чтобы открыть")
        elif k == "video_note":
            if os.path.isfile(path):
                self._spawn_local_video_thumb(circular=True)
            if self.preview:
                self.preview.setText("Готово: нажмите по центру")

    def show_download_error(self, message: str) -> None:
        if self.preview: self.preview.setText(message)
        if self.lbl_img: self.lbl_img.setText(message)
        if self.lbl_anim: self.lbl_anim.setText(message)

    def _set_pix(self, lbl: Optional[QLabel], path: str) -> None:
        if not lbl:
            return
        if not path or not os.path.isfile(path):
            lbl.setText("Файл не найден"); return
        pix = QPixmap(path)
        self._apply_pix_to_label(lbl, pix)

    # -------------------- клик по превью --------------------
    def _on_preview_clicked(self, pos: QPointF, circular: bool) -> None:
        if circular:
            size = self.VIDEO_NOTE_SIZE
            c = QPointF(self.preview.width() / 2.0, self.preview.height() / 2.0)  # type: ignore[union-attr]
            margin = 10
            thickness = int(round(8 * 0.7))
            R_out = size / 2.0 - margin
            R_in = R_out - thickness
            r = ((pos.x() - c.x()) ** 2 + (pos.y() - c.y()) ** 2) ** 0.5
            if r < R_in:
                self._toggle_play(circular=True)
        else:
            path = str(getattr(self, "file_path", "") or "").strip()
            if path and os.path.isfile(path):
                if self._request_media_activate(kind="video", path=path):
                    return
            self._toggle_play(circular=False)

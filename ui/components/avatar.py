from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QRectF
from PySide6.QtGui import QColor, QFont, QPainter, QPainterPath, QPen, QPixmap
from PySide6.QtWidgets import QWidget


def make_avatar_pixmap(size: int, image_path: Optional[str], initials: str, *, background: QColor) -> QPixmap:
    """Return a circular pixmap using the provided image or fallback initials."""
    size = max(16, size)
    canvas = QPixmap(size, size)
    canvas.fill(Qt.GlobalColor.transparent)

    painter = QPainter(canvas)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    rect = QRectF(0, 0, float(size), float(size))

    if image_path:
        path = Path(image_path)
    else:
        path = None

    if path and path.is_file():
        src = QPixmap(str(path))
        if not src.isNull():
            painter.setClipPath(_circle_path(rect))
            scaled = src.scaled(
                size,
                size,
                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                Qt.TransformationMode.SmoothTransformation,
            )
            x = (size - scaled.width()) // 2
            y = (size - scaled.height()) // 2
            painter.drawPixmap(x, y, scaled)
            painter.end()
            return canvas

    painter.setClipPath(_circle_path(rect))
    painter.fillRect(rect, background)

    # Draw initials
    text = (initials or "?").strip()
    if len(text) > 2:
        text = text[:2]
    text = text.upper()

    font = QFont()
    font.setBold(True)
    font.setPointSize(int(math.floor(size * 0.42)))
    painter.setFont(font)
    painter.setPen(QPen(QColor(0xF7, 0xF9, 0xFC)))
    painter.drawText(canvas.rect(), Qt.AlignmentFlag.AlignCenter, text or "?")
    painter.end()
    return canvas


class AvatarWidget(QWidget):
    """Simple circular avatar widget that paints the provided pixmap."""

    def __init__(self, size: int = 40, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._size = max(16, size)
        self._pixmap: Optional[QPixmap] = None
        self.setFixedSize(self._size, self._size)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)

    def set_pixmap(self, pixmap: QPixmap) -> None:
        self._pixmap = pixmap
        self.update()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        rect = QRectF(0, 0, float(self._size), float(self._size))
        painter.setClipPath(_circle_path(rect))
        painter.fillRect(rect, QColor(0x21, 0x29, 0x36))
        if self._pixmap and not self._pixmap.isNull():
            scaled = self._pixmap.scaled(
                self._size,
                self._size,
                Qt.AspectRatioMode.KeepAspectRatioByExpanding,
                Qt.TransformationMode.SmoothTransformation,
            )
            x = (self._size - scaled.width()) // 2
            y = (self._size - scaled.height()) // 2
            painter.drawPixmap(x, y, scaled)
        painter.end()


def _circle_path(rect: QRectF) -> QPainterPath:
    path = QPainterPath()
    path.addEllipse(rect)
    return path

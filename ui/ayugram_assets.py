from __future__ import annotations

from pathlib import Path
from typing import Optional

from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QColor, QIcon, QImage, QPainter, QPixmap


AYUGRAM_MAIN_ICON_DIR = Path(__file__).with_name("assets") / "icons" / "ayugram_main"


def ayugram_icon_path(name: str) -> Path:
    return AYUGRAM_MAIN_ICON_DIR / str(name or "").strip()


def _to_color(value: Optional[QColor | str]) -> Optional[QColor]:
    if value is None:
        return None
    if isinstance(value, QColor):
        return value if value.isValid() else None
    color = QColor(str(value))
    return color if color.isValid() else None


def _tint_pixmap(source: QPixmap, color: QColor) -> QPixmap:
    if source.isNull() or not color.isValid():
        return source
    fmt = getattr(QImage.Format, "Format_ARGB32", getattr(QImage, "Format_ARGB32", None))
    image = source.toImage().convertToFormat(fmt)
    ptr = image.bits()
    if ptr is None:
        return source
    width = image.width()
    height = image.height()
    nbytes = width * height * 4
    if hasattr(ptr, "setsize"):
        ptr.setsize(nbytes)
    buf = bytearray(ptr[:nbytes])
    red, green, blue = color.red(), color.green(), color.blue()
    for index in range(0, nbytes, 4):
        alpha = buf[index + 3]
        if alpha <= 0:
            continue
        buf[index] = blue
        buf[index + 1] = green
        buf[index + 2] = red
    ptr2 = image.bits()
    if ptr2 is not None:
        if hasattr(ptr2, "setsize"):
            ptr2.setsize(nbytes)
        ptr2[:nbytes] = buf
    return QPixmap.fromImage(image)


def load_ayugram_pixmap(
    name: str,
    *,
    tint: Optional[QColor | str] = None,
    size: Optional[int | QSize] = None,
) -> QPixmap:
    path = ayugram_icon_path(name)
    pixmap = QPixmap(str(path)) if path.exists() else QPixmap()
    color = _to_color(tint)
    if color is not None and not pixmap.isNull():
        pixmap = _tint_pixmap(pixmap, color)
    if pixmap.isNull() or size is None:
        return pixmap
    if isinstance(size, QSize):
        target = size
    else:
        side = max(1, int(size))
        target = QSize(side, side)
    return pixmap.scaled(
        target,
        Qt.AspectRatioMode.KeepAspectRatio,
        Qt.TransformationMode.SmoothTransformation,
    )


def load_ayugram_icon(
    name: str,
    *,
    tint: Optional[QColor | str] = None,
    size: Optional[int | QSize] = None,
) -> QIcon:
    pixmap = load_ayugram_pixmap(name, tint=tint, size=size)
    if pixmap.isNull():
        return QIcon()
    return QIcon(pixmap)


def circular_icon_pixmap(
    name: str,
    *,
    tint: Optional[QColor | str] = None,
    background: Optional[QColor | str] = None,
    box_size: int = 40,
    icon_size: int = 20,
) -> QPixmap:
    box = max(16, int(box_size))
    icon = load_ayugram_pixmap(name, tint=tint, size=icon_size)
    result = QPixmap(box, box)
    result.fill(Qt.GlobalColor.transparent)
    painter = QPainter(result)
    try:
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        bg = _to_color(background)
        if bg is not None:
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(bg)
            painter.drawEllipse(0, 0, box, box)
        if not icon.isNull():
            x = (box - icon.width()) // 2
            y = (box - icon.height()) // 2
            painter.drawPixmap(x, y, icon)
    finally:
        painter.end()
    return result

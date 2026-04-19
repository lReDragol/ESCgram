from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional

from PySide6.QtCore import QPoint, QSize, Qt, Signal
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QGraphicsDropShadowEffect,
    QPushButton,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from ui.ayugram_assets import load_icon_from_dir


AYUGRAM_MENU_ICON_DIR = Path(__file__).with_name("assets") / "icons" / "ayugram_menu"


class AyuPopupMenu(QFrame):
    closed = Signal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(
            parent,
            Qt.WindowType.Popup | Qt.WindowType.FramelessWindowHint | Qt.WindowType.NoDropShadowWindowHint,
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setObjectName("ayuPopupMenu")

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        self._panel = QFrame(self)
        self._panel.setObjectName("ayuPopupMenuPanel")
        self._panel.setStyleSheet(
            "QFrame#ayuPopupMenuPanel{background-color:#182533;border:1px solid rgba(255,255,255,0.08);border-radius:14px;}"
        )
        try:
            shadow = QGraphicsDropShadowEffect(self)
            shadow.setBlurRadius(26)
            shadow.setOffset(0, 10)
            shadow.setColor(QColor(0, 0, 0, 110))
            self._panel.setGraphicsEffect(shadow)
        except Exception:
            pass

        self._layout = QVBoxLayout(self._panel)
        self._layout.setContentsMargins(8, 8, 8, 8)
        self._layout.setSpacing(0)
        root.addWidget(self._panel)

    def add_action(
        self,
        text: str,
        callback: Callable[[], None],
        *,
        icon_name: str = "",
        destructive: bool = False,
    ) -> QPushButton:
        color = "#ff8f8f" if destructive else "#edf5ff"
        icon_tint = "#ff8f8f" if destructive else "#8ea5bf"
        hover = "rgba(255,96,96,0.14)" if destructive else "rgba(122,184,255,0.12)"
        pressed = "rgba(255,96,96,0.22)" if destructive else "rgba(122,184,255,0.18)"

        button = QPushButton(str(text or ""), self._panel)
        button.setCursor(Qt.CursorShape.PointingHandCursor)
        button.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        button.setFixedHeight(48)
        button.setIcon(load_icon_from_dir(AYUGRAM_MENU_ICON_DIR, icon_name, tint=icon_tint, size=22))
        button.setIconSize(QSize(22, 22))
        button.setStyleSheet(
            "QPushButton{background:transparent;border:none;border-radius:6px;"
            "padding:0 18px;text-align:left;color:"
            + color
            + ";font-size:16px;font-weight:400;}"
            "QPushButton:hover{background:"
            + hover
            + ";}"
            "QPushButton:pressed{background:"
            + pressed
            + ";}"
        )
        button.clicked.connect(lambda _checked=False, cb=callback: self._trigger(cb))
        self._layout.addWidget(button)
        return button

    def add_separator(self) -> QWidget:
        spacer = QWidget(self._panel)
        spacer.setFixedHeight(8)
        self._layout.addWidget(spacer)
        return spacer

    def show_at(self, global_pos: QPoint, *, align_right: bool = True, y_offset: int = 8) -> None:
        self.adjustSize()
        size = self.sizeHint()
        screen = QApplication.screenAt(global_pos)
        if screen is None:
            screen = QApplication.primaryScreen()
        geometry = screen.availableGeometry() if screen is not None else None

        width = max(size.width(), 220)
        height = size.height()
        x = int(global_pos.x() - width) if align_right else int(global_pos.x())
        y = int(global_pos.y() + y_offset)
        if geometry is not None:
            margin = 8
            x = max(geometry.left() + margin, min(x, geometry.right() - width - margin))
            y = max(geometry.top() + margin, min(y, geometry.bottom() - height - margin))
        self.resize(width, height)
        self.move(x, y)
        self.show()
        self.raise_()
        self.activateWindow()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self.closed.emit()
        super().closeEvent(event)

    def _trigger(self, callback: Callable[[], None]) -> None:
        self.close()
        callback()

from __future__ import annotations

from typing import Optional, Sequence, List

from PySide6.QtCore import Qt, Signal, QPoint
from PySide6.QtWidgets import QFrame, QGridLayout, QToolButton, QWidget


_FALLBACK_EMOJIS: Sequence[str] = [
    "😀", "😁", "😂", "🤣", "🥹", "😊", "😉", "😍", "😘", "😎", "🤔", "🫡",
    "😴", "😭", "😡", "🤯", "🥳", "😇", "🤗", "🤝", "🙏", "👍", "👎", "👏",
    "🙌", "👌", "🤌", "✌️", "🤞", "👀", "🧠", "💪", "🔥", "💯", "🎉", "✨",
    "❤️", "🩵", "💚", "💔", "💥", "⚡", "⭐", "🌚", "🌝", "☕", "🍿", "🍕",
    "🎮", "🎧", "🎵", "🎬", "📌", "📍", "📎", "🖼️", "📷", "🎙️", "📞", "🕒",
    "✅", "❌", "⚠️", "🔒", "🔓", "🧩", "🪄", "🫠", "🫶", "🥲", "🤖", "🗿",
    "🚀", "🌐", "💼", "📚", "📈", "📉", "💸", "🏆", "🥇", "🎁", "🧨", "🎯",
    "😅", "🤤", "😈", "😬", "🙃", "😐", "😮", "😱", "🤬", "😏", "🤓", "🤠",
]


def load_all_emojis(*, limit: Optional[int] = None) -> Sequence[str]:
    values: List[str] = []
    seen: set[str] = set()

    try:
        import emoji as emoji_lib  # type: ignore

        data = getattr(emoji_lib, "EMOJI_DATA", None)
        if isinstance(data, dict):
            for key in data.keys():
                value = str(key or "").strip()
                if not value or value in seen:
                    continue
                seen.add(value)
                values.append(value)
    except Exception:
        values = []

    if not values:
        values = [str(e) for e in _FALLBACK_EMOJIS if str(e).strip()]

    if isinstance(limit, int) and limit > 0:
        values = values[: int(limit)]
    return tuple(values)


DEFAULT_EMOJIS: Sequence[str] = load_all_emojis()


class EmojiPickerPopup(QFrame):
    emojiSelected = Signal(str)

    def __init__(self, parent: Optional[QWidget] = None, *, emojis: Sequence[str] = DEFAULT_EMOJIS) -> None:
        super().__init__(parent, Qt.WindowType.Popup)
        self.setObjectName("emojiPicker")
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)
        self.setStyleSheet(
            "QFrame#emojiPicker{background-color:#0f1b27;border:1px solid rgba(255,255,255,0.08);"
            "border-radius:12px;padding:8px;}"
            "QToolButton{background:transparent;border:none;font-size:18px;padding:6px;}"
            "QToolButton:hover{background-color:rgba(255,255,255,0.08);border-radius:10px;}"
        )

        layout = QGridLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(4)

        cols = 8
        for idx, emoji in enumerate(emojis):
            btn = QToolButton(self)
            btn.setText(str(emoji))
            btn.setCursor(Qt.CursorShape.PointingHandCursor)
            btn.clicked.connect(lambda _=False, e=str(emoji): self._select(e))
            layout.addWidget(btn, idx // cols, idx % cols)

    def popup_below(self, anchor: QWidget) -> None:
        self.adjustSize()
        origin = anchor.mapToGlobal(QPoint(0, 0))
        x = origin.x()
        y = origin.y() - self.height() - 8
        self.move(x, y)
        self.show()

    def _select(self, emoji: str) -> None:
        self.emojiSelected.emit(emoji)
        self.hide()

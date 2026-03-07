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


def _is_flag_sequence(value: str) -> bool:
    if not value:
        return False
    for ch in value:
        code = ord(ch)
        if 0x1F1E6 <= code <= 0x1F1FF:
            return True
    return False


def _emoji_priority(value: str, index: int) -> tuple[int, int, int]:
    first = ord(value[0]) if value else 0
    if 0x1F600 <= first <= 0x1F64F:
        bucket = 0
    elif 0x1F900 <= first <= 0x1F9FF:
        bucket = 1
    elif 0x1F300 <= first <= 0x1F5FF:
        bucket = 2
    elif 0x1F680 <= first <= 0x1F6FF:
        bucket = 3
    else:
        bucket = 4
    return (bucket, len(value), index)


def _normalize_picker_emoji(values: Sequence[str], *, limit: Optional[int] = None) -> Sequence[str]:
    max_items = int(limit) if isinstance(limit, int) and int(limit) > 0 else None
    merged: List[str] = []
    seen: set[str] = set()
    for emoji in list(_FALLBACK_EMOJIS) + [str(v or "").strip() for v in values]:
        value = str(emoji or "").strip()
        if not value or value in seen:
            continue
        if _is_flag_sequence(value):
            continue
        seen.add(value)
        merged.append(value)
        if max_items is not None and len(merged) >= max_items:
            break
    return tuple(merged)


def load_all_emojis(*, limit: Optional[int] = None) -> Sequence[str]:
    values: List[str] = []
    seen: set[str] = set()
    max_items = int(limit) if isinstance(limit, int) and int(limit) > 0 else None

    try:
        import emoji as emoji_lib  # type: ignore

        data = getattr(emoji_lib, "EMOJI_DATA", None)
        if isinstance(data, dict):
            ranked: List[tuple[tuple[int, int, int], str]] = []
            for index, key in enumerate(data.keys()):
                value = str(key or "").strip()
                if not value or value in seen:
                    continue
                if _is_flag_sequence(value):
                    continue
                seen.add(value)
                ranked.append((_emoji_priority(value, index), value))
            ranked.sort(key=lambda item: item[0])
            values = [value for _, value in ranked]
    except Exception:
        values = []

    if not values:
        values = [str(e) for e in _FALLBACK_EMOJIS if str(e).strip()]

    normalized = list(_normalize_picker_emoji(values, limit=max_items))
    if max_items is not None:
        normalized = normalized[:max_items]
    return tuple(normalized)


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

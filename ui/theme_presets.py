from __future__ import annotations

from typing import Any, Dict

_NIGHT_CHAT_BG = "#0f0f10"
_NIGHT_CHAT_LIST_BG = "#181819"
_NIGHT_TEXT = "#f1f1f1"
_NIGHT_MUTED = "#868686"
_NIGHT_BUTTON = "#1e1e1e"
_ACCENT = "#64b5ef"

NIGHT_STYLE: Dict[str, Any] = {
    "palette": {
        "Window": _NIGHT_CHAT_BG,
        "WindowText": _NIGHT_TEXT,
        "Base": _NIGHT_CHAT_BG,
        "AlternateBase": _NIGHT_CHAT_LIST_BG,
        "Button": _NIGHT_BUTTON,
        "ButtonText": _NIGHT_TEXT,
        "Text": _NIGHT_TEXT,
        "Highlight": _ACCENT,
        "HighlightedText": "#0c141f",
        "PlaceholderText": "#6f7b8a",
        "BrightText": "#ffb36b",
        "Disabled": {
            "Text": "#516071",
            "ButtonText": "#516071",
            "WindowText": "#516071",
        },
    },
    "stylesheet": [
        f"QWidget {{ background-color:{_NIGHT_CHAT_BG}; color:{_NIGHT_TEXT}; }}",
        "QLabel {{ color:%s; }}" % _NIGHT_TEXT,
        f"QTextEdit, QPlainTextEdit {{ background-color:{_NIGHT_BUTTON}; border:1px solid #222f3d; border-radius:6px; }}",
        f"QLineEdit {{ background-color:{_NIGHT_BUTTON}; border:1px solid #222f3d; border-radius:6px; padding:4px 8px; color:{_NIGHT_TEXT}; selection-background-color:{_ACCENT}; selection-color:#0b141d; }}",
        f"QListWidget {{ background-color:{_NIGHT_CHAT_LIST_BG}; border:none; }}",
        "QListWidget::item { padding:6px 4px; border-bottom:1px solid rgba(255,255,255,0.03); }",
        f"QListWidget::item:selected {{ background-color:rgba(89,183,233,0.18); color:{_NIGHT_TEXT}; }}",
        "QToolButton, QPushButton {"
        f" background-color:{_NIGHT_BUTTON}; border:1px solid rgba(255,255,255,0.08); border-radius:6px; padding:6px 10px; color:{_NIGHT_TEXT};"
        "}",
        "QToolButton:hover, QPushButton:hover { background-color:rgba(89,183,233,0.12); border-color:rgba(89,183,233,0.35); }",
        "QToolButton:pressed, QPushButton:pressed { background-color:rgba(89,183,233,0.2); }",
        "QScrollArea, QScrollArea > QWidget > QWidget { background-color:transparent; }",
        "QSplitter::handle { background-color:rgba(255,255,255,0.04); }",
        "QScrollBar:vertical { background-color:rgba(255,255,255,0.02); width:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:vertical { background-color:rgba(200,213,227,0.7); border-radius:2px; min-height:30px; }",
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0; }",
        "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background:transparent; }",
        "QScrollBar:horizontal { background-color:rgba(255,255,255,0.02); height:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:horizontal { background-color:rgba(200,213,227,0.7); border-radius:2px; min-width:30px; }",
        "QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width:0; }",
        "QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background:transparent; }",
        f"QLabel a {{ color:{_ACCENT}; }}",
        "QProgressBar { background-color:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08); border-radius:4px; text-align:center; }",
        f"QProgressBar::chunk {{ background-color:{_ACCENT}; border-radius:4px; }}",
        "QMenu { background-color:#1b2433; border:1px solid rgba(255,255,255,0.1); color:#dfe6f0; }",
        "QMenu::item:selected { background-color:rgba(89,183,233,0.2); }",
    ],
}

_DAY_BG = "#ffffff"
_DAY_ALT = "#f5f5f5"
_DAY_TEXT = "#1a1a1a"
_DAY_ACCENT = "#1a73e8"

DAY_STYLE: Dict[str, Any] = {
    "palette": {
        "Window": _DAY_BG,
        "WindowText": _DAY_TEXT,
        "Base": _DAY_ALT,
        "AlternateBase": "#efefef",
        "Button": "#e8e8e8",
        "ButtonText": _DAY_TEXT,
        "Text": _DAY_TEXT,
        "Highlight": _DAY_ACCENT,
        "HighlightedText": "#ffffff",
        "PlaceholderText": "#9e9e9e",
        "BrightText": "#d4380d",
        "Disabled": {
            "Text": "#aaaaaa",
            "ButtonText": "#aaaaaa",
            "WindowText": "#aaaaaa",
        },
    },
    "stylesheet": [
        f"QWidget {{ background-color:{_DAY_BG}; color:{_DAY_TEXT}; }}",
        f"QLabel {{ color:{_DAY_TEXT}; }}",
        f"QLineEdit, QTextEdit, QPlainTextEdit {{ background-color:{_DAY_ALT}; border:1px solid #d0d0d0; border-radius:4px; selection-background-color:{_DAY_ACCENT}; selection-color:#ffffff; color:{_DAY_TEXT}; }}",
        f"QListWidget {{ background-color:{_DAY_ALT}; border:1px solid #d0d0d0; color:{_DAY_TEXT}; }}",
        "QListWidget::item:selected { background-color:#d0e4f7; color:#1a1a1a; }",
        "QScrollArea, QScrollArea > QWidget > QWidget { background-color:transparent; }",
        f"QToolButton, QPushButton, QComboBox {{ background-color:#e8e8e8; border:1px solid #d0d0d0; border-radius:4px; padding:4px 10px; color:{_DAY_TEXT}; }}",
        "QToolButton:hover, QPushButton:hover { background-color:#d8d8d8; }",
        "QScrollBar:vertical { background-color:rgba(0,0,0,0.04); width:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:vertical { background-color:rgba(0,0,0,0.35); border-radius:2px; min-height:30px; }",
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0; }",
        "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background:transparent; }",
        "QScrollBar:horizontal { background-color:rgba(0,0,0,0.04); height:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:horizontal { background-color:rgba(0,0,0,0.35); border-radius:2px; min-width:30px; }",
        "QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width:0; }",
        "QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background:transparent; }",
        f"QLabel a {{ color:{_DAY_ACCENT}; }}",
        "QProgressBar { background-color:#e0e0e0; border:1px solid #d0d0d0; border-radius:4px; text-align:center; color:#1a1a1a; }",
        f"QProgressBar::chunk {{ background-color:{_DAY_ACCENT}; border-radius:4px; }}",
        "QMenu { background-color:#ffffff; border:1px solid #d0d0d0; color:#1a1a1a; }",
        "QMenu::item:selected { background-color:#e3f0fd; }",
    ],
}

NIGHT_BUBBLES = {
    "me": {"bg": "#366caf", "border": "#4b7daf", "text": "#fafafa", "link": "#aedfff"},
    "assistant": {"bg": "#1a6a4a", "border": "#2e6a53", "text": "#f2fff7", "link": _ACCENT},
    "other": {"bg": "#1f2123", "border": "#2a2c2e", "text": "#fafafa", "link": "#79c4fc"},
}

DAY_BUBBLES = {
    "me": {"bg": "#d4e4f7", "border": "#aecde6", "text": "#1a2740", "link": "#1a73e8"},
    "assistant": {"bg": "#d7f0dd", "border": "#a8dbb5", "text": "#14302a", "link": "#1a8c4e"},
    "other": {"bg": "#e8e8e8", "border": "#d0d0d0", "text": "#1a1a1a", "link": "#1a73e8"},
}

THEME_PRESETS: Dict[str, Dict[str, Any]] = {
    "night": {"style": NIGHT_STYLE, "bubbles": NIGHT_BUBBLES},
    "day": {"style": DAY_STYLE, "bubbles": DAY_BUBBLES},
}

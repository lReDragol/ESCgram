from __future__ import annotations

from typing import Any, Dict

_NIGHT_CHAT_BG = "#0e1621"
_NIGHT_CHAT_LIST_BG = "#17212b"
_NIGHT_TEXT = "#dee6f1"
_NIGHT_MUTED = "#8d9ba9"
_NIGHT_BUTTON = "#1d2734"
_ACCENT = "#59b7e9"

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

_DAY_BG = "#121212"
_DAY_ALT = "#181818"
_DAY_TEXT = "#e0e0e0"
_DAY_ACCENT = "#3367d6"

DAY_STYLE: Dict[str, Any] = {
    "palette": {
        "Window": _DAY_BG,
        "WindowText": _DAY_TEXT,
        "Base": _DAY_ALT,
        "AlternateBase": "#161616",
        "Button": "#1f1f1f",
        "ButtonText": _DAY_TEXT,
        "Text": _DAY_TEXT,
        "Highlight": _DAY_ACCENT,
        "HighlightedText": "#ffffff",
        "PlaceholderText": "#858585",
        "BrightText": "#ffae42",
        "Disabled": {
            "Text": "#555555",
            "ButtonText": "#555555",
            "WindowText": "#555555",
        },
    },
    "stylesheet": [
        f"QWidget {{ background-color:{_DAY_BG}; color:{_DAY_TEXT}; }}",
        f"QLineEdit, QTextEdit, QPlainTextEdit {{ background-color:{_DAY_ALT}; border:1px solid #2a2a2a; border-radius:4px; selection-background-color:{_DAY_ACCENT}; selection-color:#ffffff; }}",
        "QListWidget { background-color:#141414; border:1px solid #2a2a2a; }",
        "QListWidget::item:selected { background-color:#2f4f6f; color:#ffffff; }",
        "QScrollArea, QScrollArea > QWidget > QWidget { background-color:transparent; }",
        "QToolButton, QPushButton, QComboBox { background-color:#1f1f1f; border:1px solid #2a2a2a; border-radius:4px; padding:4px 10px; }",
        "QScrollBar:vertical { background-color:rgba(255,255,255,0.04); width:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:vertical { background-color:rgba(185,185,185,0.7); border-radius:2px; min-height:30px; }",
        "QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0; }",
        "QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background:transparent; }",
        "QScrollBar:horizontal { background-color:rgba(255,255,255,0.04); height:4px; margin:0; border-radius:2px; }",
        "QScrollBar::handle:horizontal { background-color:rgba(185,185,185,0.7); border-radius:2px; min-width:30px; }",
        "QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width:0; }",
        "QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal { background:transparent; }",
    ],
}

NIGHT_BUBBLES = {
    "me": {"bg": "#2b5278", "border": "#3a71a1", "text": "#f4f7ff", "link": _ACCENT},
    "assistant": {"bg": "#1f4a3a", "border": "#2e6a53", "text": "#f2fff7", "link": _ACCENT},
    "other": {"bg": "#182533", "border": "#243247", "text": "#dfe6f0", "link": _ACCENT},
}

DAY_BUBBLES = {
    "me": {"bg": "#2b5278", "border": "#3a71a1", "text": "#f4f7ff", "link": "#7ab8ff"},
    "assistant": {"bg": "#1f4a3a", "border": "#2e6a53", "text": "#f2fff7", "link": "#7ae0b8"},
    "other": {"bg": "#1c1c1c", "border": "#2a2a2a", "text": "#e2e2e2", "link": "#7ab8ff"},
}

THEME_PRESETS: Dict[str, Dict[str, Any]] = {
    "night": {"style": NIGHT_STYLE, "bubbles": NIGHT_BUBBLES},
    "day": {"style": DAY_STYLE, "bubbles": DAY_BUBBLES},
}

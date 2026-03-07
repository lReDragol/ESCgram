from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
import hashlib
import json
import os
import re
import shutil
from urllib.parse import quote as _url_quote, unquote as _url_unquote
from html import escape
from typing import Optional, Dict, Any, Callable, List, Sequence, Tuple
from weakref import WeakSet
from utils import app_paths

from PySide6.QtCore import Qt, QUrl, Signal, QPointF, QRectF, QSize, QThread, Slot, QTimer
from PySide6.QtGui import QDesktopServices, QPainter, QColor, QMouseEvent, QPainterPath, QPen, QLinearGradient
from PySide6.QtWidgets import (
    QLabel,
    QFrame,
    QVBoxLayout,
    QWidget,
    QHBoxLayout,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QToolButton,
    QSizePolicy,
)
try:
    from shiboken6 import isValid as _qt_is_valid
except Exception:  # pragma: no cover - fallback for unexpected runtime envs
    def _qt_is_valid(obj: object) -> bool:
        return obj is not None

from ui.media_render import MediaRenderingMixin, _fmt_time
from ui.common import HAVE_QTMULTIMEDIA, MediaPlaybackCoordinator, log
from ui.styles import StyleManager
from ui.send_media_workers import FfmpegConvertWorker

if HAVE_QTMULTIMEDIA:
    from PySide6.QtMultimedia import QMediaPlayer
else:  # pragma: no cover
    QMediaPlayer = object  # type: ignore[assignment]


def _resolve_ffmpeg_binary() -> Optional[str]:
    exe = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    try:
        local = app_paths.telegram_workdir() / "ffmpeg" / "bin" / exe
        if local.is_file():
            return str(local)
    except Exception:
        pass
    return shutil.which("ffmpeg")

import string

_STYLE_MGR = StyleManager.instance()


def _style_sheet(key: str, default: str, mapping: Optional[Dict[str, Any]] = None) -> str:
    css = _STYLE_MGR.stylesheet(key, mapping)
    if css:
        return css
    if mapping:
        return string.Template(default).safe_substitute(mapping)
    return default


def _bubble_radius() -> int:
    return int(_STYLE_MGR.metric("message_widgets.metrics.body_radius", 14) or 14)

# ---------------------------- темы пузырей -----------------------------

_raw_link_color = str(_STYLE_MGR.value("message_widgets.link_color", "#59b7ff") or "#59b7ff")
_parsed_link_color = QColor(_raw_link_color)
if not _parsed_link_color.isValid() or _parsed_link_color.hsvSaturation() < 45:
    ACCENT_LINK_COLOR = "#59b7ff"
else:
    ACCENT_LINK_COLOR = _parsed_link_color.name()

DEFAULT_BUBBLE_THEME_FALLBACK = {
    "me": {"bg": "#2b5278", "border": "#3a71a1", "text": "#f4f7ff", "link": ACCENT_LINK_COLOR},
    "assistant": {"bg": "#1f4a3a", "border": "#2e6a53", "text": "#f2fff7", "link": ACCENT_LINK_COLOR},
    "other": {"bg": "#182533", "border": "#243247", "text": "#dfe6f0", "link": ACCENT_LINK_COLOR},
}
DEFAULT_BUBBLE_THEME = _STYLE_MGR.value("message_widgets.bubbles", DEFAULT_BUBBLE_THEME_FALLBACK) or DEFAULT_BUBBLE_THEME_FALLBACK
BUBBLE_THEME = {role: dict(colors) for role, colors in DEFAULT_BUBBLE_THEME.items()}

DELETED_BUBBLE_THEME_FALLBACK = {"bg": "#101a1f", "border": "#1c2735", "text": "#7e879b", "link": ACCENT_LINK_COLOR}
DELETED_BUBBLE_THEME = _STYLE_MGR.value("message_widgets.deleted_bubble", DELETED_BUBBLE_THEME_FALLBACK) or DELETED_BUBBLE_THEME_FALLBACK

DELETED_BROOM_EMOJI = "\U0001F9F9"


def set_bubble_theme(theme: dict[str, dict[str, str]]) -> None:
    changed = False
    for role, colors in theme.items():
        if role in BUBBLE_THEME and isinstance(colors, dict):
            BUBBLE_THEME[role].update(colors)
            changed = True
    if changed:
        Bubble.refresh_all()


def _on_style_profile_changed(_profile: Dict[str, Any]) -> None:
    Bubble.refresh_all()


_STYLE_MGR.style_changed.connect(_on_style_profile_changed)


# -------------------------- автолинкинг текста -------------------------

_URL_RE = re.compile(r'(?P<url>(?:https?://|ftp://|tg://|mailto:|www\.)[^\s<]+)', re.I)
_TME_RE = re.compile(r'(?P<url>(?:t\.me|telegram\.me)/[^\s<]+)', re.I)
_EMAIL_RE = re.compile(r'(?P<email>[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})')
_HASHTAG_RE = re.compile(r'(?P<hash>#[A-Za-zА-Яа-я0-9_]{2,64})')
_MENTION_RE = re.compile(r'(?<![\w])(?P<mention>@[A-Za-z0-9_]{5,32})')
_COMMAND_RE = re.compile(r'(?<![\w/])(?P<command>/[A-Za-z0-9_]{1,64}(?:@[A-Za-z0-9_]{3,32})?)')
_QUOTE_OPEN = "\ufff0"
_QUOTE_CLOSE = "\ufff1"
_QUOTE_BLOCK_STYLE = _STYLE_MGR.value("message_widgets.quote_block_style", "margin:4px 0; padding-left:8px; border-left:3px solid rgba(89,183,233,0.45); color:#c4d4eb;")


def _mark_quote_tokens(text: str) -> str:
    lines = text.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    out: list[str] = []
    in_quote = False
    for line in lines:
        if line.startswith('>'):
            if not in_quote:
                out.append(_QUOTE_OPEN)
                in_quote = True
            out.append(line.lstrip('> '))
        else:
            if in_quote:
                out.append(_QUOTE_CLOSE)
                in_quote = False
            out.append(line)
    if in_quote:
        out.append(_QUOTE_CLOSE)
    return '\n'.join(out)


def _apply_quote_styles(html: str) -> str:
    if _QUOTE_OPEN not in html and _QUOTE_CLOSE not in html:
        return html
    html = html.replace(_QUOTE_OPEN, f'<div style="{_QUOTE_BLOCK_STYLE}">')
    html = html.replace(_QUOTE_CLOSE, '</div>')
    return html


def _prepare_rich_text(text: str) -> str:
    marked = _mark_quote_tokens(text)
    html = _autolink_plain_to_html(marked)
    return _apply_quote_styles(html)


def _looks_like_html(t: str) -> bool:
    tl = t.lower()
    return '<a ' in tl or '<br' in tl or '<p' in tl or t.strip().startswith('<')


def _autolink_plain_to_html(text: str) -> str:
    """plain → HTML (+ссылки, переносы)."""
    s = escape(text).replace('\r\n', '\n').replace('\r', '\n').replace('\n', '<br/>')

    def _u(m: re.Match) -> str:
        raw = m.group('url')
        if re.match(r'^(?:https?|ftp|tg|mailto)://', raw, re.I):
            href = raw
        elif raw.lower().startswith('www.'):
            href = 'http://' + raw
        else:
            href = raw
        return (
            f'<a href="{escape(href, True)}" '
            f'style="color:{ACCENT_LINK_COLOR}; text-decoration:none; font-weight:600;">'
            f'{escape(raw)}</a>'
        )

    s = _URL_RE.sub(_u, s)
    s = _TME_RE.sub(
        lambda m: (
            f'<a href="https://{m.group("url")}" '
            f'style="color:{ACCENT_LINK_COLOR}; text-decoration:none; font-weight:600;">'
            f'{m.group("url")}</a>'
        ),
        s,
    )
    s = _EMAIL_RE.sub(
        lambda m: (
            f'<a href="mailto:{m.group("email")}" '
            f'style="color:{ACCENT_LINK_COLOR}; text-decoration:none; font-weight:600;">'
            f'{m.group("email")}</a>'
        ),
        s,
    )

    def _hash_color(match: re.Match) -> str:
        tag = match.group('hash')
        return f'<span style="color:{ACCENT_LINK_COLOR}; font-weight:600;">{tag}</span>'

    s = _HASHTAG_RE.sub(_hash_color, s)
    s = _MENTION_RE.sub(
        lambda m: (
            f'<a href="https://t.me/{m.group("mention").lstrip("@")}" '
            f'style="color:{ACCENT_LINK_COLOR}; text-decoration:none; font-weight:600;">'
            f'{m.group("mention")}</a>'
        ),
        s,
    )
    return s


def _utf16_offsets(text: str) -> List[int]:
    offsets: List[int] = [0]
    total = 0
    for ch in text:
        total += 2 if ord(ch) > 0xFFFF else 1
        offsets.append(total)
    return offsets


def _utf16_to_index(offsets: List[int], utf16_pos: int) -> int:
    try:
        pos = int(utf16_pos or 0)
    except Exception:
        pos = 0
    pos = max(0, min(pos, offsets[-1] if offsets else 0))
    return bisect_left(offsets, pos)


@dataclass(frozen=True)
class _RichSpan:
    type: str
    start: int
    end: int
    url: Optional[str] = None
    language: Optional[str] = None


def _span_overlaps(a: Tuple[int, int], b: Tuple[int, int]) -> bool:
    return a[0] < b[1] and b[0] < a[1]


def _normalize_entity_spans(text: str, entities: Sequence[Dict[str, Any]]) -> List[_RichSpan]:
    offsets = _utf16_offsets(text)
    spans: List[_RichSpan] = []
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        etype = str(ent.get("type") or "").strip().lower()
        if not etype:
            continue
        start = _utf16_to_index(offsets, int(ent.get("offset") or 0))
        end = _utf16_to_index(offsets, int(ent.get("offset") or 0) + int(ent.get("length") or 0))
        start = max(0, min(start, len(text)))
        end = max(0, min(end, len(text)))
        if end <= start:
            continue

        url = ent.get("url")
        language = ent.get("language")
        user_id = ent.get("user_id")

        if etype == "mention":
            snippet = text[start:end].lstrip("@")
            if snippet:
                etype = "text_link"
                url = f"https://t.me/{snippet}"

        if etype == "text_mention":
            try:
                uid = int(user_id or 0)
            except Exception:
                uid = 0
            if uid > 0:
                etype = "text_link"
                url = f"tg://user?id={uid}"

        if etype == "bot_command":
            snippet = text[start:end].strip()
            if snippet:
                url = f"tgcmd://{_url_quote(snippet, safe='')}"

        if etype in {"url", "email"}:
            snippet = text[start:end]
            if etype == "email":
                url = f"mailto:{snippet}"
                etype = "text_link"
            else:
                href = snippet
                if re.match(r"^(?:https?|ftp|tg|mailto)://", href, re.I):
                    url = href
                elif href.lower().startswith("www."):
                    url = "http://" + href
                else:
                    url = href
                etype = "text_link"

        spans.append(_RichSpan(type=etype, start=start, end=end, url=str(url) if url else None, language=str(language) if language else None))
    return spans


def _autolink_spans(text: str, existing: List[_RichSpan]) -> List[_RichSpan]:
    protected: List[Tuple[int, int]] = [(s.start, s.end) for s in existing if s.type in {"code", "pre"}]
    already_linked: List[Tuple[int, int]] = [
        (s.start, s.end)
        for s in existing
        if s.type in {"text_link", "hashtag", "bot_command"}
    ]

    def _can_add(start: int, end: int) -> bool:
        rng = (start, end)
        for other in protected:
            if _span_overlaps(rng, other):
                return False
        for other in already_linked:
            if _span_overlaps(rng, other):
                return False
        return True

    spans: List[_RichSpan] = []
    for m in _URL_RE.finditer(text):
        start, end = m.span("url")
        if not _can_add(start, end):
            continue
        raw = m.group("url")
        if re.match(r"^(?:https?|ftp|tg|mailto)://", raw, re.I):
            href = raw
        elif raw.lower().startswith("www."):
            href = "http://" + raw
        else:
            href = raw
        spans.append(_RichSpan(type="text_link", start=start, end=end, url=href))
        already_linked.append((start, end))

    for m in _TME_RE.finditer(text):
        start, end = m.span("url")
        if not _can_add(start, end):
            continue
        raw = m.group("url")
        spans.append(_RichSpan(type="text_link", start=start, end=end, url=f"https://{raw}"))
        already_linked.append((start, end))

    for m in _EMAIL_RE.finditer(text):
        start, end = m.span("email")
        if not _can_add(start, end):
            continue
        email = m.group("email")
        spans.append(_RichSpan(type="text_link", start=start, end=end, url=f"mailto:{email}"))
        already_linked.append((start, end))

    for m in _HASHTAG_RE.finditer(text):
        start, end = m.span("hash")
        if not _can_add(start, end):
            continue
        spans.append(_RichSpan(type="hashtag", start=start, end=end))

    for m in _MENTION_RE.finditer(text):
        start, end = m.span("mention")
        if not _can_add(start, end):
            continue
        username = m.group("mention").lstrip("@")
        spans.append(_RichSpan(type="text_link", start=start, end=end, url=f"https://t.me/{username}"))

    for m in _COMMAND_RE.finditer(text):
        start, end = m.span("command")
        if not _can_add(start, end):
            continue
        command = m.group("command")
        spans.append(_RichSpan(type="bot_command", start=start, end=end, url=f"tgcmd://{_url_quote(command, safe='')}"))
        already_linked.append((start, end))

    return spans


def _search_spans(text: str, query: str, *, active: bool) -> List[_RichSpan]:
    needle = str(query or "").strip()
    if not needle:
        return []
    lower_text = text.casefold()
    lower_needle = needle.casefold()
    spans: List[_RichSpan] = []
    pos = 0
    while True:
        idx = lower_text.find(lower_needle, pos)
        if idx < 0:
            break
        end = idx + len(lower_needle)
        spans.append(_RichSpan(type="search_active" if active else "search_hit", start=idx, end=end))
        pos = end
    return spans


def _render_entities_html(
    text: str,
    entities: Sequence[Dict[str, Any]],
    *,
    reveal_spoilers: bool,
    search_query: str = "",
    search_active: bool = False,
) -> Tuple[str, bool]:
    spans = _normalize_entity_spans(text, entities)
    spans.extend(_autolink_spans(text, spans))
    spans.extend(_search_spans(text, search_query, active=search_active))

    has_spoilers = any(s.type == "spoiler" for s in spans)

    def _open_tag(span: _RichSpan) -> str:
        t = span.type
        if t == "bold":
            return "<b>"
        if t == "italic":
            return "<i>"
        if t == "underline":
            return "<u>"
        if t == "strikethrough":
            return "<s>"
        if t == "code":
            return '<span style="font-family:Consolas,Monaco,monospace;background-color:rgba(110,120,140,0.22);padding:1px 4px;border-radius:4px;">'
        if t == "pre":
            return '<pre style="font-family:Consolas,Monaco,monospace;background-color:rgba(110,120,140,0.18);padding:8px 10px;border-radius:8px;white-space:pre-wrap;">'
        if t in {"hashtag", "mention"}:
            return f'<span style="color:{ACCENT_LINK_COLOR};font-weight:600;">'
        if t == "bot_command":
            href = escape(str(span.url or ""), quote=True)
            return f'<a href="{href}" style="color:{ACCENT_LINK_COLOR};text-decoration:none;font-weight:700;">'
        if t == "text_link" and span.url:
            href = escape(str(span.url), quote=True)
            return f'<a href="{href}" style="color:{ACCENT_LINK_COLOR};text-decoration:none;font-weight:600;">'
        if t == "blockquote":
            return '<span style="color:#c4d4eb;border-left:3px solid rgba(89,183,233,0.45);padding-left:8px;">'
        if t == "spoiler":
            bg = "rgba(73,130,190,0.28)"
            if reveal_spoilers:
                return f'<a href="spoiler://toggle" style="text-decoration:none;color:inherit;background-color:{bg};border-radius:4px;padding:0 2px;">'
            return f'<a href="spoiler://toggle" style="text-decoration:none;color:transparent;background-color:{bg};border-radius:4px;padding:0 2px;">'
        if t == "hidden":
            bg = "rgba(140,150,165,0.28)"
            return f'<span style="background-color:{bg};border-radius:4px;padding:0 2px;">'
        if t == "search_active":
            return '<span style="background-color:rgba(89,183,255,0.34);border-radius:4px;padding:0 1px;">'
        if t == "search_hit":
            return '<span style="background-color:rgba(89,183,255,0.18);border-radius:4px;padding:0 1px;">'
        return ""

    def _close_tag(span: _RichSpan) -> str:
        t = span.type
        if t in {"bold", "italic", "underline", "strikethrough"}:
            return {"bold": "</b>", "italic": "</i>", "underline": "</u>", "strikethrough": "</s>"}[t]
        if t in {"code", "hashtag", "mention", "hidden", "blockquote", "search_active", "search_hit"}:
            return "</span>"
        if t == "pre":
            return "</pre>"
        if t in {"text_link", "spoiler", "bot_command"}:
            return "</a>"
        return ""

    opens: Dict[int, List[_RichSpan]] = {}
    closes: Dict[int, List[_RichSpan]] = {}
    for span in spans:
        opens.setdefault(span.start, []).append(span)
        closes.setdefault(span.end, []).append(span)

    def _open_sort_key(s: _RichSpan) -> Tuple[int, int]:
        # Outer first: longer spans first.
        return (s.start, -(s.end - s.start))

    def _close_sort_key(s: _RichSpan) -> Tuple[int, int]:
        # Inner first: shorter spans first (or later starts first).
        return (s.end, (s.end - s.start))

    parts: List[str] = ["<span>"]
    for idx in range(0, len(text) + 1):
        if idx in closes:
            for span in sorted(closes[idx], key=_close_sort_key):
                tag = _close_tag(span)
                if tag:
                    parts.append(tag)
        if idx in opens:
            for span in sorted(opens[idx], key=_open_sort_key):
                tag = _open_tag(span)
                if tag:
                    parts.append(tag)
        if idx == len(text):
            break
        ch = text[idx]
        if ch == "\n":
            parts.append("<br/>")
        else:
            parts.append(escape(ch))
    parts.append("</span>")
    return "".join(parts), has_spoilers


class RichTextLabel(QLabel):
    """QLabel с кликабельными ссылками и выделением текста."""
    commandActivated = Signal(str)

    def __init__(self, text: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setWordWrap(True)
        self.setTextInteractionFlags(Qt.TextInteractionFlag.TextBrowserInteraction)
        self.setOpenExternalLinks(False)
        self.setTextFormat(Qt.TextFormat.RichText)
        self.setStyleSheet(
            "font-family:'Segoe UI Emoji','Noto Color Emoji','Apple Color Emoji','Segoe UI',sans-serif;"
        )
        self._raw_text = ""
        self._entities: Optional[List[Dict[str, Any]]] = None
        self._spoilers_revealed = False
        self._has_spoilers = False
        self._search_query = ""
        self._search_active = False
        self._spoiler_phase = 0.0
        self._spoiler_flash = 0.0
        self._spoiler_timer = QTimer(self)
        self._spoiler_timer.setInterval(48)
        self._spoiler_timer.timeout.connect(self._tick_spoiler_animation)
        self.linkActivated.connect(self._on_link_activated)
        self.set_message(text)

    def _on_link_activated(self, href: str) -> None:
        if href and str(href).startswith("tgcmd://"):
            command = _url_unquote(str(href).split("://", 1)[1])
            if command:
                self.commandActivated.emit(command)
            return
        if href and str(href).startswith("spoiler://") and self._has_spoilers:
            self._spoilers_revealed = not self._spoilers_revealed
            self._spoiler_flash = 1.0
            if not self._spoiler_timer.isActive():
                self._spoiler_timer.start()
            self._render_current()
            return
        QDesktopServices.openUrl(QUrl(str(href)))

    def set_message(self, text: str, *, entities: Optional[List[Dict[str, Any]]] = None) -> None:
        self._raw_text = text or ""
        self._entities = list(entities) if isinstance(entities, list) else None
        self._spoilers_revealed = False
        self._render_current()

    def set_search(self, query: str, *, active: bool = False) -> None:
        normalized = str(query or "")
        active = bool(active and normalized)
        if self._search_query == normalized and self._search_active == active:
            return
        self._search_query = normalized
        self._search_active = active
        self._render_current()

    def _render_current(self) -> None:
        if self._entities or self._search_query:
            html, has_spoilers = _render_entities_html(
                self._raw_text,
                self._entities or [],
                reveal_spoilers=self._spoilers_revealed,
                search_query=self._search_query,
                search_active=self._search_active,
            )
            self._has_spoilers = bool(has_spoilers)
            self.setText(html)
            if self._has_spoilers and not self._spoilers_revealed and not self._spoiler_timer.isActive():
                self._spoiler_timer.start()
            elif not self._has_spoilers and self._spoiler_timer.isActive():
                self._spoiler_timer.stop()
            return
        self._has_spoilers = False
        if self._spoiler_timer.isActive():
            self._spoiler_timer.stop()
        self.set_rich_text(self._raw_text)

    def set_rich_text(self, text: str) -> None:
        if _looks_like_html(text):
            html = text
        else:
            html = _prepare_rich_text(text)
        self.setText(html)

    @Slot()
    def _tick_spoiler_animation(self) -> None:
        self._spoiler_phase = (self._spoiler_phase + 0.06) % 1.0
        if self._spoiler_flash > 0.0:
            self._spoiler_flash = max(0.0, self._spoiler_flash - 0.08)
        if not (self._has_spoilers and not self._spoilers_revealed) and self._spoiler_flash <= 0.0:
            self._spoiler_timer.stop()
        self.update()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        super().paintEvent(event)
        if not ((self._has_spoilers and not self._spoilers_revealed) or self._spoiler_flash > 0.0):
            return
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        rect = self.rect().adjusted(2, 2, -2, -2)
        if rect.isEmpty():
            painter.end()
            return
        grad = QLinearGradient(rect.left() + (rect.width() * self._spoiler_phase), rect.top(), rect.right(), rect.bottom())
        shimmer_alpha = 55 if self._has_spoilers and not self._spoilers_revealed else 0
        flash_alpha = int(110 * self._spoiler_flash)
        grad.setColorAt(0.0, QColor(36, 73, 116, shimmer_alpha))
        grad.setColorAt(0.45, QColor(89, 183, 255, max(shimmer_alpha + 20, flash_alpha)))
        grad.setColorAt(1.0, QColor(19, 34, 52, shimmer_alpha))
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(grad)
        painter.drawRoundedRect(rect, 8, 8)
        painter.end()


def _reply_markup_signature(markup: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(markup or {}, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(markup)


def _reply_markup_button_prefix(payload: Dict[str, Any]) -> str:
    if payload.get("url") or payload.get("web_app_url") or payload.get("login_url"):
        return "↗ "
    if payload.get("switch_inline_query") is not None or payload.get("switch_inline_query_current_chat") is not None:
        return "⌕ "
    if payload.get("request_contact"):
        return "☎ "
    if payload.get("request_location"):
        return "⌖ "
    if payload.get("request_poll"):
        return "◉ "
    if payload.get("request_chat") or payload.get("request_users"):
        return "👥 "
    if payload.get("callback_data") is not None:
        return "• "
    return ""


def _reply_markup_button_tooltip(payload: Dict[str, Any]) -> str:
    if payload.get("url") or payload.get("web_app_url") or payload.get("login_url"):
        return "Открыть ссылку"
    if payload.get("switch_inline_query_current_chat") is not None:
        return "Подставить inline-запрос в текущий чат"
    if payload.get("switch_inline_query") is not None:
        return "Переключить в inline-режим"
    if payload.get("request_contact"):
        return "Отправить контакт"
    if payload.get("request_location"):
        return "Отправить геопозицию"
    if payload.get("request_poll"):
        return "Запрос на отправку опроса"
    if payload.get("request_chat") or payload.get("request_users"):
        return "Запрос выбора чата или пользователей"
    if payload.get("callback_data") is not None:
        return "Вызвать действие бота"
    return ""


class MessageReplyMarkupWidget(QWidget):
    buttonActivated = Signal(dict)

    def __init__(self, mode: str = "inline", parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._mode = str(mode or "inline").strip().lower()
        self._markup: Optional[Dict[str, Any]] = None
        self._signature = ""
        self._root = QVBoxLayout(self)
        self._root.setContentsMargins(0, 0, 0, 0)
        self._root.setSpacing(0)

        self._frame: Optional[QFrame] = None
        self._title_label: Optional[QLabel] = None
        self._scroll: Optional[QScrollArea] = None

        if self._mode == "reply":
            frame = QFrame(self)
            frame.setObjectName("botReplyKeyboard")
            frame.setStyleSheet(
                "QFrame#botReplyKeyboard{background-color:rgba(255,255,255,0.04);"
                "border:1px solid rgba(255,255,255,0.06);border-radius:16px;}"
            )
            frame_layout = QVBoxLayout(frame)
            frame_layout.setContentsMargins(10, 8, 10, 10)
            frame_layout.setSpacing(8)

            title = QLabel("", frame)
            title.setStyleSheet("color:#8fb4d9;font-size:11px;font-weight:700;letter-spacing:0.3px;")
            title.hide()
            frame_layout.addWidget(title, 0)

            scroll = QScrollArea(frame)
            scroll.setWidgetResizable(True)
            scroll.setFrameShape(QFrame.Shape.NoFrame)
            scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
            scroll.setStyleSheet(
                "QScrollArea{background:transparent;border:none;}"
                "QScrollArea > QWidget > QWidget{background:transparent;}"
            )
            host = QWidget(scroll)
            self._layout = QVBoxLayout(host)
            self._layout.setContentsMargins(0, 0, 0, 0)
            self._layout.setSpacing(8)
            scroll.setWidget(host)
            frame_layout.addWidget(scroll, 1)
            self._root.addWidget(frame, 1)
            self._frame = frame
            self._title_label = title
            self._scroll = scroll
            self.setMaximumHeight(220)
        else:
            self._layout = QVBoxLayout()
            self._layout.setContentsMargins(0, 0, 0, 0)
            self._layout.setSpacing(6)
            self._root.addLayout(self._layout, 1)

    def clear_buttons(self) -> None:
        while self._layout.count():
            item = self._layout.takeAt(0)
            widget = item.widget()
            child_layout = item.layout()
            if widget is not None:
                widget.deleteLater()
            elif child_layout is not None:
                while child_layout.count():
                    child_item = child_layout.takeAt(0)
                    child_widget = child_item.widget()
                    if child_widget is not None:
                        child_widget.deleteLater()
        self._signature = ""
        if self._title_label is not None:
            self._title_label.clear()
            self._title_label.hide()
        if self._scroll is not None:
            self._scroll.setMaximumHeight(0)

    def set_markup(self, markup: Optional[Dict[str, Any]]) -> None:
        normalized = dict(markup or {}) if isinstance(markup, dict) else None
        signature = _reply_markup_signature(normalized)
        if self._signature == signature and normalized == self._markup:
            if normalized:
                self.show()
            else:
                self.hide()
            return
        self._markup = normalized
        self.clear_buttons()
        self._signature = signature if normalized else ""
        rows = list((self._markup or {}).get("rows") or [])
        markup_type = str((self._markup or {}).get("type") or "").strip().lower()
        if not rows:
            self.hide()
            return
        inline = markup_type == "inline"
        placeholder = str((self._markup or {}).get("placeholder") or "").strip()
        if self._title_label is not None:
            header_text = placeholder or "Клавиатура бота"
            self._title_label.setText(header_text.upper())
            self._title_label.setVisible(bool(header_text))
        css = (
            "QPushButton{background-color:rgba(89,183,255,0.16);border:1px solid rgba(89,183,255,0.32);"
            "border-radius:11px;color:#dff1ff;padding:7px 11px;font-size:12px;font-weight:600;text-align:center;}"
            "QPushButton:hover{background-color:rgba(89,183,255,0.24);}"
            "QPushButton:pressed{background-color:rgba(89,183,255,0.30);}"
        ) if inline else (
            "QPushButton{background-color:rgba(255,255,255,0.055);border:1px solid rgba(255,255,255,0.08);"
            "border-radius:13px;color:#dfe7f5;padding:10px 12px;font-size:12px;font-weight:600;text-align:left;}"
            "QPushButton:hover{background-color:rgba(255,255,255,0.10);}"
            "QPushButton:pressed{background-color:rgba(255,255,255,0.15);}"
        )
        built_rows = 0
        for row in rows:
            row_buttons = list(row or [])
            if not row_buttons:
                continue
            built_rows += 1
            line = QHBoxLayout()
            line.setContentsMargins(0, 0, 0, 0)
            line.setSpacing(8 if not inline else 6)
            for button_data in row_buttons:
                if not isinstance(button_data, dict):
                    continue
                raw_text = str(button_data.get("text") or "").strip() or "Кнопка"
                text = f"{_reply_markup_button_prefix(button_data)}{raw_text}".strip()
                btn = QPushButton(text, self)
                btn.setCursor(Qt.CursorShape.PointingHandCursor)
                btn.setStyleSheet(css)
                btn.setAutoDefault(False)
                btn.setDefault(False)
                btn.setFocusPolicy(Qt.FocusPolicy.NoFocus)
                btn.setMinimumHeight(34 if inline else 38)
                btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
                tooltip = _reply_markup_button_tooltip(button_data)
                if tooltip:
                    btn.setToolTip(tooltip)
                elif len(raw_text) > 32:
                    btn.setToolTip(raw_text)
                payload = dict(button_data)
                btn.clicked.connect(lambda _checked=False, p=payload: self.buttonActivated.emit(dict(p)))
                line.addWidget(btn, 1)
            self._layout.addLayout(line)
        if self._scroll is not None:
            visible_rows = max(1, min(built_rows, 4))
            base_height = 10 + (30 if self._title_label is not None and self._title_label.isVisible() else 0)
            self._scroll.setMaximumHeight((visible_rows * 46) + 6)
            self.setMaximumHeight(base_height + self._scroll.maximumHeight() + 20)
        self.show()


# -------------------------------- Bubble ---------------------------------

class Bubble(QWidget):
    """Пузырь сообщения со сглаженными углами."""

    _instances: "WeakSet[Bubble]" = WeakSet()
    _BODY_RADIUS = _bubble_radius()

    def __init__(self, text: str = "", role: str = "other", maxw: int = 720, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setObjectName("bubble")
        self._role = role
        self._align_right = role == "me"
        self._deleted = False
        self._has_hidden = False
        self._selected = False

        self._body = QVBoxLayout(self)
        self._body.setSpacing(3)
        self._status_icon = QLabel(DELETED_BROOM_EMOJI, self)
        self._status_icon.setObjectName("bubbleSweepIcon")
        self._status_icon.setStyleSheet(_style_sheet("message.bubble.deleted_icon", "color:#7e879b;font-size:12px;"))
        self._status_icon.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom)
        self._status_icon.hide()

        Bubble._instances.add(self)

        if text:
            self._body.addWidget(RichTextLabel(text, self))
        self._body.addWidget(self._status_icon, 0, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom)

        self.set_role(role)
        self.setMaximumWidth(maxw)
        try:
            self._update_status_icon()
        except Exception:
            pass

    def _current_theme(self) -> Dict[str, str]:
        base = dict(BUBBLE_THEME.get(self._role, BUBBLE_THEME["other"]))
        if self._deleted:
            base.update(DELETED_BUBBLE_THEME)
        return base

    def _content_slot_count(self) -> int:
        count = self._body.count()
        if self._status_icon is not None:
            count -= 1
        return max(0, count)

    def _update_body_margins(self) -> None:
        base = 8
        offset = 4
        left = base + (offset if not self._align_right else 0)
        right = base + (offset if self._align_right else 0)
        self._body.setContentsMargins(left, 6, right, 6)

    def _apply_styles(self) -> None:
        theme = self._current_theme()
        text = theme.get("text", "#f4f7ff")
        link = theme.get("link", "#59b7e9")
        css = _style_sheet(
            "message.bubble.label",
            "#bubble QLabel{color:$text; background-color:transparent;}#bubble QLabel a{color:$link; text-decoration:none;}",
            {"text": text, "link": link},
        )
        self.setStyleSheet(css)

    def set_role(self, role: str) -> None:
        self._role = role
        self._align_right = (role == "me")
        self._update_body_margins()
        self._apply_styles()
        self.update()

    def set_deleted(self, deleted: bool) -> None:
        self._deleted = bool(deleted)
        self._apply_styles()
        self._update_status_icon()
        self.update()

    def set_has_hidden(self, has_hidden: bool) -> None:
        self._has_hidden = bool(has_hidden)
        self._update_status_icon()
        self.update()

    def set_selected(self, selected: bool) -> None:
        selected = bool(selected)
        if self._selected == selected:
            return
        self._selected = selected
        self.update()

    def set_body_margins(self, left: int, top: int, right: int, bottom: int) -> None:
        self._body.setContentsMargins(int(left), int(top), int(right), int(bottom))

    def add_content(self, w: QWidget, *, at: Optional[int] = None) -> None:
        if not w:
            return
        limit = self._content_slot_count()
        if at is None:
            index = limit
        else:
            index = max(0, min(int(at), limit))
        self._body.insertWidget(index, w)

    def remove_content(self, w: QWidget) -> None:
        if not w:
            return
        self._body.removeWidget(w)
        w.setParent(None)

    def _update_status_icon(self) -> None:
        if not self._status_icon:
            return
        if self._deleted:
            self._status_icon.setText(DELETED_BROOM_EMOJI)
            self._status_icon.show()
        elif self._has_hidden:
            self._status_icon.setText("🔒")
            self._status_icon.show()
        else:
            self._status_icon.hide()

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        theme = self._current_theme()
        bg = QColor(theme.get("bg", "#1c1c1c"))
        border = QColor(theme.get("border", "#2a2a2a"))
        path = self._bubble_path()
        if path.isEmpty():
            painter.end()
            return
        painter.setBrush(bg)
        pen = QPen(border)
        pen.setWidthF(1.0)
        painter.setPen(pen)
        painter.drawPath(path)
        if self._selected:
            select_pen = QPen(QColor(89, 183, 233, 220))
            select_pen.setWidthF(1.8)
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.setPen(select_pen)
            painter.drawPath(path)
        painter.end()

    def _bubble_path(self) -> QPainterPath:
        rect = self.rect().adjusted(0.5, 0.5, -0.5, -0.5)
        if rect.width() <= 4 or rect.height() <= 4:
            return QPainterPath()
        radius = min(self._BODY_RADIUS, rect.width() / 2.0, rect.height() / 2.0)
        path = QPainterPath()
        path.addRoundedRect(rect, radius, radius)
        return path

    @classmethod
    def refresh_all(cls) -> None:
        cls._BODY_RADIUS = _bubble_radius()
        for bubble in list(cls._instances):
            try:
                bubble.set_role(bubble._role)
            except Exception:
                continue


class RadialDownloadWidget(QWidget):
    """Circular download indicator with arc progress and contextual icons."""

    DEFAULT_DIAMETER = 45
    clicked = Signal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._state: str = "idle"
        self._progress: float = 0.0
        self._diameter = self.DEFAULT_DIAMETER
        self.setFixedSize(self._diameter, self._diameter)
        self.setCursor(Qt.CursorShape.PointingHandCursor)

    def set_state(self, state: str, progress: Optional[float] = None) -> None:
        state = state or "idle"
        changed = state != self._state
        self._state = state
        if progress is not None:
            self.set_progress(progress)
        elif changed:
            self.update()

    def set_progress(self, progress: float) -> None:
        clamped = max(0.0, min(1.0, float(progress)))
        if abs(clamped - self._progress) > 0.001:
            self._progress = clamped
            self.update()

    def sizeHint(self) -> QSize:  # type: ignore[override]
        return QSize(self._diameter, self._diameter)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:  # type: ignore[override]
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mouseReleaseEvent(event)

    def paintEvent(self, event) -> None:  # type: ignore[override]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)

        diameter = float(min(self.width(), self.height()))
        pad = max(2.0, diameter * 0.08)
        pad_int = int(round(pad))
        rect = self.rect().adjusted(pad_int, pad_int, -pad_int, -pad_int)

        base = QColor(28, 40, 52, 230)
        accent = QColor(89, 183, 233)
        warn = QColor(226, 102, 114)

        painter.setBrush(base)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.drawEllipse(rect)

        arc_pen = max(2.0, diameter * 0.06)
        arc_pen_int = max(2, int(round(arc_pen)))
        if self._state in {"downloading", "paused"} and self._progress > 0.0:
            arc_rect = rect.adjusted(arc_pen_int, arc_pen_int, -arc_pen_int, -arc_pen_int)
            painter.setPen(QPen(accent, arc_pen_int))
            painter.setBrush(Qt.BrushStyle.NoBrush)
            start = 90 * 16
            span = -int(self._progress * 360 * 16)
            painter.drawArc(arc_rect, start, span)

        line_pen = max(2.0, diameter * 0.065)
        line_pen_int = max(2, int(round(line_pen)))
        pen_color = warn if self._state == "error" else accent
        painter.setPen(QPen(pen_color, line_pen_int, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin))
        center = rect.center()
        scale = diameter / 32.0 if diameter > 0 else 1.0

        if self._state == "completed":
            path = QPainterPath()
            path.moveTo(center.x() - 6 * scale, center.y())
            path.lineTo(center.x() - 1 * scale, center.y() + 5 * scale)
            path.lineTo(center.x() + 7 * scale, center.y() - 5 * scale)
            painter.drawPath(path)
        elif self._state == "error":
            painter.drawLine(center.x() - 5 * scale, center.y() - 5 * scale, center.x() + 5 * scale, center.y() + 5 * scale)
            painter.drawLine(center.x() + 5 * scale, center.y() - 5 * scale, center.x() - 5 * scale, center.y() + 5 * scale)
        else:
            shaft = 9 * scale
            wings = 5 * scale
            painter.drawLine(center.x(), center.y() - shaft / 2, center.x(), center.y() + shaft / 2)
            painter.drawLine(center.x(), center.y() + shaft / 2, center.x() - wings, center.y() + shaft / 2 - wings)
            painter.drawLine(center.x(), center.y() + shaft / 2, center.x() + wings, center.y() + shaft / 2 - wings)

        painter.end()


class ReplyPreviewWidget(QWidget):
    """Compact preview widget for replied messages."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(3, 2, 3, 2)
        layout.setSpacing(3)

        self._accent = QFrame()
        self._accent.setFixedWidth(3)
        self._accent.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        self._accent.setStyleSheet(_style_sheet("message.reply.accent", "background-color: rgba(89, 183, 233, 0.9); border-radius: 2px;"))
        layout.addWidget(self._accent, 0)

        body = QVBoxLayout()
        body.setContentsMargins(0, 0, 0, 0)
        body.setSpacing(1)

        self._sender_lbl = QLabel("")
        self._sender_lbl.setStyleSheet(_style_sheet("message.reply.sender", "font-weight:600; font-size:11px; color:#aacbf5;"))
        body.addWidget(self._sender_lbl, 0)

        self._context_lbl = QLabel("")
        self._context_lbl.setWordWrap(True)
        self._context_lbl.setStyleSheet(_style_sheet("message.reply.context", "font-size:10px; color:#59b7e9;"))
        self._context_lbl.hide()
        body.addWidget(self._context_lbl, 0)

        self._text_lbl = QLabel("")
        self._text_lbl.setWordWrap(True)
        self._text_lbl.setStyleSheet(_style_sheet("message.reply.text", "font-size:11px; color:#d5dbe6;"))
        body.addWidget(self._text_lbl, 0)

        layout.addLayout(body, 1)
        layout.addStretch(1)

        self._data: Dict[str, Any] = {}

    def set_data(self, data: Dict[str, Any]) -> None:
        self._data = dict(data)
        sender = str(data.get("sender") or "")
        text = str(data.get("text") or "")
        kind = str(data.get("kind") or "text")
        is_deleted = bool(data.get("is_deleted", False))

        snippet = self._format_snippet(text, kind)
        if is_deleted and snippet:
            snippet = f"[deleted] {snippet}"
        elif is_deleted:
            snippet = "[deleted]"

        self._sender_lbl.setText(sender or "Сообщение")
        self._text_lbl.setText(snippet or "")
        if is_deleted:
            self._text_lbl.setStyleSheet(_style_sheet("message.reply.text_deleted", "font-size:11px; color:#ff9aa0; font-style:italic;"))
        else:
            self._text_lbl.setStyleSheet(_style_sheet("message.reply.text", "font-size:11px; color:#d5dbe6;"))

        context = self._format_forward_context(data.get("forward_info"))
        if context:
            self._context_lbl.setText(context)
            self._context_lbl.show()
        else:
            self._context_lbl.hide()

    def data(self) -> Dict[str, Any]:
        return dict(self._data)

    @staticmethod
    def _format_snippet(text: str, kind: str) -> str:
        base = (text or "").strip()
        if not base:
            mapping = {
                "image": "[изображение]",
                "animation": "[анимация]",
                "video": "[видео]",
                "video_note": "[кружочек]",
                "audio": "[аудио]",
                "voice": "[голосовое сообщение]",
                "sticker": "[стикер]",
                "document": "[документ]",
                "file": "[файл]",
            }
            base = mapping.get(kind, "")
        if len(base) > 40:
            base = base[:37] + "..."
        return base

    @staticmethod
    def _format_forward_context(info: Optional[Dict[str, Any]]) -> str:
        if not isinstance(info, dict):
            return ""
        sender = str(info.get("sender") or "").strip()
        chat = str(info.get("chat") or "").strip()
        if sender and chat:
            return f"Переслано от {sender} → {chat}"
        if sender:
            return f"Переслано от {sender}"
        if chat:
            return f"Переслано из {chat}"
        return ""


class ForwardInfoWidget(QWidget):
    """Small striped header describing forwarded messages."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)

        icon = QLabel("↪")
        icon.setStyleSheet(_style_sheet("message.forward.icon", "color:#59b7e9;font-size:12px;"))
        layout.addWidget(icon, 0, Qt.AlignmentFlag.AlignTop)

        self._text = QLabel("")
        self._text.setWordWrap(True)
        self._text.setStyleSheet(_style_sheet("message.forward.text", "color:#59b7e9;font-size:11px;"))
        layout.addWidget(self._text, 1)

    def set_info(self, info: Dict[str, Any]) -> None:
        sender = str(info.get("sender") or "").strip()
        chat = str(info.get("chat") or "").strip()
        if sender and chat:
            self._text.setText(f"Переслано от {sender} → {chat}")
        elif sender:
            self._text.setText(f"Переслано от {sender}")
        elif chat:
            self._text.setText(f"Переслано из {chat}")
        else:
            self._text.setText("Пересланное сообщение")


class TextMessageWidget(QWidget):
    """Widget that renders plain text messages with optional reply preview."""
    commandActivated = Signal(str)
    replyMarkupButtonActivated = Signal(dict)

    def __init__(
        self,
        header: str,
        text: str,
        *,
        role: str = "other",
        entities: Optional[List[Dict[str, Any]]] = None,
        chat_id: Optional[str] = None,
        msg_id: Optional[int] = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.role = role
        self.kind = "text"
        self.chat_id = chat_id
        self.msg_id = msg_id
        self._show_header_label = not (
            self.role == "me" and str(header or "").strip().lower() in {"", "вы", "you", "me"}
        )
        self._original_text = text
        self._text_entities: Optional[List[Dict[str, Any]]] = list(entities) if isinstance(entities, list) else None
        self._reply_widget: Optional[ReplyPreviewWidget] = None
        self._reply_data: Optional[Dict[str, Any]] = None
        self._forward_widget: Optional[ForwardInfoWidget] = None
        self._forward_data: Optional[Dict[str, Any]] = None
        self._reply_markup_widget: Optional[MessageReplyMarkupWidget] = None
        self._reply_markup_data: Optional[Dict[str, Any]] = None
        self._is_deleted = False
        self._custom_header_color: Optional[str] = None
        self._content_hidden = False
        self._has_hidden = False
        self._selected = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(4)

        self.header_label = QLabel(f"<b>{header}</b>" if self._show_header_label else "")
        self.header_label.setTextFormat(Qt.TextFormat.RichText)
        self._refresh_header_style()
        alignment = Qt.AlignmentFlag.AlignRight if role == "me" else Qt.AlignmentFlag.AlignLeft
        root.addWidget(self.header_label, 0, alignment)
        self.header_label.setVisible(self._show_header_label)

        self.deleted_label = QLabel("Сообщение удалено")
        self.deleted_label.setStyleSheet(_style_sheet("message.deleted.label", "color:#ff9aa0;font-size:11px;"))
        self.deleted_label.hide()
        root.addWidget(self.deleted_label, 0, alignment)

        self.hidden_label = QLabel("Содержимое скрыто")
        self.hidden_label.setStyleSheet(_style_sheet("message.hidden.label", "color:#9fa6b1;font-size:11px;"))
        self.hidden_label.hide()
        root.addWidget(self.hidden_label, 0, alignment)

        self.bubble = Bubble("", role, parent=self)
        root.addWidget(self.bubble, 0)
        self._message_label = RichTextLabel(text, self)
        self._message_label.setStyleSheet(_style_sheet("message.body.rich_text", "background-color: transparent;"))
        self._message_label.commandActivated.connect(self.commandActivated.emit)
        if self._text_entities:
            self._message_label.set_message(text, entities=self._text_entities)
        self.bubble.add_content(self._message_label)
        if self.kind in {"document", "file"}:
            self.bubble.set_body_margins(8, 6, 8, 6)

    def set_has_hidden(self, has_hidden: bool) -> None:
        self._has_hidden = bool(has_hidden)
        if getattr(self, "bubble", None):
            try:
                self.bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass

    def set_selected(self, selected: bool) -> None:
        self._selected = bool(selected)
        if getattr(self, "bubble", None):
            try:
                self.bubble.set_selected(self._selected)
            except Exception:
                pass

    def _header_style(self) -> str:
        if getattr(self, "_is_deleted", False):
            return _style_sheet("message.header.deleted", "color:#7e879b;font-size:12px;font-weight:600;")
        if self._custom_header_color and self.role != "me":
            return f"color:{self._custom_header_color};font-size:12px;font-weight:600;"
        key = "message.header.me" if self.role == "me" else "message.header.other"
        default = "color:#d5e4ff;font-size:12px;font-weight:600;" if self.role == "me" else "color:#bfc8d6;font-size:12px;font-weight:600;"
        return _style_sheet(key, default)

    def _refresh_header_style(self) -> None:
        self.header_label.setStyleSheet(self._header_style())

    def set_header_color(self, color: Optional[str]) -> None:
        self._custom_header_color = color
        self._refresh_header_style()

    def set_reply_preview(self, data: Optional[Dict[str, Any]]) -> None:
        self._reply_data = dict(data) if data else None
        if not data:
            if self._reply_widget:
                self.bubble.remove_content(self._reply_widget)
                self._reply_widget.deleteLater()
                self._reply_widget = None
            return
        if not self._reply_widget:
            self._reply_widget = ReplyPreviewWidget(self)
        index = 1 if self._forward_widget else 0
        self.bubble.add_content(self._reply_widget, at=index)
        self._reply_widget.set_data(data)

    def refresh_reply_preview(self) -> None:
        if self._reply_data and self._reply_widget:
            self._reinsert_reply_widget()
            self._reply_widget.set_data(self._reply_data)

    def set_forward_info(self, data: Optional[Dict[str, Any]]) -> None:
        self._forward_data = dict(data) if data else None
        if not data:
            if self._forward_widget:
                self.bubble.remove_content(self._forward_widget)
                self._forward_widget.deleteLater()
                self._forward_widget = None
                self._reinsert_reply_widget()
            return
        if not self._forward_widget:
            self._forward_widget = ForwardInfoWidget(self)
        self.bubble.add_content(self._forward_widget, at=0)
        self._forward_widget.set_info(data)
        self._reinsert_reply_widget()

    def set_reply_markup(self, data: Optional[Dict[str, Any]]) -> None:
        markup = dict(data) if isinstance(data, dict) else None
        if markup and str(markup.get("type") or "").strip().lower() != "inline":
            markup = None
        self._reply_markup_data = markup
        if not markup:
            if self._reply_markup_widget:
                self.bubble.remove_content(self._reply_markup_widget)
                self._reply_markup_widget.deleteLater()
                self._reply_markup_widget = None
            return
        if not self._reply_markup_widget:
            self._reply_markup_widget = MessageReplyMarkupWidget("inline", self)
            self._reply_markup_widget.buttonActivated.connect(self._emit_reply_markup_action)
        self.bubble.remove_content(self._reply_markup_widget)
        self.bubble.add_content(self._reply_markup_widget)
        self._reply_markup_widget.set_markup(markup)

    def set_search_query(self, query: str, *, active: bool = False) -> None:
        if self._message_label:
            self._message_label.set_search(query, active=active)

    def set_message_text(self, text: str, *, entities: Optional[List[Dict[str, Any]]] = None) -> None:
        self._original_text = text
        if entities is not None:
            self._text_entities = list(entities) if isinstance(entities, list) else None
        if not self._content_hidden:
            self._message_label.set_message(self._original_text, entities=self._text_entities)

    def set_content_hidden(self, hidden: bool) -> None:
        if self._content_hidden == hidden:
            return
        self._content_hidden = hidden
        if hidden:
            self.bubble.hide()
            self.hidden_label.show()
        else:
            self.hidden_label.hide()
            self.bubble.show()
            self._message_label.set_message(self._original_text, entities=self._text_entities)

    def _reinsert_reply_widget(self) -> None:
        if self._reply_widget and self._reply_data:
            self.bubble.remove_content(self._reply_widget)
            idx = 1 if self._forward_widget else 0
            self.bubble.add_content(self._reply_widget, at=idx)

    @Slot(dict)
    def _emit_reply_markup_action(self, action: Dict[str, Any]) -> None:
        payload = dict(action or {})
        if self.chat_id:
            payload.setdefault("chat_id", str(self.chat_id))
        if self.msg_id is not None:
            payload.setdefault("message_id", int(self.msg_id))
        self.replyMarkupButtonActivated.emit(payload)

    def set_deleted(self, deleted: bool) -> None:
        self._is_deleted = bool(deleted)
        self.deleted_label.setVisible(self._is_deleted)
        self._refresh_header_style()
        if hasattr(self, "bubble") and self.bubble:
            self.bubble.set_deleted(self._is_deleted)
            try:
                self.bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass
        if self._message_label and not self._content_hidden:
            self._message_label.setEnabled(True)
            self._message_label.set_message(self._original_text, entities=self._text_entities)
        if self._reply_widget and self._reply_data:
            self.refresh_reply_preview()

    def is_deleted(self) -> bool:
        return self._is_deleted

    def safe_dispose(self) -> None:
        if self._reply_widget:
            try:
                self._reply_widget.deleteLater()
            except Exception:
                pass
        if self._forward_widget:
            try:
                self._forward_widget.deleteLater()
            except Exception:
                pass
        if self._reply_markup_widget:
            try:
                self._reply_markup_widget.deleteLater()
            except Exception:
                pass


class VoiceWaveformWidget(QWidget):
    """Waveform view with progress highlight and optional scrubbing."""

    seekRequested = Signal(float)

    def __init__(self, seed: int = 0, parent: QWidget | None = None):
        super().__init__(parent)
        self._bars = self._generate_bars(seed)
        self._progress: float = 0.0
        self._inactive_color = QColor(95, 105, 125, 130)
        self._active_color = QColor(122, 184, 255, 210)
        self.setMinimumHeight(38)
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self._seek_handler: Optional[Callable[[float], None]] = None
        self._dragging = False
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setMouseTracking(True)

    def set_progress(self, ratio: float) -> None:
        ratio = 0.0 if ratio is None else max(0.0, min(1.0, float(ratio)))
        if abs(self._progress - ratio) > 0.005:
            self._progress = ratio
            self.update()

    def set_seed(self, seed: int) -> None:
        self._bars = self._generate_bars(seed)
        self.update()

    def set_samples(self, samples: Optional[Sequence[int]]) -> None:
        if not samples:
            return
        normalized = []
        for value in samples:
            try:
                normalized.append(max(0, min(255, int(value))))
            except Exception:
                continue
        if not normalized:
            return
        target = max(32, min(96, len(normalized)))
        if len(normalized) != target:
            resampled: List[int] = []
            for i in range(target):
                start = int(i * len(normalized) / target)
                end = int((i + 1) * len(normalized) / target)
                if end <= start:
                    end = min(len(normalized), start + 1)
                segment = normalized[start:end]
                resampled.append(sum(segment) // len(segment))
            normalized = resampled
        if len(normalized) == 1:
            normalized = normalized * 2
        self._bars = [max(0, min(100, int(val / 255.0 * 100))) for val in normalized]
        self.update()

    def set_seek_handler(self, handler: Optional[Callable[[float], None]]) -> None:
        self._seek_handler = handler

    def _generate_bars(self, seed: int) -> list[int]:
        import random

        rng = random.Random(seed or 1)
        count = 36
        return [rng.randint(25, 95) for _ in range(count)]

    def paintEvent(self, event) -> None:
        qp = QPainter(self)
        qp.setRenderHint(QPainter.RenderHint.Antialiasing, True)

        count = len(self._bars)
        if count <= 1:
            qp.end()
            return

        width = float(self.width())
        height = float(self.height())
        mid = height / 2.0
        max_amp = max(6.0, (height / 2.0) - 4.0)
        step = width / float(count - 1)

        top_points: List[QPointF] = []
        bottom_points: List[QPointF] = []
        for idx, value in enumerate(self._bars):
            amp = max(0.0, min(1.0, value / 100.0)) * max_amp
            x = step * idx
            top_points.append(QPointF(x, mid - amp))
            bottom_points.append(QPointF(x, mid + amp))

        path = QPainterPath(QPointF(0.0, mid))
        for pt in top_points:
            path.lineTo(pt)
        for pt in reversed(bottom_points):
            path.lineTo(pt)
        path.closeSubpath()

        qp.save()
        qp.setClipRect(0, 0, self.width(), self.height())
        qp.fillPath(path, self._inactive_color)
        qp.restore()

        active_width = max(0.0, min(1.0, self._progress)) * width
        if active_width > 0.5:
            qp.save()
            qp.setClipRect(0, 0, int(active_width), self.height())
            qp.fillPath(path, self._active_color)
            qp.restore()
        qp.end()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self._dragging = True
            self._seek_from_pos(float(event.position().x()))
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        if self._dragging and (event.buttons() & Qt.MouseButton.LeftButton):
            self._seek_from_pos(float(event.position().x()))
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        if event.button() == Qt.MouseButton.LeftButton and self._dragging:
            self._dragging = False
            self._seek_from_pos(float(event.position().x()))
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def _seek_from_pos(self, x: float) -> None:
        width = max(1.0, float(self.width()))
        ratio = max(0.0, min(1.0, x / width))
        handler = self._seek_handler
        if handler:
            handler(ratio)
        else:
            self.seekRequested.emit(ratio)


# -------------------------- Элемент ленты: медиа --------------------------

class ChatItemWidget(MediaRenderingMixin, QWidget):
    commandActivated = Signal(str)
    replyMarkupButtonActivated = Signal(dict)

    def __init__(
        self,
        kind: str,
        header: str,
        *,
        text: str = "",
        text_entities: Optional[List[Dict[str, Any]]] = None,
        role: str = "other",
        file_path: Optional[str] = None,
        chat_id: Optional[str] = None,
        msg_id: Optional[int] = None,
        server=None,
        thumb_path: Optional[str] = None,
        file_size: Optional[int] = None,
        voice_waveform: bool = True,
        duration_ms: Optional[int] = None,
        waveform: Optional[List[int]] = None,
    ):
        super().__init__()
        self.kind = self._normalize_kind(kind)
        self.header = header
        self.role = role
        self._show_header_label = not (
            self.role == "me" and str(header or "").strip().lower() in {"", "вы", "you", "me"}
        )
        self.text = text
        self.text_entities: Optional[List[Dict[str, Any]]] = list(text_entities) if isinstance(text_entities, list) else None
        self.file_path = file_path
        self.chat_id = chat_id
        self.msg_id = msg_id
        self.server = server
        self.thumb_path = thumb_path
        self.file_size = int(file_size or 0)
        self._waveform_samples: Optional[List[int]] = list(waveform) if waveform else None
        self._voice_duration_ms: Optional[int] = int(duration_ms) if duration_ms else None

        # runtime
        self._bg_threads = []
        self._disposed = False
        self.on_media_activate: Optional[Callable[[Dict[str, Any]], bool]] = None
        try:
            self.destroyed.connect(self._on_widget_destroyed)
        except Exception:
            pass
        self._thumb_cb = None
        self._video_is_circular = False
        self.lbl_img = None
        self.lbl_anim = None
        self.preview = None
        self.video_w = None
        self.player = None
        self._audio_output = None
        self._container_layout = None
        self.play_button = None
        self._time_lbl: Optional[QLabel] = None
        self._audio_meta_lbl: Optional[QLabel] = None
        self._audio_is_voice = False
        self._document_meta_lbl: Optional[QLabel] = None
        self._document_title_lbl: Optional[QLabel] = None
        self._document_status_lbl: Optional[QLabel] = None
        self._caption_label: Optional[RichTextLabel] = None
        self._caption_bubble: Optional[Bubble] = None
        self._radial_doc_widget: Optional[RadialDownloadWidget] = None
        # Voice playback: QtMultimedia on Windows often can't decode OGG/Opus, so decode to WAV on demand.
        self._voice_decoded_path: Optional[str] = None
        self._voice_decode_thread: Optional[QThread] = None
        self._voice_decode_worker: Optional[FfmpegConvertWorker] = None
        self._voice_decode_autoplay: bool = False

        self.download_job_id: Optional[str] = None
        self.download_state: str = "idle"
        self.download_progress: int = 0
        self.download_total: int = self.file_size
        self._reply_widget: Optional[ReplyPreviewWidget] = None
        self._reply_data: Optional[Dict[str, Any]] = None
        self._forward_widget: Optional[ForwardInfoWidget] = None
        self._forward_data: Optional[Dict[str, Any]] = None
        self._reply_markup_widget: Optional[MessageReplyMarkupWidget] = None
        self._reply_markup_data: Optional[Dict[str, Any]] = None
        self._custom_header_color: Optional[str] = None
        self._content_layout: Optional[QVBoxLayout] = None
        self._deleted_placeholder: Optional[QLabel] = None
        self.deleted_label: Optional[QLabel] = None
        self._is_deleted: bool = False
        self._voice_wave_widget: Optional["VoiceWaveformWidget"] = None
        self._waveform_enabled: bool = bool(voice_waveform)
        self._selected: bool = False
        self._media_group_position: str = "single"
        self._root_layout: Optional[QVBoxLayout] = None

        # политика внешних кнопок (вторая «Скачать» под баблом)
        self._external_controls_allowed: bool = self.kind not in {"document", "audio", "voice"}
        self._content_hidden = False
        self._hidden_placeholder: Optional[QLabel] = None
        self._content_wrap: Optional[QWidget] = None
        self._has_hidden = False

        root = QVBoxLayout(self)
        root.setContentsMargins(8, 8, 8, 8)
        root.setSpacing(6)
        self._root_layout = root

        header_lbl = QLabel(f"<b>{header}</b>" if self._show_header_label else "")
        alignment = Qt.AlignmentFlag.AlignRight if role == "me" else Qt.AlignmentFlag.AlignLeft
        root.addWidget(header_lbl, 0, alignment)
        self.header_label = header_lbl
        self.header_label.setVisible(self._show_header_label)
        self._header_color: Optional[str] = None
        self._refresh_header_style()

        self.deleted_label = QLabel("Сообщение удалено")
        self.deleted_label.setStyleSheet(_style_sheet("message.deleted.label", "color:#ff9aa0;font-size:11px;"))
        self.deleted_label.hide()
        root.addWidget(self.deleted_label, 0, alignment)

        # --- контейнер: внутри контент, опционально обёрнутый в bubble ---
        self.bubble = None
        content_wrap = QWidget()
        content_wrap.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, False)
        content_wrap.setStyleSheet(_style_sheet("message.body.container", "background-color: transparent;"))
        self._content_layout = QVBoxLayout(content_wrap)
        self._content_layout.setContentsMargins(0, 0, 0, 0)
        self._content_layout.setSpacing(4)
        self._content_wrap = content_wrap

        wrap_in_bubble = self.kind not in {"video_note", "sticker", "image"}
        if self.kind in {"animation", "video"}:
            wrap_in_bubble = True
        if wrap_in_bubble:
            self.bubble = Bubble("", role, parent=self)
            if self.kind in {"video", "animation"}:
                self.bubble.set_body_margins(0, 0, 0, 0)
            self.bubble.add_content(content_wrap)
            root.addWidget(self.bubble)
        else:
            root.addWidget(content_wrap)

        target_layout = self._content_layout
        if self.kind == "image":
            self._render_image(target_layout)
        elif self.kind == "sticker":
            self._render_sticker(target_layout)
        elif self.kind == "animation":
            self._render_animation(target_layout)
        elif self.kind == "video":
            self._render_video(target_layout)
        elif self.kind == "video_note":
            self._render_video(target_layout, circular=True)
        elif self.kind == "audio":
            self._render_audio(target_layout, voice=False)
        elif self.kind == "voice":
            self._render_audio(target_layout, voice=True)
        elif self.kind in {"document", "file"}:
            self._render_document(target_layout)
        else:
            placeholder = QLabel("Медиа не поддерживается")
            placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
            target_layout.addWidget(placeholder)

        if self.text.strip():
            self._ensure_caption_label()

        self.progress_bar: Optional[QProgressBar] = None
        if self.kind not in {"document", "file"}:
            self._ensure_progress_bar()
            if self.progress_bar:
                self.progress_bar.setRange(0, 100)
                self.progress_bar.setValue(0)
                self.progress_bar.hide()
                root.addWidget(self.progress_bar)

        # внешние кнопки управления (вторая панель)
        controls = QHBoxLayout()
        controls.setContentsMargins(0, 0, 0, 0)
        controls.setSpacing(6)

        self.status_label = QLabel("")
        self.status_label.setStyleSheet(_style_sheet("message.status.label", "color:#9fa6b1;font-size:11px;"))
        self.status_label.hide()
        controls.addWidget(self.status_label)
        controls.addStretch(1)

        self.btn_cancel = QToolButton()
        self.btn_cancel.setText("Отмена")
        self.btn_cancel.clicked.connect(self._on_cancel_clicked)
        self.btn_cancel.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        controls.addWidget(self.btn_cancel)

        self.btn_primary = QToolButton()
        self.btn_primary.clicked.connect(self._on_primary_clicked)
        self.btn_primary.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        controls.addWidget(self.btn_primary)

        root.addLayout(controls)

        self._hidden_placeholder = QLabel("Содержимое скрыто")
        self._hidden_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._hidden_placeholder.setStyleSheet(_style_sheet("message.hidden.label", "color:#9fa6b1;font-size:11px;"))
        self._hidden_placeholder.hide()
        root.addWidget(self._hidden_placeholder, 0, Qt.AlignmentFlag.AlignCenter)

        # если внешние кнопки запрещены — сразу спрячем
        if not self._external_controls_allowed:
            self.btn_primary.hide()
            self.btn_cancel.hide()

        # автосостояние
        if self.file_path and os.path.isfile(self.file_path):
            self.show_downloaded_media(self.kind, self.file_path)
            self._set_download_state("completed")
        else:
            self._set_download_state("idle")

    # ---------- helpers ----------
    @staticmethod
    def _normalize_kind(kind: str) -> str:
        k = (kind or "").lower()
        mapping = {
            "photo": "image",
            "gif": "animation",
            "voice_message": "voice",
            "voice": "voice",
            "audio": "audio",
            "song": "audio",
            "music": "audio",
            "document": "document",
            "file": "document",
        }
        return mapping.get(k, k)

    @staticmethod
    def _format_file_size(size: int) -> str:
        if size <= 0:
            return ""
        units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
        value = float(size)
        idx = 0
        while value >= 1024 and idx < len(units) - 1:
            value /= 1024.0
            idx += 1
        if idx == 0:
            return f"{int(value)} {units[idx]}"
        return f"{value:.1f} {units[idx]}"

    def _ensure_progress_bar(self) -> None:
        """Создаёт progress_bar один раз и добавляет в layout (скрыт по умолчанию)."""
        if getattr(self, "progress_bar", None) is not None:
            return
        self.progress_bar = QProgressBar(self)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(4)
        self.progress_bar.setVisible(False)

        lay = self.layout()
        if lay is None:
            lay = QVBoxLayout(self)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.setSpacing(4)
            self.setLayout(lay)
        lay.addWidget(self.progress_bar)

    # ---------- renderers ----------
    @staticmethod
    def _prepare_media_container(widget: QWidget) -> QWidget:
        widget.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, False)
        widget.setStyleSheet(_style_sheet("message.media.container", "background-color: transparent;"))
        return widget

    def _render_audio(self, lay: QVBoxLayout, *, voice: bool) -> None:
        self._container_layout = lay
        container = self._prepare_media_container(QWidget())
        container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        container.setMinimumWidth(360)
        row = QHBoxLayout(container)
        row.setContentsMargins(12, 6, 12, 6)
        row.setSpacing(10)
        row.setAlignment(Qt.AlignmentFlag.AlignVCenter)

        play_btn = QToolButton()
        play_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        play_btn.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        play_btn.clicked.connect(self._toggle_audio_play)
        self.play_button = play_btn
        self._audio_is_voice = voice

        if voice:
            play_btn.setText("▶")
            play_btn.setStyleSheet(
                _style_sheet(
                    "message.audio.voice_button",
                    "QToolButton { background-color:rgba(89,183,233,0.18); border:none; border-radius:20px; padding:6px 10px; color:#59b7e9; font-size:18px; }QToolButton:hover { background-color:rgba(89,183,233,0.28); }",
                )
            )
            play_btn.setFixedSize(48, 48)
            row.addWidget(play_btn, 0, Qt.AlignmentFlag.AlignTop)

            wave_column = QVBoxLayout()
            wave_column.setContentsMargins(0, 0, 0, 0)
            wave_column.setSpacing(4)

            seed = int(self.msg_id or self.file_size or 0)
            self._voice_wave_widget = VoiceWaveformWidget(seed=seed, parent=self)
            self._voice_wave_widget.setVisible(self._waveform_enabled)
            self._voice_wave_widget.set_seek_handler(self._on_voice_wave_seek)
            if self._waveform_samples:
                self._voice_wave_widget.set_samples(self._waveform_samples)
            wave_column.addWidget(self._voice_wave_widget, 1)

            total_ms = self._voice_duration_ms
            initial_total = _fmt_time(total_ms) if total_ms else "00:00"
            self._time_lbl = QLabel(f"00:00 / {initial_total}", self)
            self._time_lbl.setStyleSheet(_style_sheet("message.audio.time_label", "color:#9fa6b1;font-size:11px;"))
            wave_column.addWidget(self._time_lbl, 0, Qt.AlignmentFlag.AlignLeft)
            row.addLayout(wave_column, 1)
        else:
            icon = QLabel("🎧")
            icon.setStyleSheet(_style_sheet("message.audio.file_icon", "font-size:24px;"))
            row.addWidget(icon, 0, Qt.AlignmentFlag.AlignTop)

            info_layout = QVBoxLayout()
            title = os.path.basename(self.file_path) if self.file_path else "Аудиофайл"
            title_lbl = QLabel(title)
            title_lbl.setStyleSheet(_style_sheet("message.audio.title", "color:#f4f7ff;font-weight:bold;"))
            info_layout.addWidget(title_lbl)

            meta_parts = []
            if self.file_size:
                meta_parts.append(self._format_file_size(self.file_size))
            if self.file_path:
                meta_parts.append(os.path.basename(self.file_path))
            meta_lbl = QLabel(" • ".join([m for m in meta_parts if m]))
            meta_lbl.setStyleSheet(_style_sheet("message.audio.meta", "color:#9fa6b1;font-size:11px;"))
            info_layout.addWidget(meta_lbl)
            info_layout.addStretch(1)
            row.addLayout(info_layout, 1)
            self._audio_meta_lbl = meta_lbl

            play_btn.setText("▶")
            play_btn.setToolTip("Воспроизвести" if self.file_path and os.path.isfile(self.file_path) else "Скачать и воспроизвести")
            row.addWidget(play_btn, 0, Qt.AlignmentFlag.AlignTop)

        lay.addWidget(container)
        if voice:
            if not (self.file_path and os.path.isfile(self.file_path)):
                play_btn.setToolTip("Скачать и воспроизвести")
            else:
                play_btn.setToolTip("Воспроизвести")

    def _render_document(self, lay: QVBoxLayout) -> None:
        self._container_layout = lay
        container = QWidget()
        row = QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)

        self._radial_doc_widget = RadialDownloadWidget(self)
        self._radial_doc_widget.clicked.connect(self._on_doc_radial_clicked)
        row.addWidget(self._radial_doc_widget, 0, Qt.AlignmentFlag.AlignTop)

        info_layout = QVBoxLayout()
        info_layout.setContentsMargins(0, 0, 0, 0)
        info_layout.setSpacing(3)
        title = os.path.basename(self.file_path) if self.file_path else "Документ"
        title_lbl = QLabel(title)
        title_lbl.setStyleSheet(_style_sheet("message.document.title", "color:#f4f7ff;font-weight:600;font-size:13px;"))
        title_lbl.setWordWrap(True)
        info_layout.addWidget(title_lbl)

        meta_parts = []
        if self.file_size:
            meta_parts.append(self._format_file_size(self.file_size))
        if self.file_path:
            meta_parts.append(os.path.basename(self.file_path))
        meta_lbl = QLabel(" • ".join([m for m in meta_parts if m]))
        meta_lbl.setStyleSheet(_style_sheet("message.document.meta", "color:#9fa6b1;font-size:11px;"))
        info_layout.addWidget(meta_lbl)

        status_lbl = QLabel("Нажмите, чтобы скачать")
        status_lbl.setStyleSheet(_style_sheet("message.document.status", "color:#768c9e;font-size:10px;"))
        info_layout.addWidget(status_lbl)

        row.addLayout(info_layout, 1)

        lay.addWidget(container)
        self._document_meta_lbl = meta_lbl
        self._document_title_lbl = title_lbl
        self._document_status_lbl = status_lbl
        if self.file_path and os.path.isfile(self.file_path):
            self._radial_doc_widget.set_state("completed", 1.0)
            self._set_doc_status_text("Файл готов")
        else:
            self._radial_doc_widget.set_state("idle", 0.0)
            self._set_doc_status_text("Нажмите, чтобы скачать")

    def _render_sticker(self, lay: QVBoxLayout) -> None:
        self._container_layout = lay
        self.lbl_sticker = QLabel("Стикер не загружен", parent=self)
        self.lbl_sticker.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_sticker.setMinimumSize(self.MIN_IMAGE_DIM, self.MIN_IMAGE_DIM)
        self.lbl_sticker.setStyleSheet(_style_sheet("message.sticker.placeholder", "color:#9fa6b1;font-size:11px;"))
        lay.addWidget(self.lbl_sticker, 0, Qt.AlignmentFlag.AlignHCenter)

        path: Optional[str] = None
        if self.file_path and os.path.isfile(self.file_path):
            path = self.file_path
        elif self.thumb_path and os.path.isfile(self.thumb_path):
            path = self.thumb_path
        if path:
            self._set_sticker_pix(path)
        elif self.server and self.chat_id and self.msg_id is not None:
            self._start_thumb(lambda p: self._set_sticker_pix(p))

    def _set_sticker_pix(self, path: str) -> None:
        lbl = getattr(self, "lbl_sticker", None)
        if not lbl:
            return
        if not path or not os.path.isfile(path):
            lbl.setText("Файл не найден")
            return
        pix = QPixmap(path)
        if pix.isNull():
            lbl.setText("Стикер не поддерживается")
            return
        max_side = int(_STYLE_MGR.metric("message.sticker.max_side", 240) or 240)
        target = QSize(max_side, max_side)
        scaled = pix.scaled(
            target,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        lbl.setFixedSize(scaled.size())
        lbl.setPixmap(scaled)
        lbl.setText("")
        self._frame_size = scaled.size()

    # ---------- actions ----------
    def _toggle_audio_play(self) -> None:
        if not (self.file_path and os.path.isfile(self.file_path)):
            self._start_download()
            return
        if self._audio_is_voice:
            self._toggle_voice_play()
            return
        player = self._ensure_player()
        if not player:
            return
        source = QUrl.fromLocalFile(self.file_path)
        current_source = player.source()
        if current_source != source:
            player.setSource(source)
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

    @staticmethod
    def _voice_decode_cache_dir() -> str:
        base = os.path.join(os.path.expanduser("~"), ".drago_gui", "voice_decode_cache")
        os.makedirs(base, exist_ok=True)
        return base

    def _voice_decode_dst_path(self, src_path: str) -> str:
        try:
            st = os.stat(src_path)
            key = f"{src_path}|{int(st.st_size)}|{int(st.st_mtime)}"
        except Exception:
            key = src_path
        digest = hashlib.sha1(key.encode("utf-8", "ignore")).hexdigest()
        return os.path.join(self._voice_decode_cache_dir(), f"voice_{digest}.wav")

    def _set_voice_status(self, text: str) -> None:
        lbl = getattr(self, "status_label", None)
        if lbl is None or not _qt_is_valid(lbl):
            return
        try:
            lbl.setText(text)
            lbl.show()
        except Exception:
            pass

    def _clear_voice_status(self) -> None:
        lbl = getattr(self, "status_label", None)
        if lbl is None or not _qt_is_valid(lbl):
            return
        try:
            lbl.setText("")
            lbl.hide()
        except Exception:
            pass

    def _toggle_voice_play(self) -> None:
        # If already playing, just pause.
        player = self._ensure_player()
        if not player:
            return
        if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            try:
                player.pause()
            except Exception:
                pass
            return

        src = str(self.file_path or "")
        if not src or not os.path.isfile(src):
            return

        # If the source is already WAV, play directly.
        if os.path.splitext(src)[1].lower() == ".wav":
            self._play_audio_path(src)
            return

        # Reuse cached decoded WAV if available.
        decoded = self._voice_decoded_path or self._voice_decode_dst_path(src)
        if decoded and os.path.isfile(decoded) and os.path.getsize(decoded) > 0:
            self._voice_decoded_path = decoded
            self._play_audio_path(decoded)
            return

        # Decode to WAV in background via ffmpeg.
        ffmpeg = _resolve_ffmpeg_binary()
        if not ffmpeg:
            self._set_voice_status("ffmpeg не найден — воспроизведение голосовых недоступно.")
            return

        th = getattr(self, "_voice_decode_thread", None)
        try:
            if th is not None and th.isRunning():
                # Already decoding; keep waiting.
                return
        except Exception:
            pass

        cmd = [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            src,
            "-vn",
            "-acodec",
            "pcm_s16le",
            "-ar",
            "48000",
            "-ac",
            "1",
            decoded,
        ]

        self._voice_decode_autoplay = True
        self._set_voice_status("Подготавливаю аудио…")
        if self.play_button and _qt_is_valid(self.play_button):
            try:
                self.play_button.setEnabled(False)
                self.play_button.setText("…")
            except Exception:
                pass

        thread = QThread(self)
        try:
            thread.setObjectName(f"voice_decode_thread_{int(self.msg_id or 0)}")
        except Exception:
            thread.setObjectName("voice_decode_thread")
        worker = FfmpegConvertWorker(cmd, decoded, timeout_sec=45.0)
        worker.moveToThread(thread)
        thread.started.connect(worker.run)
        worker.done.connect(self._on_voice_decode_done)
        worker.done.connect(thread.quit)
        worker.done.connect(worker.deleteLater)
        thread.finished.connect(thread.deleteLater)
        self._voice_decode_thread = thread
        self._voice_decode_worker = worker
        # Register for cleanup on dispose.
        try:
            self._bg_threads.append(thread)
            thread.finished.connect(lambda: self._bg_threads.remove(thread) if thread in self._bg_threads else None)
        except Exception:
            pass
        thread.start()

    def _play_audio_path(self, path: str) -> None:
        player = self._ensure_player()
        if not player:
            return
        url = QUrl.fromLocalFile(path)
        if player.source() != url:
            try:
                player.setSource(url)
            except Exception:
                return
        MediaPlaybackCoordinator.pause_others(player)
        try:
            dur = int(player.duration() or 0)
            pos = int(player.position() or 0)
            if dur > 0 and pos >= max(0, dur - 200):
                player.setPosition(0)
        except Exception:
            pass
        try:
            player.play()
        except Exception:
            pass

    @Slot(dict)
    def _on_voice_decode_done(self, payload: Dict[str, Any]) -> None:
        if getattr(self, "_disposed", False):
            return
        if self.play_button and _qt_is_valid(self.play_button):
            try:
                self.play_button.setEnabled(True)
                self.play_button.setText("▶")
            except Exception:
                pass

        ok = bool((payload or {}).get("ok"))
        if not ok:
            err = str((payload or {}).get("error") or "").strip() or "Не удалось подготовить аудио"
            log.warning("[GUI] Voice decode failed: %s", err)
            self._set_voice_status(err)
            self._voice_decode_autoplay = False
            return

        out_path = str((payload or {}).get("output_path") or "").strip()
        if not out_path:
            self._set_voice_status("Не удалось открыть подготовленный WAV.")
            self._voice_decode_autoplay = False
            return

        self._voice_decoded_path = out_path
        self._clear_voice_status()
        if self._voice_decode_autoplay:
            self._voice_decode_autoplay = False
            self._play_audio_path(out_path)

    def _on_voice_wave_seek(self, ratio: float) -> None:
        player = self._ensure_player()
        if not player:
            return
        duration = player.duration()
        if duration <= 0:
            duration = self._voice_duration_ms or 0
        if duration <= 0:
            return
        target = int(max(0.0, min(1.0, float(ratio))) * duration)
        try:
            player.setPosition(target)
        except Exception:
            pass

    def _open_document(self) -> None:
        if self.file_path and os.path.isfile(self.file_path):
            QDesktopServices.openUrl(QUrl.fromLocalFile(self.file_path))
        else:
            self._start_download()

    def _on_state_changed(self, state: QMediaPlayer.PlaybackState) -> None:
        try:
            super()._on_state_changed(state)  # type: ignore[misc]
        except AttributeError:
            pass
        if self.play_button:
            self.play_button.setText("⏸" if state == QMediaPlayer.PlaybackState.PlayingState else "▶")

    # ---------- shown/updates ----------
    def show_downloaded_media(self, kind: str, path: str) -> None:
        super().show_downloaded_media(kind, path)
        k = (kind or "").lower()
        if k in {"audio", "voice"}:
            if self._audio_meta_lbl:
                parts = []
                if self.file_size:
                    parts.append(self._format_file_size(self.file_size))
                if path:
                    parts.append(os.path.basename(path))
                self._audio_meta_lbl.setText(" • ".join([p for p in parts if p]))
            if self.play_button:
                self.play_button.setText("▶")
                self.play_button.setToolTip("Воспроизвести")
        elif k in {"document", "file"}:
            if self._document_meta_lbl:
                parts = []
                if self.file_size:
                    parts.append(self._format_file_size(self.file_size))
                if path:
                    parts.append(os.path.basename(path))
                self._document_meta_lbl.setText(" • ".join([p for p in parts if p]))
            if self._document_title_lbl and path:
                self._document_title_lbl.setText(os.path.basename(path))
            self._set_doc_status_text("Файл готов")
            if self._radial_doc_widget:
                self._radial_doc_widget.set_state("completed", 1.0)
        elif k == "sticker":
            try:
                self._set_sticker_pix(path)
            except Exception:
                pass

    def set_reply_preview(self, data: Optional[Dict[str, Any]]) -> None:
        self._reply_data = dict(data) if data else None
        if not data:
            if self._reply_widget and self._content_layout:
                self._content_layout.removeWidget(self._reply_widget)
                self._reply_widget.deleteLater()
                self._reply_widget = None
            return
        if not self._reply_widget:
            self._reply_widget = ReplyPreviewWidget(self)
        if self._content_layout and self._reply_widget:
            self._insert_assistant_widget(self._reply_widget)
        if self._reply_widget:
            self._reply_widget.set_data(data)

    def refresh_reply_preview(self) -> None:
        if self._reply_widget and self._reply_data:
            self._reply_widget.set_data(self._reply_data)
            self._insert_assistant_widget(self._reply_widget)

    def set_forward_info(self, data: Optional[Dict[str, Any]]) -> None:
        self._forward_data = dict(data) if data else None
        if not data:
            if self._forward_widget and self._content_layout:
                self._content_layout.removeWidget(self._forward_widget)
                self._forward_widget.deleteLater()
                self._forward_widget = None
                self._insert_assistant_widget(self._reply_widget)
            return
        if not self._forward_widget:
            self._forward_widget = ForwardInfoWidget(self)
        if self._content_layout and self._forward_widget:
            self._content_layout.insertWidget(0, self._forward_widget, 0)
            self._forward_widget.set_info(data)
            self._insert_assistant_widget(self._reply_widget)

    def _insert_assistant_widget(self, widget: Optional[QWidget]) -> None:
        if not (widget and self._content_layout):
            return
        self._content_layout.removeWidget(widget)
        index = 1 if self._forward_widget else 0
        self._content_layout.insertWidget(index, widget, 0)

    def set_content_hidden(self, hidden: bool) -> None:
        if self._content_hidden == hidden:
            return
        self._content_hidden = hidden
        if self._hidden_placeholder:
            self._hidden_placeholder.setVisible(hidden)
        if self._content_wrap:
            self._content_wrap.setVisible(not hidden)
        if self.bubble:
            self.bubble.setVisible(not hidden)
        if self.btn_primary and _qt_is_valid(self.btn_primary):
            self.btn_primary.setVisible(not hidden)
        if self.btn_cancel and _qt_is_valid(self.btn_cancel):
            self.btn_cancel.setVisible(not hidden)
        if self.status_label and _qt_is_valid(self.status_label):
            self.status_label.setVisible(not hidden)
        if self.progress_bar and _qt_is_valid(self.progress_bar):
            self.progress_bar.setVisible(not hidden)

    def _ensure_deleted_placeholder(self) -> Optional[QLabel]:
        if self._deleted_placeholder or not self._content_layout:
            return self._deleted_placeholder
        placeholder = QLabel("Медиа удалено", self)
        placeholder.setStyleSheet(_style_sheet("message.deleted.placeholder", "color:#8894a8;font-style:italic;"))
        placeholder.setVisible(False)
        self._content_layout.addWidget(placeholder)
        self._deleted_placeholder = placeholder
        return placeholder

    def _set_media_visible(self, visible: bool) -> None:
        if not self._content_layout:
            return
        for idx in range(self._content_layout.count()):
            item = self._content_layout.itemAt(idx)
            widget = item.widget() if item else None
            if not widget or widget in (self._reply_widget, self._forward_widget, self._deleted_placeholder):
                continue
            widget.setVisible(visible)
        if self._deleted_placeholder:
            self._deleted_placeholder.setVisible(not visible)

    def _ensure_caption_label(self) -> None:
        if self._caption_label or not self._content_layout:
            return
        caption = RichTextLabel(self.text, self)
        caption.setStyleSheet(_style_sheet("message.caption", "font-size:13px;"))
        caption.setWordWrap(True)
        caption.commandActivated.connect(self.commandActivated.emit)
        if self.text_entities:
            caption.set_message(self.text, entities=self.text_entities)
        if self.kind == "image" and not self.bubble:
            bubble = Bubble("", self.role, parent=self)
            bubble.add_content(caption)
            self._content_layout.addWidget(bubble)
            self._caption_bubble = bubble
        else:
            self._content_layout.addWidget(caption)
        self._caption_label = caption

    def set_caption(self, text: str, *, entities: Optional[List[Dict[str, Any]]] = None) -> None:
        self.text = str(text or "")
        self.text_entities = list(entities) if isinstance(entities, list) else None

        if not self.text.strip():
            if self._caption_label:
                self._caption_label.hide()
            return

        self._ensure_caption_label()
        if self._caption_label:
            self._caption_label.set_message(self.text, entities=self.text_entities)
            self._caption_label.show()

    def set_reply_markup(self, data: Optional[Dict[str, Any]]) -> None:
        markup = dict(data) if isinstance(data, dict) else None
        if markup and str(markup.get("type") or "").strip().lower() != "inline":
            markup = None
        self._reply_markup_data = markup
        if not markup:
            if self._reply_markup_widget and self._content_layout:
                self._content_layout.removeWidget(self._reply_markup_widget)
                self._reply_markup_widget.deleteLater()
                self._reply_markup_widget = None
            return
        if not self._reply_markup_widget:
            self._reply_markup_widget = MessageReplyMarkupWidget("inline", self)
            self._reply_markup_widget.buttonActivated.connect(self._emit_reply_markup_action)
        if self._content_layout and self._reply_markup_widget:
            self._content_layout.removeWidget(self._reply_markup_widget)
            self._content_layout.addWidget(self._reply_markup_widget, 0)
            self._reply_markup_widget.set_markup(markup)

    def set_search_query(self, query: str, *, active: bool = False) -> None:
        if self._caption_label:
            self._caption_label.set_search(query, active=active)

    @Slot(dict)
    def _emit_reply_markup_action(self, action: Dict[str, Any]) -> None:
        payload = dict(action or {})
        if self.chat_id:
            payload.setdefault("chat_id", str(self.chat_id))
        if self.msg_id is not None:
            payload.setdefault("message_id", int(self.msg_id))
        self.replyMarkupButtonActivated.emit(payload)


    def _set_doc_status_text(self, text: Optional[str]) -> None:
        if not self._document_status_lbl:
            return
        if text:
            self._document_status_lbl.setText(text)
            self._document_status_lbl.show()
        else:
            self._document_status_lbl.clear()
            self._document_status_lbl.hide()

    def _doc_status_default(self, state: str) -> str:
        return {
            "idle": "Нажмите, чтобы скачать",
            "downloading": "",
            "paused": "",
            "completed": "Файл готов",
            "error": "Ошибка загрузки",
        }.get(state, "")

    def _update_doc_download_ui(self, state: str, message: Optional[str]) -> None:
        if self.kind not in {"document", "file"}:
            return
        text = message or self._doc_status_default(state)
        self._set_doc_status_text(text)
        if not self._radial_doc_widget:
            return
        ratio = self._download_ratio()
        if state in {"downloading", "resumed"}:
            self._radial_doc_widget.set_state("downloading", ratio)
        elif state == "paused":
            self._radial_doc_widget.set_state("paused", ratio)
        elif state == "completed":
            self._radial_doc_widget.set_state("completed", 1.0)
        elif state == "error":
            self._radial_doc_widget.set_state("error", ratio)
        else:
            self._radial_doc_widget.set_state("idle", 0.0)

    def _update_doc_radial_progress(self, ratio: float) -> None:
        if self.kind in {"document", "file"} and self._radial_doc_widget and self.download_state in {"downloading", "paused"}:
            self._radial_doc_widget.set_progress(ratio)

    def _download_ratio(self) -> float:
        total = self.download_total or self.file_size or 0
        if total <= 0:
            return 0.0
        return max(0.0, min(1.0, float(self.download_progress) / float(total)))

    def _on_doc_radial_clicked(self) -> None:
        state = self.download_state
        if state == "downloading":
            self._pause_download()
        elif state == "paused":
            self._resume_download()
        elif state == "completed" and self.file_path and os.path.isfile(self.file_path):
            self._open_document()
        else:
            if self.file_path and os.path.isfile(self.file_path):
                self._open_document()
            else:
                self._start_download()

    def set_deleted(self, deleted: bool) -> None:
        self._is_deleted = bool(deleted)
        if self.deleted_label:
            self.deleted_label.setVisible(self._is_deleted)
        if self.bubble:
            self.bubble.set_deleted(self._is_deleted)
            try:
                self.bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass
        if self._caption_bubble:
            self._caption_bubble.set_deleted(self._is_deleted)
            try:
                self._caption_bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass
        self._refresh_header_style()
        if self._reply_widget and self._reply_data:
            self.refresh_reply_preview()

    def set_has_hidden(self, has_hidden: bool) -> None:
        self._has_hidden = bool(has_hidden)
        if self.bubble:
            try:
                self.bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass
        if self._caption_bubble:
            try:
                self._caption_bubble.set_has_hidden(self._has_hidden)
            except Exception:
                pass

    def set_selected(self, selected: bool) -> None:
        self._selected = bool(selected)
        if self.bubble:
            try:
                self.bubble.set_selected(self._selected)
            except Exception:
                pass
        if self._caption_bubble:
            try:
                self._caption_bubble.set_selected(self._selected)
            except Exception:
                pass
        if not self.bubble and self._content_wrap:
            if self._selected:
                self._content_wrap.setStyleSheet(
                    _style_sheet(
                        "message.media.selected",
                        "background-color: rgba(89,183,233,0.13); border:1px solid rgba(89,183,233,0.65); border-radius:12px;",
                    )
                )
            else:
                self._content_wrap.setStyleSheet(_style_sheet("message.body.container", "background-color: transparent;"))

    def set_media_group_position(self, position: str) -> None:
        pos = str(position or "single").lower()
        if pos not in {"single", "top", "middle", "bottom"}:
            pos = "single"
        if self._media_group_position == pos:
            return
        self._media_group_position = pos

        margins = {
            "single": (8, 8),
            "top": (8, 2),
            "middle": (2, 2),
            "bottom": (2, 8),
        }
        top, bottom = margins.get(pos, (8, 8))
        root = getattr(self, "_root_layout", None)
        if root is not None:
            try:
                root.setContentsMargins(8, top, 8, bottom)
            except Exception:
                pass

        header = getattr(self, "header_label", None)
        if header is not None:
            try:
                header.setVisible(bool(self._show_header_label) and pos in {"single", "top"})
            except Exception:
                pass

    def _refresh_header_style(self) -> None:
        if not hasattr(self, "header_label") or not self.header_label:
            return
        if getattr(self, "_is_deleted", False):
            css = _style_sheet("message.header.deleted", "color:#7e879b;font-size:12px;font-weight:600;")
        elif self.role != "me" and getattr(self, "_header_color", None):
            css = f"color:{self._header_color}; font-size:12px; font-weight:600;"  # type: ignore[attr-defined]
        else:
            key = "message.header.me" if self.role == "me" else "message.header.other"
            default = "color:#d5e4ff;font-size:12px;font-weight:600;" if self.role == "me" else "color:#bfc8d6;font-size:12px;font-weight:600;"
            css = _style_sheet(key, default)
        self.header_label.setStyleSheet(css)

    def set_header_color(self, color: Optional[str]) -> None:
        self._header_color = color
        self._refresh_header_style()

    def set_voice_waveform_enabled(self, enabled: bool) -> None:
        self._waveform_enabled = bool(enabled)
        if self._voice_wave_widget:
            self._voice_wave_widget.setVisible(self._waveform_enabled)

    def _on_widget_destroyed(self, *_args) -> None:
        self._disposed = True

    def _download_controls_alive(self) -> bool:
        if self._disposed:
            return False
        primary = getattr(self, "btn_primary", None)
        cancel = getattr(self, "btn_cancel", None)
        status = getattr(self, "status_label", None)
        return bool(_qt_is_valid(primary) and _qt_is_valid(cancel) and _qt_is_valid(status))

    # ---------- download state machine ----------
    def _set_download_state(self, state: str, message: Optional[str] = None) -> None:
        self.download_state = state
        if not self._download_controls_alive():
            return

        primary = self.btn_primary
        cancel_btn = self.btn_cancel
        status = self.status_label

        def _hide_external():
            primary.hide()
            cancel_btn.hide()

        def _show_external(text: str, cancel: bool, enabled: bool = True):
            primary.setText(text)
            primary.setEnabled(enabled)
            if cancel:
                cancel_btn.show()
            else:
                cancel_btn.hide()
            primary.show()

        bar = getattr(self, "progress_bar", None)
        if bar is not None and not _qt_is_valid(bar):
            bar = None

        if state == "idle":
            if self._external_controls_allowed:
                _show_external("Скачать", cancel=False, enabled=bool(self.server and self.chat_id and self.msg_id is not None))
            else:
                _hide_external()
            if bar:
                bar.hide()
            if message:
                status.setText(message)
                status.show()
            else:
                status.hide()

        elif state == "downloading":
            if self._external_controls_allowed:
                _show_external("Пауза", cancel=True, enabled=True)
            else:
                _hide_external()
            if bar:
                bar.show()
            if message:
                status.setText(message)
                status.show()
            else:
                status.hide()

        elif state == "paused":
            if self._external_controls_allowed:
                _show_external("Продолжить", cancel=True, enabled=True)
            else:
                _hide_external()
            if bar:
                bar.show()
            if message:
                status.setText(message)
                status.show()
            else:
                status.hide()

        elif state == "completed":
            _hide_external()
            if bar:
                bar.hide()
            if message:
                status.setText(message)
                status.show()
            else:
                status.hide()

        elif state == "error":
            if self._external_controls_allowed:
                _show_external("Повторить", cancel=False, enabled=True)
            else:
                _hide_external()
            if bar:
                bar.hide()
            status.setText(message or "Ошибка загрузки")
            status.show()

        elif state == "cancelled":
            self._set_download_state("idle", message or "Загрузка отменена")
            return

        else:
            _hide_external()
            if bar:
                bar.hide()
            if message:
                status.setText(message)
                status.show()
            else:
                status.hide()

        self._update_doc_download_ui(state, message)

    # ---------- button handlers ----------
    def _on_primary_clicked(self) -> None:
        if self.download_state in {"idle", "error"}:
            self._start_download()
        elif self.download_state == "downloading":
            self._pause_download()
        elif self.download_state == "paused":
            self._resume_download()

    def _on_cancel_clicked(self) -> None:
        self._cancel_download()

    # ---------- download ops ----------
    def _start_download(self) -> None:
        if self._disposed:
            return
        if not (self.server and self.chat_id and self.msg_id is not None):
            self._set_download_state("error", "Нет соединения с сервером")
            return
        try:
            job_id = self.server.start_media_download(self.chat_id, int(self.msg_id))
        except Exception as exc:
            self._set_download_state("error", f"Ошибка: {exc}")
            return
        self.download_job_id = job_id or None
        self._set_download_state("downloading")

    def _pause_download(self) -> None:
        if not (self.server and self.download_job_id):
            return
        try:
            ok = self.server.pause_media_download(self.download_job_id)
        except Exception as exc:
            self._set_download_state("error", f"Не удалось поставить на паузу: {exc}")
            return
        if ok:
            self._set_download_state("paused")

    def _resume_download(self) -> None:
        if not (self.server and self.download_job_id):
            return
        try:
            ok = self.server.resume_media_download(self.download_job_id)
        except Exception as exc:
            self._set_download_state("error", f"Не удалось возобновить: {exc}")
            return
        if ok:
            self._set_download_state("downloading")

    def _cancel_download(self) -> None:
        if not self.server:
            self._set_download_state("idle")
            return
        if self.download_job_id:
            try:
                self.server.cancel_media_download(self.download_job_id)
            except Exception:
                pass
        self.download_job_id = None
        self._set_download_state("idle", "Загрузка отменена")

    # ---------- event updates ----------
    def update_download_state(self, payload: dict) -> None:
        if self._disposed:
            return
        state = (payload.get("state") or "").lower()
        current = int(payload.get("current") or 0)
        total = int(payload.get("total") or 0)
        file_path = payload.get("file_path") or ""
        bar = getattr(self, "progress_bar", None)
        if bar is not None and not _qt_is_valid(bar):
            bar = None
        if payload.get("job_id"):
            self.download_job_id = payload.get("job_id") or self.download_job_id

        if state in {"downloading", "progress", "queued"}:
            self._set_download_state("downloading", "Загрузка…")
            if total > 0:
                total = max(total, current)
                self.download_total = total
                self.download_progress = current
                percent = max(0, min(100, int(current * 100 / total)))
                if bar:
                    bar.setRange(0, 100)
                    bar.setValue(percent)
                self._update_doc_radial_progress(self._download_ratio())
        elif state == "paused":
            self._set_download_state("paused")
            if total > 0:
                self.download_total = max(total, current)
                self.download_progress = current
                self._update_doc_radial_progress(self._download_ratio())
        elif state == "resumed":
            self._set_download_state("downloading")
        elif state == "completed":
            self.download_job_id = None
            if file_path:
                self.show_downloaded_media(self.kind, file_path)
            if total > 0:
                self.download_total = max(self.download_total, total)
                self.download_progress = self.download_total
            self._set_download_state("completed")
        elif state == "cancelled":
            self.download_job_id = None
            self._set_download_state("idle", "Загрузка отменена")
        elif state == "error":
            self.download_job_id = None
            err = payload.get("error") or "Ошибка загрузки"
            self._set_download_state("error", err)

    # ---------- lifecycle ----------
    def safe_dispose(self) -> None:
        self._disposed = True
        if self.download_job_id and self.server:
            try:
                self.server.cancel_media_download(self.download_job_id)
            except Exception:
                pass
        if getattr(self, "player", None):
            try:
                self.player.stop()
            except Exception:
                pass
        for th in list(self._bg_threads):
            try:
                th.quit()
                th.wait(2000)
            except Exception:
                pass
        self._bg_threads.clear()
        if self._reply_widget:
            try:
                self._reply_widget.deleteLater()
            except Exception:
                pass
            self._reply_widget = None
        if self._forward_widget:
            try:
                self._forward_widget.deleteLater()
            except Exception:
                pass
            self._forward_widget = None
        if self._reply_markup_widget:
            try:
                self._reply_markup_widget.deleteLater()
            except Exception:
                pass
            self._reply_markup_widget = None
        if self._voice_wave_widget:
            try:
                self._voice_wave_widget.deleteLater()
            except Exception:
                pass
            self._voice_wave_widget = None

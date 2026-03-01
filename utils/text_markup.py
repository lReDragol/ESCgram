from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def _utf16_is_surrogate_pair(ch: str) -> bool:
    try:
        return ord(ch) > 0xFFFF
    except Exception:
        return False


def _utf16_offsets(text: str) -> List[int]:
    offsets: List[int] = [0]
    total = 0
    for ch in text:
        total += 2 if _utf16_is_surrogate_pair(ch) else 1
        offsets.append(total)
    return offsets


def parse_tg_style_markup(text: str) -> Tuple[str, List[Dict[str, object]]]:
    """
    Parse a small Telegram/MarkdownV2-like subset and return (plain_text, entities).

    Supported markers:
      *bold* or **bold**
      _italic_
      __underline__
      ~strike~
      ||spoiler||
      `code`
      ```pre```

    Offsets/lengths are returned in UTF-16 code units (as expected by Telegram/Pyrogram).
    Unmatched markers are preserved literally.
    """
    if not text:
        return "", []

    out_chars: List[str] = []
    spans: List[Tuple[str, int, int, Optional[str]]] = []

    stack: Dict[str, List[int]] = {
        "bold": [],
        "italic": [],
        "underline": [],
        "strikethrough": [],
        "spoiler": [],
        "code": [],
        "pre": [],
    }

    def _have_closer(marker: str, start: int) -> bool:
        return text.find(marker, start) != -1

    i = 0
    while i < len(text):
        ch = text[i]

        # Backslash escaping: \* \_ \~ \| \`
        if ch == "\\" and i + 1 < len(text):
            out_chars.append(text[i + 1])
            i += 2
            continue

        # Pre block (```...```)
        if text.startswith("```", i) and not stack["code"]:
            if stack["pre"]:
                start = stack["pre"].pop()
                spans.append(("pre", start, len(out_chars), None))
                i += 3
                # Optional single newline right after closing fence.
                if i < len(text) and text[i] == "\n":
                    i += 1
                continue
            if _have_closer("```", i + 3):
                stack["pre"].append(len(out_chars))
                i += 3
                # Optional language hint until newline: ```py\ncode```
                nl = text.find("\n", i)
                if nl != -1:
                    lang = text[i:nl].strip()
                    if lang and len(lang) <= 24 and all(c.isalnum() or c in {"_", "-", "+"} for c in lang):
                        i = nl + 1
                continue
            out_chars.extend(["`", "`", "`"])
            i += 3
            continue

        if stack["pre"]:
            out_chars.append(ch)
            i += 1
            continue

        # Inline code (`...`)
        if ch == "`":
            if stack["code"]:
                start = stack["code"].pop()
                spans.append(("code", start, len(out_chars), None))
                i += 1
                continue
            if _have_closer("`", i + 1):
                stack["code"].append(len(out_chars))
                i += 1
                continue
            out_chars.append("`")
            i += 1
            continue

        if stack["code"]:
            out_chars.append(ch)
            i += 1
            continue

        # Spoiler (||...||)
        if text.startswith("||", i):
            if stack["spoiler"]:
                start = stack["spoiler"].pop()
                spans.append(("spoiler", start, len(out_chars), None))
                i += 2
                continue
            if _have_closer("||", i + 2):
                stack["spoiler"].append(len(out_chars))
                i += 2
                continue
            out_chars.extend(["|", "|"])
            i += 2
            continue

        # Underline (__...__)
        if text.startswith("__", i):
            if stack["underline"]:
                start = stack["underline"].pop()
                spans.append(("underline", start, len(out_chars), None))
                i += 2
                continue
            if _have_closer("__", i + 2):
                stack["underline"].append(len(out_chars))
                i += 2
                continue
            out_chars.extend(["_", "_"])
            i += 2
            continue

        # Bold (**...**) preferred over *...*
        if text.startswith("**", i):
            if stack["bold"]:
                start = stack["bold"].pop()
                spans.append(("bold", start, len(out_chars), None))
                i += 2
                continue
            if _have_closer("**", i + 2):
                stack["bold"].append(len(out_chars))
                i += 2
                continue
            out_chars.extend(["*", "*"])
            i += 2
            continue

        # Bold (*...*)
        if ch == "*":
            if stack["bold"]:
                start = stack["bold"].pop()
                spans.append(("bold", start, len(out_chars), None))
                i += 1
                continue
            if _have_closer("*", i + 1):
                stack["bold"].append(len(out_chars))
                i += 1
                continue
            out_chars.append("*")
            i += 1
            continue

        # Italic (_..._)
        if ch == "_":
            if stack["italic"]:
                start = stack["italic"].pop()
                spans.append(("italic", start, len(out_chars), None))
                i += 1
                continue
            if _have_closer("_", i + 1):
                stack["italic"].append(len(out_chars))
                i += 1
                continue
            out_chars.append("_")
            i += 1
            continue

        # Strikethrough (~...~)
        if ch == "~":
            if stack["strikethrough"]:
                start = stack["strikethrough"].pop()
                spans.append(("strikethrough", start, len(out_chars), None))
                i += 1
                continue
            if _have_closer("~", i + 1):
                stack["strikethrough"].append(len(out_chars))
                i += 1
                continue
            out_chars.append("~")
            i += 1
            continue

        out_chars.append(ch)
        i += 1

    plain = "".join(out_chars)
    if not spans:
        return plain, []

    offsets = _utf16_offsets(plain)
    entities: List[Dict[str, object]] = []
    for etype, start, end, extra in spans:
        start = max(0, min(start, len(plain)))
        end = max(0, min(end, len(plain)))
        if end <= start:
            continue
        entities.append(
            {
                "type": etype,
                "offset": int(offsets[start]),
                "length": int(offsets[end] - offsets[start]),
            }
        )
        if etype == "pre" and isinstance(extra, str) and extra:
            entities[-1]["language"] = extra

    entities.sort(key=lambda e: (int(e.get("offset") or 0), -int(e.get("length") or 0)))
    return plain, entities


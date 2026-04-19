from __future__ import annotations

import html
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

log = logging.getLogger("chat_export")

_MESSAGES_PER_FILE = 1000
_JOIN_WITHIN_SECONDS = 900
_USERPIC_SIZE = 42
_PHOTO_MAX_W = 520
_PHOTO_MAX_H = 520
_STICKER_MAX_W = 384
_STICKER_MAX_H = 384

_USERPIC_COLORS = [
    "#ff5555", "#64bf47", "#ffab00", "#4f9cd9",
    "#9884e8", "#e671a5", "#47bcd1", "#ff8c44",
]

_CSS = """\
body {
    margin: 0;
    font: 12px/18px 'Open Sans',"Lucida Grande","Lucida Sans Unicode",Arial,Helvetica,Verdana,sans-serif;
}
strong {
    font-weight: 700;
}
code, kbd, pre, samp {
    font-family: Menlo,Monaco,Consolas,"Courier New",monospace;
}
code {
    padding: 2px 4px;
    font-size: 90%;
    color: #c7254e;
    background-color: #f9f2f4;
    border-radius: 4px;
}
pre {
    display: block;
    margin: 0;
    line-height: 1.42857143;
    word-break: break-all;
    word-wrap: break-word;
    color: #333;
    background-color: #f5f5f5;
    border-radius: 4px;
    overflow: auto;
    padding: 3px;
    border: 1px solid #eee;
    max-height: none;
    font-size: inherit;
}
.clearfix:after {
    content: " ";
    visibility: hidden;
    display: block;
    height: 0;
    clear: both;
}
.pull_left {
    float: left;
}
.pull_right {
    float: right;
}
.page_wrap {
    background-color: #ffffff;
    color: #000000;
}
.page_wrap a {
    color: #168acd;
    text-decoration: none;
}
.page_wrap a:hover {
    text-decoration: underline;
}
.page_header {
    position: fixed;
    z-index: 10;
    background-color: #ffffff;
    width: 100%;
    border-bottom: 1px solid #e3e6e8;
}
.page_header .content {
    width: 480px;
    margin: 0 auto;
    border-radius: 0 !important;
}
.page_header a.content {
    background-repeat: no-repeat;
    background-position: 24px 21px;
    background-size: 24px 24px;
}
.bold {
    color: #212121;
    font-weight: 700;
}
.details {
    color: #70777b;
}
.page_header .content .text {
    padding: 24px 24px 22px 24px;
    font-size: 22px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.page_header a.content .text {
    padding: 24px 24px 22px 82px;
}
.page_body {
    padding-top: 64px;
    width: 480px;
    margin: 0 auto;
}
.with_divider {
    border-top: 1px solid #e3e6e8;
}
.userpic_link {
    display: block;
    text-decoration: none;
}
.userpic_link:hover {
    text-decoration: none;
}
.userpic {
    display: block;
    border-radius: 50%;
    overflow: hidden;
}
.userpic .initials {
    display: block;
    color: #fff;
    text-align: center;
    text-transform: uppercase;
    user-select: none;
}
.color_red,
.userpic1,
.media_call .fill,
.media_file .fill,
.media_live_location .fill {
    background-color: #ff5555;
}
.color_green,
.userpic2,
.media_call.success .fill,
.media_photo .fill {
    background-color: #64bf47;
}
.color_yellow,
.userpic3,
.media_venue .fill {
    background-color: #ffab00;
}
.color_blue,
.userpic4,
.media_audio_file .fill,
.media_voice_message .fill {
    background-color: #4f9cd9;
}
.color_purple,
.userpic5,
.media_game .fill {
    background-color: #9884e8;
}
.color_pink,
.userpic6,
.media_invoice .fill {
    background-color: #e671a5;
}
.color_sea,
.userpic7,
.media_location .fill,
.media_video .fill {
    background-color: #47bcd1;
}
.color_orange,
.userpic8,
.media_contact .fill {
    background-color: #ff8c44;
}
.history {
    padding: 16px 0;
}
.message {
    margin: 0 -10px;
    transition: background-color 2.0s ease;
}
div.selected {
    background-color: rgba(242,246,250,255);
    transition: background-color 0.5s ease;
}
.service {
    padding: 10px 24px;
}
.service .body {
    text-align: center;
}
.service .userpic_wrap {
    padding-top: 10px;
}
.service .userpic {
    margin: 0 auto;
}
.service .userpic .initials {
    font-size: 24px;
}
.message .userpic .initials {
    font-size: 16px;
}
.default {
    padding: 10px;
}
.default.joined {
    margin-top: -10px;
}
.default .from_name {
    color: #3892db;
    font-weight: 700;
    padding-bottom: 5px;
}
.default .from_name .details {
    font-weight: normal;
}
.default .body {
    margin-left: 60px;
}
.default .text {
    word-wrap: break-word;
    line-height: 150%;
    unicode-bidi: plaintext;
    text-align: start;
}
.default .reply_to,
.default .media_wrap {
    padding-bottom: 5px;
}
.default .media {
    margin: 0 -10px;
    padding: 5px 10px;
}
.default .media .fill,
.default .media .thumb {
    width: 48px;
    height: 48px;
    border-radius: 50%;
}
.default .media .fill {
    background-repeat: no-repeat;
    background-position: 12px 12px;
    background-size: 24px 24px;
}
.default .media .title,
.default .media_poll .question {
    padding-top: 4px;
    font-size: 14px;
}
.default .media .description {
    color: #000000;
    padding-top: 4px;
    font-size: 13px;
}
.default .media .status {
    padding-top: 4px;
    font-size: 13px;
}
.default .video_file_wrap,
.default .animated_wrap {
    position: relative;
}
.default .video_file,
.default .animated,
.default .photo,
.default .sticker {
    display: block;
}
.video_duration {
    background: rgba(0, 0, 0, .4);
    padding: 0px 5px;
    position: absolute;
    z-index: 2;
    border-radius: 2px;
    right: 3px;
    bottom: 3px;
    color: #ffffff;
    font-size: 11px;
}
.video_play_bg {
    background: rgba(0, 0, 0, .4);
    width: 40px;
    height: 40px;
    line-height: 0;
    position: absolute;
    z-index: 2;
    border-radius: 50%;
    overflow: hidden;
    margin: -20px auto 0 -20px;
    top: 50%;
    left: 50%;
    pointer-events: none;
}
.video_play {
    position: absolute;
    display: inline-block;
    top: 50%;
    left: 50%;
    margin-left: -5px;
    margin-top: -9px;
    z-index: 1;
    width: 0;
    height: 0;
    border-style: solid;
    border-width: 9px 0 9px 14px;
    border-color: transparent transparent transparent #fff;
}
.gif_play {
    font-weight: 700;
    color: #FFF;
    display: block;
    line-height: 40px;
    font-size: 13px;
    text-align: center;
}
.pagination {
    text-align: center;
    padding: 20px;
    font-size: 16px;
}
.toast_container {
    position: fixed;
    left: 50%;
    top: 50%;
    opacity: 0;
    transition: opacity 3.0s ease;
}
.toast_body {
    margin: 0 -50%;
    float: left;
    border-radius: 15px;
    padding: 10px 20px;
    background: rgba(0, 0, 0, 0.7);
    color: #ffffff;
}
div.toast_shown {
    opacity: 1;
    transition: opacity 0.4s ease;
}
.spoiler {
    background: #e8e8e8;
}
.spoiler.hidden {
    background: #a9a9a9;
    cursor: pointer;
    border-radius: 3px;
}
.spoiler.hidden span {
    opacity: 0;
    user-select: none;
}
.reactions {
    margin: 5px 0;
}
.reactions .reaction {
    display: inline-flex;
    height: 20px;
    border-radius: 15px;
    background-color: #e8f5fc;
    color: #168acd;
    font-weight: bold;
    margin-bottom: 5px;
}
.reactions .reaction.active {
    background-color: #40a6e2;
    color: #fff;
}
.reactions .reaction.paid {
    background-color: #fdf6e1;
    color: #c58523;
}
.reactions .reaction.active.paid {
    background-color: #ecae0a;
    color: #fdf6e1;
}
.reactions .reaction .emoji {
    line-height: 20px;
    margin: 0 5px;
    font-size: 15px;
}
.reactions .reaction .count {
    margin-right: 8px;
    line-height: 20px;
}
a.block_link {
    display: block;
    text-decoration: none !important;
    border-radius: 4px;
}
a.block_link:hover {
    text-decoration: none !important;
    background-color: #f5f7f8;
}
"""

_CSS_DARK = """\
.page_wrap {
    background-color: #0e1621;
    color: #f5f5f5;
}
.page_wrap a {
    color: #6ab2f2;
}
.page_header {
    background-color: #0e1621;
    border-bottom-color: #1c2a3a;
}
.bold {
    color: #f5f5f5;
}
.details {
    color: #7b8ea0;
}
.default .from_name {
    color: #6ab2f2;
}
.default .text {
    color: #f5f5f5;
}
.default .media .description {
    color: #ccc;
}
.with_divider {
    border-top-color: #1c2a3a;
}
a.block_link:hover {
    background-color: #182533;
}
code {
    background-color: #1c2a3a;
    color: #e8a;
}
pre {
    background-color: #1c2a3a;
    color: #ccc;
    border-color: #2a3a4a;
}
.spoiler {
    background: #2a3a4a;
}
.spoiler.hidden {
    background: #3a4a5a;
}
.reactions .reaction {
    background-color: #182533;
    color: #6ab2f2;
}
.reactions .reaction.active {
    background-color: #2b5278;
    color: #fff;
}
div.selected {
    background-color: rgba(43,82,120,255);
}
"""

_JS = """\
"use strict";
window.AllowBackFromHistory = false;
function CheckLocation() {
    var start = "#go_to_message";
    var hash = location.hash;
    if (hash.substr(0, start.length) == start) {
        var messageId = parseInt(hash.substr(start.length));
        if (messageId) {
            GoToMessage(messageId);
        }
    } else if (hash == "#allow_back") {
        window.AllowBackFromHistory = true;
    }
}
function ShowToast(text) {
    var container = document.createElement("div");
    container.className = "toast_container";
    var inner = container.appendChild(document.createElement("div"));
    inner.className = "toast_body";
    inner.appendChild(document.createTextNode(text));
    var appended = document.body.appendChild(container);
    setTimeout(function () {
        AddClass(appended, "toast_shown");
        setTimeout(function () {
            RemoveClass(appended, "toast_shown");
            setTimeout(function () {
                document.body.removeChild(appended);
            }, 3000);
        }, 3000);
    }, 0);
}
function ShowHashtag(tag) { ShowToast("This is a hashtag '#" + tag + "' link."); return false; }
function ShowCashtag(tag) { ShowToast("This is a cashtag '$" + tag + "' link."); return false; }
function ShowBotCommand(command) { ShowToast("This is a bot command '/" + command + "' link."); return false; }
function ShowMentionName() { ShowToast("This is a link to a user mentioned by name."); return false; }
function ShowNotLoadedEmoji() { ShowToast("This custom emoji is not included, change data exporting settings to download."); return false; }
function ShowNotAvailableEmoji() { ShowToast("This custom emoji is not available."); return false; }
function ShowTextCopied(content) { navigator.clipboard.writeText(content); ShowToast("Text copied to clipboard."); return false; }
function ShowSpoiler(target) { if (target.classList.contains("hidden")) { target.classList.toggle("hidden"); } }
function AddClass(el, cls) { el.classList.add(cls); }
function RemoveClass(el, cls) { el.classList.remove(cls); }
function GoToMessage(id) {
    var el = document.getElementById("message" + id);
    if (el) { el.scrollIntoView({ behavior: "smooth" }); el.classList.add("selected"); setTimeout(function() { el.classList.remove("selected"); }, 6000); }
}
function GoBack(backLink) { if (window.AllowBackFromHistory) return true; history.back(); return false; }
"""


def _html_escape(text: str) -> str:
    return html.escape(str(text or ""))


def _format_ts(ts: int) -> str:
    try:
        import datetime
        dt = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)
        return dt.strftime("%d.%m.%Y %H:%M:%S")
    except Exception:
        return str(ts)


def _format_date_label(ts: int) -> str:
    try:
        import datetime
        dt = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)
        return dt.strftime("%d %B %Y")
    except Exception:
        return ""


def _format_time(ts: int) -> str:
    try:
        import datetime
        dt = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)
        return dt.strftime("%H:%M")
    except Exception:
        return ""


def _format_duration(seconds: int) -> str:
    s = max(0, int(seconds or 0))
    if s >= 3600:
        return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"
    return f"{s // 60}:{s % 60:02d}"


def _human_size(n: int) -> str:
    n = max(0, int(n or 0))
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024.0
    return f"{n:.1f} TB"


def _initials(name: str) -> str:
    parts = (name or "").strip().split()
    if not parts:
        return ""
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()
    return parts[0][0].upper()


def _userpic_color_index(sender_id: Any) -> int:
    try:
        sid = abs(int(sender_id or 0))
    except Exception:
        sid = 0
    return sid % len(_USERPIC_COLORS)


def _userpic_html(sender_name: str, sender_id: Any, size: int = _USERPIC_SIZE) -> str:
    ini = _initials(sender_name)
    cidx = _userpic_color_index(sender_id)
    color_cls = f"userpic{cidx + 1}"
    return (
        f'<div class="userpic {color_cls}" style="width: {size}px; height: {size}px">'
        f'<div class="initials" style="line-height: {size}px">{_html_escape(ini)}</div>'
        f'</div>'
    )


def _text_entities_html(text: str, entities: Optional[List[Dict[str, Any]]] = None) -> str:
    if not text:
        return ""
    if not entities:
        return _html_escape(text).replace("\n", "<br>")
    sorted_ents = sorted(
        [e for e in entities if isinstance(e, dict) and e.get("offset") is not None and e.get("length") is not None],
        key=lambda e: int(e.get("offset", 0)),
    )
    parts: List[str] = []
    last = 0
    for ent in sorted_ents:
        offset = int(ent.get("offset", 0))
        length = int(ent.get("length", 0))
        if offset < last or length <= 0:
            continue
        if offset > last:
            parts.append(_html_escape(text[last:offset]).replace("\n", "<br>"))
        raw = text[offset:offset + length]
        escaped = _html_escape(raw)
        etype = str(ent.get("type", "")).lower()
        additional = str(ent.get("additional") or ent.get("url") or "")
        if etype == "bold":
            parts.append(f"<strong>{escaped}</strong>")
        elif etype == "italic":
            parts.append(f"<em>{escaped}</em>")
        elif etype == "code":
            parts.append(f"<code>{escaped}</code>")
        elif etype == "pre":
            lang = str(ent.get("language") or "").strip()
            if lang:
                parts.append(f'<pre><code class="language-{_html_escape(lang)}">{escaped}</code></pre>')
            else:
                parts.append(f"<pre>{escaped}</pre>")
        elif etype == "underline":
            parts.append(f"<u>{escaped}</u>")
        elif etype == "strikethrough":
            parts.append(f"<s>{escaped}</s>")
        elif etype == "spoiler":
            parts.append(f'<span class="spoiler hidden" onclick="ShowSpoiler(this)"><span>{escaped}</span></span>')
        elif etype == "text_link" and additional:
            parts.append(f'<a href="{_html_escape(additional)}">{escaped}</a>')
        elif etype == "link" or etype == "url":
            parts.append(f'<a href="{escaped}">{escaped}</a>')
        elif etype == "mention":
            parts.append(f'<a href="https://t.me/{escaped.lstrip("@")}">{escaped}</a>')
        elif etype == "email":
            parts.append(f'<a href="mailto:{escaped}">{escaped}</a>')
        elif etype == "phone":
            parts.append(f'<a href="tel:{escaped}">{escaped}</a>')
        elif etype == "hashtag":
            tag = raw.lstrip("#")
            parts.append(f'<a href="" onclick="return ShowHashtag(\'{_html_escape(tag)}\')">{escaped}</a>')
        elif etype == "bot_command":
            cmd = raw.lstrip("/")
            parts.append(f'<a href="" onclick="return ShowBotCommand(\'{_html_escape(cmd)}\')">{escaped}</a>')
        elif etype == "cashtag":
            tag = raw.lstrip("$")
            parts.append(f'<a href="" onclick="return ShowCashtag(\'{_html_escape(tag)}\')">{escaped}</a>')
        else:
            parts.append(escaped)
        last = offset + length
    if last < len(text):
        parts.append(_html_escape(text[last:]).replace("\n", "<br>"))
    return "".join(parts) if parts else _html_escape(text).replace("\n", "<br>")


def _reactions_html(msg: Dict[str, Any]) -> str:
    reactions = msg.get("reactions")
    if not isinstance(reactions, list) or not reactions:
        return ""
    chips: List[str] = []
    for item in reactions:
        if not isinstance(item, dict):
            continue
        emoji = str(item.get("emoji") or item.get("title") or "")
        count = int(item.get("count") or 0)
        if not emoji or count <= 0:
            continue
        is_mine = bool(item.get("chosen"))
        cls = "reaction active" if is_mine else "reaction"
        chips.append(
            f'<span class="{cls}">'
            f'<span class="emoji">{_html_escape(emoji)}</span>'
            f'<span class="count">{count}</span>'
            f'</span>'
        )
    if not chips:
        return ""
    return f'<span class="reactions">{"".join(chips)}</span>'


_MEDIA_DIR_MAP = {
    "photo": "photos",
    "image": "photos",
    "video": "video_files",
    "video_note": "video_files",
    "animation": "animations",
    "gif": "animations",
    "sticker": "stickers",
    "voice": "voice",
    "audio": "audio",
    "document": "files",
}


def _media_subdir(mtype: str) -> str:
    return _MEDIA_DIR_MAP.get(mtype, "files")


def _photo_media_html(msg: Dict[str, Any], rel_prefix: str) -> str:
    subdir = _media_subdir("photo")
    file_path = str(msg.get("file_path") or "").strip()
    href = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(file_path))}" if file_path else "#"
    w = int(msg.get("width") or 0)
    h = int(msg.get("height") or 0)
    if w > 0 and h > 0:
        ratio = min(_PHOTO_MAX_W / w, _PHOTO_MAX_H / h, 1.0)
        dw = max(80, int(w * ratio))
        dh = max(80, int(h * ratio))
        style = f'style="width: {dw}px; height: {dh}px"'
    else:
        style = ""
    thumb_path = str(msg.get("thumb_path") or "").strip()
    if thumb_path:
        thumb_src = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(thumb_path))}"
    elif file_path:
        thumb_src = href
    else:
        return _generic_media_html(msg, rel_prefix, "media_photo", "Photo", "")
    return (
        f'<div class="media_wrap clearfix">'
        f'<a class="photo_wrap clearfix pull_left" href="{href}">'
        f'<img class="photo" {style} src="{thumb_src}"/>'
        f'</a>'
        f'</div>'
    )


def _video_media_html(msg: Dict[str, Any], rel_prefix: str) -> str:
    subdir = _media_subdir("video")
    file_path = str(msg.get("file_path") or "").strip()
    href = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(file_path))}" if file_path else "#"
    dur = msg.get("duration")
    w = int(msg.get("width") or 0)
    h = int(msg.get("height") or 0)
    if w > 0 and h > 0:
        ratio = min(_PHOTO_MAX_W / w, _PHOTO_MAX_H / h, 1.0)
        dw = max(80, int(w * ratio))
        dh = max(80, int(h * ratio))
        style = f'style="width: {dw}px; height: {dh}px"'
    else:
        style = ""
    thumb_path = str(msg.get("thumb_path") or "").strip()
    if thumb_path:
        thumb_src = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(thumb_path))}"
    elif file_path:
        thumb_src = href
    else:
        dur_str = _format_duration(dur) if dur else ""
        return _generic_media_html(msg, rel_prefix, "media_video", "Video file", dur_str)
    dur_html = ""
    if dur:
        dur_html = f'<div class="video_duration">{_format_duration(dur)}</div>'
    mtype = str(msg.get("type") or "").lower()
    wrap_cls = "animated_wrap" if mtype in ("animation", "gif") else "video_file_wrap"
    img_cls = "animated" if mtype in ("animation", "gif") else "video_file"
    play_inner = '<div class="gif_play">GIF</div>' if mtype in ("animation", "gif") else '<div class="video_play"></div>'
    return (
        f'<div class="media_wrap clearfix">'
        f'<a class="{wrap_cls} clearfix pull_left" href="{href}">'
        f'<div class="video_play_bg">{play_inner}</div>'
        f'{dur_html}'
        f'<img class="{img_cls}" {style} src="{thumb_src}"/>'
        f'</a>'
        f'</div>'
    )


def _sticker_media_html(msg: Dict[str, Any], rel_prefix: str) -> str:
    subdir = _media_subdir("sticker")
    file_path = str(msg.get("file_path") or "").strip()
    href = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(file_path))}" if file_path else "#"
    w = int(msg.get("width") or 0)
    h = int(msg.get("height") or 0)
    if w > 0 and h > 0:
        ratio = min(_STICKER_MAX_W / w, _STICKER_MAX_H / h, 1.0)
        dw = max(80, int(w * ratio))
        dh = max(80, int(h * ratio))
        style = f'style="width: {dw}px; height: {dh}px"'
    else:
        style = 'style="width: 192px; height: 192px"'
    thumb_path = str(msg.get("thumb_path") or "").strip()
    if thumb_path:
        thumb_src = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(thumb_path))}"
    elif file_path:
        thumb_src = href
    else:
        emoji = str(msg.get("sticker_emoji") or "")
        size_str = _human_size(int(msg.get("file_size") or 0))
        label = f"{emoji}, {size_str}" if emoji else size_str
        return _generic_media_html(msg, rel_prefix, "media_photo", "Sticker", label)
    return (
        f'<div class="media_wrap clearfix">'
        f'<a class="sticker_wrap clearfix pull_left" href="{href}">'
        f'<img class="sticker" {style} src="{thumb_src}"/>'
        f'</a>'
        f'</div>'
    )


def _generic_media_html(msg: Dict[str, Any], rel_prefix: str, media_cls: str, title: str, status: str) -> str:
    mtype = str(msg.get("type") or "").lower()
    subdir = _media_subdir(mtype)
    file_path = str(msg.get("file_path") or "").strip()
    href = f"{rel_prefix}/{subdir}/{_html_escape(os.path.basename(file_path))}" if file_path else "#"
    block_link = ' block_link' if href != "#" else ""
    return (
        f'<div class="media_wrap clearfix">'
        f'<a class="media clearfix pull_left{block_link} {media_cls}" href="{href}">'
        f'<div class="fill pull_left"></div>'
        f'<div class="body">'
        f'<div class="title bold">{_html_escape(title)}</div>'
        f'<div class="status details">{_html_escape(status)}</div>'
        f'</div>'
        f'</a>'
        f'</div>'
    )


def _media_html(msg: Dict[str, Any], rel_prefix: str) -> str:
    mtype = str(msg.get("type") or "text").strip().lower()
    is_deleted = bool(msg.get("is_deleted"))
    if is_deleted:
        return ""
    if mtype in ("photo", "image") and msg.get("file_path"):
        return _photo_media_html(msg, rel_prefix)
    if mtype in ("video", "video_note", "animation", "gif") and msg.get("file_path"):
        return _video_media_html(msg, rel_prefix)
    if mtype == "sticker" and msg.get("file_path"):
        return _sticker_media_html(msg, rel_prefix)
    if mtype == "voice" and msg.get("file_path"):
        dur = msg.get("duration")
        status = _format_duration(dur) if dur else ""
        return _generic_media_html(msg, rel_prefix, "media_voice_message", "Voice message", status)
    if mtype == "audio" and msg.get("file_path"):
        dur = msg.get("duration")
        performer = str(msg.get("performer") or "").strip()
        title = str(msg.get("title") or msg.get("file_name") or "Audio file").strip()
        if performer:
            title = f"{performer} - {title}"
        status = _format_duration(dur) if dur else ""
        return _generic_media_html(msg, rel_prefix, "media_audio_file", title, status)
    if mtype == "document" or (msg.get("file_name") and msg.get("file_path")):
        fname = str(msg.get("file_name") or os.path.basename(str(msg.get("file_path") or "file")))
        fsize = int(msg.get("file_size") or 0)
        status = _human_size(fsize) if fsize else ""
        return _generic_media_html(msg, rel_prefix, "media_file", fname, status)
    return ""


def _message_needs_wrap(msg: Dict[str, Any], prev: Optional[Dict[str, Any]]) -> bool:
    if prev is None:
        return True
    prev_type = str(prev.get("type") or "text").lower()
    if prev_type in ("service", "date_separator"):
        return True
    prev_sender = prev.get("from_id") or prev.get("sender_id") or 0
    cur_sender = msg.get("from_id") or msg.get("sender_id") or 0
    if str(prev_sender) != str(cur_sender):
        return True
    prev_fwd = prev.get("forward_info")
    cur_fwd = msg.get("forward_info")
    if bool(prev_fwd) != bool(cur_fwd):
        return True
    prev_ts = int(prev.get("date") or prev.get("ts") or 0)
    cur_ts = int(msg.get("date") or msg.get("ts") or 0)
    if _format_date_label(prev_ts) != _format_date_label(cur_ts):
        return True
    if cur_fwd:
        threshold = 1
    else:
        threshold = _JOIN_WITHIN_SECONDS
    if cur_ts - prev_ts > threshold:
        return True
    return False


def _message_html(
    msg: Dict[str, Any],
    prev: Optional[Dict[str, Any]],
    my_id: Optional[int],
    rel_prefix: str,
) -> str:
    mid = int(msg.get("id") or msg.get("message_id") or 0)
    is_deleted = bool(msg.get("is_deleted"))
    mtype = str(msg.get("type") or "text").strip().lower()

    if mtype == "service" or mtype == "date_separator":
        return _service_message_html(msg, mid)

    needs_wrap = _message_needs_wrap(msg, prev)
    sender_id = msg.get("from_id") or msg.get("sender_id") or 0
    sender_name = str(msg.get("sender") or msg.get("from") or sender_id)
    ts = int(msg.get("date") or msg.get("ts") or 0)
    time_str = _format_time(ts)
    ts_full = _format_ts(ts)

    wrap_cls = "message default clearfix" if needs_wrap else "message default clearfix joined"
    parts: List[str] = [f'<div class="{wrap_cls}" id="message{mid}">']

    if needs_wrap:
        parts.append(f'<div class="pull_left userpic_wrap">')
        parts.append(_userpic_html(sender_name, sender_id))
        parts.append('</div>')

    parts.append('<div class="body">')
    parts.append(f'<div class="pull_right date details" title="{ts_full}">{time_str}</div>')

    if needs_wrap:
        parts.append(f'<div class="from_name">{_html_escape(sender_name)}</div>')

    reply_to = msg.get("reply_to") or msg.get("reply_to_message_id")
    if reply_to:
        rid = int(reply_to)
        parts.append(
            f'<div class="reply_to details">'
            f'In reply to <a href="#go_to_message{rid}" onclick="return GoToMessage({rid})">this message</a>'
            f'</div>'
        )

    forward_info = msg.get("forward_info")
    if isinstance(forward_info, dict) and forward_info:
        fwd_sender = str(forward_info.get("sender") or forward_info.get("from") or "Forwarded channel")
        fwd_date = forward_info.get("date") or forward_info.get("forward_date")
        date_span = ""
        if fwd_date:
            fwd_ts = int(fwd_date)
            fwd_date_str = _format_date_label(fwd_ts)
            date_span = f' <span class="date details" title="{_html_escape(_format_ts(fwd_ts))}">{_html_escape(fwd_date_str)}</span>'
        parts.append(f'<div class="pull_left forwarded userpic_wrap">')
        parts.append(_userpic_html(fwd_sender, forward_info.get("from_id", 0)))
        parts.append('</div>')
        parts.append(f'<div class="forwarded body">')
        parts.append(f'<div class="from_name">{_html_escape(fwd_sender)}{date_span}</div>')
        parts.append('</div>')

    media_html = _media_html(msg, rel_prefix)
    if media_html:
        parts.append(media_html)

    text = str(msg.get("text") or "").strip()
    entities = msg.get("text_entities") or msg.get("entities")
    if is_deleted:
        parts.append('<div class="text">🧹 Message deleted</div>')
    elif text:
        parts.append(f'<div class="text">{_text_entities_html(text, entities)}</div>')
    elif media_html:
        pass
    else:
        pass

    reactions = _reactions_html(msg)
    if reactions:
        parts.append(reactions)

    parts.append('</div>')
    parts.append('</div>')
    return "\n".join(parts)


def _service_message_html(msg: Dict[str, Any], mid: int) -> str:
    action = str(msg.get("action") or msg.get("text") or "")
    if not action:
        ts = int(msg.get("date") or msg.get("ts") or 0)
        action = _format_date_label(ts)
    return (
        f'<div class="message service" id="message{mid}">'
        f'<div class="body details">{_html_escape(action)}</div>'
        f'</div>'
    )


def _date_separator_html(ts: int, msg_id_counter: List[int]) -> str:
    label = _format_date_label(ts)
    if not label:
        return ""
    msg_id_counter[0] -= 1
    mid = msg_id_counter[0]
    return (
        f'<div class="message service" id="message{mid}">'
        f'<div class="body details">{_html_escape(label)}</div>'
        f'</div>'
    )


def _compose_html_page(
    chat_title: str,
    messages: Sequence[Dict[str, Any]],
    *,
    my_id: Optional[int] = None,
    rel_prefix: str = "../..",
    page_index: int = 0,
    total_pages: int = 1,
    dark_theme: bool = False,
) -> str:
    parts: List[str] = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html>")
    parts.append(" <head>")
    parts.append('  <meta charset="utf-8"/>')
    parts.append(f"  <title>{_html_escape(chat_title)}</title>")
    parts.append('  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>')
    parts.append(f'  <link href="{rel_prefix}/css/style.css" rel="stylesheet"/>')
    parts.append(f'  <script src="{rel_prefix}/js/script.js" type="text/javascript"></script>')
    parts.append(" </head>")
    parts.append(' <body onload="CheckLocation();">')
    parts.append('  <div class="page_wrap">')
    parts.append('   <div class="page_header">')
    parts.append(f'    <a class="content block_link" href="{rel_prefix}/lists/chats.html" onclick="return GoBack(this)">')
    parts.append(f'     <div class="text bold">{_html_escape(chat_title)}</div>')
    parts.append("    </a>")
    parts.append("   </div>")
    parts.append('   <div class="page_body chat_page">')
    parts.append('    <div class="history">')

    if page_index > 0:
        prev_name = f"messages{_page_suffix(page_index - 1)}.html"
        parts.append(f'     <a class="pagination block_link" href="{prev_name}">Previous messages</a>')

    msg_id_counter = [-1]
    prev_msg: Optional[Dict[str, Any]] = None
    last_date_str = ""

    for msg in messages:
        ts = int(msg.get("date") or msg.get("ts") or 0)
        date_str = _format_date_label(ts)
        if date_str != last_date_str and date_str:
            parts.append(_date_separator_html(ts, msg_id_counter))
            last_date_str = date_str
            prev_msg = {"type": "date_separator", "date": ts, "id": msg_id_counter[0]}
        parts.append(_message_html(msg, prev_msg, my_id, rel_prefix))
        prev_msg = msg

    if page_index < total_pages - 1:
        next_name = f"messages{_page_suffix(page_index + 1)}.html"
        parts.append(f'     <a class="pagination block_link" href="{next_name}">Next messages</a>')

    parts.append("    </div>")
    parts.append("   </div>")
    parts.append("  </div>")
    parts.append(" </body>")
    parts.append("</html>")
    return "\n".join(parts)


def _page_suffix(index: int) -> str:
    return str(index + 1) if index > 0 else ""


def _chat_type_label(chat_type: str) -> str:
    t = str(chat_type or "").lower()
    mapping = {
        "private": "personal_chat",
        "bot": "bot_chat",
        "group": "private_group",
        "supergroup": "public_supergroup",
        "megagroup": "private_supergroup",
        "channel": "public_channel",
    }
    return mapping.get(t, "personal_chat")


def build_json_export(
    chat_title: str,
    messages: Sequence[Dict[str, Any]],
    *,
    chat_type: str = "chat",
    chat_id: str = "",
    my_id: Optional[int] = None,
    total_count: int = 0,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
) -> str:
    json_msgs: List[Dict[str, Any]] = []
    for msg in messages:
        jm: Dict[str, Any] = {}
        mid = int(msg.get("id") or msg.get("message_id") or 0)
        jm["id"] = mid
        ts = int(msg.get("date") or msg.get("ts") or 0)
        try:
            import datetime
            dt = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            jm["date"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            jm["date"] = str(ts)
        jm["date_unixtime"] = str(ts)

        mtype = str(msg.get("type") or "text").lower()
        is_service = mtype == "service"
        jm["type"] = "service" if is_service else "message"

        sender_name = str(msg.get("sender") or msg.get("from") or "")
        sender_id = str(msg.get("from_id") or msg.get("sender_id") or "")
        if is_service:
            jm["actor"] = sender_name
            jm["actor_id"] = f"user{sender_id}" if sender_id else ""
            action = str(msg.get("action") or msg.get("text") or "")
            jm["action"] = action
        else:
            jm["from"] = sender_name
            jm["from_id"] = f"user{sender_id}" if sender_id else ""

        text = str(msg.get("text") or "")
        entities = msg.get("text_entities") or msg.get("entities")
        if isinstance(entities, list) and entities:
            jm["text"] = text
            jm["text_entities"] = [
                {
                    "type": str(e.get("type", "plain")).lower() if isinstance(e, dict) else "plain",
                    "text": text[int(e.get("offset", 0)):int(e.get("offset", 0)) + int(e.get("length", 0))]
                    if isinstance(e, dict) and e.get("offset") is not None and e.get("length") is not None
                    else str(e.get("text", "")),
                }
                for e in entities if isinstance(e, dict)
            ]
        else:
            jm["text"] = text
            if text:
                jm["text_entities"] = [{"type": "plain", "text": text}]

        reply_to = msg.get("reply_to") or msg.get("reply_to_message_id")
        if reply_to:
            jm["reply_to_message_id"] = int(reply_to)

        forward_info = msg.get("forward_info")
        if isinstance(forward_info, dict) and forward_info:
            fwd_from = str(forward_info.get("sender") or forward_info.get("from") or "")
            jm["forwarded_from"] = fwd_from
            fwd_date = forward_info.get("date") or forward_info.get("forward_date")
            if fwd_date:
                try:
                    import datetime
                    dt = datetime.datetime.fromtimestamp(int(fwd_date), tz=datetime.timezone.utc)
                    jm["forwarded_date"] = dt.strftime("%Y-%m-%dT%H:%M:%S")
                    jm["forwarded_date_unixtime"] = str(int(fwd_date))
                except Exception:
                    pass

        file_path = str(msg.get("file_path") or "").strip()
        if file_path:
            mtype_key = str(msg.get("type") or "").lower()
            subdir = _media_subdir(mtype_key)
            fname = os.path.basename(file_path)
            if mtype_key in ("photo", "image"):
                jm["photo"] = f"{subdir}/{fname}"
                w = int(msg.get("width") or 0)
                h = int(msg.get("height") or 0)
                if w:
                    jm["width"] = w
                if h:
                    jm["height"] = h
                fsize = int(msg.get("file_size") or 0)
                if fsize:
                    jm["photo_file_size"] = fsize
            elif mtype_key in ("video", "video_note"):
                jm["file"] = f"{subdir}/{fname}"
                dur = msg.get("duration")
                if dur:
                    jm["duration_seconds"] = int(dur)
                jm["media_type"] = "video_message" if mtype_key == "video_note" else "video_file"
            elif mtype_key in ("animation", "gif"):
                jm["file"] = f"animations/{fname}"
                jm["media_type"] = "animation"
            elif mtype_key == "sticker":
                jm["file"] = f"stickers/{fname}"
                jm["media_type"] = "sticker"
                emoji = str(msg.get("sticker_emoji") or "")
                if emoji:
                    jm["sticker_emoji"] = emoji
            elif mtype_key == "voice":
                jm["file"] = f"voice/{fname}"
                jm["media_type"] = "voice_message"
                dur = msg.get("duration")
                if dur:
                    jm["duration_seconds"] = int(dur)
            elif mtype_key == "audio":
                jm["file"] = f"audio/{fname}"
                jm["media_type"] = "audio_file"
                dur = msg.get("duration")
                if dur:
                    jm["duration_seconds"] = int(dur)
                performer = str(msg.get("performer") or "")
                title = str(msg.get("title") or "")
                if performer:
                    jm["performer"] = performer
                if title:
                    jm["title"] = title
            else:
                fname_hint = str(msg.get("file_name") or fname)
                jm["file"] = f"files/{fname}"
                jm["file_name"] = fname_hint
                fsize = int(msg.get("file_size") or 0)
                if fsize:
                    jm["file_size"] = fsize
            mime = str(msg.get("mime") or msg.get("mime_type") or "")
            if mime:
                jm["mime_type"] = mime

        reactions = msg.get("reactions")
        if isinstance(reactions, list) and reactions:
            json_reactions = []
            for r in reactions:
                if not isinstance(r, dict):
                    continue
                rtype = "custom_emoji" if r.get("document_id") else "emoji"
                jr: Dict[str, Any] = {"type": rtype, "count": int(r.get("count") or 0)}
                if rtype == "emoji":
                    jr["emoji"] = str(r.get("emoji") or "")
                else:
                    jr["document_id"] = str(r.get("document_id") or "")
                json_reactions.append(jr)
            if json_reactions:
                jm["reactions"] = json_reactions

        if msg.get("views"):
            jm["views"] = int(msg["views"])
        if msg.get("forwards"):
            jm["forwards"] = int(msg["forwards"])

        json_msgs.append(jm)

    chat_entry: Dict[str, Any] = {
        "name": chat_title,
        "type": _chat_type_label(chat_type),
        "id": str(chat_id),
        "messages": json_msgs,
    }
    if my_id:
        chat_entry["my_id"] = str(my_id)

    about_text = "This data was exported from ESCgram on %s" % _format_ts(int(time.time()))
    result: Dict[str, Any] = {
        "about": about_text,
        "chats": [chat_entry],
    }
    if my_id:
        result["personal_information"] = {"user_id": str(my_id)}

    return json.dumps(result, ensure_ascii=False, indent=2)


def _copy_media_assets(
    messages: Sequence[Dict[str, Any]],
    export_dir: Path,
    *,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> int:
    total = len(messages)
    copied = 0
    for idx, msg in enumerate(messages):
        file_path = str(msg.get("file_path") or "").strip()
        if not file_path or not os.path.isfile(file_path):
            continue
        mtype = str(msg.get("type") or "text").lower()
        subdir_name = _media_subdir(mtype)
        subdir = export_dir / subdir_name
        subdir.mkdir(parents=True, exist_ok=True)
        dest = subdir / os.path.basename(file_path)
        if dest.exists():
            continue
        try:
            shutil.copy2(file_path, dest)
            copied += 1
        except Exception:
            log.debug("Failed to copy media asset %s", file_path, exc_info=True)
        if progress_cb and idx % 50 == 0:
            try:
                progress_cb(idx, total, f"Copying media {idx}/{total}")
            except Exception:
                pass
    return copied


def _write_static_files(export_dir: Path, dark_theme: bool = False) -> None:
    css_dir = export_dir / "css"
    css_dir.mkdir(parents=True, exist_ok=True)
    css_content = _CSS
    if dark_theme:
        css_content += "\n" + _CSS_DARK
    (css_dir / "style.css").write_text(css_content, encoding="utf-8")

    js_dir = export_dir / "js"
    js_dir.mkdir(parents=True, exist_ok=True)
    (js_dir / "script.js").write_text(_JS, encoding="utf-8")

    lists_dir = export_dir / "lists"
    lists_dir.mkdir(parents=True, exist_ok=True)
    chats_html = (
        "<!DOCTYPE html>\n<html>\n <head>\n"
        '  <meta charset="utf-8"/>\n'
        "  <title>Exported Data</title>\n"
        '  <link href="../css/style.css" rel="stylesheet"/>\n'
        '  <script src="../js/script.js" type="text/javascript"></script>\n'
        " </head>\n"
        ' <body onload="CheckLocation();">\n'
        '  <div class="page_wrap">\n'
        '   <div class="page_body">\n'
        '    <div class="history">\n'
        "    </div>\n"
        "   </div>\n"
        "  </div>\n"
        " </body>\n</html>"
    )
    (lists_dir / "chats.html").write_text(chats_html, encoding="utf-8")


def export_chat_to_files(
    output_dir: str,
    chat_title: str,
    messages: Sequence[Dict[str, Any]],
    *,
    my_id: Optional[int] = None,
    chat_type: str = "chat",
    chat_id: str = "",
    total_count: int = 0,
    date_from: Optional[int] = None,
    date_to: Optional[int] = None,
    copy_media: bool = True,
    formats: Sequence[str] = ("html", "json"),
    dark_theme: bool = True,
    media_types: Optional[Sequence[str]] = None,
    max_media_size_mb: int = 8,
    progress_cb: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    out_dir = Path(str(output_dir or "")).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_name = re.sub(r"[^A-Za-z0-9А-Яа-яёЁ._-]+", "_", chat_title).strip("_") or "chat_export"
    chat_dir = out_dir / safe_name
    chat_dir.mkdir(parents=True, exist_ok=True)

    allowed_media = set(media_types) if media_types else None
    max_media_bytes = max_media_size_mb * 1024 * 1024

    filtered_messages: List[Dict[str, Any]] = []
    for msg in messages:
        mtype = str(msg.get("type") or "text").lower()
        if mtype == "text" or mtype == "service":
            filtered_messages.append(msg)
            continue
        if allowed_media is not None and mtype not in allowed_media:
            msg = dict(msg)
            msg["file_path"] = ""
            msg["_media_filtered"] = True
            filtered_messages.append(msg)
            continue
        fsize = int(msg.get("file_size") or 0)
        if fsize > 0 and fsize > max_media_bytes:
            msg = dict(msg)
            msg["file_path"] = ""
            msg["_media_filtered"] = True
            filtered_messages.append(msg)
            continue
        filtered_messages.append(msg)

    if copy_media:
        try:
            if progress_cb:
                try:
                    progress_cb(0, len(filtered_messages), "Copying media files...")
                except Exception:
                    pass
            _copy_media_assets(filtered_messages, chat_dir, progress_cb=progress_cb)
        except Exception:
            log.exception("Failed to copy media assets during export")

    result_files: Dict[str, str] = {}

    if "html" in formats:
        _write_static_files(out_dir, dark_theme=dark_theme)

        all_msgs = filtered_messages
        total_msgs = len(all_msgs)
        total_pages = max(1, (total_msgs + _MESSAGES_PER_FILE - 1) // _MESSAGES_PER_FILE)

        if progress_cb:
            try:
                progress_cb(0, total_msgs, "Generating HTML...")
            except Exception:
                pass

        for page_idx in range(total_pages):
            start = page_idx * _MESSAGES_PER_FILE
            end = min(start + _MESSAGES_PER_FILE, total_msgs)
            page_msgs = all_msgs[start:end]
            page_html = _compose_html_page(
                chat_title,
                page_msgs,
                my_id=my_id,
                rel_prefix="../..",
                page_index=page_idx,
                total_pages=total_pages,
                dark_theme=dark_theme,
            )
            fname = f"messages{_page_suffix(page_idx)}.html"
            (chat_dir / fname).write_text(page_html, encoding="utf-8")
            if page_idx == 0:
                result_files["html"] = str(chat_dir / fname)

        index_html = (
            "<!DOCTYPE html>\n<html>\n <head>\n"
            '  <meta charset="utf-8"/>\n'
            "  <title>Exported Data</title>\n"
            '  <link href="css/style.css" rel="stylesheet"/>\n'
            '  <script src="js/script.js" type="text/javascript"></script>\n'
            " </head>\n"
            ' <body onload="CheckLocation();">\n'
            '  <div class="page_wrap">\n'
            '   <div class="page_header">\n'
            '    <div class="content">\n'
            '     <div class="text bold">Exported Data</div>\n'
            "    </div>\n"
            "   </div>\n"
            '   <div class="page_body">\n'
            f'    <a class="block_link with_divider" href="{_html_escape(safe_name)}/messages.html">\n'
            f'     <div class="text bold">{_html_escape(chat_title)}</div>\n'
            f'     <div class="details">{total_msgs} messages</div>\n'
            "    </a>\n"
            "   </div>\n"
            "  </div>\n"
            " </body>\n</html>"
        )
        (out_dir / "export_results.html").write_text(index_html, encoding="utf-8")

    if "json" in formats:
        if progress_cb:
            try:
                progress_cb(0, len(messages), "Generating JSON...")
            except Exception:
                pass
        json_content = build_json_export(
            chat_title,
            filtered_messages,
            chat_type=chat_type,
            chat_id=chat_id,
            my_id=my_id,
            total_count=total_count,
            date_from=date_from,
            date_to=date_to,
        )
        json_path = out_dir / "result.json"
        json_path.write_text(json_content, encoding="utf-8")
        result_files["json"] = str(json_path)

    return {
        "ok": True,
        "output_dir": str(out_dir),
        "files": result_files,
        "total_messages": len(messages),
    }

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, List, Optional
from urllib.parse import unquote, urlparse


_TELEGRAM_HOSTS = {
    "t.me",
    "www.t.me",
    "telegram.me",
    "www.telegram.me",
    "telegram.dog",
    "www.telegram.dog",
}
_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,}$")


@dataclass(frozen=True)
class TelegramReference:
    raw: str
    username: str = ""
    invite: str = ""
    message_id: Optional[int] = None
    canonical: str = ""

    @property
    def is_username(self) -> bool:
        return bool(self.username)

    @property
    def is_invite(self) -> bool:
        return bool(self.invite)

    @property
    def search_term(self) -> str:
        if self.username:
            return self.username
        if self.invite:
            return self.canonical or self.raw
        return self.raw


def parse_telegram_reference(value: Any) -> TelegramReference:
    raw = unquote(str(value or "")).strip()
    if not raw:
        return TelegramReference(raw="")

    if raw.startswith("@"):
        username = raw[1:].split("/", 1)[0].strip()
        if _USERNAME_RE.fullmatch(username):
            return TelegramReference(
                raw=raw,
                username=username,
                canonical=f"https://t.me/{username}",
            )
        return TelegramReference(raw=raw)

    parsed_url = None
    url_candidate = raw
    lowered = raw.lower()
    if lowered.startswith(("t.me/", "telegram.me/", "www.t.me/", "www.telegram.me/", "telegram.dog/", "www.telegram.dog/")):
        url_candidate = "https://" + raw
    if "://" in url_candidate:
        try:
            candidate = urlparse(url_candidate)
        except Exception:
            candidate = None
        if candidate and str(candidate.netloc or "").lower() in _TELEGRAM_HOSTS:
            parsed_url = candidate

    if parsed_url is not None:
        segments = [part for part in str(parsed_url.path or "").split("/") if part]
        if segments[:1] == ["s"] and len(segments) > 1:
            segments = segments[1:]
        if segments:
            head = str(segments[0] or "").strip()
            if head == "joinchat" and len(segments) > 1:
                invite = str(segments[1] or "").strip()
                if invite:
                    return TelegramReference(
                        raw=raw,
                        invite=invite,
                        canonical=f"https://t.me/joinchat/{invite}",
                    )
            if head.startswith("+"):
                invite = head[1:].strip()
                if invite:
                    return TelegramReference(
                        raw=raw,
                        invite=invite,
                        canonical=f"https://t.me/+{invite}",
                    )
            username = head.lstrip("@")
            if _USERNAME_RE.fullmatch(username):
                message_id = None
                if len(segments) > 1:
                    try:
                        message_id = int(segments[1])
                    except Exception:
                        message_id = None
                return TelegramReference(
                    raw=raw,
                    username=username,
                    message_id=message_id,
                    canonical=f"https://t.me/{username}",
                )

    if _USERNAME_RE.fullmatch(raw):
        return TelegramReference(
            raw=raw,
            username=raw,
            canonical=f"https://t.me/{raw}",
        )
    return TelegramReference(raw=raw)


def build_search_aliases(username: Any) -> List[str]:
    handle = str(username or "").strip().lstrip("@")
    if not _USERNAME_RE.fullmatch(handle):
        return []
    return [
        handle.lower(),
        f"@{handle.lower()}",
        f"t.me/{handle.lower()}",
        f"https://t.me/{handle.lower()}",
        f"telegram.me/{handle.lower()}",
        f"https://telegram.me/{handle.lower()}",
    ]

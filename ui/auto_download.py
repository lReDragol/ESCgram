from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

_MB = 1024 * 1024


def _default_limits() -> Dict[str, Dict[str, int]]:
    """Return Telegram Desktop-like auto-download limits (bytes)."""
    base = {
        "image": 12 * _MB,
        "voice": 12 * _MB,
        "video_note": 50 * _MB,
        "animation": 50 * _MB,
        "video": 50 * _MB,
        "document": 12 * _MB,
        "audio": 12 * _MB,
    }
    user = dict(base)
    group = dict(base)
    channel = dict(base)
    channel["document"] = 0  # Telegram disables auto file download in channels by default.
    return {"user": user, "group": group, "channel": channel}


KIND_MAP = {
    "photo": "image",
    "image": "image",
    "gif": "animation",
    "animation": "animation",
    "video": "video",
    "video_note": "video_note",
    "circle": "video_note",
    "round_video": "video_note",
    "voice": "voice",
    "voice_message": "voice",
    "audio": "audio",
    "song": "audio",
    "music": "audio",
    "document": "document",
    "file": "document",
}

SOURCE_MAP = {
    "user": "user",
    "private": "user",
    "bot": "user",
    "self": "user",
    "group": "group",
    "supergroup": "group",
    "megagroup": "group",
    "channel": "channel",
}


@dataclass
class AutoDownloadPolicy:
    limits: Dict[str, Dict[str, int]] = field(default_factory=_default_limits)

    @classmethod
    def from_config(cls, config: Optional[Dict[str, Dict[str, int]]]) -> "AutoDownloadPolicy":
        limits = _default_limits()
        if isinstance(config, dict):
            for source, table in config.items():
                if source not in limits or not isinstance(table, dict):
                    continue
                for kind, value in table.items():
                    try:
                        limits[source][kind] = max(0, int(value))
                    except Exception:
                        continue
        return cls(limits=limits)

    def to_config(self) -> Dict[str, Dict[str, int]]:
        return {
            source: dict(table)
            for source, table in self.limits.items()
        }

    @staticmethod
    def _kind_group(kind: str) -> str:
        key = (kind or "").lower()
        return KIND_MAP.get(key, key)

    @staticmethod
    def _source_group(chat_type: Optional[str]) -> str:
        key = (chat_type or "").lower()
        return SOURCE_MAP.get(key, "user")

    def should_download(
        self,
        *,
        chat_type: Optional[str],
        kind: str,
        file_size: Optional[int],
    ) -> bool:
        source = self._source_group(chat_type)
        bucket = self.limits.get(source) or {}
        group_kind = self._kind_group(kind)
        limit = bucket.get(group_kind)
        if limit is None or limit <= 0:
            return False
        if not file_size or file_size <= 0:
            return True
        return int(file_size) <= limit

from __future__ import annotations

import json
import logging
import os
import weakref
from typing import Any

from utils import app_paths

log = logging.getLogger("gui")

# Qt multimedia (optional)
try:
    from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput  # noqa: F401
    from PySide6.QtMultimediaWidgets import QVideoWidget  # noqa: F401
    HAVE_QTMULTIMEDIA = True
except Exception:
    HAVE_QTMULTIMEDIA = False
    QMediaPlayer = object  # type: ignore[assignment]
    QAudioOutput = object  # type: ignore[assignment]
    QVideoWidget = object  # type: ignore[assignment]

# Audio recording (optional)
try:
    import sounddevice as sd  # noqa: F401
    import soundfile as sf  # noqa: F401
    HAVE_SD = True
except Exception:
    HAVE_SD = False


class MediaPlaybackCoordinator:
    """Best-effort coordinator to avoid multiple QMediaPlayer instances playing at once.

    This reduces resource usage and avoids backend edge-cases where multiple concurrent
    media players cause UI stalls/freezes on some platforms.
    """

    _players: list[weakref.ReferenceType] = []

    @classmethod
    def register(cls, player: Any) -> None:
        if not HAVE_QTMULTIMEDIA or player is None:
            return
        try:
            ref = weakref.ref(player)
        except Exception:
            return

        # Deduplicate refs pointing to the same object.
        for existing in cls._players:
            if existing() is player:
                return
        cls._players.append(ref)
        cls._players = [r for r in cls._players if r() is not None]

    @classmethod
    def pause_others(cls, active_player: Any) -> None:
        if not HAVE_QTMULTIMEDIA or active_player is None:
            return
        alive: list[weakref.ReferenceType] = []
        for ref in cls._players:
            player = ref()
            if player is None:
                continue
            alive.append(ref)
            if player is active_player:
                continue
            try:
                if player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:  # type: ignore[attr-defined]
                    player.pause()
            except Exception:
                # We do not want playback coordination to break UI actions.
                pass
        cls._players = alive

HISTORY_FILE = str(app_paths.get_data_dir() / "history.json")


def load_history() -> dict:
    if os.path.isfile(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
                if "chats" not in d:
                    d["chats"] = {}
                return d
        except Exception:
            pass
    return {"chats": {}}


def save_history(d: dict) -> None:
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


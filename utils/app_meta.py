from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List


def _candidate_roots() -> List[Path]:
    roots: List[Path] = []
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            roots.append(Path(meipass))
        except Exception:
            pass
    try:
        roots.append(Path(__file__).resolve().parents[1])
    except Exception:
        pass
    try:
        roots.append(Path.cwd())
    except Exception:
        pass
    unique: List[Path] = []
    seen: set[str] = set()
    for root in roots:
        key = str(root)
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def resolve_app_icon_path() -> str:
    env_icon = str(os.getenv("DRAGO_APP_ICON") or "").strip()
    if env_icon and os.path.isfile(env_icon):
        return env_icon
    candidates = [
        Path("ui/assets/app/ESCgram_ICO.png"),
        Path("ui/assets/app/escgram.ico"),
        Path("ESCgram_ICO.png"),
    ]
    for root in _candidate_roots():
        for rel in candidates:
            path = root / rel
            if path.exists():
                return str(path)
    return ""


def get_app_version() -> str:
    env_version = str(os.getenv("DRAGO_APP_VERSION") or "").strip()
    if env_version:
        return env_version
    for root in _candidate_roots():
        version_file = root / "version.txt"
        if version_file.exists():
            try:
                value = version_file.read_text(encoding="utf-8").strip()
                if value:
                    return value
            except Exception:
                continue
    return "0.1.0"


def get_update_repo() -> str:
    return str(os.getenv("DRAGO_UPDATE_REPO") or "lReDragol/ESCgram").strip()


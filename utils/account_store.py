from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional


class AccountStore:
    """Lightweight persistence helper for Telegram session metadata."""

    def __init__(self, workdir: Path) -> None:
        self._path = Path(workdir) / "accounts.json"
        self._data: Dict[str, object] = {"active": None, "accounts": {}}
        self._load()

    # ---------------------------- basics ----------------------------
    def _load(self) -> None:
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {"active": None, "accounts": {}}
        self._data.setdefault("accounts", {})

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")

    # ---------------------------- helpers ----------------------------
    @property
    def active_session(self) -> Optional[str]:
        active = self._data.get("active")
        return str(active) if active else None

    def set_active(self, session_name: str) -> None:
        self._data["active"] = session_name
        self._save()

    def ensure_account(self, session_name: str) -> None:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        accounts.setdefault(session_name, {"title": session_name, "phone": "", "last_used": 0})
        self._save()

    def has_account(self, session_name: str) -> bool:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        return session_name in accounts

    def update_account(self, session_name: str, *, create_if_missing: bool = True, **meta: object) -> bool:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        if create_if_missing:
            account = accounts.setdefault(session_name, {"title": session_name})
        else:
            account = accounts.get(session_name)
            if account is None:
                return False
        last_used = meta.get("last_used") if meta else None
        for key, value in meta.items():
            if key == "last_used":
                continue
            if value is not None:
                account[key] = value
        if last_used is not None:
            account["last_used"] = float(last_used)
        else:
            account.setdefault("last_used", time.time())
        self._save()
        return True

    def get_account(self, session_name: str) -> Dict[str, object]:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        return dict(accounts.get(session_name, {}))

    def list_accounts(self, active_session: Optional[str]) -> List[Dict[str, object]]:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        items: List[Dict[str, object]] = []
        for session, meta in accounts.items():
            row = dict(meta)
            row["session"] = session
            row["is_active"] = session == active_session
            items.append(row)
        items.sort(key=lambda item: (
            not item.get("is_active", False),
            -float(item.get("last_used") or 0.0),
            item.get("title") or item.get("session") or "",
        ))
        return items

    def remove_account(self, session_name: str) -> None:
        accounts: Dict[str, Dict[str, object]] = self._data.setdefault("accounts", {})  # type: ignore[assignment]
        if session_name in accounts:
            accounts.pop(session_name)
            if self._data.get("active") == session_name:
                self._data["active"] = None
            self._save()

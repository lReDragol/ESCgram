from __future__ import annotations

import sqlite3
from pathlib import Path

from telegram import TelegramAdapter
from utils import app_paths
from utils.account_store import AccountStore


def _set_test_data_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DRAGO_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(app_paths, "_DATA_DIR", tmp_path, raising=False)


def _create_telethon_session(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE version
            (
                version INTEGER PRIMARY KEY
            );

            CREATE TABLE sessions
            (
                dc_id INTEGER PRIMARY KEY,
                server_address TEXT,
                port INTEGER,
                auth_key BLOB,
                takeout_id INTEGER
            );
            """
        )
        conn.execute("INSERT INTO version VALUES (?)", (7,))
        conn.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?)",
            (2, "149.154.167.50", 443, sqlite3.Binary(b"\x11" * 256), None),
        )
        conn.commit()
    finally:
        conn.close()


def test_import_session_file_sync_converts_telethon_sqlite_session(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    source_path = tmp_path / "telethon_source.session"
    _create_telethon_session(source_path)

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._api_id = 123456
    adapter._api_hash = "test-hash"
    adapter._workdir = tmp_path / "telegram"
    adapter._workdir.mkdir(parents=True, exist_ok=True)
    adapter._account_store = AccountStore(adapter._workdir)
    adapter._session_name = "current"
    (adapter._workdir / "current.session").write_bytes(b"legacy")

    activated: list[tuple[str, bool, bool]] = []

    def _activate(session_name: str, *, clean: bool = False, register: bool = True) -> None:
        activated.append((session_name, bool(clean), bool(register)))
        adapter._session_name = session_name

    monkeypatch.setattr(adapter, "_activate_session", _activate)
    monkeypatch.setattr(adapter, "is_authorized_sync", lambda timeout=5.0: True)
    monkeypatch.setattr(
        adapter,
        "refresh_active_account_profile",
        lambda: {"title": "Imported", "session": adapter._session_name},
    )

    result = adapter.import_session_file_sync(str(source_path), timeout=5.0)

    target_path = adapter._workdir / "telethon_source.session"
    assert target_path.exists()
    assert activated == [("telethon_source", False, False)]
    assert result["session"] == "telethon_source"

    conn = sqlite3.connect(target_path)
    try:
        row = conn.execute(
            "SELECT dc_id, api_id, test_mode, auth_key, user_id, is_bot FROM sessions"
        ).fetchone()
        assert row is not None
        assert row[0] == 2
        assert row[1] == 123456
        assert row[2] == 0
        assert bytes(row[3]) == b"\x11" * 256
        assert row[4] is None
        assert row[5] is None
    finally:
        conn.close()


def test_write_pyrogram_session_file_replaces_existing_target_database(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    source_path = tmp_path / "telethon_source.session"
    _create_telethon_session(source_path)
    target_path = tmp_path / "telegram" / "account_2.session"
    target_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(target_path)
    try:
        conn.executescript(
            """
            CREATE TABLE sessions
            (
                dc_id INTEGER PRIMARY KEY,
                api_id INTEGER,
                test_mode INTEGER,
                auth_key BLOB,
                date INTEGER NOT NULL,
                user_id INTEGER,
                is_bot INTEGER
            );

            CREATE TABLE version
            (
                number INTEGER PRIMARY KEY
            );
            """
        )
        conn.execute("INSERT INTO version VALUES (?)", (3,))
        conn.execute(
            "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, 1, 0, sqlite3.Binary(b"\x22" * 256), 1, None, None),
        )
        conn.commit()
    finally:
        conn.close()

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._api_id = 999999
    adapter._api_hash = "test-hash"

    adapter._write_pyrogram_session_file(source_path, target_path, "telethon")

    conn = sqlite3.connect(target_path)
    try:
        row = conn.execute("SELECT dc_id, api_id, auth_key FROM sessions").fetchone()
        assert row is not None
        assert row[0] == 2
        assert row[1] == 999999
        assert bytes(row[2]) == b"\x11" * 256
    finally:
        conn.close()


def test_import_session_file_sync_keeps_imported_session_for_phone_login(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    source_path = tmp_path / "telethon_source.session"
    _create_telethon_session(source_path)

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._api_id = 123456
    adapter._api_hash = "test-hash"
    adapter._workdir = tmp_path / "telegram"
    adapter._workdir.mkdir(parents=True, exist_ok=True)
    adapter._account_store = AccountStore(adapter._workdir)
    adapter._session_name = "current"
    (adapter._workdir / "current.session").write_bytes(b"legacy")

    activated: list[tuple[str, bool, bool]] = []

    def _activate(session_name: str, *, clean: bool = False, register: bool = True) -> None:
        activated.append((session_name, bool(clean), bool(register)))
        adapter._session_name = session_name

    def _is_authorized(timeout: float = 5.0) -> bool:
        _ = timeout
        adapter._auth_invalid = True
        return False

    monkeypatch.setattr(adapter, "_activate_session", _activate)
    monkeypatch.setattr(adapter, "is_authorized_sync", _is_authorized)

    result = adapter.import_session_file_sync(str(source_path), timeout=5.0)

    assert result["authorized"] is False
    assert result["requires_login"] is True
    assert result["session"] == "telethon_source"
    assert "Введите номер" in result["message"]
    assert activated == [
        ("telethon_source", False, False),
        ("telethon_source", True, False),
    ]
    assert adapter._pending_session_name == "telethon_source"
    assert (adapter._workdir / "telethon_source.session").exists()

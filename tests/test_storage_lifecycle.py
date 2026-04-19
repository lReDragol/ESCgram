from __future__ import annotations

import sqlite3
import types
from pathlib import Path

import storage as storage_module
from storage import Storage


def test_storage_connect_accepts_filename_without_parent(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    st = Storage("plain.db")
    try:
        st.connect()
        assert Path("plain.db").exists()
    finally:
        st.close()


def test_storage_falls_back_to_sqlite_when_apsw_connect_fails(tmp_path, monkeypatch) -> None:
    class _BrokenApswConnection:
        def __init__(self, *_args, **_kwargs) -> None:
            raise RuntimeError("apsw unavailable at runtime")

    monkeypatch.setattr(storage_module, "HAVE_APSW", True)
    monkeypatch.setattr(
        storage_module,
        "apsw",
        types.SimpleNamespace(Connection=_BrokenApswConnection),
    )

    st = Storage(str(tmp_path / "fallback.db"))
    try:
        st.connect()
        assert st._cx is not None
        assert st._cx.is_apsw is False
    finally:
        st.close()


def test_storage_init_schema_adds_missing_legacy_message_columns(tmp_path) -> None:
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE messages (
              peer_id INTEGER NOT NULL,
              id INTEGER NOT NULL,
              date INTEGER NOT NULL,
              from_id INTEGER,
              reply_to INTEGER,
              message TEXT,
              media_type TEXT,
              media_id TEXT,
              PRIMARY KEY (peer_id, id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    st = Storage(str(db_path))
    try:
        st.connect()
        st.init_schema()
        columns = st._table_columns("messages")
        assert "is_deleted" in columns
        assert "reactions" in columns
        assert "poll" in columns
        assert "media_group_id" in columns
    finally:
        st.close()

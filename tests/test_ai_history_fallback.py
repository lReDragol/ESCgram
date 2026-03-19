from __future__ import annotations

import json
from pathlib import Path

from ai import AIChat
from storage import Storage
from utils import app_paths


class _BrokenStorage:
    def get_ai_history(self, _chat_id: str, limit: int = 30):
        _ = limit
        raise RuntimeError("db offline")

    def append_ai_messages(self, _chat_id: str, _messages, *, limit: int = 30) -> None:
        _ = limit
        raise RuntimeError("db offline")


def _set_test_data_dir(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DRAGO_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(app_paths, "_DATA_DIR", tmp_path, raising=False)


def _make_storage(tmp_path: Path) -> Storage:
    db_path = tmp_path / "test_ai_history.db"
    st = Storage(str(db_path))
    st.connect()
    st.init_schema()
    return st


def test_ai_chat_loads_legacy_history_when_storage_read_fails(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    history_path = tmp_path / "chats" / "chat_read_fail" / "history.json"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(
        json.dumps(
            [
                {"id": 3, "role": "user", "content": "legacy message", "timestamp": "2026-03-01T10:00:00Z"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    chat = AIChat("chat_read_fail", storage=_BrokenStorage())

    assert chat.history
    assert chat.history[0]["message_id"] == 3
    assert chat.history[0]["content"] == "legacy message"


def test_ai_chat_migrates_legacy_history_into_storage(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    history_path = tmp_path / "chats" / "chat_migrate" / "history.json"
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(
        json.dumps(
            [
                {"id": 1, "role": "user", "content": "старый контекст", "timestamp": "2026-03-01T09:00:00Z"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    st = _make_storage(tmp_path)
    try:
        chat = AIChat("chat_migrate", storage=st)
        stored = st.get_ai_history("chat_migrate", limit=10)

        assert chat.history[0]["message_id"] == 1
        assert stored
        assert stored[0]["content"] == "старый контекст"
    finally:
        st.close()


def test_ai_chat_falls_back_to_file_when_storage_write_fails(monkeypatch, tmp_path) -> None:
    _set_test_data_dir(monkeypatch, tmp_path)
    chat = AIChat("chat_write_fail", storage=_BrokenStorage())
    chat.history = [
        {
            "message_id": 7,
            "role": "assistant",
            "content": "persist me",
            "timestamp": "2026-03-01T11:00:00Z",
            "reply_to": None,
            "is_edited": False,
            "is_deleted": False,
        }
    ]

    chat.save_history()

    persisted = json.loads((tmp_path / "chats" / "chat_write_fail" / "history.json").read_text(encoding="utf-8"))
    assert persisted[0]["message_id"] == 7
    assert persisted[0]["content"] == "persist me"

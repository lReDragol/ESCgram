from __future__ import annotations

from ai import AIChat
from storage import Storage


def _make_storage(tmp_path) -> Storage:
    db_path = tmp_path / "test_ai.db"
    st = Storage(str(db_path))
    st.connect()
    st.init_schema()
    return st


def test_search_ai_history_excludes_current_chat(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.append_ai_messages(
            "chat_a",
            [
                {"message_id": 1, "role": "user", "content": "встреча 12.03", "timestamp": "2026-03-01T10:00:00Z"},
            ],
        )
        st.append_ai_messages(
            "chat_b",
            [
                {"message_id": 1, "role": "assistant", "content": "Ок, встреча 12.03 подтверждена", "timestamp": "2026-03-01T11:00:00Z"},
            ],
        )
        st.append_ai_messages(
            "chat_c",
            [
                {"message_id": 1, "role": "assistant", "content": "другая тема без даты", "timestamp": "2026-03-01T12:00:00Z"},
            ],
        )

        results = st.search_ai_history(["встреча", "12.03"], exclude_chat_id="chat_a", limit=4)
        assert results
        assert all(str(item.get("chat_id")) != "chat_a" for item in results)
        assert any(str(item.get("chat_id")) == "chat_b" for item in results)
    finally:
        st.close()


def test_ai_chat_builds_cross_chat_context(monkeypatch, tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.append_ai_messages(
            "chat_target",
            [
                {"message_id": 1, "role": "assistant", "content": "На 12.03 уже есть встреча в 18:00", "timestamp": "2026-03-02T18:00:00Z"},
            ],
        )
        st.append_ai_messages(
            "chat_current",
            [
                {"message_id": 1, "role": "user", "content": "локальный контекст", "timestamp": "2026-03-02T19:00:00Z"},
            ],
        )

        monkeypatch.setenv("DRAGO_AI_CROSS_CHAT", "1")
        monkeypatch.setenv("DRAGO_AI_CROSS_CHAT_LIMIT", "5")

        chat = AIChat("chat_current", storage=st)
        context = chat._build_cross_chat_context("Проверь, есть ли встреча на 12.03")

        assert "chat=chat_target" in context
        assert "chat=chat_current" not in context
    finally:
        st.close()

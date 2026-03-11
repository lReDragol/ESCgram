from __future__ import annotations

from storage import Storage
from ui.chat_panels import format_chat_subtitle


def _make_storage(tmp_path) -> Storage:
    db_path = tmp_path / "test_meta.db"
    st = Storage(str(db_path))
    st.connect()
    st.init_schema()
    return st


def test_storage_roundtrip_message_metadata(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.upsert_peers(
            [
                {"id": 777, "type": "private", "title": "Chat"},
                {"id": 111, "type": "user", "title": "Alice"},
            ]
        )
        st.upsert_messages(
            777,
            [
                {
                    "id": 15,
                    "date": 123456,
                    "from_id": 111,
                    "message": "Привет 👍",
                    "media_type": "text",
                    "reactions": [{"emoji": "👍", "count": 3}],
                    "poll": {"question": "Где?", "options": [{"text": "Тут", "voter_count": 2}]},
                    "views": 120,
                    "forwards": 7,
                }
            ],
        )

        item = st.get_message_by_id(777, 15)

        assert item is not None
        assert item["reactions"][0]["emoji"] == "👍"
        assert item["poll"]["question"] == "Где?"
        assert item["views"] == 120
        assert item["forwards"] == 7
    finally:
        st.close()


def test_storage_logs_deleted_message_snapshot(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.upsert_peers(
            [
                {"id": 999, "type": "group", "title": "Group"},
                {"id": 44, "type": "user", "title": "Sender"},
            ]
        )
        st.upsert_messages(
            999,
            [
                {
                    "id": 33,
                    "date": 123,
                    "from_id": 44,
                    "message": "Удаляемое сообщение",
                    "media_type": "text",
                }
            ],
        )
        st.log_deleted_messages(999, [33], deleted_at=987654321, source="test")
        st.mark_messages_deleted(999, [33], deleted=True)

        stats = st.get_message_statistics(999, 33)

        assert stats["message"]["is_deleted"] is True
        assert stats["deleted_snapshot"]["snapshot_text"] == "Удаляемое сообщение"
        assert stats["deleted_snapshot"]["deleted_at"] == 987654321
    finally:
        st.close()


def test_recent_emojis_prioritizes_sender_and_recency(tmp_path) -> None:
    st = _make_storage(tmp_path)
    try:
        st.upsert_peers(
            [
                {"id": 1, "type": "private", "title": "Chat"},
                {"id": 5, "type": "user", "title": "Me"},
                {"id": 6, "type": "user", "title": "Other"},
            ]
        )
        st.upsert_messages(
            1,
            [
                {"id": 1, "date": 10, "from_id": 5, "message": "старое 😀😀", "media_type": "text"},
                {"id": 2, "date": 20, "from_id": 5, "message": "новое 🔥👍", "media_type": "text"},
                {"id": 3, "date": 30, "from_id": 6, "message": "чужое 😎", "media_type": "text"},
            ],
        )

        emojis = st.get_recent_emojis(limit=4, sender_id=5)

        assert emojis[:3] == ["🔥", "👍", "😀"]
    finally:
        st.close()


def test_format_chat_subtitle_uses_type_members_and_username() -> None:
    subtitle = format_chat_subtitle(
        {
            "type": "channel",
            "members_count": 1250,
            "username": "escgram_news",
        }
    )
    assert "Канал" in subtitle
    assert "1 250" in subtitle
    assert "@escgram_news" in subtitle

from ui.chat_sidebar import ChatSidebarMixin


class _DummySidebar(ChatSidebarMixin):
    def __init__(self) -> None:
        self.history = {"chats": {}}
        self.server = None


def test_chat_sort_prioritizes_pinned_then_recency() -> None:
    sidebar = _DummySidebar()
    items = [
        {"id": "regular-new", "pinned": False, "unread_count": 0, "last_ts": 500, "title": "Regular New", "_sort_importance": 1},
        {"id": "pinned-old", "pinned": True, "unread_count": 0, "last_ts": 100, "title": "Pinned Old", "_sort_importance": 1},
        {"id": "unread-mid", "pinned": False, "unread_count": 3, "last_ts": 300, "title": "Unread Mid", "_sort_importance": 1},
        {"id": "unread-high", "pinned": False, "unread_count": 9, "last_ts": 200, "title": "Unread High", "_sort_importance": 1},
    ]

    ordered = sorted(items, key=sidebar._chat_sort_key)
    assert [item["id"] for item in ordered] == [
        "pinned-old",   # pinned always first
        "regular-new",  # then latest activity first
        "unread-mid",
        "unread-high",
    ]


def test_unread_folder_matches_only_positive_unread_count() -> None:
    info_unread = {"type": "private", "username": "", "unread_count": 2}
    info_read = {"type": "private", "username": "", "unread_count": 0}

    assert ChatSidebarMixin._folder_matches(info_unread, "unread") is True
    assert ChatSidebarMixin._folder_matches(info_read, "unread") is False

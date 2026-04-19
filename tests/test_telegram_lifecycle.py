from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import telegram as telegram_module
from telegram import TelegramAdapter
from PySide6.QtCore import QObject
from PySide6.QtWidgets import QApplication
from ui.avatar_cache import AvatarCache
from utils.account_store import AccountStore


class _DummyThread:
    def __init__(self, *, alive: bool = True) -> None:
        self._alive = alive
        self.started = False
        self.join_calls = []

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return self._alive

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)
        self._alive = False


def _ensure_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_telegram_adapter_restarts_when_previous_thread_is_stale(monkeypatch) -> None:
    created_threads: list[_DummyThread] = []

    def _make_thread(*_args, **_kwargs):
        thread = _DummyThread(alive=True)
        created_threads.append(thread)
        return thread

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._thread = _DummyThread(alive=False)

    monkeypatch.setattr(telegram_module.threading, "Thread", _make_thread)

    adapter.start()

    assert created_threads
    assert adapter._thread is created_threads[0]
    assert created_threads[0].started is True


def test_telegram_adapter_stop_joins_thread_even_before_loop_ready() -> None:
    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._thread = _DummyThread(alive=True)
    adapter._loop_ready.set()

    adapter.stop()

    assert adapter._thread is None


def test_telegram_adapter_stop_skips_closed_loop() -> None:
    loop = asyncio.new_event_loop()
    loop.close()

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._thread = _DummyThread(alive=True)
    adapter._loop = loop
    adapter._loop_ready.set()

    adapter.stop()

    assert adapter._thread is None


def test_history_target_variants_skip_username_when_public_lookups_blocked() -> None:
    class _FakeStorage:
        def get_peer(self, _peer_id: int):
            return {"id": 123, "username": "proxy"}

    adapter = TelegramAdapter()
    adapter._storage = _FakeStorage()
    adapter._public_lookup_blocked_until = float("inf")

    variants = adapter._history_target_variants("123")

    assert variants == [123]


def test_history_target_variants_do_not_fallback_to_username_for_numeric_peer() -> None:
    class _FakeStorage:
        def get_peer(self, _peer_id: int):
            return {"id": 123, "username": "proxy"}

    adapter = TelegramAdapter()
    adapter._storage = _FakeStorage()

    variants = adapter._history_target_variants("123")

    assert variants == [123]


def test_telegram_adapter_skips_avatar_fetch_when_auth_is_invalid(monkeypatch) -> None:
    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = object()
    adapter._loop = object()
    adapter._connected = True
    adapter._auth_invalid = True

    monkeypatch.setattr(adapter, "_call", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("_call should not run")))

    assert adapter.ensure_chat_avatar_sync("123") is None
    assert adapter.ensure_user_avatar_sync("456") is None


def test_telegram_adapter_pauses_file_downloads_after_flood_wait(monkeypatch, tmp_path: Path) -> None:
    class _FakeClient:
        async def download_media(self, *_args, **_kwargs):
            raise Exception(
                'Telegram says: [420 FLOOD_WAIT_X] - A wait of 37 seconds is required (caused by "auth.ExportAuthorization")'
            )

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    assert adapter.download_file_id_sync("file_1", file_name=str(tmp_path / "thumb.webp")) is None
    assert adapter._file_downloads_blocked() is True

    monkeypatch.setattr(
        adapter,
        "_call",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("_call should not run while downloads are blocked")),
    )

    assert adapter.download_file_id_sync("file_2", file_name=str(tmp_path / "thumb2.webp")) is None


def test_avatar_cache_is_qobject() -> None:
    _ensure_app()
    cache = AvatarCache(object())
    try:
        assert isinstance(cache, QObject)
    finally:
        cache.shutdown()


def test_avatar_cache_can_skip_user_fetch(monkeypatch) -> None:
    _ensure_app()
    cache = AvatarCache(object())
    scheduled: list[tuple] = []
    monkeypatch.setattr(cache, "_schedule_download", lambda **kwargs: scheduled.append(tuple(sorted(kwargs.items()))))
    try:
        cache.user("123", "Alice", allow_fetch=False)
        assert scheduled == []
    finally:
        cache.shutdown()


def test_avatar_cache_can_skip_chat_fetch(monkeypatch) -> None:
    _ensure_app()
    cache = AvatarCache(object())
    scheduled: list[tuple] = []
    monkeypatch.setattr(cache, "_schedule_download", lambda **kwargs: scheduled.append(tuple(sorted(kwargs.items()))))
    try:
        cache.chat("-1001", {"title": "Channel", "type": "channel", "photo_small_id": "abc"}, allow_fetch=False)
        assert scheduled == []
    finally:
        cache.shutdown()


def test_telegram_adapter_can_delete_active_last_account(monkeypatch, tmp_path: Path) -> None:
    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._workdir = tmp_path
    adapter._account_store = AccountStore(tmp_path)
    adapter._session_name = "primary"
    adapter._account_store.ensure_account("primary")
    adapter._account_store.set_active("primary")
    (tmp_path / "primary.session").write_bytes(b"x")

    activations: list[tuple[str, bool, bool]] = []

    def _activate(session_name: str, *, clean: bool = False, register: bool = True) -> None:
        activations.append((session_name, bool(clean), bool(register)))
        adapter._session_name = session_name

    monkeypatch.setattr(adapter, "_activate_session", _activate)

    adapter.delete_account("primary")

    assert activations == [("my_account_gui", True, False)]
    assert not (tmp_path / "primary.session").exists()
    assert adapter._account_store.get_account("primary") == {}


def test_send_login_code_sync_records_email_delivery_hint(monkeypatch) -> None:
    class _FakeSentCode:
        phone_code_hash = "hash-1"
        type = "EMAIL_CODE"

    class _FakeClient:
        async def send_code(self, _phone: str):
            return _FakeSentCode()

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._connected = True

    monkeypatch.setattr(adapter, "_wait_for_client_connection", lambda timeout=15.0: True)
    monkeypatch.setattr(
        adapter,
        "_call_with_options",
        lambda coro, timeout, allow_auth_exceptions=False: asyncio.run(coro),
    )

    result = adapter.send_login_code_sync("+14243173529")

    assert result == "hash-1"
    assert adapter.current_login_delivery_hint() == "по email"


def test_sign_in_with_code_sync_preserves_password_challenge_hint(monkeypatch) -> None:
    class _FakeSessionPasswordNeeded(Exception):
        pass

    class _FakeClient:
        async def sign_in(self, **_kwargs):
            raise _FakeSessionPasswordNeeded(
                "Telegram says: [401 SESSION_PASSWORD_NEEDED] - The two-step verification is enabled"
            )

        async def get_password_hint(self):
            return "pet-name"

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._connected = True
    adapter._current_phone_hash = "hash-2"

    monkeypatch.setattr(adapter, "_wait_for_client_connection", lambda timeout=15.0: True)
    monkeypatch.setattr(telegram_module, "SessionPasswordNeeded", _FakeSessionPasswordNeeded)
    monkeypatch.setattr(
        adapter,
        "_call_with_options",
        lambda coro, timeout, allow_auth_exceptions=False: asyncio.run(coro),
    )

    try:
        adapter.sign_in_with_code_sync("+14243173529", "12345")
    except telegram_module.UserVisibleAuthError as exc:
        assert "pet-name" in str(exc)
    else:
        raise AssertionError("Expected UserVisibleAuthError")

    assert adapter._auth_challenge_pending is True
    assert adapter.current_login_password_hint() == "pet-name"


def test_set_storage_switches_database_to_active_session(monkeypatch, tmp_path: Path) -> None:
    switched: list[str] = []

    class _FakeStorage:
        def switch_db_path(self, db_path: str, *, init_schema: bool = True) -> bool:
            _ = init_schema
            switched.append(str(db_path))
            return True

    adapter = TelegramAdapter()
    adapter._session_name = "account_7"

    monkeypatch.setattr(
        telegram_module.app_paths,
        "session_db_path",
        lambda session_name: tmp_path / f"{session_name}.db",
    )

    adapter.set_storage(_FakeStorage())

    assert switched == [str(tmp_path / "account_7.db")]


def test_get_history_sync_returns_empty_for_peer_id_invalid(monkeypatch) -> None:
    class _FakeClient:
        async def get_chat_history(self, *_args, **_kwargs):
            raise Exception("Telegram says: [400 PEER_ID_INVALID] - invalid peer")
            yield  # pragma: no cover

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    result = adapter.get_history_sync("5289699245", limit=20)

    assert result == []


def test_search_public_peers_sync_normalizes_t_me_link(monkeypatch) -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.chat_targets: list[str] = []

        async def invoke(self, *_args, **_kwargs):
            return None

        async def get_chat(self, target):
            self.chat_targets.append(str(target))
            return SimpleNamespace(
                id=777,
                title="STR Bypass",
                username="strbypass",
                type="supergroup",
                photo=None,
            )

    class _FakeStorage:
        def upsert_peers(self, rows) -> None:
            _ = rows

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    rows = adapter.search_public_peers_sync("https://t.me/strbypass", limit=5)

    assert rows
    assert rows[0]["username"] == "strbypass"
    assert adapter._client.chat_targets[0] == "strbypass"


def test_resolve_chat_reference_sync_uses_raw_username_resolution(monkeypatch) -> None:
    class PeerChannel:
        def __init__(self, channel_id: int) -> None:
            self.channel_id = channel_id

    class ChannelForbidden:
        def __init__(self) -> None:
            self.id = 2477958568
            self.title = "MIX КИБЕРПОРТАЛ"
            self.access_hash = 123
            self.broadcast = False
            self.megagroup = True
            self.photo = None

    class ResolvedPeer:
        def __init__(self) -> None:
            self.peer = PeerChannel(2477958568)
            self.chats = [ChannelForbidden()]
            self.users = []

    class _FakeClient:
        async def invoke(self, _query):
            return ResolvedPeer()

        async def get_chat(self, _target):
            raise KeyError("Username not found")

    class _FakeStorage:
        def __init__(self) -> None:
            self.saved: list[list[dict]] = []

        def upsert_peers(self, rows) -> None:
            self.saved.append(list(rows))

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    row = adapter.resolve_chat_reference_sync("https://t.me/strbypass", join=False, timeout=20.0)

    assert row is not None
    assert row["id"] == telegram_module.pyrogram_utils.get_channel_id(2477958568)
    assert row["username"] == "strbypass"


def test_search_public_peers_sync_normalizes_raw_channel_id(monkeypatch) -> None:
    class Channel:
        def __init__(self) -> None:
            self.id = 2477958568
            self.title = "MIX КИБЕРПОРТАЛ"
            self.username = "strbypass"
            self.broadcast = False
            self.photo = None
            self.type = None

    class _FakeClient:
        async def invoke(self, *_args, **_kwargs):
            return SimpleNamespace(users=[], chats=[Channel()])

        async def get_chat(self, _target):
            raise RuntimeError("no exact entity")

    class _FakeStorage:
        def upsert_peers(self, rows) -> None:
            _ = rows

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    rows = adapter.search_public_peers_sync("https://t.me/strbypass", limit=5)

    assert rows
    assert rows[0]["id"] == str(telegram_module.pyrogram_utils.get_channel_id(2477958568))
    assert rows[0]["type"] == "supergroup"


def test_search_public_peers_sync_prefers_larger_normalized_username_match(monkeypatch) -> None:
    class ChannelRoot:
        def __init__(self) -> None:
            self.id = 2477958568
            self.title = "MIX КИБЕРПОРТАЛ"
            self.username = "strbypass"
            self.broadcast = False
            self.megagroup = True
            self.members_count = 104
            self.photo = None
            self.type = None

    class ChannelMain:
        def __init__(self) -> None:
            self.id = 2307338893
            self.title = "Киберпортал"
            self.username = "STR_BYPASS"
            self.broadcast = True
            self.megagroup = False
            self.members_count = 125462
            self.photo = None
            self.type = SimpleNamespace(name="CHANNEL")

    class _FakeClient:
        async def invoke(self, query):
            query_name = str(getattr(getattr(query, "__class__", None), "__name__", "") or "")
            if "ResolveUsername" in query_name:
                return SimpleNamespace(peer=None, users=[], chats=[])
            return SimpleNamespace(users=[], chats=[ChannelRoot(), ChannelMain()])

        async def get_chat(self, _target):
            raise RuntimeError("no direct entity")

    class _FakeStorage:
        def upsert_peers(self, rows) -> None:
            _ = rows

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_resolve_username_row", lambda username: asyncio.sleep(0, result={
        "id": str(telegram_module.pyrogram_utils.get_channel_id(2477958568)),
        "title": "MIX КИБЕРПОРТАЛ",
        "type": "supergroup",
        "username": username,
        "members_count": 104,
        "photo_small": None,
    }))
    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    rows = adapter.search_public_peers_sync("strbypass", limit=5)

    assert rows
    assert rows[0]["username"] == "STR_BYPASS"
    assert int(rows[0]["members_count"] or 0) == 125462


def test_search_public_peers_sync_promotes_linked_discussion(monkeypatch) -> None:
    linked_id = telegram_module.pyrogram_utils.get_channel_id(2477958568)
    root_id = -1002307338893

    class LinkedChat:
        def __init__(self) -> None:
            self.id = linked_id
            self.title = "MIX КИБЕРПОРТАЛ"
            self.username = ""
            self.type = SimpleNamespace(name="SUPERGROUP")
            self.megagroup = True
            self.broadcast = False
            self.members_count = 41000
            self.photo = None

    class RootChat:
        def __init__(self) -> None:
            self.id = root_id
            self.title = "STR_BYPASS"
            self.username = "strbypass"
            self.type = SimpleNamespace(name="CHANNEL")
            self.megagroup = False
            self.broadcast = True
            self.members_count = 125462
            self.photo = None
            self.linked_chat = LinkedChat()

    class _FakeClient:
        async def invoke(self, _query):
            return SimpleNamespace(users=[], chats=[])

        async def get_chat(self, _target):
            return RootChat()

    class _FakeStorage:
        def upsert_peers(self, rows) -> None:
            _ = rows

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_resolve_username_row", lambda username: asyncio.sleep(0, result=None))
    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    rows = adapter.search_public_peers_sync("strbypass", limit=5)

    assert rows
    assert rows[0]["id"] == str(linked_id)
    assert rows[0]["type"] == "supergroup"
    assert rows[1]["id"] == str(root_id)


def test_get_chat_full_info_sync_includes_linked_chat(monkeypatch) -> None:
    linked_id = telegram_module.pyrogram_utils.get_channel_id(2477958568)

    class LinkedChat:
        def __init__(self) -> None:
            self.id = linked_id
            self.title = "MIX КИБЕРПОРТАЛ"
            self.username = ""
            self.type = SimpleNamespace(name="SUPERGROUP")
            self.megagroup = True
            self.broadcast = False
            self.members_count = 41000
            self.photo = None

    class RootChat:
        def __init__(self) -> None:
            self.id = -1002307338893
            self.title = "STR_BYPASS"
            self.username = "strbypass"
            self.type = SimpleNamespace(name="CHANNEL")
            self.megagroup = False
            self.broadcast = True
            self.members_count = 125462
            self.photo = None
            self.linked_chat = LinkedChat()
            self.send_as_chat = None
            self.available_reactions = None
            self.bio = None
            self.description = "about"
            self.about = None
            self.is_verified = False
            self.is_scam = False
            self.is_fake = False
            self.is_restricted = False
            self.is_premium = False
            self.is_creator = False
            self.phone_number = ""
            self.invite_link = "https://t.me/strbypass"

    class _FakeClient:
        async def get_chat(self, _target):
            return RootChat()

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    info = adapter.get_chat_full_info_sync("strbypass")

    assert info is not None
    assert info["linked_chat_id"] == str(linked_id)
    assert info["linked_chat"]["type"] == "supergroup"
    assert info["invite_link"] == "https://t.me/strbypass"


def test_chat_to_peer_row_keeps_pyrogram_channel_type() -> None:
    class Chat:
        def __init__(self) -> None:
            self.id = -1002307338893
            self.title = "Киберпортал"
            self.username = "STR_BYPASS"
            self.type = SimpleNamespace(name="CHANNEL")
            self.photo = None

    adapter = TelegramAdapter()

    row = adapter._chat_to_peer_row(Chat())

    assert row is not None
    assert row["id"] == -1002307338893
    assert row["type"] == "channel"


def test_get_history_sync_retries_with_username_from_storage(monkeypatch) -> None:
    class _FakeStorage:
        def get_peer(self, peer_id: int) -> dict:
            assert peer_id == 5289699245
            return {"id": peer_id, "username": "strbypass"}

    class _FakeClient:
        def __init__(self) -> None:
            self.targets: list[object] = []

        async def get_chat_history(self, target, **_kwargs):
            self.targets.append(target)
            if target == 5289699245:
                raise Exception("Telegram says: [400 PEER_ID_INVALID] - invalid peer")
            yield SimpleNamespace(
                id=1,
                date=None,
                from_user=None,
                text="hello",
                caption=None,
                entities=None,
                caption_entities=None,
                reply_markup=None,
            )

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))
    monkeypatch.setattr(adapter, "_extract_media_meta", lambda _m: ("text", None, 0, None, None, None))
    monkeypatch.setattr(adapter, "_extract_reactions", lambda _m: [])
    monkeypatch.setattr(adapter, "_extract_poll", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_message_counters", lambda _m: (0, 0))
    monkeypatch.setattr(adapter, "_reply_markup_to_dict", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_reply_to_id", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_forward_info", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_file_name", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_media_group_id", lambda _m: None)
    monkeypatch.setattr(adapter, "_entities_to_dicts", lambda _m: [])
    monkeypatch.setattr(adapter, "_store_message_record", lambda *_args, **_kwargs: None)

    result = adapter.get_history_sync("5289699245", limit=20)

    assert result == []
    assert adapter._client.targets == [5289699245]


def test_get_history_sync_retries_when_peer_id_invalid_valueerror(monkeypatch) -> None:
    class _FakeStorage:
        def get_peer(self, peer_id: int) -> dict:
            assert peer_id == -8426139458
            return {"id": peer_id, "username": "GithubGitlabDownloader_bot"}

    class _FakeClient:
        def __init__(self) -> None:
            self.targets: list[object] = []

        async def get_chat_history(self, target, **_kwargs):
            self.targets.append(target)
            if target == -8426139458:
                raise ValueError("Peer id invalid: -8426139458")
            yield SimpleNamespace(
                id=7,
                date=None,
                from_user=None,
                text="ok",
                caption=None,
                entities=None,
                caption_entities=None,
                reply_markup=None,
            )

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))
    monkeypatch.setattr(adapter, "_extract_media_meta", lambda _m: ("text", None, 0, None, None, None))
    monkeypatch.setattr(adapter, "_extract_reactions", lambda _m: [])
    monkeypatch.setattr(adapter, "_extract_poll", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_message_counters", lambda _m: (0, 0))
    monkeypatch.setattr(adapter, "_reply_markup_to_dict", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_reply_to_id", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_forward_info", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_file_name", lambda _m: None)
    monkeypatch.setattr(adapter, "_extract_media_group_id", lambda _m: None)
    monkeypatch.setattr(adapter, "_entities_to_dicts", lambda _m: [])
    monkeypatch.setattr(adapter, "_store_message_record", lambda *_args, **_kwargs: None)

    result = adapter.get_history_sync("-8426139458", limit=20)

    assert result == []
    assert adapter._client.targets == [-8426139458]


def test_get_chat_full_info_sync_retries_with_username_and_handles_chat_reactions(monkeypatch) -> None:
    class _FakeStorage:
        def get_peer(self, peer_id: int) -> dict:
            assert peer_id == -8426139458
            return {"id": peer_id, "username": "GithubGitlabDownloader_bot"}

    class _ChatReactions:
        def __init__(self) -> None:
            self.all_are_enabled = True
            self.allow_custom_emoji = True
            self.reactions = None

    class _FakeClient:
        def __init__(self) -> None:
            self.targets: list[object] = []

        async def get_chat(self, target):
            self.targets.append(target)
            if target == -8426139458:
                raise ValueError("Peer id invalid: -8426139458")
            return SimpleNamespace(
                id=-1002889413946,
                title="FunMix Discussion",
                username="strbypasss",
                type="supergroup",
                bio="community",
                description=None,
                about=None,
                is_verified=False,
                is_scam=False,
                is_fake=False,
                is_restricted=False,
                is_premium=False,
                is_creator=False,
                phone_number=None,
                phone=None,
                members_count=42,
                photo=None,
                available_reactions=_ChatReactions(),
            )

    adapter = TelegramAdapter()
    adapter._enabled = True
    adapter._client = _FakeClient()
    adapter._loop = object()
    adapter._connected = True
    adapter._storage = _FakeStorage()

    monkeypatch.setattr(adapter, "_call", lambda coro, timeout: asyncio.run(coro))

    info = adapter.get_chat_full_info_sync("-8426139458", timeout=20.0)

    assert info is not None
    assert info["id"] == "-1002889413946"
    assert info["available_reactions"] == []
    assert adapter._client.targets == [-8426139458, "GithubGitlabDownloader_bot"]

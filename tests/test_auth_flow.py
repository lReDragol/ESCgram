from __future__ import annotations

from types import SimpleNamespace

from PySide6.QtWidgets import QApplication

from ui.auth_dialog import AuthDialog
import ui.main_window as main_window_module
from ui.main_window import ChatWindow


def _ensure_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeTelegramAdapter:
    def __init__(self, *, authorize_after: int) -> None:
        self._enabled = True
        self._auth_invalid = False
        self._pending_session_name = None
        self._thread = object()
        self._authorize_after = int(authorize_after)
        self.calls = 0

    def is_authorized_sync(self) -> bool:
        self.calls += 1
        return self.calls >= self._authorize_after

    def list_accounts(self):
        return [{"session": "primary"}]

    def current_session_name(self) -> str:
        return "primary"

    def _session_exists(self, session_name: str) -> bool:
        return str(session_name or "") == "primary"

    def start(self) -> None:
        self._thread = object()


def _make_window_stub(tg: _FakeTelegramAdapter):
    actions: list[str] = []
    state = {"auth_visible": False, "auth_reason": ""}

    def _show_auth_page(prompt_reason: str = "manual") -> None:
        reason = str(prompt_reason or "manual")
        if (not state["auth_visible"]) or state["auth_reason"] != reason:
            actions.append(f"auth:{reason}")
        state["auth_visible"] = True
        state["auth_reason"] = reason

    def _show_main_page(resize_window: bool = True) -> None:
        _ = resize_window
        if state["auth_visible"]:
            actions.append("main")
        state["auth_visible"] = False
        state["auth_reason"] = ""

    window = SimpleNamespace(
        tg=tg,
        _startup_auth_retries=0,
        _startup_auth_retry_reason="",
        _pending_account_revert=None,
        _auth_page_active=False,
        _refresh_account_profile_async=lambda: actions.append("profile"),
        refresh_telegram_chats_async=lambda: actions.append("chats"),
        _reset_state_after_account_change=lambda: actions.append("reset"),
        _sync_account_card=lambda: actions.append("sync"),
        _show_auth_page=_show_auth_page,
        _show_main_page=_show_main_page,
        close=lambda: actions.append("close"),
        _actions=actions,
    )
    window._ensure_authorized = lambda prompt_reason="manual": ChatWindow._ensure_authorized(  # type: ignore[attr-defined]
        window,
        prompt_reason=prompt_reason,
    )
    return window


def test_ensure_authorized_waits_for_existing_session_until_it_recovers(monkeypatch) -> None:
    tg = _FakeTelegramAdapter(authorize_after=7)
    window = _make_window_stub(tg)

    monkeypatch.setattr(
        main_window_module.QTimer,
        "singleShot",
        staticmethod(lambda _delay, callback: callback()),
    )

    ChatWindow._ensure_authorized(window, prompt_reason="startup")

    assert window._actions == ["auth:startup", "main", "profile", "chats"]
    assert tg.calls >= 7


class _FakeDialogTelegramAdapter:
    def __init__(self) -> None:
        self._enabled = True
        self.calls = 0

    def is_authorized_sync(self, timeout: float = 1.0) -> bool:
        _ = timeout
        self.calls += 1
        return self.calls >= 2


class _FakePhoneFlowTelegramAdapter(_FakeDialogTelegramAdapter):
    def __init__(self, *, delivery_hint: str = "", submit_error: Exception | None = None, password_hint: str = "") -> None:
        super().__init__()
        self._delivery_hint = delivery_hint
        self._submit_error = submit_error
        self._password_hint = password_hint

    def send_login_code_sync(self, _phone: str) -> str:
        return "hash"

    def current_login_delivery_hint(self) -> str:
        return self._delivery_hint

    def sign_in_with_code_sync(self, _phone: str, _code: str, _password: str | None = None) -> bool:
        if self._submit_error is not None:
            raise self._submit_error
        return True

    def current_login_password_hint(self) -> str:
        return self._password_hint


def test_auth_dialog_auto_closes_when_session_becomes_authorized(monkeypatch) -> None:
    _ensure_app()
    tg = _FakeDialogTelegramAdapter()
    dlg = AuthDialog(tg)
    accepted = {"count": 0}
    emitted = {"count": 0}

    dlg.login_success.connect(lambda: emitted.__setitem__("count", int(emitted["count"]) + 1))
    monkeypatch.setattr(dlg, "accept", lambda: accepted.__setitem__("count", int(accepted["count"]) + 1))

    dlg._poll_existing_authorization()
    assert accepted["count"] == 0
    assert emitted["count"] == 0

    dlg._poll_existing_authorization()
    assert accepted["count"] == 1
    assert emitted["count"] == 1


def test_auth_dialog_can_run_embedded_inside_main_window() -> None:
    _ensure_app()
    tg = _FakeDialogTelegramAdapter()
    host = main_window_module.QWidget()
    dlg = AuthDialog(tg, host, embedded=True)

    assert dlg.isModal() is False
    assert dlg.parent() is host


def test_auth_dialog_shows_delivery_hint_for_email_code() -> None:
    _ensure_app()
    tg = _FakePhoneFlowTelegramAdapter(delivery_hint="по email")
    dlg = AuthDialog(tg)
    dlg.ed_phone.setText("+79990001122")

    dlg._request_phone_code()

    assert "по email" in dlg.lbl_phone_status.text()


def test_auth_dialog_shows_password_hint_when_telegram_requires_password() -> None:
    _ensure_app()
    tg = _FakePhoneFlowTelegramAdapter(
        submit_error=Exception("Telegram says: [401 SESSION_PASSWORD_NEEDED] - password required"),
        password_hint="pet-name",
    )
    dlg = AuthDialog(tg)
    dlg._phone_cached_number = "+79990001122"
    dlg.ed_phone_code.setText("12345")

    dlg._submit_phone_code()

    assert "pet-name" in dlg.lbl_phone_status.text()
    assert dlg.ed_phone_code_password.isHidden() is False


def test_startup_unauthorized_shows_single_auth_page_when_cached_dialogs_exist(monkeypatch) -> None:
    tg = _FakeTelegramAdapter(authorize_after=999)
    tg._auth_invalid = True
    window = _make_window_stub(tg)
    window.server = SimpleNamespace(list_cached_dialogs=lambda limit=1: [{"id": "42"}])

    ChatWindow._ensure_authorized(window, prompt_reason="startup")

    assert window._actions == ["auth:startup"]


def test_ensure_authorized_uses_auth_page_without_modal_dialog(monkeypatch) -> None:
    tg = _FakeTelegramAdapter(authorize_after=999)
    window = _make_window_stub(tg)

    monkeypatch.setattr(
        main_window_module.QTimer,
        "singleShot",
        staticmethod(lambda _delay, _callback: None),
    )

    ChatWindow._ensure_authorized(window, prompt_reason="manual")

    assert window._actions == ["auth:manual"]


def test_auth_dialog_switches_to_phone_tab_when_import_requires_login() -> None:
    _ensure_app()
    tg = _FakeDialogTelegramAdapter()
    dlg = AuthDialog(tg)

    dlg._on_session_import_done(False, "Введите номер и код", True)

    assert dlg.tabs.currentIndex() == dlg._phone_tab_index
    assert "Введите номер и код" in dlg.lbl_session_status.text()
    assert "Введите номер и код" in dlg.lbl_phone_status.text()

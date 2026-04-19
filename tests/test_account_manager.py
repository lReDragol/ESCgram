from __future__ import annotations

from PySide6.QtWidgets import QApplication, QMessageBox

from ui.account_manager import AccountManagerDialog


def _ensure_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeAccountsTelegram:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def list_accounts(self):
        return [
            {
                "session": "primary",
                "title": "Primary",
                "phone": "+79990001122",
                "is_active": True,
            }
        ]

    def is_authorized_sync(self, timeout: float = 0.8) -> bool:
        _ = timeout
        return False

    _auth_invalid = True

    def delete_account(self, session_name: str) -> None:
        self.deleted.append(str(session_name))


def test_account_manager_enables_delete_for_active_account(monkeypatch) -> None:
    _ensure_app()
    dlg = AccountManagerDialog(_FakeAccountsTelegram())

    assert dlg.list_widget.currentItem() is not None
    assert dlg.btn_delete.isEnabled() is True
    assert dlg.btn_use.text() == "Войти"


def test_account_manager_deletes_active_account_and_emits_switch(monkeypatch) -> None:
    _ensure_app()
    tg = _FakeAccountsTelegram()
    dlg = AccountManagerDialog(tg)
    events: list[str] = []

    dlg.account_deleted.connect(lambda: events.append("deleted"))
    dlg.account_switched.connect(lambda: events.append("switched"))
    monkeypatch.setattr(QMessageBox, "question", staticmethod(lambda *_args, **_kwargs: QMessageBox.StandardButton.Yes))
    monkeypatch.setattr(dlg, "accept", lambda: events.append("accept"))

    dlg._delete_selected()

    assert tg.deleted == ["primary"]
    assert events == ["accept", "deleted", "switched"]


def test_account_manager_add_request_is_emitted_after_accept(monkeypatch) -> None:
    _ensure_app()
    dlg = AccountManagerDialog(_FakeAccountsTelegram())
    events: list[str] = []

    dlg.account_add_requested.connect(lambda: events.append("add"))
    monkeypatch.setattr(dlg, "accept", lambda: events.append("accept"))

    dlg._emit_add()

    assert events == ["accept", "add"]

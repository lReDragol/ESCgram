from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)


class AccountManagerDialog(QDialog):
    account_switched = Signal()
    account_add_requested = Signal()

    def __init__(self, tg_adapter, parent=None) -> None:
        super().__init__(parent)
        self.tg = tg_adapter
        self.setWindowTitle("Аккаунты Telegram")
        self.setModal(True)
        self.resize(360, 420)

        root = QVBoxLayout(self)

        self.list_widget = QListWidget()
        self.list_widget.itemDoubleClicked.connect(lambda _item: self._switch_selected())
        root.addWidget(self.list_widget, 1)

        btn_row = QHBoxLayout()
        self.btn_add = QPushButton("Добавить аккаунт")
        self.btn_use = QPushButton("Использовать")
        self.btn_use.setEnabled(False)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_use)
        btn_row.addStretch(1)
        root.addLayout(btn_row)

        box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        box.rejected.connect(self.reject)
        root.addWidget(box)

        self.btn_add.clicked.connect(self._emit_add)
        self.btn_use.clicked.connect(self._switch_selected)
        self.list_widget.currentItemChanged.connect(lambda *_: self._update_buttons())

        self._populate()

    # ------------------------------------------------------------------
    def _populate(self) -> None:
        self.list_widget.clear()
        accounts = self.tg.list_accounts() if hasattr(self.tg, "list_accounts") else []
        for account in accounts:
            session = account.get("session", "")
            title = account.get("title") or session
            subtitle = account.get("phone") or account.get("username") or session
            item = QListWidgetItem(f"{title}\n{subtitle}")
            item.setData(Qt.ItemDataRole.UserRole, session)
            item.setData(Qt.ItemDataRole.UserRole + 1, bool(account.get("is_active")))
            if account.get("is_active"):
                item.setSelected(True)
            self.list_widget.addItem(item)
        self._update_buttons()

    def _selected_session(self) -> str:
        item = self.list_widget.currentItem()
        if not item:
            return ""
        return str(item.data(Qt.ItemDataRole.UserRole) or "")

    def _update_buttons(self) -> None:
        item = self.list_widget.currentItem()
        if not item:
            self.btn_use.setEnabled(False)
            return
        is_active = bool(item.data(Qt.ItemDataRole.UserRole + 1))
        self.btn_use.setEnabled(not is_active)

    def _emit_add(self) -> None:
        self.account_add_requested.emit()
        self.accept()

    def _switch_selected(self) -> None:
        session = self._selected_session()
        if not session:
            QMessageBox.information(self, "Аккаунты", "Выберите аккаунт для переключения")
            return
        try:
            self.tg.switch_account(session)
        except FileNotFoundError:
            QMessageBox.critical(self, "Аккаунты", "Файл сессии не найден. Возможно, вы ещё не входили в этот аккаунт.")
            return
        except Exception as exc:
            QMessageBox.critical(self, "Аккаунты", str(exc))
            return
        self.account_switched.emit()
        self.accept()

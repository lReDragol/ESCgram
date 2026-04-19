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
from ui.qt_threading import invoke_in_gui_thread


class AccountManagerDialog(QDialog):
    account_switched = Signal()
    account_add_requested = Signal()
    account_deleted = Signal()

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
        self.btn_delete = QPushButton("Удалить")
        self.btn_use.setEnabled(False)
        self.btn_delete.setEnabled(False)
        btn_row.addWidget(self.btn_add)
        btn_row.addWidget(self.btn_use)
        btn_row.addWidget(self.btn_delete)
        btn_row.addStretch(1)
        root.addLayout(btn_row)

        box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        box.rejected.connect(self.reject)
        root.addWidget(box)

        self.btn_add.clicked.connect(self._emit_add)
        self.btn_use.clicked.connect(self._switch_selected)
        self.btn_delete.clicked.connect(self._delete_selected)
        self.list_widget.currentItemChanged.connect(lambda *_: self._update_buttons())

        self._populate()

    # ------------------------------------------------------------------
    def _populate(self) -> None:
        self.list_widget.clear()
        accounts = self.tg.list_accounts() if hasattr(self.tg, "list_accounts") else []
        active_item = None
        for account in accounts:
            session = account.get("session", "")
            title = account.get("title") or session
            subtitle = account.get("phone") or account.get("username") or session
            item = QListWidgetItem(f"{title}\n{subtitle}")
            item.setData(Qt.ItemDataRole.UserRole, session)
            item.setData(Qt.ItemDataRole.UserRole + 1, bool(account.get("is_active")))
            if account.get("is_active"):
                item.setSelected(True)
                active_item = item
            self.list_widget.addItem(item)
        if active_item is not None:
            self.list_widget.setCurrentItem(active_item)
        elif self.list_widget.count() > 0:
            self.list_widget.setCurrentRow(0)
        self._update_buttons()

    def _selected_session(self) -> str:
        item = self.list_widget.currentItem()
        if not item:
            return ""
        return str(item.data(Qt.ItemDataRole.UserRole) or "")

    def _update_buttons(self) -> None:
        item = self.list_widget.currentItem()
        if not item:
            self.btn_use.setText("Использовать")
            self.btn_use.setEnabled(False)
            self.btn_delete.setEnabled(False)
            return
        is_active = bool(item.data(Qt.ItemDataRole.UserRole + 1))
        active_needs_login = is_active and (not self._is_active_session_authorized())
        self.btn_use.setText("Войти" if active_needs_login else "Использовать")
        self.btn_use.setEnabled((not is_active) or active_needs_login)
        self.btn_delete.setEnabled(True)

    def _is_active_session_authorized(self) -> bool:
        checker = getattr(self.tg, "is_authorized_sync", None)
        if not callable(checker):
            return True
        try:
            return bool(checker(timeout=0.8))
        except TypeError:
            try:
                return bool(checker())
            except Exception:
                pass
        except Exception:
            pass
        return not bool(getattr(self.tg, "_auth_invalid", False))

    def _emit_add(self) -> None:
        self.accept()
        invoke_in_gui_thread(self.account_add_requested.emit)

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
        self.accept()
        invoke_in_gui_thread(self.account_switched.emit)

    def _delete_selected(self) -> None:
        session = self._selected_session()
        if not session:
            QMessageBox.information(self, "Аккаунты", "Выберите аккаунт для удаления")
            return

        item = self.list_widget.currentItem()
        is_active = bool(item.data(Qt.ItemDataRole.UserRole + 1)) if item else False

        answer = QMessageBox.question(
            self,
            "Удалить аккаунт",
            (
                "Удалить активный аккаунт и выйти из него?"
                if is_active
                else "Удалить выбранный аккаунт из менеджера и удалить его session-файлы?"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if answer != QMessageBox.StandardButton.Yes:
            return

        try:
            if hasattr(self.tg, "delete_account"):
                self.tg.delete_account(session)
            else:
                raise RuntimeError("Удаление аккаунта недоступно в этой сборке.")
        except Exception as exc:
            QMessageBox.critical(self, "Аккаунты", str(exc))
            return

        if is_active:
            self.accept()
            invoke_in_gui_thread(self.account_deleted.emit)
            invoke_in_gui_thread(self.account_switched.emit)
            return

        self._populate()
        self.account_deleted.emit()

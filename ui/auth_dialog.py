from __future__ import annotations

import threading

from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ui.styles import StyleManager


class AuthDialog(QDialog):
    login_success = Signal()
    sig_qr_png = Signal(bytes)
    sig_qr_status = Signal(str)
    sig_qr_done = Signal(bool)

    PHONE_STEP_START = "start"
    PHONE_STEP_CODE = "code"

    def __init__(self, tg_adapter, parent=None):
        super().__init__(parent)
        self.tg = tg_adapter
        self.setWindowTitle("Вход в Telegram")
        self.setModal(True)
        self.resize(460, 540)

        self._phone_step = self.PHONE_STEP_START
        self._phone_cached_number = ""
        self._phone_cached_password = ""

        self._qr_thread_started = False
        self._qr_needs_secret = False
        self._qr_login_done = False

        root = QVBoxLayout(self)

        self.tabs = QTabWidget()
        StyleManager.instance().bind_stylesheet(self.tabs, "settings.tabs")
        root.addWidget(self.tabs)

        self._build_phone_tab()
        self._build_qr_tab()

        bb = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        bb.rejected.connect(self.reject)
        root.addWidget(bb)

        self.tabs.currentChanged.connect(self._on_tab_changed)
        self.sig_qr_png.connect(self._on_qr_png)
        self.sig_qr_status.connect(self._on_qr_status)
        self.sig_qr_done.connect(self._on_qr_done)

        self._telegram_enabled = bool(getattr(self.tg, "_enabled", False))
        if not self._telegram_enabled:
            self._apply_disabled_auth_state()

    def _build_phone_tab(self) -> None:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        self.phone_step1_box = QWidget()
        step1_form = QFormLayout(self.phone_step1_box)
        step1_form.setContentsMargins(0, 0, 0, 0)
        step1_form.setSpacing(8)

        self.ed_phone = QLineEdit()
        self.ed_phone.setPlaceholderText("+79990001122")
        step1_form.addRow("Телефон:", self.ed_phone)

        self.ed_phone_password = QLineEdit()
        self.ed_phone_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.ed_phone_password.setPlaceholderText("Пароль 2FA (если включен)")
        step1_form.addRow("Пароль:", self.ed_phone_password)

        self.btn_phone_request_code = QPushButton("Продолжить")
        self.btn_phone_request_code.clicked.connect(self._request_phone_code)
        step1_form.addRow(self.btn_phone_request_code)

        self.lbl_phone_step1_hint = QLabel(
            "Сначала введите номер. Пароль можно оставить пустым, если 2FA не включен."
        )
        self.lbl_phone_step1_hint.setWordWrap(True)
        step1_form.addRow(self.lbl_phone_step1_hint)

        self.phone_step2_box = QWidget()
        step2_form = QFormLayout(self.phone_step2_box)
        step2_form.setContentsMargins(0, 0, 0, 0)
        step2_form.setSpacing(8)

        self.ed_phone_code = QLineEdit()
        self.ed_phone_code.setPlaceholderText("Код из Telegram/SMS")
        step2_form.addRow("Код:", self.ed_phone_code)

        self.lbl_phone_code_password = QLabel("Пароль 2FA:")
        self.ed_phone_code_password = QLineEdit()
        self.ed_phone_code_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.ed_phone_code_password.setPlaceholderText("Введите пароль 2FA")
        self.lbl_phone_code_password.setVisible(False)
        self.ed_phone_code_password.setVisible(False)
        step2_form.addRow(self.lbl_phone_code_password, self.ed_phone_code_password)

        phone_step2_actions = QWidget()
        phone_step2_actions_layout = QHBoxLayout(phone_step2_actions)
        phone_step2_actions_layout.setContentsMargins(0, 0, 0, 0)
        phone_step2_actions_layout.setSpacing(8)

        self.btn_phone_login = QPushButton("Войти")
        self.btn_phone_login.clicked.connect(self._submit_phone_code)
        phone_step2_actions_layout.addWidget(self.btn_phone_login)

        self.btn_phone_back = QPushButton("Назад")
        self.btn_phone_back.clicked.connect(self._back_to_phone_start)
        phone_step2_actions_layout.addWidget(self.btn_phone_back)

        step2_form.addRow(phone_step2_actions)

        self.lbl_phone_status = QLabel("")
        self.lbl_phone_status.setWordWrap(True)
        step2_form.addRow(self.lbl_phone_status)

        layout.addWidget(self.phone_step1_box)
        layout.addWidget(self.phone_step2_box)
        layout.addStretch(1)

        self._phone_tab_index = self.tabs.addTab(tab, "По номеру")
        self._set_phone_step(self.PHONE_STEP_START)

    def _build_qr_tab(self) -> None:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        self.lbl_qr_status = QLabel(
            "Откройте Telegram на телефоне и сканируйте QR: Настройки -> Устройства -> Подключить устройство."
        )
        self.lbl_qr_status.setWordWrap(True)
        layout.addWidget(self.lbl_qr_status)

        self.lbl_qr = QLabel("QR загружается...")
        self.lbl_qr.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_qr.setMinimumSize(300, 300)
        StyleManager.instance().bind_stylesheet(self.lbl_qr, "auth.qr_label")
        layout.addWidget(self.lbl_qr, 1)

        self.qr_secret_box = QWidget()
        qr_secret_form = QFormLayout(self.qr_secret_box)
        qr_secret_form.setContentsMargins(0, 0, 0, 0)
        qr_secret_form.setSpacing(8)

        self.ed_qr_secret = QLineEdit()
        self.ed_qr_secret.setEchoMode(QLineEdit.EchoMode.Password)
        self.ed_qr_secret.setPlaceholderText("Введите пароль 2FA")
        qr_secret_form.addRow("Код/пароль:", self.ed_qr_secret)

        qr_secret_actions = QWidget()
        qr_secret_actions_layout = QHBoxLayout(qr_secret_actions)
        qr_secret_actions_layout.setContentsMargins(0, 0, 0, 0)
        qr_secret_actions_layout.setSpacing(8)

        self.btn_qr_submit = QPushButton("Продолжить")
        self.btn_qr_submit.clicked.connect(self._submit_qr_secret)
        qr_secret_actions_layout.addWidget(self.btn_qr_submit)

        self.btn_qr_restart = QPushButton("Обновить QR")
        self.btn_qr_restart.clicked.connect(self._restart_qr_flow)
        qr_secret_actions_layout.addWidget(self.btn_qr_restart)

        qr_secret_form.addRow(qr_secret_actions)
        self.qr_secret_box.setVisible(False)
        layout.addWidget(self.qr_secret_box)

        self.btn_qr_refresh = QPushButton("Обновить QR")
        self.btn_qr_refresh.clicked.connect(self._restart_qr_flow)
        layout.addWidget(self.btn_qr_refresh)
        layout.addStretch(1)

        self._qr_tab_index = self.tabs.addTab(tab, "По QR-коду")

    def _set_phone_step(self, step: str) -> None:
        self._phone_step = step
        is_start = step == self.PHONE_STEP_START
        self.phone_step1_box.setVisible(is_start)
        self.phone_step2_box.setVisible(not is_start)
        if not is_start:
            self.ed_phone_code.setFocus()

    @staticmethod
    def _is_session_password_needed(exc: Exception) -> bool:
        text = str(exc or "").upper()
        return "SESSION_PASSWORD_NEEDED" in text

    @staticmethod
    def _is_invalid_password(exc: Exception) -> bool:
        text = str(exc or "").upper()
        return "PASSWORD_HASH_INVALID" in text or "PASSWORD" in text and "INVALID" in text

    def _apply_disabled_auth_state(self) -> None:
        hint = (
            "Telegram API не настроен: не найден telegram_api_id/telegram_api_hash.\n"
            "Нужен файл config.json (в папке данных или рядом с программой) "
            "или переменные окружения DRAGO_TG_API_ID / DRAGO_TG_API_HASH."
        )
        self.lbl_phone_step1_hint.setText(hint)
        self.lbl_phone_status.setText(hint)
        self.lbl_qr_status.setText(hint)
        self.lbl_qr.setText("QR недоступен: Telegram API не настроен.")

        for widget in (
            self.ed_phone,
            self.ed_phone_password,
            self.ed_phone_code,
            self.ed_phone_code_password,
            self.ed_qr_secret,
            self.btn_phone_request_code,
            self.btn_phone_login,
            self.btn_qr_submit,
            self.btn_qr_restart,
            self.btn_qr_refresh,
        ):
            widget.setEnabled(False)

    def _request_phone_code(self) -> None:
        phone = (self.ed_phone.text() or "").strip()
        if not phone:
            QMessageBox.warning(self, "Телефон", "Введите номер телефона.")
            return

        self._phone_cached_number = phone
        self._phone_cached_password = (self.ed_phone_password.text() or "").strip()
        self.lbl_phone_step1_hint.setText("Отправляем код...")
        self.btn_phone_request_code.setEnabled(False)
        try:
            self.tg.send_login_code_sync(phone)
            self._set_phone_step(self.PHONE_STEP_CODE)
            self.lbl_phone_status.setText("Код отправлен. Введите код из Telegram/SMS.")
        except Exception as exc:
            QMessageBox.critical(self, "Ошибка", str(exc))
        finally:
            self.btn_phone_request_code.setEnabled(True)

    def _submit_phone_code(self) -> None:
        code = (self.ed_phone_code.text() or "").strip()
        if not code:
            QMessageBox.warning(self, "Код", "Введите код.")
            return
        if not self._phone_cached_number:
            QMessageBox.warning(self, "Вход", "Сначала укажите номер телефона и отправьте код.")
            self._set_phone_step(self.PHONE_STEP_START)
            return

        entered_code_pwd = (self.ed_phone_code_password.text() or "").strip()
        pwd = entered_code_pwd or self._phone_cached_password or None

        self.btn_phone_login.setEnabled(False)
        try:
            ok = self.tg.sign_in_with_code_sync(self._phone_cached_number, code, pwd)
            if ok:
                self.login_success.emit()
                self.accept()
                return
            QMessageBox.critical(self, "Ошибка", "Не удалось выполнить вход.")
        except Exception as exc:
            if self._is_session_password_needed(exc):
                self.lbl_phone_status.setText("Нужен пароль 2FA. Введите пароль и повторите вход.")
                self.lbl_phone_code_password.setVisible(True)
                self.ed_phone_code_password.setVisible(True)
                self.ed_phone_code_password.setFocus()
                self.adjustSize()
                return
            if self._is_invalid_password(exc):
                self.lbl_phone_status.setText("Неверный пароль 2FA. Проверьте и попробуйте снова.")
                self.lbl_phone_code_password.setVisible(True)
                self.ed_phone_code_password.setVisible(True)
                self.ed_phone_code_password.setFocus()
                self.adjustSize()
                return
            QMessageBox.critical(self, "Ошибка", str(exc))
        finally:
            self.btn_phone_login.setEnabled(True)

    def _back_to_phone_start(self) -> None:
        self._set_phone_step(self.PHONE_STEP_START)
        self.ed_phone_code.clear()
        self.ed_phone_code_password.clear()
        self.lbl_phone_code_password.setVisible(False)
        self.ed_phone_code_password.setVisible(False)
        self.lbl_phone_status.setText("")

    def _on_tab_changed(self, index: int) -> None:
        if not self._telegram_enabled:
            return
        if index == getattr(self, "_qr_tab_index", -1):
            self._start_qr_login()

    def _start_qr_login(self) -> None:
        if self._qr_login_done or self._qr_thread_started:
            return
        self._qr_thread_started = True
        self._qr_needs_secret = False
        self.qr_secret_box.setVisible(False)
        self.lbl_qr.setVisible(True)
        self.lbl_qr.setText("QR загружается...")
        self.lbl_qr_status.setText("Генерируем QR-код...")

        def worker() -> None:
            try:
                ok = self.tg.start_qr_login_sync(
                    on_qr_png=lambda data: self.sig_qr_png.emit(data),
                    on_status=lambda text: self.sig_qr_status.emit(text),
                    timeout_total=240.0,
                )
                self.sig_qr_done.emit(bool(ok))
            except Exception as exc:
                self.sig_qr_status.emit(f"QR: ошибка — {exc}")
                self.sig_qr_done.emit(False)

        threading.Thread(target=worker, daemon=True).start()

    def _restart_qr_flow(self) -> None:
        self._qr_thread_started = False
        self._qr_login_done = False
        self._qr_needs_secret = False
        self.ed_qr_secret.clear()
        self.qr_secret_box.setVisible(False)
        self.lbl_qr.clear()
        self.lbl_qr.setVisible(True)
        self.lbl_qr.setText("QR загружается...")
        self.lbl_qr_status.setText("Перезапускаем QR...")
        self._start_qr_login()

    def _submit_qr_secret(self) -> None:
        secret = (self.ed_qr_secret.text() or "").strip()
        if not secret:
            QMessageBox.warning(self, "Код/пароль", "Введите код или пароль для продолжения.")
            return
        try:
            self.tg.submit_qr_2fa_password_sync(secret)
            self.lbl_qr_status.setText("Данные отправлены. Завершаем вход...")
            self.btn_qr_submit.setEnabled(False)
        except Exception as exc:
            QMessageBox.critical(self, "Ошибка", str(exc))

    @Slot(bytes)
    def _on_qr_png(self, data: bytes) -> None:
        if self._qr_needs_secret:
            return
        pm = QPixmap()
        if not pm.loadFromData(data, "PNG"):
            return
        pm = pm.scaled(
            360,
            360,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.lbl_qr.setPixmap(pm)
        self.lbl_qr.setFixedSize(pm.size())
        self.lbl_qr.setVisible(True)

    @Slot(str)
    def _on_qr_status(self, text: str) -> None:
        self.lbl_qr_status.setText(text)
        marker = (text or "").lower()
        needs_secret = ("2fa" in marker) or ("двухэтап" in marker) or ("парол" in marker) or ("код" in marker)
        if needs_secret:
            self._qr_needs_secret = True
            self.lbl_qr.setVisible(False)
            self.qr_secret_box.setVisible(True)
            self.ed_qr_secret.setFocus()
            self.btn_qr_submit.setEnabled(True)
            self.adjustSize()

    @Slot(bool)
    def _on_qr_done(self, ok: bool) -> None:
        self._qr_thread_started = False
        self.btn_qr_submit.setEnabled(True)
        if ok:
            self._qr_login_done = True
            self.login_success.emit()
            self.accept()
        elif not self._qr_needs_secret:
            self.lbl_qr_status.setText("QR вход не завершен. Нажмите «Обновить QR» и повторите.")

from __future__ import annotations

import threading

from PySide6.QtCore import Qt, Signal, Slot, QTimer, QByteArray
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from ui.styles import StyleManager


class AuthDialog(QDialog):
    login_success = Signal()
    sig_qr_png = Signal(bytes)
    sig_status = Signal(str)
    sig_done = Signal(bool)

    def __init__(self, tg_adapter, parent=None):
        super().__init__(parent)
        self.tg = tg_adapter
        self.setWindowTitle("Вход в Telegram")
        self.setModal(True)

        root = QVBoxLayout(self)

        # Телефон/код (+2FA) — запасной путь
        f = QFormLayout()
        self.ed_phone = QLineEdit()
        self.btn_send = QPushButton("Отправить код")
        f.addRow("Телефон (+XXX…):", self.ed_phone)
        f.addRow(self.btn_send)

        self.ed_code = QLineEdit()
        f.addRow("Код:", self.ed_code)

        # метка 2FA и поле скрыты до запроса
        self.lbl_pwd = QLabel("Пароль 2FA:")
        self._qr_needs_pwd = False
        self.ed_password = QLineEdit()
        self.ed_password.setEchoMode(QLineEdit.EchoMode.Password)
        self.lbl_pwd.setVisible(False)
        self.ed_password.setVisible(False)
        f.addRow(self.lbl_pwd, self.ed_password)

        self.btn_login = QPushButton("Войти")
        f.addRow(self.btn_login)

        self.lbl_info = QLabel("")
        f.addRow(self.lbl_info)
        root.addLayout(f)

        # Автоматический QR (без кнопки)
        self.lbl_qr = QLabel()
        self.lbl_qr.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_qr.setMinimumSize(280, 280)
        StyleManager.instance().bind_stylesheet(self.lbl_qr, "auth.qr_label")
        root.addWidget(self.lbl_qr)

        bb = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        bb.rejected.connect(self.reject)
        root.addWidget(bb)

        # wire
        self.btn_send.clicked.connect(self._send_code)
        self.btn_login.clicked.connect(self._login_phone)

        # сигналы из QR-потока → UI
        self.sig_qr_png.connect(self._on_qr_png)
        self.sig_status.connect(self._on_status)
        self.sig_done.connect(self._on_qr_done)

        # автозапуск QR-логина
        QTimer.singleShot(30, self._start_qr_login)

    # ---------- QR ----------
    def _start_qr_login(self):
        def worker():
            try:
                ok = self.tg.start_qr_login_sync(
                    on_qr_png=lambda b: self.sig_qr_png.emit(b),
                    on_status=lambda s: self.sig_status.emit(s),
                    timeout_total=180.0
                )
                self.sig_done.emit(bool(ok))
            except Exception as e:
                self.sig_status.emit(f"QR: ошибка — {e}")
        threading.Thread(target=worker, daemon=True).start()

    @Slot(bytes)
    def _on_qr_png(self, data: bytes):
        pm = QPixmap()
        pm.loadFromData(QByteArray(data), "PNG")
        pm = pm.scaled(
            360, 360,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation
        )
        self.lbl_qr.setPixmap(pm)
        self.lbl_qr.setFixedSize(pm.size())
        self.adjustSize()

    @Slot(str)
    def _on_status(self, text: str):
        self.lbl_info.setText(text)
        if "2FA" in text or "двухэтапн" in text:
            self.lbl_pwd.setVisible(True)
            self.ed_password.setVisible(True)
            self._qr_needs_pwd = True
            self.btn_login.setText("Продолжить (QR + пароль)")

    @Slot(bool)
    def _on_qr_done(self, ok: bool):
        if ok:
            self.login_success.emit()
            self.accept()

    # ---------- Телефон + код (+2FA) ----------
    def _send_code(self):
        phone = (self.ed_phone.text() or "").strip()
        if not phone:
            QMessageBox.warning(self, "Телефон", "Введите номер телефона.")
            return
        try:
            _ = self.tg.send_login_code_sync(phone)
            self.lbl_info.setText("Код отправлен. Проверьте Telegram/SMS.")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def _login_phone(self):
        phone = (self.ed_phone.text() or "").strip()
        code = (self.ed_code.text() or "").strip()
        pwd = (self.ed_password.text() or "").strip() or None

        # QR + 2FA: нет телефона и кода, но QR-поток ждёт пароль
        if self._qr_needs_pwd and not phone and not code:
            if not pwd:
                QMessageBox.warning(self, "Пароль 2FA", "Введите пароль 2FA для завершения QR-входа.")
                return
            try:
                self.tg.submit_qr_2fa_password_sync(pwd)
                self.lbl_info.setText("Пароль отправлен. Завершаем вход…")
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))
            return

        if not (phone and code):
            QMessageBox.warning(self, "Вход", "Введите телефон и код.")
            return
        try:
            ok = self.tg.sign_in_with_code_sync(phone, code, pwd)
            if ok:
                self.login_success.emit()
                self.accept()
        except Exception as e:
            if "SESSION_PASSWORD_NEEDED" in str(e):
                self.lbl_pwd.setVisible(True)
                self.ed_password.setVisible(True)
                self.adjustSize()
                self.lbl_info.setText("Введите пароль 2FA и нажмите «Войти».")
            else:
                QMessageBox.critical(self, "Ошибка", str(e))

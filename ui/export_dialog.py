from __future__ import annotations

from typing import Dict, List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QDialog,
    QFileDialog,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSlider,
    QVBoxLayout,
    QWidget,
)


class ExportSettingsDialog(QDialog):
    export_requested = Signal(dict)

    def __init__(self, chat_title: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._chat_title = str(chat_title or "").strip()
        self.setWindowTitle("Выгрузка чата")
        self.resize(486, 720)
        self.setMinimumWidth(460)
        self.setMinimumHeight(620)
        self.setModal(True)
        self.setStyleSheet(
            "QDialog{background-color:#0e1621;color:#edf5ff;}"
            "QFrame#exportSheet{background-color:#17212b;border:1px solid rgba(255,255,255,0.05);border-radius:24px;}"
            "QFrame#exportHandle{background-color:rgba(255,255,255,0.18);border-radius:2px;}"
            "QFrame#exportCard{background-color:#1b2733;border:1px solid rgba(255,255,255,0.05);border-radius:18px;}"
            "QLabel#exportTitle{color:#edf5ff;font-size:22px;font-weight:700;}"
            "QLabel#exportSubtitle{color:#7f95b0;font-size:13px;}"
            "QLabel#exportSectionTitle{color:#edf5ff;font-size:15px;font-weight:700;}"
            "QLabel#exportSectionHint{color:#7f95b0;font-size:12px;}"
            "QLabel#exportValuePill{background-color:#213447;color:#edf5ff;border-radius:12px;padding:6px 12px;font-size:12px;font-weight:700;}"
            "QRadioButton,QCheckBox{spacing:12px;padding:11px 14px;color:#edf5ff;font-size:15px;}"
            "QRadioButton:hover,QCheckBox:hover{background-color:rgba(255,255,255,0.04);border-radius:12px;}"
            "QRadioButton::indicator{width:18px;height:18px;border-radius:9px;border:1px solid rgba(255,255,255,0.22);background-color:#0f1823;}"
            "QRadioButton::indicator:checked{background-color:#58a8f6;border:5px solid #0f1823;}"
            "QCheckBox::indicator{width:18px;height:18px;border-radius:6px;border:1px solid rgba(255,255,255,0.22);background-color:#0f1823;}"
            "QCheckBox::indicator:checked{background-color:#58a8f6;border:1px solid #58a8f6;}"
            "QLineEdit{min-height:44px;background-color:#101923;border:1px solid rgba(255,255,255,0.07);border-radius:12px;padding:0 14px;color:#edf5ff;font-size:15px;}"
            "QLineEdit:focus{border:1px solid rgba(88,168,246,0.58);}"
            "QPushButton{min-height:44px;border:none;border-radius:12px;padding:0 16px;font-size:14px;font-weight:600;}"
            "QPushButton#secondaryButton{background-color:#213447;color:#dcecff;}"
            "QPushButton#secondaryButton:hover{background-color:#284056;}"
            "QPushButton#primaryButton{background-color:#58a8f6;color:#ffffff;}"
            "QPushButton#primaryButton:hover{background-color:#67b1fa;}"
            "QPushButton#dangerButton{background-color:rgba(255,96,96,0.18);color:#ffd4d4;}"
            "QSlider::groove:horizontal{height:4px;background:rgba(255,255,255,0.10);border-radius:2px;}"
            "QSlider::sub-page:horizontal{background:#58a8f6;border-radius:2px;}"
            "QSlider::add-page:horizontal{background:rgba(255,255,255,0.10);border-radius:2px;}"
            "QSlider::handle:horizontal{width:18px;height:18px;margin:-7px 0;border:none;border-radius:9px;background:#58a8f6;}"
            "QScrollArea{border:none;background:transparent;}"
        )

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(0)

        shell = QFrame(self)
        shell.setObjectName("exportSheet")
        shell_layout = QVBoxLayout(shell)
        shell_layout.setContentsMargins(16, 12, 16, 16)
        shell_layout.setSpacing(14)
        root.addWidget(shell)

        handle = QFrame(shell)
        handle.setObjectName("exportHandle")
        handle.setFixedSize(36, 4)
        shell_layout.addWidget(handle, 0, Qt.AlignmentFlag.AlignHCenter)

        title = QLabel("Выгрузка истории чата", shell)
        title.setObjectName("exportTitle")
        shell_layout.addWidget(title)

        subtitle = QLabel(self._chat_title or "Текущий чат", shell)
        subtitle.setObjectName("exportSubtitle")
        subtitle.setWordWrap(True)
        shell_layout.addWidget(subtitle)

        scroll = QScrollArea(shell)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        shell_layout.addWidget(scroll, 1)

        content = QWidget(scroll)
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.setSpacing(12)
        scroll.setWidget(content)

        self._build_format_section(content_layout)
        self._build_media_section(content_layout)
        self._build_media_size_section(content_layout)
        self._build_date_section(content_layout)
        self._build_location_section(content_layout)

        button_row = QHBoxLayout()
        button_row.setContentsMargins(0, 0, 0, 0)
        button_row.setSpacing(10)
        shell_layout.addLayout(button_row)

        btn_cancel = QPushButton("Отмена", shell)
        btn_cancel.setObjectName("secondaryButton")
        btn_cancel.clicked.connect(self.reject)
        button_row.addWidget(btn_cancel, 1)

        btn_export = QPushButton("Выгрузить", shell)
        btn_export.setObjectName("primaryButton")
        btn_export.setDefault(True)
        btn_export.clicked.connect(self._on_export)
        button_row.addWidget(btn_export, 1)

    def _make_card(
        self,
        root: QVBoxLayout,
        title: str,
        *,
        hint: str = "",
    ) -> QVBoxLayout:
        card = QFrame(self)
        card.setObjectName("exportCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(14, 14, 14, 14)
        layout.setSpacing(8)

        heading = QLabel(str(title or ""), card)
        heading.setObjectName("exportSectionTitle")
        layout.addWidget(heading)

        if hint:
            hint_label = QLabel(str(hint or ""), card)
            hint_label.setObjectName("exportSectionHint")
            hint_label.setWordWrap(True)
            layout.addWidget(hint_label)

        root.addWidget(card)
        return layout

    def _build_format_section(self, root: QVBoxLayout) -> None:
        layout = self._make_card(root, "Формат выгрузки")
        self._format_group = QButtonGroup(self)
        self._rb_html = QRadioButton("HTML", self)
        self._rb_json = QRadioButton("JSON", self)
        self._rb_both = QRadioButton("HTML и JSON", self)
        self._rb_html.setChecked(True)
        self._format_group.addButton(self._rb_html, 0)
        self._format_group.addButton(self._rb_json, 1)
        self._format_group.addButton(self._rb_both, 2)
        layout.addWidget(self._rb_html)
        layout.addWidget(self._rb_json)
        layout.addWidget(self._rb_both)

    def _build_media_section(self, root: QVBoxLayout) -> None:
        layout = self._make_card(root, "Медиа", hint="Отметьте типы вложений, которые нужно включить в архив.")

        self._chk_photos = QCheckBox("Фотографии", self)
        self._chk_photos.setChecked(True)
        self._chk_videos = QCheckBox("Видеозаписи", self)
        self._chk_voice = QCheckBox("Голосовые сообщения", self)
        self._chk_video_notes = QCheckBox("Видеосообщения", self)
        self._chk_stickers = QCheckBox("Стикеры", self)
        self._chk_gifs = QCheckBox("GIF-анимации", self)
        self._chk_files = QCheckBox("Файлы", self)
        self._chk_audio = QCheckBox("Музыка", self)

        self._media_checks = {
            "photo": self._chk_photos,
            "video": self._chk_videos,
            "voice": self._chk_voice,
            "video_note": self._chk_video_notes,
            "sticker": self._chk_stickers,
            "animation": self._chk_gifs,
            "document": self._chk_files,
            "audio": self._chk_audio,
        }
        for widget in self._media_checks.values():
            layout.addWidget(widget)

    def _build_media_size_section(self, root: QVBoxLayout) -> None:
        layout = self._make_card(root, "Ограничение размера медиа")

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(8)
        label = QLabel("Максимальный размер файла", self)
        label.setObjectName("exportSectionHint")
        top_row.addWidget(label, 1)

        self._size_label = QLabel("8 MB", self)
        self._size_label.setObjectName("exportValuePill")
        top_row.addWidget(self._size_label, 0, Qt.AlignmentFlag.AlignRight)
        layout.addLayout(top_row)

        self._size_slider = QSlider(Qt.Orientation.Horizontal, self)
        self._size_slider.setMinimum(0)
        self._size_slider.setMaximum(100)
        self._size_slider.setValue(7)
        self._size_slider.valueChanged.connect(self._update_size_label)
        layout.addWidget(self._size_slider)

    @staticmethod
    def _slider_to_mb(index: int) -> int:
        if index <= 9:
            return max(1, index + 1)
        if index <= 29:
            return 10 + (index - 10) * 2
        if index <= 39:
            return 50 + (index - 30) * 5
        if index <= 59:
            return 100 + (index - 40) * 10
        if index <= 69:
            return 300 + (index - 60) * 20
        if index <= 79:
            return 500 + (index - 70) * 50
        if index <= 89:
            return 1000 + (index - 80) * 100
        return 2000 + (index - 90) * 200

    def _update_size_label(self, value: int) -> None:
        self._size_label.setText(f"{self._slider_to_mb(value)} MB")

    def _build_date_section(self, root: QVBoxLayout) -> None:
        layout = self._make_card(
            root,
            "Диапазон дат",
            hint="Поддерживаются форматы ГГГГ-ММ-ДД и ГГГГ-ММ-ДД ЧЧ:ММ.",
        )

        self._date_from = QLineEdit(self)
        self._date_from.setPlaceholderText("С начала")
        layout.addWidget(self._labeled_field("С", self._date_from))

        self._date_to = QLineEdit(self)
        self._date_to.setPlaceholderText("До конца")
        layout.addWidget(self._labeled_field("По", self._date_to))

    def _build_location_section(self, root: QVBoxLayout) -> None:
        layout = self._make_card(root, "Папка сохранения")

        self._path_edit = QLineEdit(self)
        self._path_edit.setReadOnly(True)
        self._path_edit.setPlaceholderText("Выберите папку для экспорта")

        row = QHBoxLayout()
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(8)
        row.addWidget(self._path_edit, 1)

        btn_browse = QPushButton("Обзор", self)
        btn_browse.setObjectName("secondaryButton")
        btn_browse.clicked.connect(self._browse_output_dir)
        row.addWidget(btn_browse, 0)
        layout.addLayout(row)

    def _labeled_field(self, label_text: str, field: QLineEdit) -> QWidget:
        wrap = QWidget(self)
        row = QHBoxLayout(wrap)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(10)

        label = QLabel(label_text, wrap)
        label.setObjectName("exportSectionHint")
        label.setMinimumWidth(22)
        row.addWidget(label, 0)
        row.addWidget(field, 1)
        return wrap

    def _browse_output_dir(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Выберите папку для экспорта")
        if path:
            self._path_edit.setText(path)

    def _on_export(self) -> None:
        fmt_id = self._format_group.checkedId()
        if fmt_id == 0:
            formats = ["html"]
        elif fmt_id == 1:
            formats = ["json"]
        else:
            formats = ["html", "json"]

        media_types: List[str] = []
        for media_type, checkbox in self._media_checks.items():
            if checkbox.isChecked():
                media_types.append(media_type)

        max_size_mb = self._slider_to_mb(self._size_slider.value())

        date_from: Optional[int] = None
        date_to: Optional[int] = None
        try:
            from_str = self._date_from.text().strip()
            to_str = self._date_to.text().strip()
            import datetime

            if from_str:
                try:
                    dt = datetime.datetime.strptime(from_str[:16], "%Y-%m-%d %H:%M")
                except ValueError:
                    dt = datetime.datetime.strptime(from_str[:10], "%Y-%m-%d")
                date_from = int(dt.timestamp())
            if to_str:
                try:
                    dt = datetime.datetime.strptime(to_str[:16], "%Y-%m-%d %H:%M")
                except ValueError:
                    dt = datetime.datetime.strptime(to_str[:10], "%Y-%m-%d")
                dt = dt.replace(hour=23, minute=59, second=59)
                date_to = int(dt.timestamp())
        except Exception:
            pass

        settings = {
            "formats": formats,
            "media_types": media_types,
            "max_media_size_mb": max_size_mb,
            "date_from": date_from,
            "date_to": date_to,
            "output_dir": self._path_edit.text().strip() or None,
            "dark_theme": True,
        }
        self.export_requested.emit(settings)
        self.accept()

    def set_output_dir(self, path: str) -> None:
        self._path_edit.setText(str(path or ""))

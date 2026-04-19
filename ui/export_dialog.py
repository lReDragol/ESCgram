from __future__ import annotations

from typing import Dict, List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QRadioButton,
    QSizePolicy,
    QSlider,
    QVBoxLayout,
    QWidget,
)

from ui.styles import StyleManager


class ExportSettingsDialog(QDialog):
    """Telegram Desktop-style export settings dialog."""

    export_requested = Signal(dict)

    def __init__(self, chat_title: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._style_mgr = StyleManager.instance()
        self.setWindowTitle("Выгрузка чата")
        self.setMinimumWidth(420)
        self.setMinimumHeight(520)
        self._style_mgr.bind_stylesheet(self, "settings.drawer.background")

        root = QVBoxLayout(self)
        root.setContentsMargins(20, 20, 20, 20)
        root.setSpacing(12)

        header = QLabel(f"Выгрузка чата «{chat_title}»")
        self._style_mgr.bind_stylesheet(header, "settings.drawer.title")
        header.setWordWrap(True)
        root.addWidget(header)

        self._build_format_section(root)
        self._build_media_section(root)
        self._build_media_size_section(root)
        self._build_date_section(root)
        self._build_location_section(root)
        self._build_buttons(root)

    def _build_format_section(self, root: QVBoxLayout) -> None:
        group = QGroupBox("Формат")
        self._style_mgr.bind_stylesheet(group, "settings.drawer.card")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)
        self._format_group = QButtonGroup(self)
        self._rb_html = QRadioButton("HTML")
        self._rb_json = QRadioButton("JSON")
        self._rb_both = QRadioButton("HTML и JSON")
        self._rb_html.setChecked(True)
        self._format_group.addButton(self._rb_html, 0)
        self._format_group.addButton(self._rb_json, 1)
        self._format_group.addButton(self._rb_both, 2)
        for rb in (self._rb_html, self._rb_json, self._rb_both):
            self._style_mgr.bind_stylesheet(rb, "settings.toggle_row.label")
            layout.addWidget(rb)
        root.addWidget(group)

    def _build_media_section(self, root: QVBoxLayout) -> None:
        group = QGroupBox("Медиа")
        self._style_mgr.bind_stylesheet(group, "settings.drawer.card")
        layout = QVBoxLayout(group)
        layout.setSpacing(4)

        self._chk_photos = QCheckBox("Фотографии")
        self._chk_photos.setChecked(True)
        self._chk_videos = QCheckBox("Видеозаписи")
        self._chk_voice = QCheckBox("Голосовые сообщения")
        self._chk_video_notes = QCheckBox("Видеосообщения")
        self._chk_stickers = QCheckBox("Стикеры")
        self._chk_gifs = QCheckBox("GIF-анимации")
        self._chk_files = QCheckBox("Файлы")
        self._chk_audio = QCheckBox("Музыка")

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
        for chk in self._media_checks.values():
            self._style_mgr.bind_stylesheet(chk, "settings.toggle_row.label")
            layout.addWidget(chk)

        root.addWidget(group)

    def _build_media_size_section(self, root: QVBoxLayout) -> None:
        row = QHBoxLayout()
        row.setSpacing(8)
        label = QLabel("Макс. размер медиа:")
        self._style_mgr.bind_stylesheet(label, "settings.toggle_row.label")
        row.addWidget(label)

        self._size_slider = QSlider(Qt.Orientation.Horizontal)
        self._size_slider.setMinimum(0)
        self._size_slider.setMaximum(100)
        self._size_slider.setValue(7)
        self._size_slider.setTickPosition(QSlider.TickPosition.TicksBelow)
        self._size_slider.setTickInterval(10)
        row.addWidget(self._size_slider, 1)

        self._size_label = QLabel("8 MB")
        self._style_mgr.bind_stylesheet(self._size_label, "settings.toggle_row.label")
        self._size_label.setMinimumWidth(70)
        row.addWidget(self._size_label)

        self._size_slider.valueChanged.connect(self._update_size_label)

        container = QWidget()
        container.setLayout(row)
        root.addWidget(container)

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
        mb = self._slider_to_mb(value)
        self._size_label.setText(f"{mb} MB")

    def _build_date_section(self, root: QVBoxLayout) -> None:
        group = QGroupBox("Диапазон дат")
        self._style_mgr.bind_stylesheet(group, "settings.drawer.card")
        layout = QVBoxLayout(group)
        layout.setSpacing(6)

        row1 = QHBoxLayout()
        row1.setSpacing(6)
        lbl1 = QLabel("С:")
        self._style_mgr.bind_stylesheet(lbl1, "settings.toggle_row.label")
        self._date_from = QLineEdit()
        self._date_from.setPlaceholderText("С начала")
        self._date_from.setMaximumWidth(140)
        self._style_mgr.bind_stylesheet(self._date_from, "settings.drawer.card")
        row1.addWidget(lbl1)
        row1.addWidget(self._date_from)
        row1.addStretch(1)
        layout.addLayout(row1)

        row2 = QHBoxLayout()
        row2.setSpacing(6)
        lbl2 = QLabel("По:")
        self._style_mgr.bind_stylesheet(lbl2, "settings.toggle_row.label")
        self._date_to = QLineEdit()
        self._date_to.setPlaceholderText("До конца")
        self._date_to.setMaximumWidth(140)
        self._style_mgr.bind_stylesheet(self._date_to, "settings.drawer.card")
        row2.addWidget(lbl2)
        row2.addWidget(self._date_to)
        row2.addStretch(1)
        layout.addLayout(row2)

        hint = QLabel("Формат: ГГГГ-ММ-ДД или ГГГГ-ММ-ДД ЧЧ:ММ")
        self._style_mgr.bind_stylesheet(hint, "settings.account.hint")
        hint.setWordWrap(True)
        layout.addWidget(hint)

        root.addWidget(group)

    def _build_location_section(self, root: QVBoxLayout) -> None:
        row = QHBoxLayout()
        row.setSpacing(8)
        label = QLabel("Папка:")
        self._style_mgr.bind_stylesheet(label, "settings.toggle_row.label")
        row.addWidget(label)

        self._path_label = QLabel("")
        self._style_mgr.bind_stylesheet(self._path_label, "settings.toggle_row.label")
        self._path_label.setWordWrap(True)
        row.addWidget(self._path_label, 1)

        btn_browse = QPushButton("Обзор...")
        btn_browse.setCursor(Qt.CursorShape.PointingHandCursor)
        self._style_mgr.bind_stylesheet(btn_browse, "settings.action_button")
        btn_browse.clicked.connect(self._browse_output_dir)
        row.addWidget(btn_browse)

        container = QWidget()
        container.setLayout(row)
        root.addWidget(container)

    def _browse_output_dir(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Выберите папку для экспорта")
        if path:
            self._path_label.setText(path)

    def _build_buttons(self, root: QVBoxLayout) -> None:
        row = QHBoxLayout()
        row.setSpacing(12)
        row.addStretch(1)

        btn_cancel = QPushButton("Отмена")
        btn_cancel.setCursor(Qt.CursorShape.PointingHandCursor)
        self._style_mgr.bind_stylesheet(btn_cancel, "settings.action_button")
        btn_cancel.clicked.connect(self.reject)
        row.addWidget(btn_cancel)

        btn_export = QPushButton("Выгрузить")
        btn_export.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_export.setDefault(True)
        self._style_mgr.bind_stylesheet(btn_export, "settings.action_button")
        btn_export.clicked.connect(self._on_export)
        row.addWidget(btn_export)

        root.addLayout(row)

    def _on_export(self) -> None:
        fmt_id = self._format_group.checkedId()
        if fmt_id == 0:
            formats = ["html"]
        elif fmt_id == 1:
            formats = ["json"]
        else:
            formats = ["html", "json"]

        media_types: List[str] = []
        for mtype, chk in self._media_checks.items():
            if chk.isChecked():
                media_types.append(mtype)

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

        output_dir = self._path_label.text().strip()

        settings = {
            "formats": formats,
            "media_types": media_types,
            "max_media_size_mb": max_size_mb,
            "date_from": date_from,
            "date_to": date_to,
            "output_dir": output_dir or None,
            "dark_theme": True,
        }
        self.export_requested.emit(settings)
        self.accept()

    def set_output_dir(self, path: str) -> None:
        self._path_label.setText(str(path or ""))

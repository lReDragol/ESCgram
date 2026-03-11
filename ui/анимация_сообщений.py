# telegram_showcase_grid_dark.py
# ──────────────────────────────────────────────────────────────────────────────
# 10 равных зон (5x2). Голубые «пузырьки», тёмно-серый фон,
# у каждой зоны — обводка, сообщение по центру зоны. Цикличные анимации.
#
# Зависимости: PySide6
# Запуск:      python telegram_showcase_grid_dark.py
# ──────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

from PySide6.QtCore import (
    Qt, QPoint, QRect, QTimer,
    QPropertyAnimation, QEasingCurve,
    QParallelAnimationGroup, QSequentialAnimationGroup,
    QPauseAnimation
)
from PySide6.QtGui import QColor, QFont
from typing import Dict

from PySide6.QtWidgets import (
    QApplication, QWidget, QFrame, QLabel, QVBoxLayout, QHBoxLayout,
    QGridLayout, QSizePolicy, QGraphicsOpacityEffect, QGraphicsBlurEffect,
    QGraphicsDropShadowEffect
)

from ui.styles import StyleManager

# ─────────────────────────────── кирпичики UI ────────────────────────────────

DEFAULT_DEMO_COLORS = {
    "bubble_bg": "#62b3ff",
    "bubble_fg": "#ffffff",
    "app_bg": "#141519",
    "zone_bg": "#1f2125",
    "zone_border": "#3a3d42",
}


def _demo_colors() -> Dict[str, str]:
    mgr = StyleManager.instance()
    colors = mgr.value("animation_demo.colors", {}) or {}
    merged = dict(DEFAULT_DEMO_COLORS)
    if isinstance(colors, dict):
        for key, value in colors.items():
            if isinstance(value, str) and value:
                merged[key] = value
    return merged

class ChatBubble(QFrame):
    """Минималистичный «пузырёк» (всегда голубой)."""
    def __init__(self, text: str, parent: QWidget | None = None):
        super().__init__(parent)
        self.setObjectName("bubble")
        self.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.Fixed)

        self.label = QLabel(text, self)
        self.label.setWordWrap(True)
        self.label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.label.setFont(QFont("Segoe UI", 10))

        lay = QHBoxLayout(self)
        lay.setContentsMargins(12, 8, 12, 8)
        lay.addWidget(self.label)

        colors = _demo_colors()
        bubble_css = StyleManager.instance().stylesheet(
            "animation_demo.bubble",
            {"bubble_bg": colors["bubble_bg"], "bubble_fg": colors["bubble_fg"]},
        )
        if not bubble_css:
            bubble_css = (
                "QFrame#bubble {"
                f" background: {colors['bubble_bg']};"
                f" color: {colors['bubble_fg']};"
                " border-radius: 14px;"
                "}"
                f"QFrame#bubble QLabel {{ color: {colors['bubble_fg']}; }}"
            )
        self.setStyleSheet(bubble_css)

        # лёгкая тень для читаемости на тёмном фоне
        fx = QGraphicsDropShadowEffect(self)
        fx.setBlurRadius(18)
        fx.setOffset(0, 4)
        fx.setColor(QColor(0, 0, 0, 110))
        self.setGraphicsEffect(fx)

    def idealWidth(self, viewport_width: int) -> int:
        return int(viewport_width * 0.68)


class RowContainer(QWidget):
    """
    Контейнер строки: внутри один ChatBubble, которым мы свободно двигаем и
    анимируем (без layout у самого пузырька).
    """
    def __init__(self, bubble: ChatBubble, parent=None):
        super().__init__(parent)
        self.bubble = bubble
        self.bubble.setParent(self)
        self._hpad = 10
        self._vpad = 6
        self.setMinimumHeight(10)
        self._current_anim = None  # держим ссылку

    def resizeEvent(self, e):
        super().resizeEvent(e)
        self._place_final_geometry()

    # ───────── финальные координаты, куда «приезжает» пузырёк ─────────
    def _place_final_geometry(self):
        vpw = max(200, self.width())
        self.bubble.setMaximumWidth(self.bubble.idealWidth(vpw))
        self.bubble.adjustSize()
        bw, bh = self.bubble.width(), self.bubble.height()
        # центр по горизонтали и вертикали
        x = max(self._hpad, (self.width() - bw) // 2)
        y = max(self._vpad, (self.height() - bh) // 2)
        self.bubble.move(x, y)
        self.setMinimumHeight(bh + self._vpad * 2)

    def _final_pos(self) -> QPoint:
        self._place_final_geometry()
        return self.bubble.pos()

    def _final_rect(self) -> QRect:
        self._place_final_geometry()
        return self.bubble.geometry()

    # ──────────── 10 анимаций (по одной на зону) ────────────

    def animate_fade(self, dur=300):
        # чистый fade через QGraphicsOpacityEffect
        op = QGraphicsOpacityEffect(self.bubble)
        op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        a = QPropertyAnimation(op, b"opacity", self)
        a.setDuration(dur)
        a.setStartValue(0.0); a.setEndValue(1.0)
        a.setEasingCurve(QEasingCurve.OutCubic)
        return a

    def animate_slide_left_fade(self, dx=36, dur=360):
        final = self._final_pos()
        start = QPoint(final.x() - dx, final.y())
        self.bubble.move(start)

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        move = QPropertyAnimation(self.bubble, b"pos", self)
        move.setDuration(dur)
        move.setStartValue(start); move.setEndValue(final)
        move.setEasingCurve(QEasingCurve.OutCubic)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(dur); fade.setStartValue(0.0); fade.setEndValue(1.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(move); grp.addAnimation(fade)
        return grp

    def animate_slide_right_fade(self, dx=36, dur=360):
        final = self._final_pos()
        start = QPoint(final.x() + dx, final.y())
        self.bubble.move(start)

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        move = QPropertyAnimation(self.bubble, b"pos", self)
        move.setDuration(dur)
        move.setStartValue(start); move.setEndValue(final)
        move.setEasingCurve(QEasingCurve.OutCubic)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(dur); fade.setStartValue(0.0); fade.setEndValue(1.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(move); grp.addAnimation(fade)
        return grp

    def animate_pop_grow(self, dur=260):
        final = self._final_rect()
        start = QRect(final)
        start.setWidth(int(final.width() * 0.88))
        start.setHeight(int(final.height() * 0.88))
        start.moveTopLeft(QPoint(final.x() + (final.width()-start.width())//2,
                                 final.y() + (final.height()-start.height())//2))
        self.bubble.setGeometry(start)

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        grow = QPropertyAnimation(self.bubble, b"geometry", self)
        grow.setDuration(dur)
        grow.setStartValue(start); grow.setEndValue(final)
        grow.setEasingCurve(QEasingCurve.OutBack)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(int(dur*0.85))
        fade.setStartValue(0.0); fade.setEndValue(1.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(grow); grp.addAnimation(fade)
        return grp

    def animate_drop_bounce(self, dy=28, dur=520):
        final = self._final_pos()
        start = QPoint(final.x(), final.y() - dy)
        self.bubble.move(start)

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        move = QPropertyAnimation(self.bubble, b"pos", self)
        move.setDuration(dur)
        move.setStartValue(start); move.setEndValue(final)
        move.setEasingCurve(QEasingCurve.OutBounce)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(int(dur*0.6))
        fade.setStartValue(0.0); fade.setEndValue(1.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(move); grp.addAnimation(fade)
        return grp

    def animate_wipe_width(self, dur=360):
        final_rect = self._final_rect()
        self.bubble.setMaximumWidth(20)
        self.bubble.setMinimumWidth(20)
        self.bubble.move(final_rect.topLeft())

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        w = QPropertyAnimation(self.bubble, b"maximumWidth", self)
        w.setDuration(dur); w.setStartValue(20); w.setEndValue(final_rect.width())
        w.setEasingCurve(QEasingCurve.OutCubic)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(int(dur*0.85)); fade.setStartValue(0.0); fade.setEndValue(1.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(w); grp.addAnimation(fade)

        def cleanup():
            self.bubble.setMaximumWidth(self.bubble.idealWidth(self.width()))
            self.bubble.setMinimumWidth(0)
            self._place_final_geometry()
            self.bubble.setGraphicsEffect(None)

        grp.finished.connect(cleanup)
        return grp

    def animate_blur_in(self, dur=340):
        """Размытие 8→0 (без параллельного fade, эффект один за раз)."""
        blur = QGraphicsBlurEffect(self.bubble)
        blur.setBlurRadius(8.0)
        self.bubble.setGraphicsEffect(blur)

        a_blur = QPropertyAnimation(blur, b"blurRadius", self)
        a_blur.setDuration(dur)
        a_blur.setStartValue(8.0); a_blur.setEndValue(0.0)
        a_blur.setEasingCurve(QEasingCurve.OutCubic)

        def cleanup():
            self.bubble.setGraphicsEffect(None)

        a_blur.finished.connect(cleanup)
        return a_blur

    def animate_shadow_lift(self, dur=380):
        """Подхват тенью: blurRadius 28→12 и yOffset 14→2."""
        fx = QGraphicsDropShadowEffect(self.bubble)
        fx.setBlurRadius(28); fx.setColor(QColor(0, 0, 0, 140))
        fx.setXOffset(0); fx.setYOffset(14)
        self.bubble.setGraphicsEffect(fx)

        move = QPropertyAnimation(fx, b"yOffset", self)
        move.setDuration(dur); move.setStartValue(14.0); move.setEndValue(2.0)
        move.setEasingCurve(QEasingCurve.OutCubic)

        blur = QPropertyAnimation(fx, b"blurRadius", self)
        blur.setDuration(dur); blur.setStartValue(28.0); blur.setEndValue(12.0)

        grp = QParallelAnimationGroup(self); grp.addAnimation(move); grp.addAnimation(blur)

        def cleanup():
            fx.setBlurRadius(12); fx.setYOffset(2)

        grp.finished.connect(cleanup)
        return grp

    def animate_elastic_slide(self, dx=22, dur=440, from_right=True):
        final = self._final_pos()
        start = QPoint(final.x() + (dx if from_right else -dx), final.y())
        self.bubble.move(start)

        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        step1 = QPropertyAnimation(self.bubble, b"pos", self)
        step1.setDuration(int(dur*0.55))
        step1.setStartValue(start)
        overshoot = QPoint(final.x() - (dx//2 if from_right else -(dx//2)), final.y())
        step1.setEndValue(overshoot)
        step1.setEasingCurve(QEasingCurve.OutCubic)

        step2 = QPropertyAnimation(self.bubble, b"pos", self)
        step2.setDuration(int(dur*0.45))
        step2.setStartValue(overshoot); step2.setEndValue(final)
        step2.setEasingCurve(QEasingCurve.OutBack)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(dur); fade.setStartValue(0.0); fade.setEndValue(1.0)

        seq = QSequentialAnimationGroup(self); seq.addAnimation(step1); seq.addAnimation(step2)
        grp = QParallelAnimationGroup(self); grp.addAnimation(seq); grp.addAnimation(fade)
        return grp

    def animate_cascade_triplet(self, dx=16):
        """Три быстрых прилёта подряд к центру (уменьшая амплитуду)."""
        final = self._final_pos()
        op = QGraphicsOpacityEffect(self.bubble); op.setOpacity(0.0)
        self.bubble.setGraphicsEffect(op)

        def one_jump(offset, dur):
            start = QPoint(final.x() - offset, final.y())
            step = QPropertyAnimation(self.bubble, b"pos", self)
            step.setDuration(dur); step.setStartValue(start); step.setEndValue(final)
            step.setEasingCurve(QEasingCurve.OutBack)
            return step

        jump1 = one_jump(dx*1.0, 180)
        pause1 = QPauseAnimation(90, self)
        jump2 = one_jump(dx*0.6, 150)
        pause2 = QPauseAnimation(70, self)
        jump3 = one_jump(dx*0.35, 120)

        fade = QPropertyAnimation(op, b"opacity", self)
        fade.setDuration(180 + 90 + 150 + 70 + 120)
        fade.setStartValue(0.0); fade.setEndValue(1.0)

        seq = QSequentialAnimationGroup(self)
        seq.addAnimation(jump1); seq.addAnimation(pause1)
        seq.addAnimation(jump2); seq.addAnimation(pause2)
        seq.addAnimation(jump3)

        grp = QParallelAnimationGroup(self); grp.addAnimation(seq); grp.addAnimation(fade)
        return grp


# ──────────────────────────── Зона с цикличной анимацией ─────────────────────

STYLES = [
    "fade",             # 1
    "slide_left_fade",  # 2
    "slide_right_fade", # 3
    "pop_grow",         # 4
    "drop_bounce",      # 5
    "wipe_width",       # 6
    "blur_in",          # 7
    "shadow_lift",      # 8
    "elastic_slide",    # 9
    "cascade_triplet",  # 10
]

class AnimZone(QWidget):
    def __init__(self, index_1based: int, style_name: str):
        super().__init__()
        self.setObjectName("zone")
        colors = _demo_colors()
        zone_css = StyleManager.instance().stylesheet(
            "animation_demo.zone",
            {"zone_bg": colors["zone_bg"], "zone_border": colors["zone_border"]},
        )
        if not zone_css:
            zone_css = (
                "QWidget#zone {"
                f" background: {colors['zone_bg']};"
                f" border: 1px solid {colors['zone_border']};"
                " border-radius: 8px;"
                "}"
            )
        self.setStyleSheet(zone_css)
        # сам пузырёк (голубой)
        bubble = ChatBubble(str(index_1based))
        self.row = RowContainer(bubble, self)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 8, 8, 8)
        lay.addWidget(self.row, 1)

        self.style_name = style_name
        self._current_anim = None
        self._loop_pause_ms = 900
        QTimer.singleShot(30, self._loop_once)

    def _make_anim(self):
        creator = {
            "fade":             self.row.animate_fade,
            "slide_left_fade":  self.row.animate_slide_left_fade,
            "slide_right_fade": self.row.animate_slide_right_fade,
            "pop_grow":         self.row.animate_pop_grow,
            "drop_bounce":      self.row.animate_drop_bounce,
            "wipe_width":       self.row.animate_wipe_width,
            "blur_in":          self.row.animate_blur_in,
            "shadow_lift":      self.row.animate_shadow_lift,
            "elastic_slide":    lambda: self.row.animate_elastic_slide(from_right=True),
            "cascade_triplet":  self.row.animate_cascade_triplet,
        }[self.style_name]
        return creator()

    def _loop_once(self):
        # новый объект анимации каждый цикл
        self._current_anim = self._make_anim()
        self._current_anim.finished.connect(
            lambda: QTimer.singleShot(self._loop_pause_ms, self._loop_once)
        )
        self._current_anim.start()


# ─────────────────────────────── Основное окно ────────────────────────────────

class Showcase(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Telegram-style Animations • 10 Zones • PySide6")
        self.resize(1100, 680)
        colors = _demo_colors()
        bg_css = StyleManager.instance().stylesheet(
            "animation_demo.window",
            {"app_bg": colors["app_bg"]},
        )
        self.setStyleSheet(bg_css or f"background: {colors['app_bg']};")

        grid = QGridLayout(self)
        grid.setContentsMargins(12, 12, 12, 12)
        grid.setSpacing(10)

        # 5 столбцов × 2 ряда = 10 равных зон
        idx = 1
        for r in range(2):
            for c in range(5):
                style_name = STYLES[idx - 1]
                zone = AnimZone(idx, style_name)
                grid.addWidget(zone, r, c)
                idx += 1

        # одинаковые растяжения, чтобы зоны были равные
        for c in range(5):
            grid.setColumnStretch(c, 1)
        for r in range(2):
            grid.setRowStretch(r, 1)


if __name__ == "__main__":
    app = QApplication([])
    w = Showcase()
    w.show()
    app.exec()

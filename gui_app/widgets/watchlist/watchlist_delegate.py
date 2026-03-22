from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from PyQt5.QtCore import QMargins, QRect, Qt
from PyQt5.QtGui import QColor, QPainter, QPixmap
from PyQt5.QtWidgets import QStyledItemDelegate, QStyleOptionViewItem


class WatchlistDelegate(QStyledItemDelegate):
    _ICON_PATHS = {
        "CN": Path(__file__).resolve().parents[2] / "resources" / "flags" / "cn.svg",
        "HK": Path(__file__).resolve().parents[2] / "resources" / "flags" / "hk.svg",
        "US": Path(__file__).resolve().parents[2] / "resources" / "flags" / "us.svg",
    }

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._icon_cache: dict[str, QPixmap] = {}

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: Any) -> None:
        model = cast(Any, index.model())
        source_index: Any = index
        map_to_source = getattr(model, "mapToSource", None)
        if callable(map_to_source):
            source_index = map_to_source(index)
        source_model = cast(Any, source_index.model())
        columns = getattr(source_model, "COLUMNS", [])
        col_key = columns[source_index.column()][1] if columns else ""
        painter.save()
        self._paint_background_flash(painter, option, source_model, source_index)
        if col_key == "name":
            self._paint_name_with_icon(painter, option, source_index)
            painter.restore()
            return
        if col_key in ("change", "change_pct"):
            raw_value = getattr(source_model, "raw_value", None)
            num = self._to_float(raw_value(source_index.row(), col_key) if callable(raw_value) else None)
            color_getter = getattr(source_model, "_change_color", None)
            color = cast(QColor, color_getter(num)) if callable(color_getter) else QColor("#cfd8dc")
            option.palette.setColor(option.palette.Text, color)
        super().paint(painter, option, index)
        painter.restore()

    @staticmethod
    def _to_float(value) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    def _paint_background_flash(self, painter: QPainter, option: QStyleOptionViewItem, source_model, source_index) -> None:
        col_key = ""
        columns = getattr(source_model, "COLUMNS", [])
        if columns:
            col_key = columns[source_index.column()][1]
        if col_key not in ("price", "change", "change_pct"):
            return
        alpha_getter = getattr(source_model, "flash_alpha", None)
        alpha = self._to_float(alpha_getter(source_index.row()) if callable(alpha_getter) else 0.0)
        if alpha <= 0.0:
            return
        rect = option.rect.marginsRemoved(QMargins(0, 1, 0, 1))
        flash_color_getter = getattr(source_model, "flash_color", None)
        base = flash_color_getter(source_index.row()) if callable(flash_color_getter) else QColor("#4fc3f7")
        color = QColor(base)
        color.setAlpha(max(8, int(84 * alpha)))
        painter.fillRect(rect, color)

    def _paint_name_with_icon(self, painter: QPainter, option: QStyleOptionViewItem, source_index: Any) -> None:
        text = str(source_index.data(Qt.DisplayRole) or "--")
        source_model = cast(Any, source_index.model())
        raw_value = getattr(source_model, "raw_value", None)
        market = str(raw_value(source_index.row(), "market") if callable(raw_value) else "CN")
        rect = option.rect.marginsRemoved(QMargins(6, 0, 6, 0))
        icon_rect = QRect(rect.left(), rect.center().y() - 6, 12, 12)
        painter.setRenderHint(QPainter.Antialiasing, True)
        pix = self._load_icon(market)
        if pix is not None and not pix.isNull():
            painter.drawPixmap(icon_rect, pix)
        else:
            painter.setBrush(QColor("#4fc3f7"))
            painter.setPen(Qt.NoPen)
            painter.drawEllipse(icon_rect)
        text_rect = QRect(icon_rect.right() + 6, rect.top(), rect.width() - 20, rect.height())
        painter.setPen(QColor("#e5e7eb"))
        painter.drawText(text_rect, int(Qt.AlignVCenter | Qt.AlignLeft), text)

    def _load_icon(self, market: str) -> QPixmap | None:
        key = str(market or "CN").upper()
        if key in self._icon_cache:
            return self._icon_cache[key]
        path = self._ICON_PATHS.get(key)
        if path is None or not path.exists():
            self._icon_cache[key] = QPixmap()
            return self._icon_cache[key]
        pix = QPixmap(str(path))
        self._icon_cache[key] = pix
        return pix

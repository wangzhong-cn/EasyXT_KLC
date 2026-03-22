from __future__ import annotations

import time
from typing import Any

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt
from PyQt5.QtGui import QColor


class WatchlistModel(QAbstractTableModel):
    COLUMNS: list[tuple[str, str]] = [
        ("名称", "name"),
        ("代码", "symbol"),
        ("最新价", "price"),
        ("涨跌", "change"),
        ("幅度%", "change_pct"),
        ("买价", "bid1"),
        ("卖价", "ask1"),
        ("昨收", "prev_close"),
        ("开盘", "open"),
        ("最高", "high"),
        ("最低", "low"),
        ("成交量", "volume"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self._rows: list[dict[str, Any]] = []
        self._row_index: dict[str, int] = {}
        self._flash_until: dict[int, float] = {}
        self._flash_direction: dict[int, int] = {}
        self._color_mode = "red_up_green_down"

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self.COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.COLUMNS):
            return self.COLUMNS[section][0]
        return int(section + 1) if orientation == Qt.Vertical else None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None
        row = index.row()
        col = index.column()
        if row < 0 or row >= len(self._rows) or col < 0 or col >= len(self.COLUMNS):
            return None
        record = self._rows[row]
        key = self.COLUMNS[col][1]
        value = record.get(key)
        if role == Qt.TextAlignmentRole:
            return int(Qt.AlignVCenter | (Qt.AlignLeft if col < 2 else Qt.AlignRight))
        if role == Qt.ForegroundRole and key in ("change", "change_pct"):
            try:
                num = float(value or 0)
                return self._change_color(num)
            except Exception:
                return QColor("#cfd8dc")
        if role == Qt.DisplayRole:
            if key in ("price", "change", "change_pct", "bid1", "ask1", "prev_close", "open", "high", "low"):
                if value in (None, ""):
                    return "--"
                try:
                    return f"{float(value):.2f}"
                except Exception:
                    return str(value)
            if key == "volume":
                if value in (None, ""):
                    return "--"
                try:
                    return f"{int(float(value))}"
                except Exception:
                    return str(value)
            return str(value or "--")
        return None

    def raw_value(self, row: int, key: str):
        if row < 0 or row >= len(self._rows):
            return None
        return self._rows[row].get(key)

    def is_row_flashing(self, row: int) -> bool:
        if row < 0:
            return False
        return self._flash_until.get(row, 0.0) > time.monotonic()

    def flash_alpha(self, row: int) -> float:
        until = self._flash_until.get(row, 0.0)
        now = time.monotonic()
        if until <= now:
            return 0.0
        remain = max(0.0, until - now)
        return min(1.0, remain / 0.3)

    def flash_color(self, row: int) -> QColor:
        direction = self._flash_direction.get(row, 0)
        if direction > 0:
            return self._change_color(1.0)
        if direction < 0:
            return self._change_color(-1.0)
        return QColor("#4fc3f7")

    def set_color_mode(self, mode_text: str) -> None:
        self._color_mode = "red_up_green_down" if mode_text == "红涨绿跌" else "red_down_green_up"
        if self.rowCount() <= 0:
            return
        tl = self.index(0, 0)
        br = self.index(self.rowCount() - 1, len(self.COLUMNS) - 1)
        self.dataChanged.emit(tl, br, [Qt.ForegroundRole])

    @staticmethod
    def _infer_market(symbol: Any) -> str:
        s = str(symbol or "").upper()
        if s.endswith(".SZ") or s.endswith(".SH"):
            return "CN"
        if s.endswith(".HK"):
            return "HK"
        return "US"

    def set_symbols(self, symbols: list[dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = []
        self._row_index = {}
        self._flash_until = {}
        self._flash_direction = {}
        for i, item in enumerate(symbols):
            row = {
                "name": item.get("name") or item.get("symbol") or "--",
                "symbol": item.get("symbol") or "--",
                "market": item.get("market") or self._infer_market(item.get("symbol")),
                "price": item.get("price"),
                "change": item.get("change"),
                "change_pct": item.get("change_pct"),
                "bid1": item.get("bid1"),
                "ask1": item.get("ask1"),
                "prev_close": item.get("prev_close"),
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "volume": item.get("volume"),
            }
            self._rows.append(row)
            self._row_index[str(row["symbol"])] = i
        self.endResetModel()

    def upsert_quote(self, symbol: str, quote: dict[str, Any]) -> None:
        key = str(symbol or "").strip()
        if not key:
            return
        idx = self._row_index.get(key)
        if idx is None:
            idx = len(self._rows)
            self.beginInsertRows(QModelIndex(), idx, idx)
            self._rows.append({"name": key, "symbol": key})
            self._row_index[key] = idx
            self.endInsertRows()
        row = self._rows[idx]
        prev_price = row.get("price")
        row["symbol"] = key
        row["name"] = quote.get("name") or row.get("name") or key
        row["market"] = quote.get("market") or self._infer_market(key)
        row["price"] = quote.get("price")
        row["change"] = quote.get("change")
        row["change_pct"] = quote.get("change_pct")
        row["bid1"] = quote.get("bid1")
        row["ask1"] = quote.get("ask1")
        row["prev_close"] = quote.get("prev_close")
        row["open"] = quote.get("open")
        row["high"] = quote.get("high")
        row["low"] = quote.get("low")
        row["volume"] = quote.get("volume")
        price_value = quote.get("price")
        if price_value is not None and price_value != prev_price:
            try:
                new_price = float(price_value)
                old_price = float(prev_price) if prev_price not in (None, "") else new_price
                self._flash_direction[idx] = 1 if new_price > old_price else -1 if new_price < old_price else 0
            except Exception:
                self._flash_direction[idx] = 0
            self._flash_until[idx] = time.monotonic() + 0.3
        tl = self.index(idx, 0)
        br = self.index(idx, len(self.COLUMNS) - 1)
        self.dataChanged.emit(tl, br, [Qt.DisplayRole, Qt.ForegroundRole])

    def _change_color(self, value: float) -> QColor:
        if self._color_mode == "red_down_green_up":
            return QColor("#26a69a" if value > 0 else "#ef5350" if value < 0 else "#cfd8dc")
        return QColor("#ef5350" if value > 0 else "#26a69a" if value < 0 else "#cfd8dc")

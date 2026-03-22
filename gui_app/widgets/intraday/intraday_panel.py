from __future__ import annotations

import datetime as dt
import random
from typing import Any

from PyQt5.QtCore import QAbstractTableModel, QModelIndex, Qt, pyqtSignal
from PyQt5.QtGui import QColor, QPainter, QPen
from PyQt5.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from core.events import Events
from core.signal_bus import signal_bus

_INTRA_COLUMNS: list[tuple[str, str]] = [
    ("时间", "time"),
    ("价格", "price"),
    ("涨跌%", "change_pct"),
    ("成交量", "volume"),
]


class IntradayTableModel(QAbstractTableModel):
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._rows: list[dict[str, Any]] = []

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(_INTRA_COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return _INTRA_COLUMNS[section][0]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._rows):
            return None
        key = _INTRA_COLUMNS[index.column()][1]
        value = self._rows[index.row()].get(key)
        if role == Qt.DisplayRole:
            if isinstance(value, float):
                if key == "change_pct":
                    return f"{value:+.2f}%"
                if key == "price":
                    return f"{value:.3f}"
                return f"{value:,.2f}"
            return str(value) if value is not None else ""
        if role == Qt.TextAlignmentRole and index.column() > 0:
            return int(Qt.AlignRight | Qt.AlignVCenter)
        if role == Qt.ForegroundRole and key == "change_pct":
            v = float(value or 0)
            if v > 0:
                return QColor("#ef5350")
            if v < 0:
                return QColor("#26a69a")
        return None

    def load(self, rows: list[dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def update_last_quote(self, price: float, change_pct: float, volume: float, tick_time: str) -> None:
        if not self._rows:
            self.load([{"time": tick_time, "price": price, "change_pct": change_pct, "volume": volume}])
            return
        self._rows[-1] = {"time": tick_time, "price": price, "change_pct": change_pct, "volume": volume}
        last = self.index(len(self._rows) - 1, 0)
        self.dataChanged.emit(last, self.index(len(self._rows) - 1, len(_INTRA_COLUMNS) - 1), [Qt.DisplayRole])

    def rows_snapshot(self) -> list[dict[str, Any]]:
        return list(self._rows)


class IntradayCurveCanvas(QWidget):
    crosshair_moved = pyqtSignal(str, float)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setMinimumHeight(160)
        self.setMouseTracking(True)
        self._rows: list[dict[str, Any]] = []
        self._crosshair_idx = -1
        self._crosshair_price = 0.0

    def set_rows(self, rows: list[dict[str, Any]]) -> None:
        self._rows = list(rows)
        if self._rows:
            self._crosshair_idx = len(self._rows) - 1
            self._crosshair_price = float(self._rows[-1].get("price") or 0.0)
        self.update()

    def set_external_crosshair(self, time_text: str, price: float) -> None:
        if not self._rows:
            return
        idx = self._find_row_index_by_time(str(time_text or ""))
        if idx < 0:
            return
        self._crosshair_idx = idx
        self._crosshair_price = float(price)
        self.update()

    def mouseMoveEvent(self, a0) -> None:
        if not self._rows:
            return
        w = max(1, self.width() - 16)
        x = max(0, min(w, a0.x() - 8))
        idx = min(len(self._rows) - 1, int(round(x / max(1, w) * (len(self._rows) - 1))))
        self._crosshair_idx = idx
        row = self._rows[idx]
        self._crosshair_price = float(row.get("price") or 0.0)
        self.crosshair_moved.emit(str(row.get("time") or ""), self._crosshair_price)
        self.update()

    def paintEvent(self, a0) -> None:
        _ = a0
        p = QPainter(self)
        p.fillRect(self.rect(), QColor("#121722"))
        if len(self._rows) < 2:
            return
        prices = [float(r.get("price") or 0.0) for r in self._rows]
        pmin = min(prices)
        pmax = max(prices)
        span = (pmax - pmin) or 1.0
        left, top, right, bottom = 8, 8, self.width() - 8, self.height() - 8
        w = max(1, right - left)
        h = max(1, bottom - top)

        def to_xy(i: int, price: float) -> tuple[int, int]:
            x = int(left + (i / max(1, len(self._rows) - 1)) * w)
            y = int(bottom - ((price - pmin) / span) * h)
            return x, y

        p.setRenderHint(QPainter.Antialiasing, True)
        p.setPen(QPen(QColor("#4fc3f7"), 1.5))
        prev = to_xy(0, prices[0])
        for i in range(1, len(prices)):
            cur = to_xy(i, prices[i])
            p.drawLine(prev[0], prev[1], cur[0], cur[1])
            prev = cur

        if 0 <= self._crosshair_idx < len(self._rows):
            cx, cy = to_xy(self._crosshair_idx, self._crosshair_price)
            p.setPen(QPen(QColor("#90a4ae"), 1, Qt.DashLine))
            p.drawLine(cx, top, cx, bottom)
            p.drawLine(left, cy, right, cy)

    def _find_row_index_by_time(self, text: str) -> int:
        if not text:
            return -1
        key = text[:5]
        try:
            if text.isdigit():
                ts = int(text)
                if ts > 10_000_000_000:
                    ts //= 1000
                key = dt.datetime.fromtimestamp(ts).strftime("%H:%M")
        except Exception:
            key = text[:5]
        for i, row in enumerate(self._rows):
            if str(row.get("time") or "").startswith(key):
                return i
        return -1


class IntradayPanel(QWidget):
    symbol_clicked = pyqtSignal(str)

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._symbol = ""
        self._build_ui()
        signal_bus.subscribe(Events.SYMBOL_SELECTED, self._on_symbol_event)
        signal_bus.subscribe(Events.CHART_CROSSHAIR_MOVED, self._on_crosshair_event)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setSpacing(4)
        toolbar = QWidget(self)
        hl = QHBoxLayout(toolbar)
        hl.setContentsMargins(0, 0, 0, 0)
        hl.addWidget(QLabel("分时标的:"))
        self._symbol_edit = QLineEdit(self)
        self._symbol_edit.setPlaceholderText("例如 600000.SH")
        self._symbol_edit.returnPressed.connect(self._on_symbol_enter)
        hl.addWidget(self._symbol_edit)
        btn = QPushButton("切换", self)
        btn.clicked.connect(self._on_symbol_enter)
        hl.addWidget(btn)
        self._status = QLabel("等待标的", self)
        self._status.setStyleSheet("color:#8a95a5;")
        hl.addWidget(self._status)
        layout.addWidget(toolbar)
        self._curve = IntradayCurveCanvas(self)
        self._curve.crosshair_moved.connect(self._on_curve_crosshair_moved)
        layout.addWidget(self._curve)
        self._model = IntradayTableModel(self)
        self._table = QTableView(self)
        self._table.setModel(self._model)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setEditTriggers(QTableView.NoEditTriggers)
        self._table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._table, 1)

    def _on_symbol_enter(self) -> None:
        symbol = self._symbol_edit.text().strip()
        if symbol:
            self.set_symbol(symbol)
            self.symbol_clicked.emit(symbol)

    def set_symbol(self, symbol: str) -> None:
        self._symbol = str(symbol or "").strip()
        if not self._symbol:
            return
        self._symbol_edit.setText(self._symbol)
        self._status.setText(f"联动中: {self._symbol}")
        rows = self._build_intraday_skeleton(self._symbol)
        self._model.load(rows)
        self._curve.set_rows(rows)

    def update_quote(self, quote: dict[str, Any]) -> None:
        symbol = str(quote.get("symbol") or "").strip()
        if not symbol or symbol != self._symbol:
            return
        price = float(quote.get("price") or 0.0)
        change_pct = float(quote.get("change_pct") or 0.0)
        volume = float(quote.get("volume") or 0.0)
        tick_time = dt.datetime.now().strftime("%H:%M:%S")
        self._model.update_last_quote(price, change_pct, volume, tick_time)
        self._curve.set_rows(self._model.rows_snapshot())

    def _on_symbol_event(self, symbol: str, **kwargs) -> None:
        _ = kwargs
        self.set_symbol(symbol)

    def _on_crosshair_event(self, time=None, price=None, symbol: str = "", **kwargs) -> None:
        payload = kwargs.get("payload") if isinstance(kwargs, dict) else None
        if isinstance(payload, dict) and payload.get("source") == "intraday":
            return
        if symbol and symbol == self._symbol and time is not None and price is not None:
            self._status.setText(f"联动中: {symbol} @ {time} / {price}")
            self._curve.set_external_crosshair(str(time), float(price))

    def _on_curve_crosshair_moved(self, time_text: str, price: float) -> None:
        if not self._symbol:
            return
        self._status.setText(f"联动中: {self._symbol} @ {time_text} / {price:.3f}")
        signal_bus.emit(
            Events.CHART_CROSSHAIR_MOVED,
            time=time_text,
            price=price,
            symbol=self._symbol,
            period="",
            payload={"source": "intraday"},
        )

    def _build_intraday_skeleton(self, symbol: str) -> list[dict[str, Any]]:
        seed = sum(ord(ch) for ch in symbol)
        rng = random.Random(seed)
        base = round(10 + (seed % 300) / 10, 3)
        rows: list[dict[str, Any]] = []
        t0 = dt.datetime.combine(dt.date.today(), dt.time(9, 30))
        for i in range(30):
            cur = t0 + dt.timedelta(minutes=i)
            p = round(base * (1 + rng.uniform(-0.005, 0.005)), 3)
            cp = round((p / base - 1) * 100, 2)
            rows.append(
                {
                    "time": cur.strftime("%H:%M"),
                    "price": p,
                    "change_pct": cp,
                    "volume": int(rng.uniform(1000, 9000)),
                }
            )
        return rows

    def closeEvent(self, a0: Any) -> None:
        try:
            signal_bus.unsubscribe(Events.SYMBOL_SELECTED, self._on_symbol_event)
            signal_bus.unsubscribe(Events.CHART_CROSSHAIR_MOVED, self._on_crosshair_event)
        except Exception:
            pass
        super().closeEvent(a0)

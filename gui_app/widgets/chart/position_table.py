from __future__ import annotations

import logging
from typing import Any, Protocol, cast

from core.events import Events
from core.signal_bus import signal_bus


class PositionTable:
    def __init__(self, chart, position: str = "right", width: float = 0.24, height: float = 0.18):
        self.chart = chart
        self.table = None
        self._row_by_symbol: dict[str, int] = {}
        self._logger = logging.getLogger(__name__)
        self._create_table(position, width, height)

    def _create_table(self, position: str, width: float, height: float):
        try:
            self.table = self.chart.win.create_table(
                width=width,
                height=height,
                headings=("Symbol", "Side", "Qty", "Price", "PnL"),
                widths=(0.26, 0.12, 0.16, 0.22, 0.24),
                alignments=("left", "center", "right", "right", "right"),
                position=position,
                draggable=True,
                func=self._on_row_click,
            )
            self.table.format("Price", f"{self.table.VALUE}")
            self.table.format("PnL", f"{self.table.VALUE}")
        except Exception:
            self._logger.exception("Failed to create position table")

    def update_from_order(self, symbol: str, side: str, price: float, volume: int):
        if not self.table or not symbol:
            return
        row = self._get_or_create_row(symbol)
        if row is None:
            return
        try:
            row_map = row
            row["Side"] = (side or "").upper()
            row["Qty"] = int(volume)
            row["Price"] = float(price)
            row["PnL"] = row_map.get("PnL", 0)
        except Exception:
            self._logger.exception("Failed to update position table row")

    def update_positions(self, positions: list[dict]):
        if not self.table:
            return
        for position in positions:
            symbol = position.get("symbol") or position.get("code") or ""
            if not symbol:
                continue
            side = position.get("side") or ""
            volume = position.get("volume") or position.get("qty") or 0
            price = position.get("price") or position.get("avg_price") or 0
            pnl = position.get("pnl") or position.get("profit") or 0
            row = self._get_or_create_row(symbol)
            if row is None:
                continue
            try:
                row["Side"] = (side or "").upper()
                row["Qty"] = int(volume)
                row["Price"] = float(price)
                row["PnL"] = float(pnl)
            except Exception:
                self._logger.exception("Failed to update position row for %s", symbol)

    def _get_or_create_row(self, symbol: str) -> "_RowLike | None":
        if not self.table:
            return None
        row_id = self._row_by_symbol.get(symbol)
        if row_id is None:
            row_id = abs(hash(symbol)) % 100_000_000
            while row_id in self._row_by_symbol.values():
                row_id = (row_id + 1) % 100_000_000
            try:
                row = self.table.new_row(symbol, "", 0, 0, 0, id=row_id)
                self._row_by_symbol[symbol] = row_id
                return cast(_RowLike, row) if isinstance(row, dict) else None
            except Exception:
                self._logger.exception("Failed to create table row for %s", symbol)
                return None
        try:
            row = self.table.get(row_id)
            return cast(_RowLike, row) if isinstance(row, dict) else None
        except Exception:
            self._logger.exception("Failed to read table row for %s", symbol)
            return None

    def _on_row_click(self, row):
        try:
            symbol = row.get("Symbol") if row else ""
            if symbol:
                signal_bus.emit(Events.SYMBOL_SELECTED, symbol=symbol)
        except Exception:
            self._logger.exception("Failed to handle position row click")

    def set_visible(self, visible: bool):
        if not self.table:
            return
        try:
            self.table.visible(bool(visible))
        except Exception:
            self._logger.exception("Failed to toggle table visibility")


class _RowLike(Protocol):
    def __setitem__(self, key: str, value: Any) -> None:
        ...

    def get(self, key: str, default: Any = ...) -> Any:
        ...

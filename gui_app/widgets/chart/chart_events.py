from __future__ import annotations

import logging

from core.events import Events
from core.signal_bus import signal_bus


class ChartEvents:
    def __init__(self, chart, enable_topbar: bool = True):
        self.chart = chart
        self._logger = logging.getLogger(__name__)
        self._bound = False
        self._enable_topbar = enable_topbar
        self._symbol: str = ""
        self._period: str = ""
        self._build_topbar()

    def bind_signal_bus(self):
        if self._bound:
            return
        self._bound = True
        try:
            self.chart.events.search += self._on_search
            self.chart.events.click += self._on_click
            crosshair = getattr(self.chart.events, "crosshair_move", None)
            if crosshair is not None:
                crosshair += self._on_crosshair_move
        except Exception:
            self._logger.exception("Failed to bind chart search event")

    def set_symbol(self, symbol: str):
        if not symbol:
            return
        self._symbol = symbol
        try:
            widget = self.chart.topbar.get("symbol")
            if widget:
                widget.set(symbol)
        except Exception:
            self._logger.exception("Failed to update topbar symbol")

    def set_period(self, period: str):
        if not period:
            return
        self._period = period
        try:
            widget = self.chart.topbar.get("period")
            if widget:
                widget.set(period)
        except Exception:
            self._logger.exception("Failed to update topbar period")

    def _build_topbar(self):
        if not self._enable_topbar:
            return
        try:
            self.chart.topbar.textbox("symbol", "")
            self.chart.topbar.switcher(
                "period",
                ("1d", "1m", "5m", "tick"),
                default="1d",
                func=self._on_period_change,
            )
        except Exception:
            self._logger.exception("Failed to build chart topbar")

    def _on_search(self, chart, searched_string):
        try:
            signal_bus.emit(Events.SYMBOL_SELECTED, symbol=searched_string)
        except Exception:
            self._logger.exception("Failed to emit symbol search event")

    def _on_period_change(self, chart):
        try:
            widget = chart.topbar.get("period")
            period = widget.value if widget else ""
            if period:
                signal_bus.emit(Events.PERIOD_CHANGED, period=period)
        except Exception:
            self._logger.exception("Failed to emit period change event")

    def _on_click(self, chart, time, price):
        if price is None:
            return
        try:
            signal_bus.emit(Events.CHART_PRICE_CLICKED, price=price, time=time)
        except Exception:
            self._logger.exception("Failed to emit chart price click event")

    def _on_crosshair_move(self, chart, time, price):
        try:
            signal_bus.emit(
                Events.CHART_CROSSHAIR_MOVED,
                price=price,
                time=time,
                symbol=self._symbol,
                period=self._period,
                payload={"source": "lwc"},
            )
        except Exception:
            self._logger.exception("Failed to emit chart crosshair event")

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.events import Events
from core.signal_bus import signal_bus
from gui_app.widgets.backtest_widget import BacktestWidget
from gui_app.widgets.grid_trading_widget import GridTradingWidget


def test_event_chain_chart_to_grid(qapp):
    backtest = BacktestWidget()
    grid = GridTradingWidget()
    try:
        grid.stock_pool_edit.setText("000001.SZ")

        signal_bus.emit(Events.CHART_DATA_LOADED, symbol="000001.SZ")
        assert backtest.stock_code_edit.text() == "000001.SZ"

        signal_bus.emit(Events.ORDER_SUBMITTED, side="buy", symbol="000001.SZ", price=10.5, volume=100)
        assert grid.trade_table.rowCount() == 1
        assert "订单提交" in grid.log_text.toPlainText()
    finally:
        signal_bus.unsubscribe(Events.CHART_DATA_LOADED, backtest.on_chart_data_loaded)
        signal_bus.unsubscribe(Events.ORDER_SUBMITTED, grid.on_order_submitted)
        backtest.close()
        grid.close()

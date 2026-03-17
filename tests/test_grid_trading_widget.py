import os
import sys

from PyQt5.QtWidgets import QApplication

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gui_app.widgets.grid_trading_widget import GridTradingWidget


def get_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_on_order_submitted_adds_row_in_pool():
    get_app()
    widget = GridTradingWidget()
    widget.stock_pool_edit.setText("000001.SZ,600000.SH")

    widget.on_order_submitted(symbol="000001.SZ", side="buy", price=10.5, volume=100)

    assert widget.trade_table.rowCount() == 1
    assert "订单提交" in widget.log_text.toPlainText()


def test_on_order_submitted_ignores_out_of_pool():
    get_app()
    widget = GridTradingWidget()
    widget.stock_pool_edit.setText("000001.SZ")

    widget.on_order_submitted(symbol="600000.SH", side="sell", price=8.2, volume=200)

    assert widget.trade_table.rowCount() == 0
    assert widget.log_text.toPlainText() == ""

from __future__ import annotations

from gui_app.widgets.orders.orders_panel import OrdersModel, OrdersPanel, TradesModel


def test_orders_model_columns_and_load() -> None:
    model = OrdersModel()
    model.load(
        [
            {
                "order_id": 1,
                "time": "09:30:00",
                "code": "600000.SH",
                "name": "浦发银行",
                "order_type": "买入",
                "volume": 100,
                "price": 8.5,
                "traded_volume": 100,
                "status": "已成",
                "remark": "",
            }
        ]
    )
    assert model.columnCount() == 10
    assert model.rowCount() == 1
    assert model.data(model.index(0, 2)) == "600000.SH"


def test_trades_model_columns() -> None:
    model = TradesModel()
    assert model.columnCount() == 9


def test_orders_panel_refresh_and_emit(qapp) -> None:
    panel = OrdersPanel()
    panel._on_refresh()
    assert panel._orders_model.rowCount() > 0
    assert panel._trades_model.rowCount() > 0
    emitted: list[str] = []
    panel.symbol_clicked.connect(lambda s: emitted.append(s))
    idx = panel._orders_model.index(0, 0)
    panel._on_order_double_clicked(idx)
    assert emitted and emitted[0].endswith((".SH", ".SZ"))


def test_orders_panel_update_orders() -> None:
    panel = OrdersPanel()
    orders = [
        {
            "order_id": 2,
            "time": "10:00:00",
            "code": "000001.SZ",
            "name": "平安银行",
            "order_type": "卖出",
            "volume": 200,
            "price": 10.5,
            "traded_volume": 0,
            "status": "已撤",
            "remark": "test",
        }
    ]
    trades = [
        {
            "time": "10:01:00",
            "code": "000001.SZ",
            "name": "平安银行",
            "order_type": "卖出",
            "volume": 100,
            "price": 10.4,
            "amount": 1040.0,
            "order_id": 2,
            "remark": "",
        }
    ]
    panel.update_orders(orders, trades)
    assert panel._orders_model.rowCount() == 1
    assert panel._trades_model.rowCount() == 1
    assert "委托: 1" in panel._order_count_label.text()

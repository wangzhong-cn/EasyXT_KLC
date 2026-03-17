import os

import pytest

if os.environ.get("EASYXT_ENABLE_GUI_TESTS", "0") not in ("1", "true", "True"):
    pytest.skip("GUI tests disabled", allow_module_level=True)

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt5.QtWidgets import QApplication

from easy_xt.realtime_data.persistence.duckdb_sink import RealtimeDuckDBSink
from gui_app.widgets.orderbook_panel import OrderbookPanel


def _ensure_app():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_orderbook_panel_replay_snapshot(tmp_path):
    _ensure_app()
    db_path = tmp_path / "test_replay.ddb"
    sink = RealtimeDuckDBSink(duckdb_path=str(db_path))
    sink.write_quotes(
        [
            {
                "symbol": "000001.SZ",
                "source": "tdx",
                "timestamp": 1710000000,
                "price": 10.5,
                "volume": 1000,
                "amount": 10500.0,
                "bid1": 10.4,
                "ask1": 10.6,
                "bid1_vol": 500,
                "ask1_vol": 400,
            }
        ]
    )
    snapshot = sink.query_latest_orderbook("000001.SZ", source="tdx")
    panel = OrderbookPanel()
    panel.update_orderbook(snapshot)
    assert panel.rows[("bid", 1)]["price"].text() == "10.40"
    assert panel.rows[("ask", 1)]["price"].text() == "10.60"

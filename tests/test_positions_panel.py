from __future__ import annotations

from gui_app.widgets.positions.positions_panel import (
    PositionsModel,
    PositionsPanel,
    SettlementModel,
)


def test_positions_model_columns_and_load() -> None:
    model = PositionsModel()
    model.load(
        [
            {
                "code": "600000.SH",
                "name": "浦发银行",
                "volume": 100,
                "can_use_volume": 80,
                "cost_price": 8.5,
                "current_price": 9.0,
                "market_value": 900.0,
                "pnl": 50.0,
                "pnl_pct": 5.88,
                "today_pnl": 12.0,
                "account_id": "demo",
            }
        ]
    )
    assert model.columnCount() == 11
    assert model.rowCount() == 1
    assert model.data(model.index(0, 0)) == "600000.SH"
    assert model.data(model.index(0, 8)) == "+5.88%"


def test_settlement_model_columns() -> None:
    model = SettlementModel()
    assert model.columnCount() == 9


def test_positions_panel_refresh_and_emit(qapp) -> None:
    panel = PositionsPanel()
    panel._on_refresh()
    # 演示数据已移除，_on_refresh 后持仓列表为空（等待 QMT 真实数据注入）
    assert panel._pos_model.rowCount() == 0

    # 通过 update_positions 注入测试数据后，验证双击信号
    panel.update_positions(
        [
            {
                "code": "600519.SH",
                "name": "贵州茅台",
                "volume": 10,
                "can_use_volume": 10,
                "cost_price": 1700.0,
                "current_price": 1750.0,
                "market_value": 17500.0,
                "pnl": 500.0,
                "pnl_pct": 2.94,
                "today_pnl": 100.0,
                "account_id": "demo",
            }
        ],
        "demo",
    )
    assert panel._pos_model.rowCount() > 0
    emitted: list[str] = []
    panel.symbol_clicked.connect(lambda s: emitted.append(s))
    idx = panel._pos_model.index(0, 0)
    panel._on_row_double_clicked(idx)
    assert emitted and emitted[0].endswith((".SH", ".SZ"))


def test_positions_panel_update_positions() -> None:
    panel = PositionsPanel()
    rows = [
        {
            "code": "000001.SZ",
            "name": "平安银行",
            "volume": 200,
            "can_use_volume": 200,
            "cost_price": 10.0,
            "current_price": 10.5,
            "market_value": 2100.0,
            "pnl": 100.0,
            "pnl_pct": 5.0,
            "today_pnl": 20.0,
            "account_id": "demo",
        }
    ]
    panel.update_positions(rows, "demo")
    assert panel._pos_model.rowCount() == 1
    assert "持仓: 1" in panel._count_label.text()

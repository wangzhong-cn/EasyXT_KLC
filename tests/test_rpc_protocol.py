from __future__ import annotations

import pandas as pd

from gui_app.widgets.chart import rpc_protocol as rpc


def test_build_set_data_filters_columns() -> None:
    df = pd.DataFrame(
        [
            {
                "time": 1,
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 100,
                "extra": "x",
            }
        ]
    )
    payload = rpc.build_set_data(df)
    assert payload["fitContent"] is True
    assert payload["bars"][0]["time"] == 1
    assert "extra" not in payload["bars"][0]


def test_build_update_bar_filters_columns() -> None:
    row = pd.Series({"time": 2, "close": 20.1, "volume": 200, "other": 999})
    payload = rpc.build_update_bar(row)
    assert payload["bar"] == {"time": 2.0, "close": 20.1, "volume": 200.0}


def test_build_add_drawing_hline_includes_fields() -> None:
    payload = rpc.build_add_drawing("hline", price=12.3, title="S1", axis_label=False)
    assert payload["type"] == "hline"
    assert payload["price"] == 12.3
    assert payload["title"] == "S1"
    assert payload["axisLabel"] is False
    assert isinstance(payload["id"], str) and len(payload["id"]) > 8


# ── Sprint 4: 扩展画线类型测试 ──────────────────────────────────────────────


def test_drawing_types_frozenset() -> None:
    assert isinstance(rpc.DRAWING_TYPES, frozenset)
    assert len(rpc.DRAWING_TYPES) == 15
    assert "hline" in rpc.DRAWING_TYPES
    assert "fibonacci" in rpc.DRAWING_TYPES
    assert "priceChannel" in rpc.DRAWING_TYPES


def test_build_add_drawing_fibonacci_two_points() -> None:
    payload = rpc.build_add_drawing(
        "fibonacci", time1=100, price1=10.0, time2=200, price2=20.0
    )
    assert payload["type"] == "fibonacci"
    assert payload["time1"] == 100
    assert payload["price1"] == 10.0
    assert payload["time2"] == 200
    assert payload["price2"] == 20.0


def test_build_add_drawing_price_channel_three_points() -> None:
    payload = rpc.build_add_drawing(
        "priceChannel", time=100, price=10.0, time2=200, price2=20.0,
        time3=150, price3=15.0,
    )
    assert payload["type"] == "priceChannel"
    assert payload["time3"] == 150
    assert payload["price3"] == 15.0


def test_build_add_drawing_annotation_with_text() -> None:
    payload = rpc.build_add_drawing("annotation", time=100, price=50.0, text="买入")
    assert payload["type"] == "annotation"
    assert payload["text"] == "买入"
    assert payload["time"] == 100
    assert payload["price"] == 50.0


def test_build_add_drawing_generic_points() -> None:
    pts = [{"timestamp": 100, "value": 10.0}, {"timestamp": 200, "value": 20.0}]
    payload = rpc.build_add_drawing("straightLine", points=pts)
    assert payload["type"] == "straightLine"
    assert payload["points"] == pts


def test_build_add_drawing_unknown_type_returns_base() -> None:
    """Unknown types still produce a valid payload (no exception)."""
    payload = rpc.build_add_drawing("nonexistent_type")
    assert payload["type"] == "nonexistent_type"
    assert "id" in payload


def test_set_timezone_constant() -> None:
    assert rpc.M_SET_TIMEZONE == "chart.setTimezone"


def test_set_watermark_constant() -> None:
    assert rpc.M_SET_WATERMARK == "chart.setWatermark"


def test_start_draw_constant() -> None:
    assert rpc.M_START_DRAW == "chart.startDraw"


def test_build_start_draw_payload() -> None:
    payload = rpc.build_start_draw("hline", {"color": "#ff0"})
    assert payload["type"] == "hline"
    assert payload["style"]["color"] == "#ff0"
    assert isinstance(payload["id"], str) and len(payload["id"]) > 8

from __future__ import annotations

from gui_app.widgets.chart.pipeline_guard import validate_pipeline_bar_for_period


def _bar(time_val: str, close: float = 10.0):
    return {
        "time": time_val,
        "open": close - 0.1,
        "high": close + 0.2,
        "low": close - 0.3,
        "close": close,
        "volume": 1000,
    }


def test_intraday_valid_alignment():
    ok, reason = validate_pipeline_bar_for_period(_bar("2024-01-02 09:30:00"), "5m")
    assert ok is True
    assert reason == "ok"


def test_intraday_invalid_alignment_rejected():
    ok, reason = validate_pipeline_bar_for_period(_bar("2024-01-02 09:31:00"), "5m")
    assert ok is False
    assert reason == "intraday_time_not_aligned"


def test_date_period_not_midnight_rejected():
    ok, reason = validate_pipeline_bar_for_period(_bar("2024-01-02 09:30:00"), "1d")
    assert ok is False
    assert reason == "date_period_not_midnight"

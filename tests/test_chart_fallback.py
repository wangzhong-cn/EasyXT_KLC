from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from gui_app.widgets.chart.chart_adapter import LwcPythonChartAdapter


def test_lwc_python_update_zero_division_is_swallowed():
    mock_chart = MagicMock()
    mock_chart.update.side_effect = ZeroDivisionError("float floor division by zero")
    adapter = LwcPythonChartAdapter(mock_chart)
    row = pd.Series({"time": "2024-01-02 09:31:00", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0})
    adapter.update_data(row)
    assert mock_chart.update.called


def test_lwc_python_update_type_error_recovers_with_set():
    mock_chart = MagicMock()
    mock_chart._last_bar = None
    mock_chart.update.side_effect = TypeError("'NoneType' object is not subscriptable")
    adapter = LwcPythonChartAdapter(mock_chart)
    row = pd.Series({"time": "2024-01-02 09:31:00", "open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0})
    adapter.update_data(row)
    assert mock_chart.set.called

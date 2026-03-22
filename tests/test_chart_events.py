"""
gui_app/widgets/chart/chart_events.py 单元测试。

ChartEvents 的依赖（轻量 chart 和 signal_bus）均可以 MagicMock 替代，
无需真实 Qt 环境。重点覆盖：
  - bind_signal_bus (idempotent + exception)
  - set_symbol / set_period (empty guard + exception)
  - _build_topbar (enable/disable + exception)
  - _on_search / _on_period_change / _on_click (全路径 + exception)
"""

from unittest.mock import MagicMock, patch

import pytest

from gui_app.widgets.chart.chart_events import ChartEvents

# ---------------------------------------------------------------------------
# 存根辅助
# ---------------------------------------------------------------------------

def _make_events(enable_topbar: bool = True) -> ChartEvents:
    """返回注入了 MagicMock chart 的 ChartEvents 实例。"""
    chart = MagicMock()
    return ChartEvents(chart, enable_topbar=enable_topbar)


# ===========================================================================
# bind_signal_bus
# ===========================================================================

class TestBindSignalBus:
    def test_bind_sets_bound_flag(self):
        ce = _make_events()
        assert ce._bound is False
        ce.bind_signal_bus()
        assert ce._bound is True

    def test_bind_attaches_both_events(self):
        ce = _make_events()
        # Save refs before += reassigns the attribute via __iadd__.return_value
        original_search = ce.chart.events.search
        original_click = ce.chart.events.click
        ce.bind_signal_bus()
        original_search.__iadd__.assert_called_once_with(ce._on_search)
        original_click.__iadd__.assert_called_once_with(ce._on_click)

    def test_bind_idempotent_second_call_noop(self):
        ce = _make_events()
        original_search = ce.chart.events.search
        ce.bind_signal_bus()
        ce.bind_signal_bus()  # second call should be no-op
        original_search.__iadd__.assert_called_once()

    def test_bind_exception_swallowed(self):
        ce = _make_events()
        ce.chart.events.search.__iadd__ = MagicMock(side_effect=RuntimeError("bind error"))
        ce.bind_signal_bus()  # must not raise
        assert ce._bound is True


# ===========================================================================
# set_symbol
# ===========================================================================

class TestSetSymbol:
    def test_set_symbol_calls_widget_set(self):
        ce = _make_events()
        ce.set_symbol("000001.SZ")
        ce.chart.topbar.get.assert_called_with("symbol")
        ce.chart.topbar.get.return_value.set.assert_called_with("000001.SZ")

    def test_set_symbol_empty_returns_early(self):
        ce = _make_events()
        ce.set_symbol("")
        ce.chart.topbar.get.assert_not_called()

    def test_set_symbol_widget_none_no_set(self):
        ce = _make_events()
        ce.chart.topbar.get.return_value = None
        ce.set_symbol("X")  # widget is None — .set() should not be called, no crash

    def test_set_symbol_exception_swallowed(self):
        ce = _make_events()
        ce.chart.topbar.get.side_effect = RuntimeError("topbar error")
        ce.set_symbol("X")  # must not raise


# ===========================================================================
# set_period
# ===========================================================================

class TestSetPeriod:
    def test_set_period_calls_widget_set(self):
        ce = _make_events()
        ce.set_period("1d")
        ce.chart.topbar.get.assert_called_with("period")
        ce.chart.topbar.get.return_value.set.assert_called_with("1d")

    def test_set_period_empty_returns_early(self):
        ce = _make_events()
        ce.set_period("")
        ce.chart.topbar.get.assert_not_called()

    def test_set_period_widget_none_no_crash(self):
        ce = _make_events()
        ce.chart.topbar.get.return_value = None
        ce.set_period("1d")  # must not raise

    def test_set_period_exception_swallowed(self):
        ce = _make_events()
        ce.chart.topbar.get.side_effect = RuntimeError("topbar error")
        ce.set_period("1d")  # must not raise


# ===========================================================================
# _build_topbar
# ===========================================================================

class TestBuildTopbar:
    def test_topbar_enabled_calls_textbox_and_switcher(self):
        chart = MagicMock()
        ChartEvents(chart, enable_topbar=True)
        chart.topbar.textbox.assert_called_once_with("symbol", "")
        chart.topbar.switcher.assert_called_once()

    def test_topbar_disabled_no_topbar_calls(self):
        chart = MagicMock()
        ChartEvents(chart, enable_topbar=False)
        chart.topbar.textbox.assert_not_called()
        chart.topbar.switcher.assert_not_called()

    def test_topbar_exception_swallowed(self):
        chart = MagicMock()
        chart.topbar.textbox.side_effect = RuntimeError("topbar error")
        ChartEvents(chart, enable_topbar=True)  # must not raise


# ===========================================================================
# _on_search
# ===========================================================================

class TestOnSearch:
    def test_on_search_emits_symbol_selected(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_search(MagicMock(), "600000.SH")
        mock_bus.emit.assert_called_once()
        kwargs = mock_bus.emit.call_args.kwargs
        assert kwargs.get("symbol") == "600000.SH"

    def test_on_search_exception_swallowed(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            mock_bus.emit.side_effect = RuntimeError("emit error")
            ce._on_search(MagicMock(), "X")  # must not raise


# ===========================================================================
# _on_period_change
# ===========================================================================

class TestOnPeriodChange:
    def test_on_period_change_emits_period_changed(self):
        ce = _make_events()
        chart = MagicMock()
        chart.topbar.get.return_value.value = "1d"
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_period_change(chart)
        mock_bus.emit.assert_called_once()
        kwargs = mock_bus.emit.call_args.kwargs
        assert kwargs.get("period") == "1d"

    def test_on_period_change_widget_none_no_emit(self):
        ce = _make_events()
        chart = MagicMock()
        chart.topbar.get.return_value = None
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_period_change(chart)
        mock_bus.emit.assert_not_called()

    def test_on_period_change_empty_period_no_emit(self):
        ce = _make_events()
        chart = MagicMock()
        chart.topbar.get.return_value.value = ""
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_period_change(chart)
        mock_bus.emit.assert_not_called()

    def test_on_period_change_exception_swallowed(self):
        ce = _make_events()
        chart = MagicMock()
        chart.topbar.get.side_effect = RuntimeError("topbar error")
        ce._on_period_change(chart)  # must not raise


# ===========================================================================
# _on_click
# ===========================================================================

class TestOnClick:
    def test_on_click_emits_chart_price_clicked(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_click(MagicMock(), "2024-01-01", 10.5)
        mock_bus.emit.assert_called_once()
        kwargs = mock_bus.emit.call_args.kwargs
        assert kwargs.get("price") == pytest.approx(10.5)

    def test_on_click_none_price_returns_early(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_click(MagicMock(), "2024-01-01", None)
        mock_bus.emit.assert_not_called()

    def test_on_click_exception_swallowed(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            mock_bus.emit.side_effect = RuntimeError("emit error")
            ce._on_click(MagicMock(), "t", 10.0)  # must not raise


# ===========================================================================
# set_symbol / set_period — 缓存行为
# ===========================================================================

class TestSymbolPeriodCache:
    def test_set_symbol_updates_cache(self):
        ce = _make_events()
        assert ce._symbol == ""
        ce.set_symbol("000001.SZ")
        assert ce._symbol == "000001.SZ"

    def test_set_symbol_empty_does_not_update_cache(self):
        ce = _make_events()
        ce._symbol = "old"
        ce.set_symbol("")
        assert ce._symbol == "old"

    def test_set_period_updates_cache(self):
        ce = _make_events()
        assert ce._period == ""
        ce.set_period("5m")
        assert ce._period == "5m"

    def test_set_period_empty_does_not_update_cache(self):
        ce = _make_events()
        ce._period = "1d"
        ce.set_period("")
        assert ce._period == "1d"


# ===========================================================================
# _on_crosshair_move
# ===========================================================================

class TestOnCrosshairMove:
    def test_crosshair_emits_with_symbol_and_period(self):
        ce = _make_events()
        ce._symbol = "000001.SZ"
        ce._period = "1m"
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_crosshair_move(MagicMock(), "2024-01-01 09:31", 11.5)
        mock_bus.emit.assert_called_once()
        kwargs = mock_bus.emit.call_args.kwargs
        assert kwargs["price"] == pytest.approx(11.5)
        assert kwargs["symbol"] == "000001.SZ"
        assert kwargs["period"] == "1m"

    def test_crosshair_emits_empty_symbol_when_not_set(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            ce._on_crosshair_move(MagicMock(), None, None)
        kwargs = mock_bus.emit.call_args.kwargs
        assert kwargs["symbol"] == ""
        assert kwargs["period"] == ""

    def test_crosshair_exception_swallowed(self):
        ce = _make_events()
        with patch("gui_app.widgets.chart.chart_events.signal_bus") as mock_bus:
            mock_bus.emit.side_effect = RuntimeError("bus error")
            ce._on_crosshair_move(MagicMock(), "t", 1.0)  # must not raise

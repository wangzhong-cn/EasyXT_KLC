"""
新策略 BaseStrategy 子类的单元测试

覆盖 S1/S2 中创建的 5 个原生策略和策略工厂：
  - DualMovingAverageStrategy
  - FixedGridStrategy
  - ConditionalStopStrategy
  - RSIReversionStrategy
  - MomentumFactorStrategy
  - create_strategy_from_config (策略工厂)
"""

from __future__ import annotations

import types
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from strategies.base_strategy import BarData, StrategyContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(params: dict | None = None, *, with_executor: bool = True) -> StrategyContext:
    ctx = StrategyContext(
        strategy_id="test",
        account_id="test_account",
        params=params or {},
    )
    if with_executor:
        executor = MagicMock()
        resp = types.SimpleNamespace(order_id="ORD001", status="submitted", msg="")
        resp.__bool__ = lambda self: True  # noqa: ARG005 – truthy
        executor.submit_order.return_value = resp
        ctx.executor = executor
    return ctx


def _make_bar(code: str, close: float, *, time: str = "2024-01-01") -> BarData:
    return BarData(
        code=code, period="1d",
        open=close, high=close + 0.5, low=close - 0.5, close=close,
        volume=10000, time=time,
    )


# ===========================================================================
# DualMovingAverageStrategy
# ===========================================================================

class TestDualMovingAverageStrategy:
    def _cls(self):
        from strategies.trend_following.dual_ma_strategy import DualMovingAverageStrategy
        return DualMovingAverageStrategy

    def test_instantiation_and_init(self):
        s = self._cls()("dm")
        ctx = _make_context({"stock_code": "600519.SH", "short_period": 3, "long_period": 5})
        s.on_init(ctx)
        assert s._stock_code == "600519.SH"
        assert s._short_period == 3
        assert s._long_period == 5

    def test_no_signal_before_window_filled(self):
        s = self._cls()("dm")
        ctx = _make_context({"stock_code": "X", "short_period": 2, "long_period": 3, "trade_volume": 100})
        s.on_init(ctx)
        # 只喂 2 根 bar，long_period=3 还不够
        for p in [10.0, 11.0]:
            s.on_bar(ctx, _make_bar("X", p))
        ctx.executor.submit_order.assert_not_called()

    def test_golden_cross_triggers_buy(self):
        s = self._cls()("dm")
        ctx = _make_context({"stock_code": "X", "short_period": 2, "long_period": 3, "trade_volume": 100})
        s.on_init(ctx)
        # 构造 golden cross: 先下降再快速上升使短均线上穿长均线
        for p in [10, 9, 8, 7, 8, 10, 14]:
            s.on_bar(ctx, _make_bar("X", p))
        # 应有买入调用
        calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "buy"]
        assert len(calls) >= 1

    def test_ignores_other_stock(self):
        s = self._cls()("dm")
        ctx = _make_context({"stock_code": "AAA", "short_period": 2, "long_period": 3})
        s.on_init(ctx)
        s.on_bar(ctx, _make_bar("BBB", 10.0))
        ctx.executor.submit_order.assert_not_called()


# ===========================================================================
# FixedGridStrategy
# ===========================================================================

class TestFixedGridStrategy:
    def _cls(self):
        from strategies.grid_trading.fixed_grid_strategy import FixedGridStrategy
        return FixedGridStrategy

    def test_init_sets_grid_levels(self):
        s = self._cls()("fg")
        ctx = _make_context({"stock_code": "X", "base_price": 10.0, "grid_spacing": 0.02, "grid_count": 6})
        s.on_init(ctx)
        # grid_count=6, half=3, levels: -3,-2,-1,+1,+2,+3 → 6 grids
        assert len(s._grids) == 6

    def test_grid_trigger_buy_when_price_drops(self):
        s = self._cls()("fg")
        ctx = _make_context({
            "stock_code": "X", "base_price": 10.0,
            "grid_spacing": 0.10, "grid_count": 6, "grid_quantity": 100,
        })
        s.on_init(ctx)
        # grid_spacing=10%, buy grids at 9.0, 8.0, 7.0 → price 9.0 triggers L-1
        s.on_bar(ctx, _make_bar("X", 9.0))
        buy_calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "buy"]
        assert len(buy_calls) >= 1

    def test_grid_trigger_sell_when_price_rises(self):
        s = self._cls()("fg")
        ctx = _make_context({
            "stock_code": "X", "base_price": 10.0,
            "grid_spacing": 0.10, "grid_count": 6, "grid_quantity": 100,
            "current_position": 1000,  # 需要有持仓才能卖出
        })
        s.on_init(ctx)
        # 先手动设置持仓
        s._current_position = 1000
        # sell grid at 11.0 → price 11.0 triggers
        s.on_bar(ctx, _make_bar("X", 11.0))
        sell_calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "sell"]
        assert len(sell_calls) >= 1


# ===========================================================================
# ConditionalStopStrategy
# ===========================================================================

class TestConditionalStopStrategy:
    def _cls(self):
        from strategies.conditional_orders.conditional_stop_strategy import ConditionalStopStrategy
        return ConditionalStopStrategy

    def test_price_condition_sell(self):
        """price condition + sell: 价格 >= trigger 时触发"""
        s = self._cls()("cs")
        ctx = _make_context({
            "stock_code": "X", "condition_type": "price",
            "trigger_price": 10.0, "trade_direction": "sell", "trade_volume": 100,
        })
        s.on_init(ctx)
        # below trigger → no fire
        s.on_bar(ctx, _make_bar("X", 9.5))
        ctx.executor.submit_order.assert_not_called()
        # at/above trigger → fire
        s.on_bar(ctx, _make_bar("X", 10.5))
        assert ctx.executor.submit_order.call_count == 1

    def test_only_fires_once(self):
        s = self._cls()("cs")
        ctx = _make_context({
            "stock_code": "X", "condition_type": "price",
            "trigger_price": 10.0, "trade_direction": "sell", "trade_volume": 100,
        })
        s.on_init(ctx)
        s.on_bar(ctx, _make_bar("X", 10.5))
        s.on_bar(ctx, _make_bar("X", 11.0))
        assert ctx.executor.submit_order.call_count == 1

    def test_chinese_direction_normalized(self):
        s = self._cls()("cs")
        ctx = _make_context({
            "stock_code": "X", "condition_type": "price",
            "trigger_price": 10.0, "trade_direction": "买入", "trade_volume": 100,
        })
        s.on_init(ctx)
        assert s._direction == "buy"


# ===========================================================================
# RSIReversionStrategy
# ===========================================================================

class TestRSIReversionStrategy:
    def _cls(self):
        from strategies.trend_following.rsi_reversion_strategy import RSIReversionStrategy
        return RSIReversionStrategy

    def test_no_signal_before_period(self):
        s = self._cls()("rsi")
        ctx = _make_context({"stock_code": "X", "rsi_period": 5})
        s.on_init(ctx)
        for i in range(5):
            s.on_bar(ctx, _make_bar("X", 10.0 + i))
        ctx.executor.submit_order.assert_not_called()

    def test_oversold_triggers_buy(self):
        """持续下跌应使 RSI < 30 → 触发买入"""
        s = self._cls()("rsi")
        ctx = _make_context({"stock_code": "X", "rsi_period": 5, "rsi_lower": 30, "trade_volume": 100})
        s.on_init(ctx)
        # 持续大幅下跌让 RSI 极低
        prices = [100, 95, 90, 85, 80, 75, 70]
        for p in prices:
            s.on_bar(ctx, _make_bar("X", p))
        buy_calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "buy"]
        assert len(buy_calls) >= 1

    def test_compute_rsi_all_gains(self):
        """纯涨 → RSI=100"""
        s = self._cls()("rsi")
        s._rsi_period = 3
        from collections import deque
        s._closes = deque([10, 11, 12, 13])
        assert s._compute_rsi() == 100.0

    def test_compute_rsi_all_losses(self):
        """纯跌 → RSI=0"""
        s = self._cls()("rsi")
        s._rsi_period = 3
        from collections import deque
        s._closes = deque([13, 12, 11, 10])
        assert s._compute_rsi() == 0.0


# ===========================================================================
# MomentumFactorStrategy
# ===========================================================================

class TestMomentumFactorStrategy:
    def _cls(self):
        from strategies.trend_following.momentum_factor_strategy import MomentumFactorStrategy
        return MomentumFactorStrategy

    def test_no_signal_before_lookback(self):
        s = self._cls()("mom")
        ctx = _make_context({"stock_code": "X", "momentum_lookback": 5})
        s.on_init(ctx)
        for i in range(5):
            s.on_bar(ctx, _make_bar("X", 10.0 + i))
        ctx.executor.submit_order.assert_not_called()

    def test_positive_momentum_triggers_buy(self):
        s = self._cls()("mom")
        ctx = _make_context({"stock_code": "X", "momentum_lookback": 3, "trade_volume": 100})
        s.on_init(ctx)
        prices = [10, 10, 10, 13]  # +30% momentum
        for p in prices:
            s.on_bar(ctx, _make_bar("X", p))
        buy_calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "buy"]
        assert len(buy_calls) == 1

    def test_negative_momentum_triggers_sell_when_has_position(self):
        s = self._cls()("mom")
        ctx = _make_context({"stock_code": "X", "momentum_lookback": 3, "trade_volume": 100})
        s.on_init(ctx)
        # first buy (positive momentum)
        for p in [10, 10, 10, 13]:
            s.on_bar(ctx, _make_bar("X", p))
        assert s._has_position is True
        # then negative momentum
        for p in [12, 11, 9]:
            s.on_bar(ctx, _make_bar("X", p))
        sell_calls = [c for c in ctx.executor.submit_order.call_args_list if c.kwargs.get("direction") == "sell"]
        assert len(sell_calls) >= 1


# ===========================================================================
# Strategy Factory
# ===========================================================================

class TestStrategyFactory:
    def test_all_5_types_resolve(self):
        from strategies.strategy_factory import create_strategy_from_config
        for stype in ["trend", "reversion", "grid", "conditional", "factor"]:
            cfg = types.SimpleNamespace(strategy_type=stype, strategy_id=f"test_{stype}")
            s = create_strategy_from_config(cfg)
            assert hasattr(s, "on_init")
            assert hasattr(s, "on_bar")

    def test_unknown_type_raises(self):
        from strategies.strategy_factory import create_strategy_from_config
        cfg = types.SimpleNamespace(strategy_type="unknown_xyz", strategy_id="bad")
        with pytest.raises(ValueError, match="未知策略类型"):
            create_strategy_from_config(cfg)

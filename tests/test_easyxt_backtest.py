"""
easyxt_backtest 包 — 单元测试

测试范围：
  1. BacktestConfig 默认值
  2. _Position buy/sell/market_value 逻辑
  3. _Executor submit_order（最小交易单位约束）
  4. performance 模块各指标函数
  5. StrategyRunner 使用模拟策略（无 DuckDB 依赖）
"""

from __future__ import annotations

import sys
from pathlib import Path

# 确保能导入项目包
_ROOT = str(Path(__file__).resolve().parents[1])
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd
import pytest

from easyxt_backtest.engine import BacktestConfig, BacktestEngine, BacktestResult, _Executor, _Position
from easyxt_backtest.performance import (
    calc_all_metrics,
    calc_calmar,
    calc_cagr,
    calc_max_drawdown,
    calc_monthly_returns,
    calc_sharpe,
    calc_win_rate,
)


# ---------------------------------------------------------------------------
# 辅助工厂
# ---------------------------------------------------------------------------


def _make_equity(values: list[float], start: str = "2023-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="B")
    return pd.Series(values, index=idx, dtype=float, name="equity")


def _make_trades(*pairs: tuple[str, str, float, float]) -> pd.DataFrame:
    """pairs = (code, direction, volume, price)"""
    return pd.DataFrame(
        [{"code": c, "direction": d, "volume": v, "price": p} for c, d, v, p in pairs]
    )


# ---------------------------------------------------------------------------
# BacktestConfig
# ---------------------------------------------------------------------------


class TestBacktestConfig:
    def test_defaults(self):
        cfg = BacktestConfig()
        assert cfg.initial_capital == 1_000_000.0
        assert cfg.commission_rate == 0.0003
        assert cfg.stamp_duty == 0.001
        assert cfg.slippage_pct == 0.0002
        assert cfg.min_trade_unit == 100
        assert cfg.fill_on == "next_open"
        assert cfg.allow_short is False

    def test_custom(self):
        cfg = BacktestConfig(initial_capital=500_000, commission_rate=0.0)
        assert cfg.initial_capital == 500_000
        assert cfg.commission_rate == 0.0


# ---------------------------------------------------------------------------
# _Position
# ---------------------------------------------------------------------------


class TestPosition:
    def test_buy(self):
        pos = _Position("000001.SZ")
        pos.buy(100, 10.0)
        assert pos.quantity == 100
        assert pos.avg_cost == pytest.approx(10.0)

    def test_buy_accumulate(self):
        pos = _Position("000001.SZ")
        pos.buy(100, 10.0)
        pos.buy(100, 12.0)
        assert pos.quantity == 200
        assert pos.avg_cost == pytest.approx(11.0)

    def test_sell_partial(self):
        pos = _Position("000001.SZ")
        pos.buy(200, 10.0)
        pos.sell(100)
        assert pos.quantity == 100
        assert pos.avg_cost == pytest.approx(10.0)

    def test_sell_all(self):
        pos = _Position("000001.SZ")
        pos.buy(100, 10.0)
        pos.sell(100)
        assert pos.quantity == 0
        assert pos.avg_cost == 0.0

    def test_market_value(self):
        pos = _Position("000001.SZ")
        pos.buy(100, 10.0)
        assert pos.market_value(15.0) == pytest.approx(1500.0)

    def test_zero_position_market_value(self):
        pos = _Position("600000.SH")
        assert pos.market_value(20.0) == 0.0

    def test_buy_zero_volume_noop(self):
        pos = _Position("000001.SZ")
        pos.buy(0, 10.0)   # should not raise
        assert pos.quantity == 0


# ---------------------------------------------------------------------------
# _Executor
# ---------------------------------------------------------------------------


class TestExecutor:
    def test_submit_order_rounds_to_min_unit(self):
        cfg = BacktestConfig(min_trade_unit=100)
        ex = _Executor(cfg)
        oid = ex.submit_order("000001.SZ", 150, 10.0, "buy")
        assert oid != ""
        assert ex._submitted_orders[0]["volume"] == 100  # 150 → 100

    def test_submit_zero_volume_returns_empty(self):
        cfg = BacktestConfig(min_trade_unit=100)
        ex = _Executor(cfg)
        oid = ex.submit_order("000001.SZ", 50, 10.0, "buy")
        assert oid == ""
        assert len(ex._submitted_orders) == 0

    def test_submit_order_fields(self):
        cfg = BacktestConfig(min_trade_unit=100)
        ex = _Executor(cfg)
        oid = ex.submit_order("600000.SH", 300, 8.5, "sell", signal_id="sig-1")
        order = ex._submitted_orders[0]
        assert order["code"] == "600000.SH"
        assert order["direction"] == "sell"
        assert order["volume"] == 300
        assert order["price"] == pytest.approx(8.5)
        assert order["signal_id"] == "sig-1"
        assert order["status"] == "submitted"
        assert order["order_id"] == oid

    def test_multiple_orders(self):
        cfg = BacktestConfig(min_trade_unit=100)
        ex = _Executor(cfg)
        ex.submit_order("A", 200, 5.0, "buy")
        ex.submit_order("B", 100, 3.0, "sell")
        ex.submit_order("C", 50, 1.0, "buy")   # filtered
        assert len(ex._submitted_orders) == 2


# ---------------------------------------------------------------------------
# performance.calc_sharpe
# ---------------------------------------------------------------------------


class TestCalcSharpe:
    def test_zero_std(self):
        ret = pd.Series([0.0] * 100)
        assert calc_sharpe(ret) == 0.0

    def test_positive_returns(self):
        np.random.seed(1)
        ret = pd.Series(np.random.normal(0.001, 0.01, 500))
        sharpe = calc_sharpe(ret)
        assert sharpe > 0

    def test_negative_returns(self):
        np.random.seed(1)
        ret = pd.Series(np.random.normal(-0.001, 0.01, 500))
        sharpe = calc_sharpe(ret)
        assert sharpe < 0

    def test_short_series(self):
        ret = pd.Series([0.01])
        assert calc_sharpe(ret) == 0.0  # < 2 elements


# ---------------------------------------------------------------------------
# performance.calc_max_drawdown
# ---------------------------------------------------------------------------


class TestCalcMaxDrawdown:
    def test_monotone_up(self):
        eq = _make_equity([100, 110, 120, 130])
        assert calc_max_drawdown(eq) == pytest.approx(0.0)

    def test_known_drawdown(self):
        # peak=120, trough=90 → mdd = (90-120)/120 = -0.25
        eq = _make_equity([100, 110, 120, 90, 95])
        assert calc_max_drawdown(eq) == pytest.approx(0.25, rel=1e-3)

    def test_empty(self):
        assert calc_max_drawdown(pd.Series(dtype=float)) == 0.0


# ---------------------------------------------------------------------------
# performance.calc_cagr
# ---------------------------------------------------------------------------


class TestCalcCagr:
    def test_doubling(self):
        # 252 bars of steady growth doubling total value → CAGR = 100%
        eq = _make_equity([100.0 * (2 ** (i / 252)) for i in range(253)])
        cagr = calc_cagr(eq)
        assert cagr == pytest.approx(1.0, rel=0.01)

    def test_negative_return(self):
        eq = _make_equity([100, 50])
        cagr = calc_cagr(eq)
        assert cagr < 0


# ---------------------------------------------------------------------------
# performance.calc_calmar
# ---------------------------------------------------------------------------


class TestCalcCalmar:
    def test_zero_drawdown(self):
        eq = _make_equity([100, 110, 120])
        # mdd=0 → calmar=0 (guard against division by zero)
        assert calc_calmar(eq) == 0.0

    def test_positive(self):
        eq = _make_equity([100, 120, 100, 140])
        calmar = calc_calmar(eq)
        # cagr>0, mdd=16.67% → calmar>0
        assert calmar > 0


# ---------------------------------------------------------------------------
# performance.calc_win_rate
# ---------------------------------------------------------------------------


class TestCalcWinRate:
    def test_all_winners(self):
        trades = _make_trades(
            ("A", "buy", 100, 10.0),
            ("A", "sell", 100, 12.0),
        )
        assert calc_win_rate(trades) == pytest.approx(1.0)

    def test_all_losers(self):
        trades = _make_trades(
            ("A", "buy", 100, 15.0),
            ("A", "sell", 100, 10.0),
        )
        assert calc_win_rate(trades) == pytest.approx(0.0)

    def test_half_half(self):
        trades = _make_trades(
            ("A", "buy", 100, 10.0),
            ("A", "sell", 100, 12.0),
            ("B", "buy", 100, 20.0),
            ("B", "sell", 100, 18.0),
        )
        assert calc_win_rate(trades) == pytest.approx(0.5)

    def test_empty(self):
        assert calc_win_rate(pd.DataFrame()) == 0.0


# ---------------------------------------------------------------------------
# performance.calc_monthly_returns
# ---------------------------------------------------------------------------


class TestCalcMonthlyReturns:
    def test_returns_index_format(self):
        eq = _make_equity([100 + i for i in range(60)])
        mr = calc_monthly_returns(eq)
        assert not mr.empty
        for idx in mr.index:
            assert len(idx) == 7  # "YYYY-MM"

    def test_empty(self):
        mr = calc_monthly_returns(pd.Series(dtype=float))
        assert mr.empty


# ---------------------------------------------------------------------------
# performance.calc_all_metrics
# ---------------------------------------------------------------------------


class TestCalcAllMetrics:
    def test_basic_keys(self):
        eq = _make_equity([1_000_000 * (1.0005 ** i) for i in range(252)])
        trades = _make_trades(
            ("000001.SZ", "buy", 100, 10.0),
            ("000001.SZ", "sell", 100, 12.0),
        )
        m = calc_all_metrics(eq, trades, 1_000_000)
        for key in ("sharpe", "calmar", "max_drawdown", "cagr", "total_return",
                    "win_rate", "trade_count", "monthly_returns", "start_equity", "end_equity"):
            assert key in m

    def test_total_return_positive(self):
        eq = _make_equity([1_000_000 * (1.001 ** i) for i in range(252)])
        m = calc_all_metrics(eq, pd.DataFrame(), 1_000_000)
        assert m["total_return"] > 0

    def test_empty_equity(self):
        m = calc_all_metrics(pd.Series(dtype=float), pd.DataFrame(), 1_000_000)
        assert m["sharpe"] == 0.0
        assert m["trade_count"] == 0


# ---------------------------------------------------------------------------
# StrategyRunner — mock strategy, no DuckDB
# ---------------------------------------------------------------------------


class TestStrategyRunnerMock:
    """验证 StrategyRunner 使用 mock 数据能正常跑通生命周期。"""

    def _make_mock_engine(self, data: dict[str, pd.DataFrame]) -> BacktestEngine:
        """返回一个 data 已预加载的 BacktestEngine，跳过 DuckDB IO。"""
        engine = BacktestEngine(config=BacktestConfig())
        # Monkey-patch _load_data to return pre-baked dict
        engine._load_data = lambda *a, **kw: data
        return engine

    def test_simple_buy_hold_strategy(self):
        from strategies.base_strategy import BarData, BaseStrategy, OrderData, StrategyContext

        class BuyHoldStrategy(BaseStrategy):
            def __init__(self):
                self.strategy_id = "buy_hold_test"
                self.bought = False

            def on_init(self, ctx: StrategyContext) -> None:
                pass

            def on_bar(self, ctx: StrategyContext, bar: BarData) -> None:
                if not self.bought and bar.close > 0:
                    ctx.executor.submit_order(bar.code, 1000, bar.close, "buy")
                    self.bought = True

        # Synthetic daily price data
        dates = pd.date_range("2023-01-03", periods=30, freq="B")
        prices = [10.0 + i * 0.1 for i in range(30)]
        df = pd.DataFrame(
            {"open": prices, "high": prices, "low": prices, "close": prices, "volume": [1e6] * 30},
            index=dates,
        )

        strat = BuyHoldStrategy()
        engine = self._make_mock_engine({"000001.SZ": df})
        result = engine.run(strat, ["000001.SZ"], "2023-01-03", "2023-02-15")

        assert isinstance(result, BacktestResult)
        assert result.strategy_id == "buy_hold_test"
        assert result.initial_capital == 1_000_000.0
        assert result.final_equity > 0
        assert not result.equity_curve.empty
        assert "sharpe" in result.metrics

    def test_no_data_raises(self):
        from strategies.base_strategy import BarData, BaseStrategy, StrategyContext

        class DummyStrategy(BaseStrategy):
            def __init__(self):
                self.strategy_id = "dummy"

            def on_init(self, ctx):
                pass

            def on_bar(self, ctx, bar):
                pass

        engine = BacktestEngine()
        engine._load_data = lambda *a, **kw: {}
        with pytest.raises(ValueError):
            engine.run(DummyStrategy(), ["X"], "2023-01-01", "2023-12-31")

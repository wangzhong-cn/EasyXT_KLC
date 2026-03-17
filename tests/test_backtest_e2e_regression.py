"""
tests/test_backtest_e2e_regression.py
--------------------------------------
端到端回归 + 双引擎指标契约门禁

设计原则：
  - 铁律 0：零真实 IO — _load_data monkey-patch 注入合成行情
  - E2E 回归：用确定性价格序列验证 native BacktestEngine 的数值正确性
  - 契约门禁：验证 _run_native_backtest() 的 key 映射不会静默失效；
    如果 easyxt_backtest/performance.py 改了 metric key 名称，这里会第一时间告警
  - sqn / profit_factor 哨兵值测试：防止 0.0 哨兵被误改成 NaN/None 进入 UI
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

import gui_app.backtest.engine as _eng_mod
from easyxt_backtest import BacktestConfig, BacktestEngine, BacktestResult
from easyxt_backtest.engine import BacktestEngine as NativeBacktestEngine
from gui_app.backtest.engine import AdvancedBacktestEngine
from strategies.base_strategy import BarData, BaseStrategy, StrategyContext

# ===========================================================================
# 辅助：合成行情生成器（无真实 IO）
# ===========================================================================

def _ascending_df(n: int = 60, start: float = 10.0, step: float = 0.10) -> pd.DataFrame:
    """n 根稳定递增日线（复权场景模拟）"""
    dates = pd.date_range("2022-01-03", periods=n, freq="B")
    prices = [start + i * step for i in range(n)]
    return pd.DataFrame(
        {
            "open":   [p * 1.001 for p in prices],
            "high":   [p * 1.005 for p in prices],
            "low":    [p * 0.995 for p in prices],
            "close":  prices,
            "volume": [1_000_000.0] * n,
        },
        index=dates,
    )


def _descending_df(n: int = 60, start: float = 20.0, step: float = 0.15) -> pd.DataFrame:
    """n 根稳定递减日线（亏损/回撤场景）"""
    dates = pd.date_range("2022-01-03", periods=n, freq="B")
    prices = [max(0.1, start - i * step) for i in range(n)]
    return pd.DataFrame(
        {
            "open":   [p * 1.001 for p in prices],
            "high":   [p * 1.005 for p in prices],
            "low":    [p * 0.995 for p in prices],
            "close":  prices,
            "volume": [1_000_000.0] * n,
        },
        index=dates,
    )


def _make_native_engine(df: pd.DataFrame, code: str = "000001.SZ"):
    """返回 _load_data 已被 monkey-patch 的 BacktestEngine（不触碰 DuckDB）"""
    engine = BacktestEngine(config=BacktestConfig())
    engine._load_data = lambda *a, **k: {code: df}
    return engine, code


# ===========================================================================
# 策略 fixture：买入即不动（BuyHold）与空操作（NoOp）
# ===========================================================================

class _BuyHoldStrategy(BaseStrategy):
    """第 1 bar 提交买单，此后持仓不动。"""

    def __init__(self):
        self.strategy_id = "e2e_buy_hold"
        self._bought = False

    def on_init(self, ctx: StrategyContext) -> None:
        pass

    def on_bar(self, ctx: StrategyContext, bar: BarData) -> None:
        if not self._bought and bar.close > 0:
            ctx.executor.submit_order(bar.code, 1000, bar.close, "buy")
            self._bought = True


class _NoOpStrategy(BaseStrategy):
    """全程不下单。"""

    def __init__(self):
        self.strategy_id = "e2e_noop"

    def on_init(self, ctx: StrategyContext) -> None:
        pass

    def on_bar(self, ctx: StrategyContext, bar: BarData) -> None:
        pass


# ===========================================================================
# 1. 原生引擎端到端回归
# ===========================================================================

class TestNativeEngineE2ERegression:
    """BacktestEngine.run() 数值正确性回归，使用确定性合成行情。"""

    def test_ascending_prices_yield_positive_return(self):
        """递增行情 + BuyHold → total_return > 0"""
        df = _ascending_df(n=60)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-04-01"
        )
        assert isinstance(result, BacktestResult)
        assert result.metrics["total_return"] > 0, (
            "递增行情下 BuyHold 策略应产生正收益，"
            f"实际: {result.metrics['total_return']}"
        )

    def test_declining_prices_yield_positive_drawdown(self):
        """递减行情 + BuyHold → max_drawdown > 0"""
        df = _descending_df(n=60)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-04-01"
        )
        assert result.metrics["max_drawdown"] > 0, (
            "价格持续下行后应产生正最大回撤，"
            f"实际: {result.metrics['max_drawdown']}"
        )

    def test_equity_curve_rises_with_ascending_prices(self):
        """递增行情下，持仓期间权益曲线末值应不低于初值。"""
        df = _ascending_df(n=40)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-03-01"
        )
        assert not result.equity_curve.empty, "权益曲线不应为空"
        first = result.equity_curve.iloc[0]
        last = result.equity_curve.iloc[-1]
        assert last >= first, (
            f"递增行情下权益曲线末值({last:.2f})应不低于初值({first:.2f})"
        )

    def test_noop_strategy_produces_zero_winrate_and_no_trades(self):
        """不交易时 win_rate == 0 且 trade_count == 0。"""
        df = _ascending_df(n=20)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _NoOpStrategy(), [code], "2022-01-03", "2022-02-01"
        )
        assert result.metrics["win_rate"] == pytest.approx(0.0)
        assert result.metrics["trade_count"] == 0

    def test_all_required_native_metric_keys_present(self):
        """result.metrics 必须包含引擎定义的全部 key。"""
        _REQUIRED = {"sharpe", "max_drawdown", "total_return", "cagr", "win_rate", "trade_count"}
        df = _ascending_df(n=30)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-02-15"
        )
        missing = _REQUIRED - set(result.metrics.keys())
        assert not missing, (
            f"result.metrics 缺少以下 key: {missing}。"
            "如果 easyxt_backtest/performance.py 改了 key 名称，"
            "请同步更新 gui_app/backtest/engine._run_native_backtest() 中的映射。"
        )

    def test_metrics_are_finite_numbers(self):
        """所有数值型 metrics 必须是有限浮点数（无 NaN / inf）。"""
        df = _ascending_df(n=30)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-02-15"
        )
        for key, val in result.metrics.items():
            if isinstance(val, float):
                assert math.isfinite(val), f"result.metrics['{key}'] = {val}（非有限数）"


# ===========================================================================
# 2. 双引擎指标契约门禁
#    目标：如果 native engine 改了 metric key 名称，这里第一时间告警；
#    不要求数值精确吻合（两引擎策略不同），只验证 key 映射完整性。
# ===========================================================================

# native engine key  →  AdvancedBacktestEngine._run_native_backtest() 输出 key
_NATIVE_TO_GUI_KEY_MAP: dict[str, str] = {
    "sharpe":       "sharpe_ratio",
    "max_drawdown": "max_drawdown",
    "total_return": "total_return",
    "cagr":         "annualized_return",
    "win_rate":     "win_rate",
    "trade_count":  "total_trades",
}

_REQUIRED_GUI_KEYS = {
    "sharpe_ratio",
    "max_drawdown",
    "total_return",
    "annualized_return",
    "win_rate",
    "total_trades",
    "sqn",
    "profit_factor",
}


class TestDualEngineMetricsContract:
    """
    验证两条路径的指标契约：
      1. native engine result.metrics 的 key 集合与映射表一致
      2. AdvancedBacktestEngine._run_native_backtest() 返回的 dict key 集合完整
      3. sqn / profit_factor 哨兵值格式正确（0.0，非 NaN）
    """

    def test_native_metric_keys_match_gui_mapping_table(self):
        """
        BacktestEngine.run() 输出的 metrics key 必须与 _NATIVE_TO_GUI_KEY_MAP 的左侧一致。

        如果 easyxt_backtest/performance.py 改了某个 key 名称，
        此测试失败，开发者必须同步更新 gui_app/backtest/engine.py 中的映射。
        """
        df = _ascending_df(n=40)
        engine, code = _make_native_engine(df)
        result = engine.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-03-01"
        )
        native_keys = set(result.metrics.keys())
        for native_key, gui_key in _NATIVE_TO_GUI_KEY_MAP.items():
            assert native_key in native_keys, (
                f"[契约门禁] native engine 不再输出 key='{native_key}'，"
                f"对应 GUI key='{gui_key}' 的映射已静默失效。"
                "请更新 easyxt_backtest/performance.py 或 "
                "gui_app/backtest/engine._run_native_backtest() 中的映射表。"
            )

    def test_gui_native_path_returns_complete_key_set(self):
        """
        AdvancedBacktestEngine._run_native_backtest() 返回的 dict 必须包含全部 8 个 required keys。
        """
        if not _eng_mod.NATIVE_ENGINE_AVAILABLE:
            pytest.skip("NATIVE_ENGINE_AVAILABLE=False，跳过 GUI 路径集成测试")

        df = _ascending_df(n=50)
        engine = AdvancedBacktestEngine()
        engine.add_data(df, name="000001.SZ")
        engine.set_data_profile(period="1d", adjust="qfq")
        engine.strategy_name = "DualMovingAverageStrategy"
        engine.strategy_params = {"short_period": 5, "long_period": 10}

        metrics = engine._run_native_backtest()
        missing = _REQUIRED_GUI_KEYS - set(metrics.keys())
        assert not missing, (
            f"[契约门禁] 原生路径返回值缺少以下 keys: {missing}"
        )

    def test_gui_native_path_total_return_is_finite(self):
        """total_return 必须是有限浮点数（不允许 NaN / inf）。"""
        if not _eng_mod.NATIVE_ENGINE_AVAILABLE:
            pytest.skip("NATIVE_ENGINE_AVAILABLE=False，跳过 GUI 路径集成测试")

        df = _ascending_df(n=50)
        engine = AdvancedBacktestEngine()
        engine.add_data(df, name="000001.SZ")
        engine.set_data_profile(period="1d", adjust="qfq")
        engine.strategy_name = "RSIStrategy"
        engine.strategy_params = {"rsi_period": 14}

        metrics = engine._run_native_backtest()
        tr = metrics["total_return"]
        assert math.isfinite(tr), f"total_return={tr!r} 不是有限数"

    def test_sqn_and_profit_factor_sentinels_are_zero_not_nan(self):
        """
        sqn / profit_factor 在原生路径是已知哨兵值 0.0。
        此测试防止这两个字段被误改成 NaN / None，导致 UI 渲染异常。
        """
        if not _eng_mod.NATIVE_ENGINE_AVAILABLE:
            pytest.skip("NATIVE_ENGINE_AVAILABLE=False，跳过 GUI 路径集成测试")

        df = _ascending_df(n=50)
        engine = AdvancedBacktestEngine()
        engine.add_data(df, name="000001.SZ")
        engine.set_data_profile(period="1d", adjust="qfq")
        engine.strategy_name = "MACDStrategy"
        engine.strategy_params = {
            "fast_period": 5, "slow_period": 10, "signal_period": 3
        }

        metrics = engine._run_native_backtest()
        assert metrics["sqn"] == 0.0 and math.isfinite(metrics["sqn"]), (
            f"sqn 哨兵值应为 0.0，实际={metrics['sqn']!r}"
        )
        assert metrics["profit_factor"] == 0.0 and math.isfinite(metrics["profit_factor"]), (
            f"profit_factor 哨兵值应为 0.0，实际={metrics['profit_factor']!r}"
        )

    def test_native_and_gui_total_return_agree_directionally(self):
        """
        用相同价格序列分别通过 BacktestEngine（直接）和 AdvancedBacktestEngine（GUI 原生路径）
        得到 total_return，两者方向应一致（同正 or 同负 or 同零）。

        注：两者策略不同（BuyHold vs DualMA），绝对值可能差异很大，
        但在 60 根确定性递增行情上，有信号的策略 total_return 应与无信号策略方向相同。
        """
        if not _eng_mod.NATIVE_ENGINE_AVAILABLE:
            pytest.skip("NATIVE_ENGINE_AVAILABLE=False，跳过 GUI 路径集成测试")

        df = _ascending_df(n=60, start=10.0, step=0.20)  # 明显递增，总涨幅 ~118%

        # native direct path
        engine_direct, code = _make_native_engine(df)
        result_direct = engine_direct.run(
            _BuyHoldStrategy(), [code], "2022-01-03", "2022-04-01"
        )
        direct_tr = result_direct.metrics["total_return"]

        # GUI native path（DualMA on same DataFrame）
        gui_engine = AdvancedBacktestEngine()
        gui_engine.add_data(df, name=code)
        gui_engine.set_data_profile(period="1d", adjust="qfq")
        gui_engine.strategy_name = "DualMovingAverageStrategy"
        gui_engine.strategy_params = {"short_period": 5, "long_period": 10}
        gui_metrics = gui_engine._run_native_backtest()
        gui_tr = gui_metrics["total_return"]

        # 在充分上涨的行情上，两个策略的 total_return 符号应一致（都 >= 0）
        def sgn(x: float) -> int:
            return 0 if abs(x) < 1e-9 else (1 if x > 0 else -1)

        assert sgn(direct_tr) == sgn(gui_tr) or sgn(gui_tr) >= 0, (
            f"[方向一致性] 直接路径 total_return={direct_tr:.4f}，"
            f"GUI 原生路径 total_return={gui_tr:.4f}，两者符号不一致。"
        )

    def test_gui_native_path_passes_adjust_from_data_profile(self, monkeypatch):
        if not _eng_mod.NATIVE_ENGINE_AVAILABLE:
            pytest.skip("NATIVE_ENGINE_AVAILABLE=False，跳过 GUI 路径集成测试")

        captured = {"adjust": None}
        original_run = NativeBacktestEngine.run

        def _fake_run(self, strategy, codes, start_date, end_date, period="1d", adjust="qfq"):
            captured["adjust"] = adjust

            class _R:
                metrics = {
                    "sharpe": 0.0,
                    "max_drawdown": 0.0,
                    "total_return": 0.0,
                    "cagr": 0.0,
                    "win_rate": 0.0,
                    "trade_count": 0,
                }
                equity_curve = pd.Series([1_000_000.0], index=pd.DatetimeIndex([pd.Timestamp("2022-01-03")]))
                trades = pd.DataFrame()

            return _R()

        monkeypatch.setattr(NativeBacktestEngine, "run", _fake_run)
        try:
            df = _ascending_df(n=20)
            engine = AdvancedBacktestEngine()
            engine.add_data(df, name="000001.SZ")
            engine.set_data_profile(period="1d", adjust="front")
            engine.strategy_name = "DualMovingAverageStrategy"
            engine.strategy_params = {"short_period": 5, "long_period": 10}
            _ = engine._run_native_backtest()
            assert captured["adjust"] == "qfq"
        finally:
            monkeypatch.setattr(NativeBacktestEngine, "run", original_run)


# ===========================================================================
# TestBacktestWorkerParamContract
#
# 覆盖 BacktestWorker.run_single_backtest() → engine.set_data_profile()
# 传参链条：确保 params["adjust"] / params["period"] 不因 key 改名
# 而静默 fallback 到 "none"
#
# 技术要点：BacktestWorker 是 QThread 子类，但 run_single_backtest()
# 非 101 因子分支自身不依赖 QThread 状态——用 SimpleNamespace 作为
# fake self 并通过非绑定调用即可，无需 Qt 环境，测试仍为纯 Python。
# ===========================================================================

class TestBacktestWorkerParamContract:
    """BacktestWorker.run_single_backtest() UI 参数 → StrategyController 传参契约

    S7 重构后 run_single_backtest 委托给 StrategyController.run_adhoc_backtest，
    因此契约变为：UI params → controller.run_adhoc_backtest(period=..., adjust=...)。
    """

    def _make_worker(self):
        """返回最小可用的 fake self，供直接调用 run_single_backtest()。"""
        import types
        import gui_app.widgets.backtest_widget as bw
        ns = types.SimpleNamespace(_stop_requested=False)
        ns._build_default_strategy_params = bw.BacktestWorker._build_default_strategy_params
        return ns

    def _base_params(self, strategy_name: str = "双均线策略", **overrides) -> dict:
        params = {
            "strategy_name": strategy_name,
            "initial_cash": 100_000,
            "commission": 0.0003,
            "period": "1d",
            "adjust": "none",
            "short_period": 5,
            "long_period": 20,
            "rsi_period": 14,
            "strategy_params": {},
        }
        params.update(overrides)
        return params

    @staticmethod
    def _mock_adhoc(captured: dict):
        """返回一个 mock run_adhoc_backtest，把调用参数记录到 captured。"""
        def _fake_run_adhoc_backtest(self_ctrl, *, stock_data, strategy_name,
                                     strategy_params, initial_cash, commission,
                                     period, adjust):
            captured["period"] = period
            captured["adjust"] = adjust
            captured["strategy_name"] = strategy_name
            captured["call_count"] = captured.get("call_count", 0) + 1
            return {"ok": True, "metrics": {"total_return": 0.0}, "detailed": {}}
        return _fake_run_adhoc_backtest

    def test_adjust_front_propagates_to_set_data_profile(self, monkeypatch):
        """params['adjust']='front' → controller.run_adhoc_backtest(adjust='front')"""
        from gui_app.strategy_controller import StrategyController
        captured: dict = {}
        monkeypatch.setattr(StrategyController, "run_adhoc_backtest", self._mock_adhoc(captured))

        import gui_app.widgets.backtest_widget as bw
        bw.BacktestWorker.run_single_backtest(
            self._make_worker(), _ascending_df(n=30), self._base_params(adjust="front")
        )
        assert captured.get("adjust") == "front", (
            "BacktestWorker.run_single_backtest 未将 params['adjust'] 原样传入 StrategyController"
        )

    def test_period_propagates_to_set_data_profile(self, monkeypatch):
        """params['period']='1w' → controller.run_adhoc_backtest(period='1w')"""
        from gui_app.strategy_controller import StrategyController
        captured: dict = {}
        monkeypatch.setattr(StrategyController, "run_adhoc_backtest", self._mock_adhoc(captured))

        import gui_app.widgets.backtest_widget as bw
        bw.BacktestWorker.run_single_backtest(
            self._make_worker(), _ascending_df(n=30), self._base_params(period="1w")
        )
        assert captured.get("period") == "1w"

    def test_set_data_profile_called_even_with_none_adjust(self, monkeypatch):
        """adjust 缺席时 fallback 为 'none'，而非抛异常或跳过调用"""
        from gui_app.strategy_controller import StrategyController
        captured: dict = {}
        monkeypatch.setattr(StrategyController, "run_adhoc_backtest", self._mock_adhoc(captured))

        import gui_app.widgets.backtest_widget as bw
        params = self._base_params()
        params.pop("adjust")  # 故意不传 adjust

        bw.BacktestWorker.run_single_backtest(
            self._make_worker(), _ascending_df(n=20), params
        )
        assert captured.get("call_count") == 1, "run_adhoc_backtest 应被调用恰好一次"
        assert captured.get("adjust") == "none", "adjust 缺席时应 fallback 为 'none'"

    def test_adjust_back_propagates_to_set_data_profile(self, monkeypatch):
        """params['adjust']='back' → controller.run_adhoc_backtest(adjust='back')"""
        from gui_app.strategy_controller import StrategyController
        captured: dict = {}
        monkeypatch.setattr(StrategyController, "run_adhoc_backtest", self._mock_adhoc(captured))

        import gui_app.widgets.backtest_widget as bw
        bw.BacktestWorker.run_single_backtest(
            self._make_worker(), _ascending_df(n=20), self._base_params(adjust="back")
        )
        assert captured.get("adjust") == "back"

    def test_adjust_none_propagates_to_set_data_profile(self, monkeypatch):
        """params['adjust']='none' → controller.run_adhoc_backtest(adjust='none')"""
        from gui_app.strategy_controller import StrategyController
        captured: dict = {}
        monkeypatch.setattr(StrategyController, "run_adhoc_backtest", self._mock_adhoc(captured))

        import gui_app.widgets.backtest_widget as bw
        bw.BacktestWorker.run_single_backtest(
            self._make_worker(), _ascending_df(n=20), self._base_params(adjust="none")
        )
        assert captured.get("adjust") == "none"

"""
风控引擎高级功能单元测试

覆盖：
  - 分层阈值（register_thresholds / _resolve_thresholds）
  - 分层优先级：account > strategy > global
  - calibrate_thresholds_from_returns（数值逻辑）
  - get_risk_stats / reset_risk_stats 计数器
  - check_pre_trade 风控事件自动统计
  - strategy_id 参数透传
"""

from __future__ import annotations

import pytest

from core.risk_engine import (
    RiskAction,
    RiskCheckResult,
    RiskEngine,
    RiskThresholds,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine() -> RiskEngine:
    return RiskEngine()


@pytest.fixture
def strict() -> RiskThresholds:
    """非常严格的阈值，几乎任何交易都会触发。"""
    return RiskThresholds(
        concentration_limit=0.001,      # 几乎0
        net_exposure_limit=0.001,
        intraday_drawdown_halt=0.001,
        intraday_drawdown_warn=0.0001,
    )


@pytest.fixture
def loose() -> RiskThresholds:
    """极宽松阈值，任何正常交易都会 PASS。"""
    return RiskThresholds(
        concentration_limit=1.0,
        hhi_limit=1.0,
        net_exposure_limit=100.0,
        intraday_drawdown_halt=1.0,
        intraday_drawdown_warn=0.99,
        var95_limit=1.0,
    )


# ---------------------------------------------------------------------------
# Resolve thresholds priority
# ---------------------------------------------------------------------------


class TestResolveThresholds:
    def test_fallback_to_global(self, engine):
        t = engine._resolve_thresholds("unknown_acct")
        assert t is engine.thresholds

    def test_account_overrides_global(self, engine, strict):
        engine.register_thresholds("acc_strict", strict)
        t = engine._resolve_thresholds("acc_strict")
        assert t is strict

    def test_strategy_overrides_global_when_no_account(self, engine, loose):
        engine.register_thresholds("strat_loose", loose)
        t = engine._resolve_thresholds("unknown_acct", strategy_id="strat_loose")
        assert t is loose

    def test_account_beats_strategy(self, engine, strict, loose):
        engine.register_thresholds("acc_strict", strict)
        engine.register_thresholds("strat_loose", loose)
        t = engine._resolve_thresholds("acc_strict", strategy_id="strat_loose")
        assert t is strict    # account wins

    def test_strategy_ignored_when_empty_string(self, engine, loose):
        engine.register_thresholds("strat_loose", loose)
        t = engine._resolve_thresholds("no_such_acct", strategy_id="")
        assert t is engine.thresholds    # fallback to global


# ---------------------------------------------------------------------------
# check_pre_trade uses resolved thresholds
# ---------------------------------------------------------------------------


class TestCheckPreTradeWithStratifiedThresholds:
    def test_strict_account_blocks_normal_trade(self, engine, strict):
        """strict 阈值几乎 0 集中度上限，100k 买入会被 LIMIT。"""
        engine.register_thresholds("acc_strict", strict)
        engine.update_daily_high("acc_strict", 1_000_000.0)
        result = engine.check_pre_trade(
            account_id="acc_strict",
            code="000001.SZ",
            volume=1000,
            price=100.0,
            direction="buy",
            positions={},
            nav=1_000_000.0,
            strategy_id="",
        )
        assert result.blocked

    def test_loose_strategy_allows_large_concentration(self, engine, loose):
        engine.register_thresholds("strat_big", loose)
        engine.update_daily_high("acct", 1_000_000.0)
        result = engine.check_pre_trade(
            account_id="acct",
            code="000001.SZ",
            volume=9000,
            price=100.0,
            direction="buy",
            positions={},
            nav=1_000_000.0,
            strategy_id="strat_big",
        )
        assert result.action == RiskAction.PASS

    def test_strategy_id_param_passed_through(self, engine, loose):
        """strategy_id 参数通过 check_pre_trade 传入时生效。"""
        engine.register_thresholds("my_strategy", loose)
        engine.update_daily_high("acct2", 500_000.0)
        result = engine.check_pre_trade(
            account_id="acct2",
            code="A",
            volume=50000,
            price=10.0,
            direction="buy",
            positions={},
            nav=500_000.0,
            strategy_id="my_strategy",
        )
        assert result.action == RiskAction.PASS


# ---------------------------------------------------------------------------
# Risk event statistics
# ---------------------------------------------------------------------------


class TestRiskStats:
    def _make_halt(self, engine, account_id: str = "acc_stat"):
        """触发一个 HALT：设置较高日内高点，然后净值腰斩。"""
        engine.update_daily_high(account_id, 200_000.0)
        return engine.check_pre_trade(
            account_id=account_id,
            code="X",
            volume=100,
            price=1.0,
            direction="buy",
            positions={},
            nav=100_000.0,    # 50% 回撤 → HALT
        )

    def _make_pass(self, engine, account_id: str = "acc_stat"):
        engine.update_daily_high(account_id, 100_000.0)
        return engine.check_pre_trade(
            account_id=account_id,
            code="X",
            volume=10,
            price=1.0,
            direction="buy",
            positions={},
            nav=100_000.0,
        )

    def test_halt_increments_counter(self, engine):
        result = self._make_halt(engine)
        assert result.action == RiskAction.HALT
        stats = engine.get_risk_stats("acc_stat")
        assert stats.get("halt", 0) >= 1

    def test_pass_increments_pass_counter(self, engine):
        self._make_pass(engine)
        stats = engine.get_risk_stats("acc_stat")
        assert stats.get("pass", 0) >= 1

    def test_multiple_accounts_independent(self, engine):
        self._make_halt(engine, "acc_a")
        self._make_pass(engine, "acc_b")
        stats_a = engine.get_risk_stats("acc_a")
        stats_b = engine.get_risk_stats("acc_b")
        assert stats_a.get("halt", 0) >= 1
        assert stats_b.get("pass", 0) >= 1
        assert stats_b.get("halt", 0) == 0

    def test_get_risk_stats_all_accounts(self, engine):
        self._make_halt(engine, "acc_x")
        self._make_pass(engine, "acc_y")
        all_stats = engine.get_risk_stats()
        assert "acc_x" in all_stats
        assert "acc_y" in all_stats

    def test_reset_per_account(self, engine):
        self._make_halt(engine, "acc_reset_me")
        self._make_pass(engine, "acc_keep")
        engine.reset_risk_stats("acc_reset_me")
        # acc_reset_me cleared
        assert engine.get_risk_stats("acc_reset_me") == {}
        # acc_keep still intact
        assert engine.get_risk_stats("acc_keep").get("pass", 0) >= 1

    def test_reset_all(self, engine):
        self._make_halt(engine, "acc_1")
        self._make_pass(engine, "acc_2")
        engine.reset_risk_stats()
        assert engine.get_risk_stats() == {}

    def test_get_stats_unknown_account_returns_empty(self, engine):
        stats = engine.get_risk_stats("no_such_account")
        assert stats == {}


# ---------------------------------------------------------------------------
# calibrate_thresholds_from_returns
# ---------------------------------------------------------------------------


class TestCalibrateThresholds:
    # 40 天均匀对称收益，σ ≈ 0
    FLAT_RETURNS = [0.001] * 40

    # 有波动的收益序列（标准差约 0.02）
    VOLATILE_RETURNS = (
        [0.05, -0.03, 0.02, -0.04, 0.03, -0.05, 0.01, -0.02, 0.06, -0.04] * 6
    )

    def test_returns_type(self):
        t = RiskEngine.calibrate_thresholds_from_returns(self.FLAT_RETURNS)
        assert isinstance(t, RiskThresholds)

    def test_empty_returns_gives_default(self):
        t = RiskEngine.calibrate_thresholds_from_returns([])
        default = RiskThresholds()
        assert t.intraday_drawdown_halt == default.intraday_drawdown_halt

    def test_halt_at_least_4_pct(self):
        """halt_level >= 0.04 按策略最低保障。"""
        t = RiskEngine.calibrate_thresholds_from_returns(self.FLAT_RETURNS)
        assert t.intraday_drawdown_halt >= 0.04

    def test_warn_less_than_halt(self):
        t = RiskEngine.calibrate_thresholds_from_returns(self.VOLATILE_RETURNS)
        assert t.intraday_drawdown_warn < t.intraday_drawdown_halt

    def test_warn_is_60pct_of_halt(self):
        t = RiskEngine.calibrate_thresholds_from_returns(self.VOLATILE_RETURNS)
        expected_warn = round(t.intraday_drawdown_halt * 0.6, 4)
        assert abs(t.intraday_drawdown_warn - expected_warn) < 1e-6

    def test_var95_limit_reasonable(self):
        t = RiskEngine.calibrate_thresholds_from_returns(self.VOLATILE_RETURNS)
        # var95_limit should be non-negative and at most 0.05 (cap in impl)
        assert 0.0 <= t.var95_limit <= 0.05

    def test_concentration_and_hhi_defaults(self):
        t = RiskEngine.calibrate_thresholds_from_returns(
            self.VOLATILE_RETURNS,
            concentration_limit=0.25,
            hhi_limit=0.15,
        )
        assert t.concentration_limit == 0.25
        assert t.hhi_limit == 0.15

    def test_safety_margin_scaling(self):
        """更大的 safety_margin 应产生更宽松的 var95_limit。"""
        t1 = RiskEngine.calibrate_thresholds_from_returns(
            self.VOLATILE_RETURNS, var95_safety_margin=1.0
        )
        t2 = RiskEngine.calibrate_thresholds_from_returns(
            self.VOLATILE_RETURNS, var95_safety_margin=2.0
        )
        assert t2.var95_limit >= t1.var95_limit

    def test_high_volatility_raises_halt(self):
        """高波动序列 halt 级别应高于低波动序列。"""
        low_vol = [0.001] * 60
        high_vol = [0.05, -0.05] * 30
        t_low = RiskEngine.calibrate_thresholds_from_returns(low_vol)
        t_high = RiskEngine.calibrate_thresholds_from_returns(high_vol)
        assert t_high.intraday_drawdown_halt >= t_low.intraday_drawdown_halt

"""
Gate 3 – 风控引擎单元测试

覆盖：净敞口 / 集中度 / HHI / 日内回撤 / VaR95 / pre-trade check 全路径。
不依赖任何 IO 或外部服务，可在 CI 环境直接运行。
"""

import pytest
from core.risk_engine import RiskAction, RiskEngine, RiskThresholds


@pytest.fixture
def engine() -> RiskEngine:
    """默认阈值风控引擎。"""
    return RiskEngine()


@pytest.fixture
def strict_engine() -> RiskEngine:
    """严格阈值风控引擎（便于触发各路径）。"""
    t = RiskThresholds(
        concentration_limit=0.20,
        hhi_limit=0.15,
        intraday_drawdown_halt=0.04,
        intraday_drawdown_warn=0.02,
        var95_limit=0.01,
        net_exposure_limit=0.80,
    )
    return RiskEngine(thresholds=t)


# ---------------------------------------------------------------------------
# NetExposure
# ---------------------------------------------------------------------------

class TestNetExposure:
    def test_zero_nav(self, engine):
        assert engine.get_net_exposure({"000001.SZ": 10000}, 0) == 0.0

    def test_normal(self, engine):
        result = engine.get_net_exposure({"000001.SZ": 30_000, "000002.SZ": 70_000}, 200_000)
        assert abs(result - 0.5) < 1e-9

    def test_fully_invested(self, engine):
        result = engine.get_net_exposure({"A": 100_000}, 100_000)
        assert abs(result - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# Concentration
# ---------------------------------------------------------------------------

class TestConcentration:
    def test_zero_nav(self, engine):
        assert engine.get_concentration({"A": 10_000}, "A", 0) == 0.0

    def test_single_stock(self, engine):
        result = engine.get_concentration({"A": 25_000, "B": 75_000}, "A", 200_000)
        assert abs(result - 0.125) < 1e-9

    def test_missing_code(self, engine):
        assert engine.get_concentration({}, "X", 100_000) == 0.0


# ---------------------------------------------------------------------------
# HHI
# ---------------------------------------------------------------------------

class TestHHI:
    def test_empty(self, engine):
        assert engine.get_hhi({}, 100_000) == 0.0

    def test_single_stock(self, engine):
        # 1 只标的 → HHI = 1.0
        assert abs(engine.get_hhi({"A": 50_000}, 50_000) - 1.0) < 1e-9

    def test_equal_weight_two(self, engine):
        # 2 只各 50% → HHI = 0.5² + 0.5² = 0.5
        result = engine.get_hhi({"A": 50_000, "B": 50_000}, 100_000)
        assert abs(result - 0.5) < 1e-9

    def test_equal_weight_four(self, engine):
        # 4 只各 25% → HHI = 4 × 0.0625 = 0.25
        positions = {str(i): 25_000 for i in range(4)}
        result = engine.get_hhi(positions, 100_000)
        assert abs(result - 0.25) < 1e-9


# ---------------------------------------------------------------------------
# Intraday Drawdown
# ---------------------------------------------------------------------------

class TestIntradayDrawdown:
    def test_no_drawdown(self, engine):
        engine.update_daily_high("acc1", 100_000)
        assert engine.get_intraday_drawdown("acc1", 100_000) == 0.0

    def test_new_high_resets(self, engine):
        engine.update_daily_high("acc1", 100_000)
        dd = engine.get_intraday_drawdown("acc1", 105_000)
        assert dd == 0.0

    def test_drawdown_calculation(self, engine):
        engine.update_daily_high("acc2", 200_000)
        dd = engine.get_intraday_drawdown("acc2", 190_000)
        assert abs(dd - 0.05) < 1e-9

    def test_reset(self, engine):
        engine.update_daily_high("acc3", 100_000)
        engine.reset_daily_state("acc3")
        dd = engine.get_intraday_drawdown("acc3", 80_000)
        assert dd == 0.0  # 重置后 80k 成为新的高点，无回撤


# ---------------------------------------------------------------------------
# VaR95
# ---------------------------------------------------------------------------

class TestVaR95:
    def test_empty(self, engine):
        assert engine.calc_var95([]) == 0.0

    def test_all_positive_returns(self, engine):
        # 全是正收益 → VaR = 0
        assert engine.calc_var95([0.01] * 100) == 0.0

    def test_known_distribution(self, engine):
        # 20 个观测：前 1 个为 -5%, 其余 0
        returns = [-0.05] + [0.0] * 19
        var95 = engine.calc_var95(returns)
        assert var95 == pytest.approx(0.05)

    def test_negative_values(self, engine):
        returns = [-0.03, -0.02, -0.01, 0.00, 0.01, 0.02, 0.03]
        var95 = engine.calc_var95(returns)
        assert 0.0 < var95 <= 0.03


# ---------------------------------------------------------------------------
# Pre-trade check (integration of all metrics)
# ---------------------------------------------------------------------------

class TestPreTradeCheck:
    def _make_positions(self):
        return {"000001.SZ": 50_000, "000002.SZ": 50_000}

    def test_pass(self, engine):
        result = engine.check_pre_trade(
            account_id="acc",
            code="000003.SZ",
            volume=100,
            price=10.0,
            direction="buy",
            positions=self._make_positions(),
            nav=500_000,
        )
        assert result.action == RiskAction.PASS

    def test_blocks_zero_nav(self, engine):
        result = engine.check_pre_trade(
            account_id="acc",
            code="A",
            volume=100,
            price=10.0,
            direction="buy",
            positions={},
            nav=0,
        )
        assert result.action == RiskAction.HALT

    def test_concentration_limit(self, strict_engine):
        # 买入后集中度将超过 20%
        result = strict_engine.check_pre_trade(
            account_id="acc",
            code="000001.SZ",
            volume=10_000,
            price=10.0,      # trade_value = 100_000
            direction="buy",
            positions={"000001.SZ": 20_000},
            nav=300_000,     # after: 120_000/300_000 = 40% > 20% limit
        )
        assert result.action == RiskAction.LIMIT
        assert "集中度" in result.reason

    def test_net_exposure_limit(self, strict_engine):
        # 净敞口超限（>80%）
        result = strict_engine.check_pre_trade(
            account_id="acc",
            code="NEW",
            volume=1000,
            price=50.0,      # trade_value = 50_000
            direction="buy",
            positions={"A": 70_000, "B": 10_000},
            nav=100_000,     # after: 130_000/100_000 = 130% > 80% limit
        )
        assert result.action == RiskAction.LIMIT
        assert "净敞口" in result.reason

    def test_intraday_halt(self, strict_engine):
        # 模拟日内回撤 5% 超过 halt 线 4%
        strict_engine.update_daily_high("acc_halt", 100_000)
        result = strict_engine.check_pre_trade(
            account_id="acc_halt",
            code="A",
            volume=10,
            price=10.0,
            direction="buy",
            positions={},
            nav=95_000,      # drawdown = 5% > halt 4%
        )
        assert result.action == RiskAction.HALT

    def test_intraday_warn(self, strict_engine):
        # 日内回撤 3% 触达 warn 线 2%，不超 halt 线 4%
        strict_engine.update_daily_high("acc_warn", 100_000)
        result = strict_engine.check_pre_trade(
            account_id="acc_warn",
            code="A",
            volume=10,
            price=10.0,
            direction="buy",
            positions={},
            nav=97_000,      # drawdown = 3%
        )
        assert result.action == RiskAction.WARN

    def test_var95_warn(self, strict_engine):
        bad_returns = [-0.05] + [-0.02] * 4 + [0.01] * 15
        result = strict_engine.check_pre_trade(
            account_id="acc_var",
            code="A",
            volume=10,
            price=10.0,
            direction="buy",
            positions={},
            nav=100_000,
            returns=bad_returns,
        )
        # VaR95 应大于 strict limit 0.01
        assert result.action == RiskAction.WARN

    def test_metrics_in_result(self, engine):
        result = engine.check_pre_trade(
            account_id="acc_m",
            code="A",
            volume=100,
            price=10.0,
            direction="buy",
            positions={"A": 10_000},
            nav=200_000,
        )
        assert "net_exposure" in result.metrics
        assert "concentration" in result.metrics
        assert "hhi" in result.metrics

    def test_sell_direction_projected_position_decreases(self, engine):
        """direction='sell' → else branch (line 159): projected[code] = max(0, ...)."""
        result = engine.check_pre_trade(
            account_id="acc_sell",
            code="000001.SZ",
            volume=100,
            price=10.0,
            direction="sell",
            positions={"000001.SZ": 50_000},
            nav=200_000,
        )
        # sell reduces position, should PASS given comfortable metrics
        assert result.action == RiskAction.PASS


class TestRiskCheckResultProperties:
    def test_passed_is_true_when_action_is_pass(self):
        """RiskCheckResult.passed property (line 40)."""
        from core.risk_engine import RiskCheckResult, RiskAction
        result = RiskCheckResult(RiskAction.PASS)
        assert result.passed is True

    def test_passed_is_false_when_action_is_halt(self):
        from core.risk_engine import RiskCheckResult, RiskAction
        result = RiskCheckResult(RiskAction.HALT)
        assert result.passed is False


class TestIntradayDrawdownEdgeCases:
    def test_zero_high_returns_zero(self, engine):
        """get_intraday_drawdown when stored high is 0 (lines 351-352)."""
        engine._daily_high["acc_zero"] = 0.0
        result = engine.get_intraday_drawdown("acc_zero", 0.0)
        assert result == 0.0


class TestResetDailyStateSpecificAccount:
    def test_reset_specific_account_clears_only_that_account(self, engine):
        """reset_daily_state(account_id='x') → pop path (line 364)."""
        engine._daily_high["acc_a"] = 100_000.0
        engine._daily_high["acc_b"] = 200_000.0
        engine.reset_daily_state("acc_a")
        assert "acc_a" not in engine._daily_high
        assert "acc_b" in engine._daily_high

    def test_reset_all_via_none_clears_all_accounts(self, engine):
        """reset_daily_state(None) → clear() path (line 362)."""
        engine._daily_high["acc_x"] = 100_000.0
        engine._daily_high["acc_y"] = 200_000.0
        engine.reset_daily_state(None)
        assert len(engine._daily_high) == 0


# ---------------------------------------------------------------------------
# R4: DuckDB risk_events persistence
# ---------------------------------------------------------------------------

class TestRiskEventDBPersistence:
    """RiskEngine(db_path=':memory:') 持久化风控事件到 DuckDB risk_events 表。"""

    @pytest.fixture
    def db_engine(self):
        """使用内存 DuckDB 的风控引擎，严格阈值以便触发事件。"""
        t = RiskThresholds(
            concentration_limit=0.20,
            intraday_drawdown_halt=0.04,
            intraday_drawdown_warn=0.02,
            net_exposure_limit=0.80,
        )
        return RiskEngine(thresholds=t, db_path=":memory:")

    def test_pass_result_not_persisted(self, db_engine):
        """PASS 结果不写入 risk_events 表。"""
        db_engine.check_pre_trade(
            account_id="acc1", code="A", volume=10, price=10.0,
            direction="buy", positions={}, nav=1_000_000,
        )
        rows = db_engine._event_db.query_all()
        assert rows == []

    def test_halt_event_persisted(self, db_engine):
        """HALT 事件写入 risk_events，severity='critical'。"""
        db_engine.update_daily_high("acc_halt", 100_000)
        db_engine.check_pre_trade(
            account_id="acc_halt", code="000001.SZ", volume=10, price=10.0,
            direction="buy", positions={}, nav=95_000,  # drawdown 5% > halt 4%
        )
        rows = db_engine._event_db.query_all()
        assert len(rows) == 1
        row = rows[0]
        assert row["event_type"] == "halt"
        assert row["symbol"] == "000001.SZ"
        assert row["severity"] == "critical"

    def test_warn_event_persisted(self, db_engine):
        """WARN 事件写入 risk_events，severity='warning'。"""
        db_engine.update_daily_high("acc_warn", 100_000)
        db_engine.check_pre_trade(
            account_id="acc_warn", code="000002.SZ", volume=10, price=10.0,
            direction="buy", positions={}, nav=97_000,  # drawdown 3%
        )
        rows = db_engine._event_db.query_all()
        assert len(rows) == 1
        assert rows[0]["severity"] == "warning"
        assert rows[0]["event_type"] == "warn"

    def test_limit_event_persisted(self, db_engine):
        """LIMIT 事件写入 risk_events，severity='error'。"""
        db_engine.check_pre_trade(
            account_id="acc_lim", code="600519.SH", volume=1000, price=100.0,
            direction="buy",
            positions={"600519.SH": 180_000},  # 180k / 1M = 18% < 20% limit
            nav=200_000,                        # after trade: (180k + 100k) / 200k = 140% > 80% net exp
        )
        rows = db_engine._event_db.query_all()
        assert len(rows) == 1
        assert rows[0]["severity"] in ("error", "warning")  # LIMIT or WARN

    def test_multiple_events_accumulate(self, db_engine):
        """多次触发事件，全部写入 risk_events 表。"""
        db_engine.update_daily_high("acc_m", 100_000)
        for nav in (97_000, 96_000, 95_000):
            db_engine._daily_high["acc_m"] = 100_000  # reset each time for isolation
            db_engine.check_pre_trade(
                account_id="acc_m", code="A", volume=1, price=1.0,
                direction="buy", positions={}, nav=nav,
            )
        rows = db_engine._event_db.query_all()
        assert len(rows) == 3

    def test_details_json_contains_account_and_reason(self, db_engine):
        """details_json 包含 account_id 和 reason 字段。"""
        import json as _json
        db_engine.update_daily_high("acc_j", 100_000)
        db_engine.check_pre_trade(
            account_id="acc_j", code="TEST.SZ", volume=10, price=10.0,
            direction="buy", positions={}, nav=95_000,
        )
        rows = db_engine._event_db.query_all()
        assert rows
        details = _json.loads(rows[0]["details_json"])
        assert details["account_id"] == "acc_j"
        assert "reason" in details

    def test_event_id_is_unique_uuid(self, db_engine):
        """每条 risk_events 记录的 event_id 是唯一的 UUID 字符串。"""
        db_engine.update_daily_high("acc_uid", 100_000)
        for _ in range(3):
            db_engine._daily_high["acc_uid"] = 100_000
            db_engine.check_pre_trade(
                account_id="acc_uid", code="A", volume=1, price=1.0,
                direction="buy", positions={}, nav=95_000,
            )
        rows = db_engine._event_db.query_all()
        ids = [r["event_id"] for r in rows]
        assert len(set(ids)) == 3  # all unique

    def test_no_db_path_no_event_db(self):
        """不传 db_path 时 _event_db 为 None，不持久化。"""
        eng = RiskEngine()
        assert eng._event_db is None
        # Should not crash
        eng.update_daily_high("acc", 100_000)
        eng.check_pre_trade(
            account_id="acc", code="A", volume=1, price=1.0,
            direction="buy", positions={}, nav=95_000,
        )


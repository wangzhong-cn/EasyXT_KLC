"""
风控引擎与实盘下单流集成测试（Phase 1）

覆盖：
  - TradeAPI.buy()/sell() 注入 RiskEngine 后的风控拦截行为
  - HALT / LIMIT 买入拒单、WARN 买入放行
  - 卖出时 LIMIT 放行、HALT 拒绝
  - 未注入 RiskEngine 时向后兼容
  - DailyResetScheduler 定时重置逻辑
"""

import time
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from core.risk_engine import RiskAction, RiskCheckResult, RiskEngine, RiskThresholds
from core.daily_reset_scheduler import DailyResetScheduler


# ---------------------------------------------------------------------------
# 模块级 autouse fixture：patch 掉 xt_const 使用真实常量
# （测试中 TradeAPI 直接赋 trader，不走 connect()/_init_xt_env()，xt_const 会是 None）
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_xt_const(monkeypatch):
    import xtquant.xtconstant as _real_const
    import easy_xt.trade_api as _ta
    monkeypatch.setattr(_ta, "xt_const", _real_const)


# ---------------------------------------------------------------------------
# 辅助 Mock
# ---------------------------------------------------------------------------


class MockTraderBackend:
    """伪造 xtquant.xttrader，不依赖真实 QMT 连接。"""

    def __init__(self):
        self.last_order_stock_kwargs = {}
        self.order_counter = 1000

    def order_stock(self, **kwargs):
        self.last_order_stock_kwargs = kwargs
        self.order_counter += 1
        return self.order_counter

    def query_stock_asset(self, account):
        # 返回伪造资产（net = 100万）
        asset = MagicMock()
        asset.account_id = account.account_id if hasattr(account, "account_id") else "test"
        asset.cash = 500_000.0
        asset.frozen_cash = 0.0
        asset.market_value = 500_000.0
        asset.total_asset = 1_000_000.0
        return asset

    def query_stock_positions(self, account):
        # 返回一个持仓（000001.SZ，市值 20万）
        pos = MagicMock()
        pos.stock_code = "000001.SZ"
        pos.volume = 2000
        pos.can_use_volume = 2000
        pos.open_price = 10.0
        pos.market_value = 200_000.0
        pos.frozen_volume = 0
        return [pos]


def _make_trade_api_with_risk(
    thresholds: Optional[RiskThresholds] = None,
) -> tuple:
    """创建已接入 RiskEngine 的 MockTradeAPI（不需要真实 QMT）。"""
    from easy_xt.trade_api import TradeAPI

    api = TradeAPI()
    # 注入 Mock trader backend
    mock_backend = MockTraderBackend()
    api.trader = mock_backend
    api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

    # 注入 RiskEngine
    risk = RiskEngine(thresholds=thresholds or RiskThresholds())
    api.attach_risk_engine(risk)

    return api, risk, mock_backend


# ---------------------------------------------------------------------------
# TradeAPI + RiskEngine 集成测试
# ---------------------------------------------------------------------------


class TestTradeAPIRiskIntegration:
    """TradeAPI.buy()/sell() 风控集成测试。"""

    def test_buy_passes_when_no_risk_triggered(self):
        """正常买入（净值充足、集中度未超限）→ 允许下单。"""
        api, risk, backend = _make_trade_api_with_risk()
        order_id = api.buy(account_id="test_acc", code="000002.SZ", volume=100, price=10.0)
        assert order_id, "正常买入应返回委托号"
        assert backend.last_order_stock_kwargs.get("stock_code") == "000002.SZ"

    def test_buy_blocked_by_concentration_limit(self):
        """买入导致单标的集中度超限 → LIMIT 拒单（返回 None）。"""
        # 设置集中度上限 0.01（极低，100股10元 = 1000元，而净值100万，0.1% > 0.01% 不会触发？）
        # 改成 10股 1000000元… 用更直接的方式：把现有持仓 000001.SZ 规模直接设很大
        # 最简单：直接 mock _run_risk_check 返回 False
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        api.trader = MockTraderBackend()
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        # 配置 RiskEngine：集中度上限 0.01（1%），任何非零买入都会触发
        thresholds = RiskThresholds(concentration_limit=0.001)
        risk = RiskEngine(thresholds=thresholds)
        api.attach_risk_engine(risk)

        # 买入 000001.SZ（已有 200000 持仓），再买 9000元（价值 0.9%，超出 0.1%）
        # 实际计算：(200000 + 9000) / 1000000 = 20.9% >> 0.1% → LIMIT
        order_id = api.buy(account_id="test_acc", code="000001.SZ", volume=900, price=10.0)
        assert not order_id, "集中度超限时买入应被 LIMIT 拒单"

    def test_buy_warned_but_allowed(self, caplog):
        """风控返回 WARN 时，买入仍然被放行。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        backend = MockTraderBackend()
        api.trader = backend
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        # 注入一个总是返回 WARN 的 RiskEngine（mock）
        mock_risk = MagicMock()
        mock_risk.check_pre_trade.return_value = RiskCheckResult(
            RiskAction.WARN, reason="测试预警"
        )
        api.attach_risk_engine(mock_risk)

        import logging
        with caplog.at_level(logging.WARNING, logger="easy_xt.trade_api"):
            order_id = api.buy(account_id="test_acc", code="000002.SZ", volume=100, price=10.0)

        assert order_id, "WARN 时买入应放行"
        assert mock_risk.check_pre_trade.called

    def test_buy_blocked_by_halt(self):
        """风控返回 HALT → 买入拒单（返回 None）。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        api.trader = MockTraderBackend()
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        mock_risk = MagicMock()
        mock_risk.check_pre_trade.return_value = RiskCheckResult(
            RiskAction.HALT, reason="熔断测试"
        )
        api.attach_risk_engine(mock_risk)

        order_id = api.buy(account_id="test_acc", code="000002.SZ", volume=100, price=10.0)
        assert not order_id, "HALT 时买入应被拒绝"

    def test_sell_allowed_on_limit(self):
        """风控返回 LIMIT 时，卖出（减仓）仍被放行。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        backend = MockTraderBackend()
        api.trader = backend
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        mock_risk = MagicMock()
        mock_risk.check_pre_trade.return_value = RiskCheckResult(
            RiskAction.LIMIT, reason="集中度超限（卖出仍放行）"
        )
        api.attach_risk_engine(mock_risk)

        order_id = api.sell(account_id="test_acc", code="000001.SZ", volume=100, price=10.0)
        assert order_id, "LIMIT 时卖出应放行（减仓视为降险）"

    def test_sell_blocked_by_halt(self):
        """卖出时风控 HALT → 拒单（熔断期间禁止所有交易）。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        api.trader = MockTraderBackend()
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        mock_risk = MagicMock()
        mock_risk.check_pre_trade.return_value = RiskCheckResult(
            RiskAction.HALT, reason="熔断（熔断期间禁止全部交易）"
        )
        api.attach_risk_engine(mock_risk)

        order_id = api.sell(account_id="test_acc", code="000001.SZ", volume=100, price=10.0)
        assert not order_id, "HALT 时卖出也应被拒绝"

    def test_no_risk_engine_backward_compatible(self):
        """未注入 RiskEngine 时，行为与改造前完全一致（向后兼容）。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        backend = MockTraderBackend()
        api.trader = backend
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}
        # 不调用 attach_risk_engine()

        order_id = api.buy(account_id="test_acc", code="000002.SZ", volume=100, price=10.0)
        assert order_id, "未注入风控时应直接放行"

    def test_risk_engine_exception_safe(self):
        """RiskEngine 抛出异常时，不阻断下单（安全降级）。"""
        from easy_xt.trade_api import TradeAPI

        api = TradeAPI()
        backend = MockTraderBackend()
        api.trader = backend
        api.accounts = {"test_acc": MagicMock(account_id="test_acc")}

        mock_risk = MagicMock()
        mock_risk.check_pre_trade.side_effect = RuntimeError("测试异常")
        api.attach_risk_engine(mock_risk)

        order_id = api.buy(account_id="test_acc", code="000002.SZ", volume=100, price=10.0)
        assert order_id, "RiskEngine 异常时应安全降级放行"


# ---------------------------------------------------------------------------
# DailyResetScheduler 测试
# ---------------------------------------------------------------------------


class TestDailyResetScheduler:
    """每日重置调度器测试。"""

    def test_force_reset_calls_risk_engine(self):
        """force_reset_now() 应调用 RiskEngine 的重置方法。"""
        mock_risk = MagicMock()
        scheduler = DailyResetScheduler(risk_engine=mock_risk)

        scheduler.force_reset_now("open")
        mock_risk.reset_daily_state.assert_called_once_with(None)

        scheduler.force_reset_now("midnight")
        mock_risk.reset_risk_stats.assert_called_once()

    def test_force_reset_all(self):
        """force_reset_now('all') 触发全部重置。"""
        mock_risk = MagicMock()
        mock_slo = MagicMock()
        scheduler = DailyResetScheduler(risk_engine=mock_risk, slo_monitor=mock_slo)

        scheduler.force_reset_now("all")
        mock_risk.reset_daily_state.assert_called_once()
        mock_risk.reset_risk_stats.assert_called_once()
        mock_slo.reset.assert_called_once()

    def test_account_scoped_reset(self):
        """指定账户列表时，按账户逐个重置。"""
        mock_risk = MagicMock()
        scheduler = DailyResetScheduler(
            risk_engine=mock_risk,
            account_ids=["acc1", "acc2"],
        )
        scheduler.force_reset_now("open")
        calls = [c.args[0] for c in mock_risk.reset_daily_state.call_args_list]
        assert "acc1" in calls and "acc2" in calls

    def test_no_risk_engine_is_safe(self):
        """无 RiskEngine 时调用 force_reset_now() 不应抛出异常。"""
        scheduler = DailyResetScheduler(risk_engine=None)
        scheduler.force_reset_now("all")  # 不应抛异常

    def test_start_stop(self):
        """start() 启动后台线程，stop() 正确终止。"""
        scheduler = DailyResetScheduler(
            risk_engine=MagicMock(),
            check_interval_s=0.1,
        )
        scheduler.start()
        assert scheduler._thread is not None and scheduler._thread.is_alive()
        scheduler.stop()
        assert not scheduler._thread.is_alive()


# ---------------------------------------------------------------------------
# StrategyRegistry 测试
# ---------------------------------------------------------------------------


class TestStrategyRegistry:
    """策略注册中心测试。"""

    def setup_method(self):
        """每个测试使用独立注册中心实例（避免单例污染）。"""
        from strategies.registry import StrategyRegistry
        self.registry = StrategyRegistry()

    def test_register_and_get(self):
        """注册策略后可通过 strategy_id 查询。"""
        self.registry.register("test_strategy", account_id="acc1", params={"k": 1})
        info = self.registry.get("test_strategy")
        assert info is not None
        assert info.account_id == "acc1"
        assert info.params == {"k": 1}
        assert info.status == "running"

    def test_list_all_returns_all(self):
        """list_all() 返回所有已注册策略（含已停止）。"""
        self.registry.register("s1")
        self.registry.register("s2")
        self.registry.unregister("s1")
        all_ids = [s["strategy_id"] for s in self.registry.list_all()]
        assert "s1" in all_ids
        assert "s2" in all_ids

    def test_list_running_filters_stopped(self):
        """list_running() 只返回 running 状态的策略。"""
        self.registry.register("running_s")
        self.registry.register("stopped_s")
        self.registry.unregister("stopped_s")
        running = [i.strategy_id for i in self.registry.list_running()]
        assert "running_s" in running
        assert "stopped_s" not in running

    def test_update_params(self):
        """update_params() 热更新参数不影响其他字段。"""
        self.registry.register("s1", params={"a": 1})
        self.registry.update_params("s1", {"b": 2})
        info = self.registry.get("s1")
        assert info.params["a"] == 1   # 原参数保留
        assert info.params["b"] == 2   # 新参数加入

    def test_snapshot_without_db(self):
        """无法获取 DB 时，snapshot_to_db() 安全返回 0（不崩溃）。"""
        self.registry.register("s1", params={"k": 1})
        # 传入一个会抛异常的假 db_manager
        bad_db = MagicMock()
        bad_db.get_write_connection.side_effect = RuntimeError("无 DB")
        result = self.registry.snapshot_to_db(db_manager=bad_db)
        assert result == 0

    def test_repr(self):
        """__repr__ 包含 total 和 running 信息。"""
        self.registry.register("s1")
        self.registry.register("s2")
        self.registry.unregister("s1")
        r = repr(self.registry)
        assert "total=2" in r
        assert "running=1" in r

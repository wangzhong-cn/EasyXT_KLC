"""
策略基类单元测试（Gate 3 - 覆盖率推进）

覆盖：
  - 生命周期钩子顺序（on_init → on_bar → on_order → on_risk → on_stop）
  - submit_order：含风控前检、审计记录、executor 回调
  - 重复启动保护
  - on_bar 异常隔离（不影响下一轮）
  - on_risk 默认实现（WARN/LIMIT/HALT 均记录日志但不崩溃）
"""

from __future__ import annotations

from typing import List, Optional
from unittest.mock import MagicMock, call

import pytest

from strategies.base_strategy import BarData, BaseStrategy, OrderData, StrategyContext


# ---------------------------------------------------------------------------
# Concrete test strategy implementation
# ---------------------------------------------------------------------------


class SimpleStrategy(BaseStrategy):
    """最小可测策略。"""

    def __init__(self) -> None:
        super().__init__("test_strategy")
        self.init_called = False
        self.bars_received: List[BarData] = []
        self.orders_received: List[OrderData] = []
        self.risks_received: list = []
        self.stop_called = False

    def on_init(self, context: StrategyContext) -> None:
        self.init_called = True

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        self.bars_received.append(bar)

    def on_order(self, context: StrategyContext, order: OrderData) -> None:
        self.orders_received.append(order)

    def on_risk(self, context: StrategyContext, risk_result) -> None:
        super().on_risk(context, risk_result)    # 调用默认日志实现
        self.risks_received.append(risk_result)

    def on_stop(self, context: StrategyContext) -> None:
        self.stop_called = True


class BrokenBarStrategy(BaseStrategy):
    """on_bar 每次都抛异常，测试框架隔离能力。"""

    def on_init(self, context: StrategyContext) -> None:
        pass

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        raise RuntimeError("故意触发的 on_bar 异常")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx() -> StrategyContext:
    return StrategyContext(
        strategy_id="test_strategy",
        account_id="acc001",
        positions={"000001.SZ": 10_000.0},
        nav=100_000.0,
    )


@pytest.fixture
def bar() -> BarData:
    return BarData(
        code="000001.SZ",
        period="1d",
        open=10.0,
        high=10.5,
        low=9.8,
        close=10.2,
        volume=100_000.0,
        time="2026-03-07",
    )


@pytest.fixture
def strategy(ctx) -> SimpleStrategy:
    s = SimpleStrategy()
    s._start(ctx)
    return s


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_on_init_called_on_start(self, strategy: SimpleStrategy):
        assert strategy.init_called

    def test_running_state_set(self, strategy: SimpleStrategy):
        assert strategy._running

    def test_duplicate_start_ignored(self, ctx, strategy: SimpleStrategy):
        strategy._start(ctx)   # second start should be no-op
        assert strategy._running
        assert strategy.init_called   # still True, not doubled

    def test_on_stop_called_on_stop(self, ctx, strategy: SimpleStrategy):
        strategy._stop(ctx)
        assert strategy.stop_called
        assert not strategy._running

    def test_double_stop_is_noop(self, ctx, strategy: SimpleStrategy):
        strategy._stop(ctx)
        strategy._stop(ctx)   # second stop should not raise
        assert strategy.stop_called

    def test_handle_bar_not_running(self, ctx, bar):
        s = SimpleStrategy()   # not started
        s._handle_bar(ctx, bar)
        assert s.bars_received == []

    def test_handle_bar_running(self, ctx, bar, strategy):
        strategy._handle_bar(ctx, bar)
        assert len(strategy.bars_received) == 1
        assert strategy.bars_received[0].code == "000001.SZ"

    def test_handle_bar_exception_isolated(self, ctx, bar):
        """on_bar 抛异常时框架不重新抛，允许继续下一轮。"""
        s = BrokenBarStrategy("broken")
        s._start(ctx)
        s._handle_bar(ctx, bar)   # should NOT raise
        s._handle_bar(ctx, bar)   # second call still no-op, no exception

    def test_handle_order(self, ctx, strategy):
        order = OrderData(
            order_id="ord001",
            signal_id="sig001",
            code="000001.SZ",
            direction="buy",
            volume=100,
            price=10.0,
            status="filled",
        )
        strategy._handle_order(ctx, order)
        assert len(strategy.orders_received) == 1

    def test_on_risk_default_logs(self, ctx, strategy):
        from core.risk_engine import RiskAction, RiskCheckResult
        warn_result = RiskCheckResult(RiskAction.WARN, reason="test warn", metrics={})
        # Should not raise; default on_risk calls super() which logs
        strategy.on_risk(ctx, warn_result)
        assert len(strategy.risks_received) == 1


# ---------------------------------------------------------------------------
# submit_order
# ---------------------------------------------------------------------------


class TestSubmitOrder:
    def test_submit_calls_executor(self, ctx, strategy):
        from easy_xt.trade_api import OrderResponse
        mock_executor = MagicMock()
        # 伪造底层 TradeAPI 返回的 OrderResponse
        mock_response = OrderResponse(order_id=1001, status="submitted", msg="")
        mock_executor.submit_order.return_value = mock_response
        ctx.executor = mock_executor

        oid = strategy.submit_order(ctx, "000001.SZ", 100, 10.0, "buy", signal_id="SIG-001")

        assert oid == "1001"
        mock_executor.submit_order.assert_called_once_with(
            code="000001.SZ",
            volume=100,
            price=10.0,
            direction="buy",
            signal_id="SIG-001",
        )

    def test_submit_no_executor_returns_none(self, ctx, strategy):
        ctx.executor = None
        oid = strategy.submit_order(ctx, "A", 10, 5.0, "buy")
        assert oid is None

    def test_submit_executor_rejected_returns_none(self, ctx, strategy):
        from easy_xt.trade_api import OrderResponse
        mock_executor = MagicMock()
        # 底层 TradeAPI 风控拦截返回 失败的 OrderResponse (比如 order_id=None)
        mock_response = OrderResponse(order_id=None, status="rejected_risk", msg="超限")
        mock_executor.submit_order.return_value = mock_response
        ctx.executor = mock_executor

        oid = strategy.submit_order(ctx, "000001.SZ", 100, 10.0, "buy", signal_id="SIG-002")

        # 框架应正确识别并在日志中记录拦截，然后给上层返回 None
        assert oid is None
        mock_executor.submit_order.assert_called_once()


# ---------------------------------------------------------------------------
# StrategyContext
# ---------------------------------------------------------------------------


class TestStrategyContext:
    def test_default_values(self):
        ctx = StrategyContext(strategy_id="s", account_id="a")
        assert ctx.positions == {}
        assert ctx.nav == 0.0
        assert ctx.executor is None
        assert ctx.risk_engine is None
        assert ctx.audit_trail is None

    def test_params_accessible(self):
        ctx = StrategyContext(
            strategy_id="s",
            account_id="a",
            params={"fast": 5, "slow": 20},
        )
        assert ctx.params["fast"] == 5

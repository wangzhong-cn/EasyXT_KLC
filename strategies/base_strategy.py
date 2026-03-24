"""
策略基类（Phase 1）

所有实盘/回测策略继承 :class:`BaseStrategy`，实现统一的生命周期钩子：
  on_init → on_bar / on_tick → on_order → on_risk → on_stop

调用方（回测引擎/实盘调度器）负责构造 :class:`StrategyContext` 并在合适时机驱动各钩子。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------


@dataclass
class BarData:
    """单根 K 线数据。"""

    code: str
    period: str  # "1d" | "60m" | "1m" …
    open: float
    high: float
    low: float
    close: float
    volume: float
    time: Any  # datetime / timestamp


@dataclass
class TickData:
    """逐笔行情数据（tick 级）。"""

    code: str
    last: float  # 最新价
    volume: float  # tick 成交量
    time: Any  # datetime / timestamp
    # 可选五档行情
    ask1: float = 0.0
    bid1: float = 0.0
    ask1_vol: float = 0.0
    bid1_vol: float = 0.0
    ask2: float = 0.0
    ask3: float = 0.0
    ask2_vol: float = 0.0
    ask3_vol: float = 0.0


@dataclass
class OrderData:
    """订单回报（简化版）。"""

    order_id: str
    signal_id: str
    code: str
    direction: str  # "buy" | "sell"
    volume: float
    price: float
    status: str  # "submitted" | "filled" | "cancelled" | "rejected"
    filled_volume: float = 0.0
    filled_price: float = 0.0
    error_msg: str = ""


@dataclass
class StrategyContext:
    """
    统一策略上下文，由框架在每次驱动前注入最新状态。

    ``executor`` 是带 ``submit_order(code, volume, price, direction, signal_id)``
    接口的对象；可在测试中注入 Mock。

    ``factor_snapshot`` 保存当前 bar 各代码的因子截面值，形如::

        {"600519.SH": {"momentum_20d": 0.032, "rsi_14": 58.7}, ...}

    策略可通过 ``context.factor_snapshot.get(code, {}).get("momentum_20d", 0.0)``
    访问当前 bar 的因子值，无需重复计算。
    """

    strategy_id: str
    account_id: str
    positions: dict[str, float] = field(default_factory=dict)  # code -> market_value
    nav: float = 0.0
    params: dict[str, Any] = field(default_factory=dict)
    executor: Any | None = None  # 订单执行器（实盘 / 仿真）
    risk_engine: Any | None = None  # RiskEngine 实例
    audit_trail: Any | None = None  # AuditTrail 实例
    factor_snapshot: dict[str, dict[str, float]] = field(default_factory=dict)
    # code → {factor_name → float}；由回测引擎 / 实盘调度器在每个 bar 前填充


# ---------------------------------------------------------------------------
# Base Strategy
# ---------------------------------------------------------------------------


class BaseStrategy(ABC):
    """
    策略基类。

    子类只需 override 抽象方法 :meth:`on_init` 和 :meth:`on_bar`；
    其余钩子提供默认空实现，按需 override。
    """

    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id
        self._running = False
        self.logger = logging.getLogger(f"strategy.{strategy_id}")

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def on_init(self, context: StrategyContext) -> None:
        """
        初始化钩子。框架在策略启动*前*调用一次。
        用途：初始化内部状态、订阅行情、加载历史数据。
        """

    @abstractmethod
    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        """
        K 线驱动钩子。每根新 K 线到达时调用。
        策略信号生成、下单逻辑均应在此实现。
        """

    def on_order(self, context: StrategyContext, order: OrderData) -> None:
        """
        订单回报钩子。默认空实现，子类按需 override。
        当订单状态变更（成交/撤单/拒单）时由框架调用。
        """

    def on_tick(self, context: StrategyContext, tick: TickData) -> None:
        """
        逐笔行情钩子。tick 模式下每笔 tick 到达时调用。
        默认空实现，子类按需 override。
        """

    def on_risk(self, context: StrategyContext, risk_result: Any) -> None:
        """
        风控触发钩子。当 RiskEngine 返回 WARN/LIMIT/HALT 时由框架调用。
        默认记录日志，子类可 override 实现定制化响应（如强制减仓）。
        """
        self.logger.warning(
            "风控触发 [%s]: %s | metrics=%s",
            risk_result.action.value,
            risk_result.reason,
            risk_result.metrics,
        )

    def on_stop(self, context: StrategyContext) -> None:
        """
        停止钩子。框架在策略停止*后*调用一次（正常停止 / 熔断 / 异常均会触发）。
        用途：持久化状态、发送通知、清理资源。
        """

    # ------------------------------------------------------------------
    # Framework interface (called by engine, not by subclass directly)
    # ------------------------------------------------------------------

    def _start(self, context: StrategyContext) -> None:
        """框架调用：启动策略。"""
        if self._running:
            self.logger.warning("策略 %s 已在运行，忽略重复启动", self.strategy_id)
            return
        self._running = True
        self.logger.info("策略 %s 启动", self.strategy_id)
        try:
            self.on_init(context)
        except Exception:
            self._running = False
            raise

    def _stop(self, context: StrategyContext) -> None:
        """框架调用：停止策略。"""
        if not self._running:
            return
        self._running = False
        self.logger.info("策略 %s 停止", self.strategy_id)
        try:
            self.on_stop(context)
        except Exception:
            self.logger.exception("策略 %s on_stop 异常", self.strategy_id)

    def _handle_bar(self, context: StrategyContext, bar: BarData) -> None:
        """框架调用：驱动 K 线，含风控前检。"""
        if not self._running:
            return
        try:
            self.on_bar(context, bar)
        except Exception:
            self.logger.exception(
                "策略 %s on_bar 异常 @ %s %s", self.strategy_id, bar.code, bar.time
            )

    def _handle_tick(self, context: StrategyContext, tick: TickData) -> None:
        """框架调用：驱动 tick 行情。"""
        if not self._running:
            return
        try:
            self.on_tick(context, tick)
        except Exception:
            self.logger.exception(
                "策略 %s on_tick 异常 @ %s %s", self.strategy_id, tick.code, tick.time
            )

    def _handle_order(self, context: StrategyContext, order: OrderData) -> None:
        """框架调用：分发订单回报。"""
        try:
            self.on_order(context, order)
        except Exception:
            self.logger.exception(
                "策略 %s on_order 异常 order_id=%s", self.strategy_id, order.order_id
            )

    # ------------------------------------------------------------------
    # Helper: submit order via context executor with risk pre-check
    # ------------------------------------------------------------------

    def submit_order(
        self,
        context: StrategyContext,
        code: str,
        volume: float,
        price: float,
        direction: str,
        signal_id: str = "",
        returns: list[float] | None = None,
    ) -> str | None:
        """
        提交委托（依赖底层的 TradeAPI 执行前置风控拦截及审计写入）。

        Returns:
            order_id（字符串）或 None（风控拒单/错误）。
        """
        if context.executor is None:
            self.logger.error("submit_order: context.executor 未设置")
            return None

        # executor_submit_order 返回 OrderResponse(order_id, status, msg)
        res = context.executor.submit_order(
            code=code,
            volume=volume,
            price=price,
            direction=direction,
            signal_id=signal_id,
        )

        if not res:
            # 被拒单或发生错误，底层 TradeAPI 已经抛出了错误或警告日志，且记录了 audit_trail
            self.logger.warning(f"委托失败被拦截 [{direction} {code} x{volume}]: {res.msg}")
            # 如果需要，这里可以构造一个风险触发事件发送给 on_risk
            # self.on_risk(context, ...)
            return None

        return str(res.order_id)

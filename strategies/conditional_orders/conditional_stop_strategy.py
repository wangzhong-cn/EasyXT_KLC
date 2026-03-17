"""
条件止损止盈策略 —— 原生 BaseStrategy 实现

基于价格条件自动止损止盈。当价格触及预设阈值时自动执行买入/卖出。

迁移自 strategies/conditional_orders/止损止盈.py (遗留 strategy_template 协议)。
"""

from __future__ import annotations

import uuid
from typing import Any, Optional

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext


class ConditionalStopStrategy(BaseStrategy):
    """
    条件止损止盈策略。

    参数（通过 ``context.params`` 传入）：
        stock_code      : str   目标股票代码
        trigger_price   : float 触发价格
        trade_direction : str   "buy" | "sell"
        trade_volume    : float 交易量
        condition_type  : str   "price" | "pct_stop_loss" | "pct_take_profit"
        stop_loss_pct   : float 止损百分比（仅 pct_stop_loss 模式）
        take_profit_pct : float 止盈百分比（仅 pct_take_profit 模式）
    """

    def __init__(self, strategy_id: str = "conditional_stop") -> None:
        super().__init__(strategy_id)
        self._stock_code = "000001.SZ"
        self._trigger_price = 0.0
        self._direction = "sell"
        self._trade_volume = 1000.0
        self._condition_type = "price"
        self._stop_loss_pct = 0.05
        self._take_profit_pct = 0.10

        self._triggered = False
        self._executed = False
        self._entry_price: Optional[float] = None
        self._current_position: float = 0.0

    def on_init(self, context: StrategyContext) -> None:
        p = context.params
        self._stock_code = p.get("stock_code", p.get("股票代码", "000001.SZ"))
        self._trigger_price = float(p.get("trigger_price", p.get("触发价格", 0.0)))
        self._direction = p.get("trade_direction", p.get("交易方向", "sell"))
        # 标准化方向
        if self._direction in ("买入",):
            self._direction = "buy"
        elif self._direction in ("卖出",):
            self._direction = "sell"
        self._trade_volume = float(p.get("trade_volume", p.get("交易数量", 1000)))
        self._condition_type = p.get("condition_type", p.get("条件类型", "price"))
        if self._condition_type == "价格条件":
            self._condition_type = "price"
        self._stop_loss_pct = float(p.get("stop_loss_pct", p.get("止损比例", 0.05)))
        self._take_profit_pct = float(p.get("take_profit_pct", p.get("止盈比例", 0.10)))

        self._triggered = False
        self._executed = False
        self._entry_price = p.get("entry_price", None)
        if self._entry_price is not None:
            self._entry_price = float(self._entry_price)
        self._current_position = float(p.get("current_position", p.get("当前持仓", 0)))

        self.logger.info(
            "条件止损止盈初始化: code=%s trigger=%.2f dir=%s type=%s",
            self._stock_code, self._trigger_price, self._direction, self._condition_type,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code != self._stock_code or self._executed:
            return

        price = bar.close
        condition_met = False

        if self._condition_type == "price":
            condition_met = self._check_price_condition(price)
        elif self._condition_type == "pct_stop_loss" and self._entry_price:
            pnl = (price - self._entry_price) / self._entry_price
            condition_met = pnl <= -self._stop_loss_pct
        elif self._condition_type == "pct_take_profit" and self._entry_price:
            pnl = (price - self._entry_price) / self._entry_price
            condition_met = pnl >= self._take_profit_pct

        if condition_met and not self._triggered:
            self._triggered = True
            self.logger.info("条件触发: type=%s price=%.2f", self._condition_type, price)
            self._execute_order(context, price)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_price_condition(self, current_price: float) -> bool:
        if self._direction == "buy":
            return current_price <= self._trigger_price
        return current_price >= self._trigger_price

    def _execute_order(self, context: StrategyContext, price: float) -> None:
        volume = self._trade_volume
        if self._direction == "sell":
            volume = min(volume, self._current_position) if self._current_position > 0 else volume

        oid = self.submit_order(
            context, self._stock_code, volume, price,
            self._direction, signal_id=f"cond_{self._condition_type}_{uuid.uuid4().hex[:8]}",
        )
        if oid:
            self._executed = True
            if self._direction == "buy":
                self._current_position += volume
            else:
                self._current_position -= volume
            self.logger.info("条件单执行: %s %.0f股 @%.2f", self._direction, volume, price)

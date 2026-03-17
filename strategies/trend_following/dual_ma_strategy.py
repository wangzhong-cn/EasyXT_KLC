"""
双均线策略 —— 原生 BaseStrategy 实现

基于短期和长期移动平均线的交叉信号进行交易。
金叉买入、死叉卖出，支持止损止盈。

迁移自 strategies/trend_following/双均线策略.py (遗留 strategy_template 协议)。
"""

from __future__ import annotations

import uuid
from collections import deque
from typing import Any, Dict, Optional

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext


class DualMovingAverageStrategy(BaseStrategy):
    """
    双均线趋势跟踪策略。

    参数（通过 ``context.params`` 传入）：
        stock_code   : str   目标股票代码（默认 '000001.SZ'）
        short_period : int   短期均线周期（默认 5）
        long_period  : int   长期均线周期（默认 20）
        trade_volume : float 每次交易量（默认 1000）
        stop_loss    : float 止损比例（默认 0.05）
        take_profit  : float 止盈比例（默认 0.10）
        enable_sl    : bool  启用止损（默认 True）
        enable_tp    : bool  启用止盈（默认 True）
    """

    def __init__(self, strategy_id: str = "dual_ma") -> None:
        super().__init__(strategy_id)
        # 将在 on_init 中从 context.params 初始化
        self._short_period = 5
        self._long_period = 20
        self._stock_code = "000001.SZ"
        self._trade_volume = 1000.0
        self._stop_loss = 0.05
        self._take_profit = 0.10
        self._enable_sl = True
        self._enable_tp = True

        # 内部状态
        self._closes: deque[float] = deque()
        self._last_signal: Optional[str] = None
        self._entry_price: Optional[float] = None
        self._position_size: float = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_init(self, context: StrategyContext) -> None:
        p = context.params
        self._stock_code = p.get("stock_code", p.get("股票代码", "000001.SZ"))
        self._short_period = int(p.get("short_period", p.get("短期均线", 5)))
        self._long_period = int(p.get("long_period", p.get("长期均线", 20)))
        self._trade_volume = float(p.get("trade_volume", p.get("交易数量", 1000)))
        self._stop_loss = float(p.get("stop_loss", p.get("止损比例", 0.05)))
        self._take_profit = float(p.get("take_profit", p.get("止盈比例", 0.10)))
        self._enable_sl = bool(p.get("enable_sl", p.get("启用止损", True)))
        self._enable_tp = bool(p.get("enable_tp", p.get("启用止盈", True)))

        self._closes.clear()
        self._last_signal = None
        self._entry_price = None
        self._position_size = 0.0

        self.logger.info(
            "双均线策略初始化: code=%s short=%d long=%d vol=%.0f sl=%.2f%% tp=%.2f%%",
            self._stock_code,
            self._short_period,
            self._long_period,
            self._trade_volume,
            self._stop_loss * 100,
            self._take_profit * 100,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code != self._stock_code:
            return

        self._closes.append(bar.close)
        # 只保留计算所需的最小窗口 + 1（需要前一根 bar 判交叉）
        max_len = self._long_period + 1
        while len(self._closes) > max_len:
            self._closes.popleft()

        # 止损止盈检测
        if self._check_stop(context, bar.close):
            return

        # 均线信号
        signal = self._generate_signal()
        if signal == self._last_signal:
            return

        self.logger.info("信号: %s -> %s @ %.2f", self._last_signal, signal, bar.close)

        if signal == "buy" and self._position_size <= 0:
            # 平空（如有）再开多
            if self._position_size < 0:
                self.submit_order(
                    context, self._stock_code, abs(self._position_size),
                    bar.close, "buy", signal_id=f"close_short_{uuid.uuid4().hex[:8]}",
                )
            oid = self.submit_order(
                context, self._stock_code, self._trade_volume,
                bar.close, "buy", signal_id=f"open_long_{uuid.uuid4().hex[:8]}",
            )
            if oid:
                self._position_size = self._trade_volume
                self._entry_price = bar.close

        elif signal == "sell" and self._position_size > 0:
            self.submit_order(
                context, self._stock_code, self._position_size,
                bar.close, "sell", signal_id=f"close_long_{uuid.uuid4().hex[:8]}",
            )
            self._position_size = 0.0
            self._entry_price = None

        self._last_signal = signal

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _generate_signal(self) -> str:
        """根据均线交叉生成信号：buy / sell / hold。"""
        if len(self._closes) < self._long_period + 1:
            return "hold"

        closes = list(self._closes)
        short_cur = sum(closes[-self._short_period:]) / self._short_period
        short_prev = sum(closes[-self._short_period - 1:-1]) / self._short_period
        long_cur = sum(closes[-self._long_period:]) / self._long_period
        long_prev = sum(closes[-self._long_period - 1:-1]) / self._long_period

        if short_prev <= long_prev and short_cur > long_cur:
            return "buy"  # 金叉
        if short_prev >= long_prev and short_cur < long_cur:
            return "sell"  # 死叉
        return "hold"

    def _check_stop(self, context: StrategyContext, current_price: float) -> bool:
        """检查止损/止盈，触发则平仓。返回是否触发。"""
        if self._entry_price is None or self._position_size == 0:
            return False

        pnl_ratio = (current_price - self._entry_price) / self._entry_price

        if self._enable_sl and pnl_ratio <= -self._stop_loss:
            self.logger.info("触发止损: %.2f%%", pnl_ratio * 100)
            self.submit_order(
                context, self._stock_code, abs(self._position_size),
                current_price, "sell", signal_id=f"stop_loss_{uuid.uuid4().hex[:8]}",
            )
            self._position_size = 0.0
            self._entry_price = None
            return True

        if self._enable_tp and pnl_ratio >= self._take_profit:
            self.logger.info("触发止盈: %.2f%%", pnl_ratio * 100)
            self.submit_order(
                context, self._stock_code, abs(self._position_size),
                current_price, "sell", signal_id=f"take_profit_{uuid.uuid4().hex[:8]}",
            )
            self._position_size = 0.0
            self._entry_price = None
            return True

        return False

"""
动量因子策略 —— 原生 BaseStrategy 实现

基于 N 日动量正负判断买卖方向。

迁移自引擎B ``management/backtest_engine.py::_generate_signal(factor)``。
"""

from __future__ import annotations

import uuid
from collections import deque
from typing import Optional

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext


class MomentumFactorStrategy(BaseStrategy):
    """
    动量因子策略。

    参数（通过 ``context.params`` 传入）：
        stock_code         : str   目标股票代码
        momentum_lookback  : int   动量回看周期（默认 20）
        trade_volume       : float 交易量（默认 1000）
    """

    def __init__(self, strategy_id: str = "momentum_factor") -> None:
        super().__init__(strategy_id)
        self._stock_code = "000001.SZ"
        self._lookback = 20
        self._trade_volume = 1000.0

        self._closes: deque[float] = deque()
        self._has_position = False

    def on_init(self, context: StrategyContext) -> None:
        p = context.params
        self._stock_code = p.get("stock_code", p.get("股票代码", "000001.SZ"))
        self._lookback = int(p.get("momentum_lookback", p.get("动量回看", 20)))
        self._trade_volume = float(p.get("trade_volume", p.get("交易数量", 1000)))

        self._closes.clear()
        self._has_position = False

        self.logger.info(
            "动量因子策略初始化: code=%s lookback=%d",
            self._stock_code, self._lookback,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code != self._stock_code:
            return

        self._closes.append(bar.close)
        max_len = self._lookback + 2
        while len(self._closes) > max_len:
            self._closes.popleft()

        if len(self._closes) < self._lookback + 1:
            return

        closes = list(self._closes)
        momentum = closes[-1] / closes[-1 - self._lookback] - 1.0

        if momentum > 0 and not self._has_position:
            oid = self.submit_order(
                context, self._stock_code, self._trade_volume,
                bar.close, "buy", signal_id=f"mom_buy_{uuid.uuid4().hex[:8]}",
            )
            if oid:
                self._has_position = True
                self.logger.info("动量买入: mom=%.4f price=%.2f", momentum, bar.close)

        elif momentum < 0 and self._has_position:
            oid = self.submit_order(
                context, self._stock_code, self._trade_volume,
                bar.close, "sell", signal_id=f"mom_sell_{uuid.uuid4().hex[:8]}",
            )
            if oid:
                self._has_position = False
                self.logger.info("动量卖出: mom=%.4f price=%.2f", momentum, bar.close)

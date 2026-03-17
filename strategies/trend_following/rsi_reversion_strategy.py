"""
RSI 均值回归策略 —— 原生 BaseStrategy 实现

RSI 超卖时买入、超买时卖出。

迁移自引擎B ``management/backtest_engine.py::_generate_signal(reversion)``。
"""

from __future__ import annotations

import uuid
from collections import deque
from typing import Optional

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext


class RSIReversionStrategy(BaseStrategy):
    """
    RSI 均值回归策略。

    参数（通过 ``context.params`` 传入）：
        stock_code : str   目标股票代码
        rsi_period : int   RSI 计算周期（默认 14）
        rsi_lower  : float 超卖阈值（默认 30）
        rsi_upper  : float 超买阈值（默认 70）
        trade_volume : float 交易量（默认 1000）
    """

    def __init__(self, strategy_id: str = "rsi_reversion") -> None:
        super().__init__(strategy_id)
        self._stock_code = "000001.SZ"
        self._rsi_period = 14
        self._rsi_lower = 30.0
        self._rsi_upper = 70.0
        self._trade_volume = 1000.0

        self._closes: deque[float] = deque()
        self._has_position = False

    def on_init(self, context: StrategyContext) -> None:
        p = context.params
        self._stock_code = p.get("stock_code", p.get("股票代码", "000001.SZ"))
        self._rsi_period = int(p.get("rsi_period", p.get("RSI周期", 14)))
        self._rsi_lower = float(p.get("rsi_lower", p.get("超卖阈值", 30)))
        self._rsi_upper = float(p.get("rsi_upper", p.get("超买阈值", 70)))
        self._trade_volume = float(p.get("trade_volume", p.get("交易数量", 1000)))

        self._closes.clear()
        self._has_position = False

        self.logger.info(
            "RSI均值回归初始化: code=%s period=%d lower=%.0f upper=%.0f",
            self._stock_code, self._rsi_period, self._rsi_lower, self._rsi_upper,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code != self._stock_code:
            return

        self._closes.append(bar.close)
        max_len = self._rsi_period + 2
        while len(self._closes) > max_len:
            self._closes.popleft()

        if len(self._closes) < self._rsi_period + 1:
            return

        rsi = self._compute_rsi()
        if rsi is None:
            return

        if rsi < self._rsi_lower and not self._has_position:
            oid = self.submit_order(
                context, self._stock_code, self._trade_volume,
                bar.close, "buy", signal_id=f"rsi_buy_{uuid.uuid4().hex[:8]}",
            )
            if oid:
                self._has_position = True
                self.logger.info("RSI超卖买入: RSI=%.1f price=%.2f", rsi, bar.close)

        elif rsi > self._rsi_upper and self._has_position:
            oid = self.submit_order(
                context, self._stock_code, self._trade_volume,
                bar.close, "sell", signal_id=f"rsi_sell_{uuid.uuid4().hex[:8]}",
            )
            if oid:
                self._has_position = False
                self.logger.info("RSI超买卖出: RSI=%.1f price=%.2f", rsi, bar.close)

    def _compute_rsi(self) -> Optional[float]:
        closes = list(self._closes)
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        if len(deltas) < self._rsi_period:
            return None

        recent = deltas[-self._rsi_period:]
        gains = [d for d in recent if d > 0]
        losses = [-d for d in recent if d < 0]

        avg_gain = sum(gains) / self._rsi_period if gains else 0.0
        avg_loss = sum(losses) / self._rsi_period if losses else 0.0

        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

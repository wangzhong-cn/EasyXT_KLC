"""
固定网格策略 —— 原生 BaseStrategy 实现

在基准价格上下设置固定间距的网格，低买高卖。
支持动态重置已成交网格、最大持仓限制。

迁移自 strategies/grid_trading/固定网格.py (遗留 strategy_template 协议)。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext


@dataclass
class GridLevel:
    """单个网格层。"""
    level: int
    price: float
    direction: str       # "buy" | "sell"
    quantity: float
    filled: bool = False


class FixedGridStrategy(BaseStrategy):
    """
    固定网格交易策略。

    参数（通过 ``context.params`` 传入）：
        stock_code     : str   目标股票代码
        grid_count     : int   网格层数（默认 10）
        grid_spacing   : float 网格间距比例（默认 0.02 = 2%）
        base_price     : float 基准价格
        grid_quantity  : float 单网格交易量（默认 100）
        max_position   : float 最大持仓量（默认 10000）
        enable_dynamic : bool  启用动态重置（默认 False）
    """

    def __init__(self, strategy_id: str = "fixed_grid") -> None:
        super().__init__(strategy_id)
        self._stock_code = "000001.SZ"
        self._grid_count = 10
        self._grid_spacing = 0.02
        self._base_price = 10.0
        self._grid_quantity = 100.0
        self._max_position = 10000.0
        self._enable_dynamic = False

        self._grids: List[GridLevel] = []
        self._current_position: float = 0.0

    def on_init(self, context: StrategyContext) -> None:
        p = context.params
        self._stock_code = p.get("stock_code", p.get("股票代码", "000001.SZ"))
        self._grid_count = int(p.get("grid_count", p.get("网格数量", 10)))
        self._grid_spacing = float(p.get("grid_spacing", p.get("网格间距", 0.02)))
        self._base_price = float(p.get("base_price", p.get("基准价格", 10.0)))
        self._grid_quantity = float(p.get("grid_quantity", p.get("单网格数量", 100)))
        self._max_position = float(p.get("max_position", p.get("最大持仓", 10000)))
        self._enable_dynamic = bool(p.get("enable_dynamic", p.get("启用动态调整", False)))

        self._current_position = 0.0
        self._setup_grids()

        self.logger.info(
            "固定网格策略初始化: code=%s grids=%d spacing=%.1f%% base=%.2f",
            self._stock_code, self._grid_count, self._grid_spacing * 100, self._base_price,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code != self._stock_code:
            return

        price = bar.close

        # 查找并执行触发的网格
        for grid in self._grids:
            if grid.filled:
                continue

            if grid.direction == "buy" and price <= grid.price and self._current_position < self._max_position:
                oid = self.submit_order(
                    context, self._stock_code, grid.quantity,
                    grid.price, "buy", signal_id=f"grid_buy_L{grid.level}_{uuid.uuid4().hex[:8]}",
                )
                if oid:
                    self._current_position += grid.quantity
                    grid.filled = True
                    self.logger.info("网格买入: L%d %.0f股 @%.2f", grid.level, grid.quantity, grid.price)

            elif grid.direction == "sell" and price >= grid.price and self._current_position > 0:
                sell_qty = min(grid.quantity, self._current_position)
                oid = self.submit_order(
                    context, self._stock_code, sell_qty,
                    grid.price, "sell", signal_id=f"grid_sell_L{grid.level}_{uuid.uuid4().hex[:8]}",
                )
                if oid:
                    self._current_position -= sell_qty
                    grid.filled = True
                    self.logger.info("网格卖出: L%d %.0f股 @%.2f", grid.level, sell_qty, grid.price)

        # 动态重置已成交网格
        if self._enable_dynamic:
            self._reset_filled_grids(price)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _setup_grids(self) -> None:
        self._grids.clear()
        half = self._grid_count // 2
        for i in range(-half, half + 1):
            if i == 0:
                continue
            grid_price = self._base_price * (1 + i * self._grid_spacing)
            self._grids.append(GridLevel(
                level=i,
                price=grid_price,
                direction="buy" if i < 0 else "sell",
                quantity=self._grid_quantity,
            ))
        self._grids.sort(key=lambda g: g.price)

    def _reset_filled_grids(self, current_price: float) -> None:
        for grid in self._grids:
            if not grid.filled:
                continue
            threshold = self._grid_spacing * 0.5
            if grid.direction == "buy" and current_price > grid.price * (1 + threshold):
                grid.filled = False
            elif grid.direction == "sell" and current_price < grid.price * (1 - threshold):
                grid.filled = False

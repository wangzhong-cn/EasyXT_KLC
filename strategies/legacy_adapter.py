"""
遗留策略适配器

将 ``strategy_template.BaseStrategy`` 协议（initialize / on_data）
桥接到 Phase 1 ``base_strategy.BaseStrategy`` 协议（on_init / on_bar）,
使遗留策略无需修改即可被 StrategyRunner / BacktestEngine(A) 驱动。

用法::

    from strategies.legacy_adapter import LegacyStrategyAdapter
    from strategies.trend_following.双均线策略 import 双均线策略 as Legacy双均线

    adapted = LegacyStrategyAdapter(
        legacy_cls=Legacy双均线,
        strategy_id="dual_ma_001",
        params={"股票代码": "000001.SZ", "短期均线": 5, "长期均线": 20},
    )
    # adapted 是 BaseStrategy 子类，可直接交给 StrategyRunner
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Optional, Type

import pandas as pd

from strategies.base_strategy import BaseStrategy, BarData, StrategyContext

warnings.warn(
    "LegacyStrategyAdapter 仅作为过渡工具，请尽快将遗留策略迁移到 BaseStrategy 协议。",
    DeprecationWarning,
    stacklevel=1,
)


class LegacyStrategyAdapter(BaseStrategy):
    """
    将遗留 ``strategy_template.BaseStrategy`` 子类包装为
    Phase 1 ``BaseStrategy`` 实例。

    适配逻辑：
    - ``on_init``  → 调用遗留策略的 ``initialize()``
    - ``on_bar``   → 将 BarData 转为 DataFrame 行，调用遗留策略的 ``on_data()``
    - ``on_order`` → 调用遗留策略的 ``on_order()``（如存在）
    - 下单         → 将遗留的 ``buy/sell`` 重定向到 ``context.executor``
    """

    def __init__(
        self,
        legacy_cls: Type[Any],
        strategy_id: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(strategy_id)
        self._legacy = legacy_cls(params)
        self._bar_history: list[dict[str, Any]] = []

    # ---- lifecycle hooks ----

    def on_init(self, context: StrategyContext) -> None:
        # 把 context.executor 注入遗留策略的 buy/sell，
        # 使下单走 RiskEngine + AuditTrail 闭环
        self._patch_legacy_order_methods(context)
        self._legacy.initialize()

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        # 累积 bar 构造 DataFrame（遗留策略 on_data 需要 DataFrame）
        self._bar_history.append({
            "close": bar.close,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "volume": bar.volume,
            "time": bar.time,
        })
        df = pd.DataFrame(self._bar_history)
        self._legacy.on_data(df)

    def on_order(self, context: StrategyContext, order: Any) -> None:
        if hasattr(self._legacy, "on_order"):
            self._legacy.on_order(order)

    # ---- 将遗留 buy/sell 重定向到统一执行器 ----

    def _patch_legacy_order_methods(self, context: StrategyContext) -> None:
        """用闭包替换遗留策略的 buy/sell，走 context.executor 统一路径。"""
        adapter = self

        def _buy(stock_code: str, quantity: int, price: float | None = None) -> Any:
            p = price if price is not None else 0.0
            return adapter.submit_order(
                context, stock_code, float(quantity), p, "buy"
            )

        def _sell(stock_code: str, quantity: int, price: float | None = None) -> Any:
            p = price if price is not None else 0.0
            return adapter.submit_order(
                context, stock_code, float(quantity), p, "sell"
            )

        self._legacy.buy = _buy  # type: ignore[assignment]
        self._legacy.sell = _sell  # type: ignore[assignment]

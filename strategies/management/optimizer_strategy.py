"""
组合优化再平衡策略（Phase 2）

继承 :class:`strategies.base_strategy.BaseStrategy`，实现"周期性运行
PortfolioOptimizer → PortfolioRiskAnalyzer → 提交再平衡委托"的完整流程。

设计说明：
- 每根 K 线驱动时收集各标的收盘价。
- 检测到时间戳变换（新交易日）时"推进"前一日：追加价格序列、递增日计数。
- 满足 ``lookback + 1`` 条价格后，每隔 ``rebalance_every`` 个交易日触发一次优化。
- 优化/风控任一不通过则禁止下单并写 WARNING 日志（审计可追溯）。
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from strategies.base_strategy import BarData, BaseStrategy, StrategyContext

log = logging.getLogger(__name__)


class PortfolioOptimizerStrategy(BaseStrategy):
    """周期性组合权重优化再平衡策略。

    Args:
        strategy_id:        策略唯一标识。
        codes:              标的代码列表（与回测引擎使用的 codes 一致）。
        lookback:           优化器所需历史收益率天数（默认 60）。
        rebalance_every:    每隔多少个交易日触发一次再平衡（默认 20）。
        max_single_weight:  单仓风控上限（默认 0.3）。
        max_hhi:            HHI 集中度上限（默认 0.2）。
        optimizer_config:   :class:`core.portfolio_optimizer.PortfolioOptimizeConfig`，
                            ``None`` 时使用默认配置（risk_parity）。
    """

    def __init__(
        self,
        strategy_id: str,
        codes: list[str],
        lookback: int = 60,
        rebalance_every: int = 20,
        max_single_weight: float = 0.3,
        max_hhi: float = 0.2,
        optimizer_config: Any = None,
    ) -> None:
        super().__init__(strategy_id)
        self._codes = list(codes)
        self._lookback = lookback
        self._rebalance_every = rebalance_every
        self._max_single_weight = max_single_weight
        self._max_hhi = max_hhi
        self._optimizer_config = optimizer_config

        # 运行时状态
        self._price_series: dict[str, list[float]] = {c: [] for c in self._codes}
        self._day_count: int = 0
        self._last_ts: Any = None
        self._pending_closes: dict[str, float] = {}   # 当前 bar 各标的收盘价（待推进）

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_init(self, context: StrategyContext) -> None:
        self.logger.info(
            "[%s] 初始化：codes=%s lookback=%d rebalance_every=%d",
            self.strategy_id, self._codes, self._lookback, self._rebalance_every,
        )

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        if bar.code not in self._codes:
            return

        ts = bar.time

        # 检测到新时间戳 → 推进上一日
        if ts != self._last_ts:
            if self._last_ts is not None:
                self._advance_day(context)
            self._last_ts = ts
            self._pending_closes = {}

        self._pending_closes[bar.code] = bar.close

    def on_stop(self, context: StrategyContext) -> None:
        # 推进最后一日（避免 off-by-one 遗漏末日数据）
        if self._pending_closes:
            self._advance_day(context)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _advance_day(self, context: StrategyContext) -> None:
        """以 ``_pending_closes`` 中的收盘价推进价格序列，检查是否触发再平衡。"""
        for code in self._codes:
            price = self._pending_closes.get(code)
            if price is not None and price > 0:
                self._price_series[code].append(price)

        self._day_count += 1

        # 触发条件：足够的历史数据 + 到达再平衡周期
        min_prices = min(len(v) for v in self._price_series.values())
        if (
            min_prices >= self._lookback + 1
            and self._day_count % self._rebalance_every == 0
        ):
            self._run_rebalance(context)

    def _build_returns(self) -> pd.DataFrame:
        """从价格序列构造历史收益率 DataFrame。"""
        closes = {
            code: prices[-(self._lookback + 1):]
            for code, prices in self._price_series.items()
        }
        return pd.DataFrame(closes).pct_change().dropna()

    def _run_rebalance(self, context: StrategyContext) -> None:
        """运行优化器 → 风控校验 → 提交委托（任一不通过则禁止下单）。"""
        from core.portfolio_optimizer import PortfolioOptimizer
        from core.portfolio_risk import PortfolioRiskAnalyzer

        returns = self._build_returns()

        optimizer = PortfolioOptimizer(self._optimizer_config)
        opt_result = optimizer.optimize_result(returns)

        if not opt_result.feasible:
            self.logger.warning(
                "[%s] 优化不可行，禁止再平衡 [day=%d] status=%s warnings=%s",
                self.strategy_id, self._day_count,
                opt_result.status, opt_result.warnings,
            )
            return

        analyzer = PortfolioRiskAnalyzer()
        risk_check = analyzer.check_optimal_weights(
            opt_result.weights,
            max_single_weight=self._max_single_weight,
            max_hhi=self._max_hhi,
        )

        if not risk_check.feasible:
            self.logger.warning(
                "[%s] 风控校验不通过，禁止再平衡 [day=%d] warnings=%s",
                self.strategy_id, self._day_count, risk_check.warnings,
            )
            return

        self.logger.info(
            "[%s] 触发再平衡 [day=%d] method=%s vol=%.4f",
            self.strategy_id, self._day_count,
            opt_result.method, opt_result.portfolio_vol,
        )
        self._submit_rebalance_orders(context, opt_result.weights)

    def _submit_rebalance_orders(
        self,
        context: StrategyContext,
        target_weights: dict[str, float],
    ) -> None:
        """按目标权重 vs 当前持仓计算 delta，提交买/卖委托。"""
        if context.executor is None:
            self.logger.warning("[%s] executor=None，无法提交再平衡委托", self.strategy_id)
            return

        nav = context.nav
        if nav <= 0:
            return

        for code, target_weight in target_weights.items():
            target_value = nav * target_weight
            current_value = context.positions.get(code, 0.0)
            delta_value = target_value - current_value

            # 优先使用 pending_closes 中最新价，否则回退到 price_series 末项
            current_price: float = self._pending_closes.get(code) or (
                self._price_series[code][-1] if self._price_series.get(code) else 0.0
            )
            if current_price <= 0:
                continue

            volume = delta_value / current_price
            if abs(volume) < 1:
                continue

            direction = "buy" if volume > 0 else "sell"
            context.executor.submit_order(
                code=code,
                volume=abs(volume),
                price=current_price,
                direction=direction,
                signal_id=f"opt:{self.strategy_id}:{self._day_count}",
            )

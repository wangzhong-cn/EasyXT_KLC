"""
strategies/management/optimizer_strategy.py

基于投资组合优化的再平衡策略。

策略逻辑：
  1. 收集各标的历史 close 价格序列（滚动窗口 lookback 天）
  2. 每 rebalance_every 天调用一次 PortfolioOptimizer.optimize_result
  3. 通过 PortfolioRiskAnalyzer.check_optimal_weights 进行风控校验
  4. 校验通过且优化可行时，按目标权重计算差值并提交委托

时序约定（测试对齐）：
  - on_bar 收到 **新一天**（时间戳与上一 bar 不同）的第 1 根 bar 时 advance_day
  - day_count 从 1 开始计数
  - 再平衡触发条件：day_count >= lookback + 1  AND  day_count % rebalance_every == 0
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional

import pandas as pd

from core.portfolio_optimizer import PortfolioOptimizeConfig, PortfolioOptimizer
from core.portfolio_risk import PortfolioRiskAnalyzer
from strategies.base_strategy import BarData, BaseStrategy, OrderData, StrategyContext

log = logging.getLogger(__name__)

_MIN_TRADE_UNIT = 100


class PortfolioOptimizerStrategy(BaseStrategy):
    """
    基于量化优化的多标的再平衡策略。

    Args:
        strategy_id:      策略唯一标识。
        codes:            标的代码列表。
        lookback:         计算收益率所需的最少历史天数。
        rebalance_every:  再平衡周期（trading days）。
        opt_config:       PortfolioOptimizeConfig 实例（可选）。
        max_single_weight: 单仓最大权重约束（风控）。
        max_hhi:          HHI 约束（风控）。
    """

    def __init__(
        self,
        strategy_id: str,
        codes: List[str],
        lookback: int = 20,
        rebalance_every: int = 5,
        opt_config: Optional[PortfolioOptimizeConfig] = None,
        max_single_weight: float = 0.3,
        max_hhi: float = 0.25,
    ) -> None:
        super().__init__(strategy_id=strategy_id)
        self.codes = list(codes)
        self.lookback = lookback
        self.rebalance_every = rebalance_every
        self.opt_config = opt_config or PortfolioOptimizeConfig(
            method="risk_parity",
            max_weight=max_single_weight,
        )
        self.max_single_weight = max_single_weight
        self.max_hhi = max_hhi

        # 内部状态
        self._price_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=lookback + 10)
        )
        self._day_count: int = 0
        self._last_ts: Optional[int] = None
        self._optimizer = PortfolioOptimizer(self.opt_config)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_init(self, context: StrategyContext) -> None:
        self._price_history.clear()
        self._day_count = 0
        self._last_ts = None
        for code in self.codes:
            self._price_history[code]  # 初始化 deque

    def on_bar(self, context: StrategyContext, bar: BarData) -> None:
        # 检测新的一天（时间戳变化 → advance_day）
        if bar.time != self._last_ts:
            if self._last_ts is not None:
                self._day_count += 1
                if self._should_rebalance():
                    self._do_rebalance(context)
            self._last_ts = bar.time

        # 记录价格
        if bar.close > 0:
            self._price_history[bar.code].append(bar.close)

    def on_order(self, context: StrategyContext, order: OrderData) -> None:
        pass

    def on_stop(self, context: StrategyContext) -> None:
        pass

    # ------------------------------------------------------------------
    # 内部逻辑
    # ------------------------------------------------------------------

    def _should_rebalance(self) -> bool:
        """判断当前 day_count 是否触发再平衡。"""
        if self._day_count < self.lookback + 1:
            return False
        return self._day_count % self.rebalance_every == 0

    def _build_returns_df(self) -> Optional[pd.DataFrame]:
        """从价格历史构造日收益率 DataFrame。"""
        min_len = min(len(h) for h in self._price_history.values()) if self._price_history else 0
        if min_len < self.lookback + 1:
            return None

        data: Dict[str, List[float]] = {}
        for code in self.codes:
            hist = list(self._price_history[code])[-( self.lookback + 1):]
            if len(hist) < 2:
                return None
            rets = [(hist[i] - hist[i - 1]) / hist[i - 1] for i in range(1, len(hist))]
            data[code] = rets

        lengths = {len(v) for v in data.values()}
        if len(lengths) != 1:
            return None

        return pd.DataFrame(data)

    def _do_rebalance(self, context: StrategyContext) -> None:
        """执行一次再平衡。"""
        returns = self._build_returns_df()
        if returns is None:
            log.debug("[%s] 数据不足，跳过再平衡", self.strategy_id)
            return

        opt_result = self._optimizer.optimize_result(returns)
        if not opt_result.feasible:
            log.info("[%s] 优化不可行 (status=%s)，跳过再平衡", self.strategy_id, opt_result.status)
            return

        risk_check = PortfolioRiskAnalyzer.check_optimal_weights(
            opt_result.weights,
            max_single_weight=self.max_single_weight,
            max_hhi=self.max_hhi,
        )
        if not risk_check.feasible:
            log.warning("[%s] 风控校验未通过：%s", self.strategy_id, risk_check.warnings)
            return

        if context.executor is None:
            log.warning("[%s] executor 为 None，跳过下单", self.strategy_id)
            return

        self._submit_rebalance_orders(context, opt_result.weights)

    def _submit_rebalance_orders(
        self,
        context: StrategyContext,
        target_weights: Dict[str, float],
    ) -> None:
        """根据目标权重与当前持仓差值提交委托。"""
        nav = context.nav
        if nav <= 0:
            return

        for code, target_w in target_weights.items():
            target_value = nav * target_w
            current_value = context.positions.get(code, 0.0)
            diff = target_value - current_value

            # 获取当前价格（使用最新历史价格作为参考价）
            hist = self._price_history.get(code)
            ref_price = hist[-1] if hist else 0.0
            if ref_price <= 0:
                continue

            volume_raw = abs(diff) / ref_price
            volume = int(volume_raw / _MIN_TRADE_UNIT) * _MIN_TRADE_UNIT
            if volume < _MIN_TRADE_UNIT:
                continue

            direction = "buy" if diff > 0 else "sell"
            context.executor.submit_order(
                code=code,
                volume=float(volume),
                price=ref_price,
                direction=direction,
            )
            log.debug(
                "[%s] 委托 %s %s %d @ %.2f（diff=%.0f）",
                self.strategy_id, direction, code, volume, ref_price, diff,
            )

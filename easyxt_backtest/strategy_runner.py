"""
策略回测驱动入口

将 BaseStrategy 的生命周期与 BacktestEngine 对接，提供一行启动的高层 API。

使用示例::

    from easyxt_backtest.strategy_runner import StrategyRunner
    from strategies.my_strategy import MyStrategy

    result = StrategyRunner(
        strategy=MyStrategy("demo"),
        codes=["000001.SZ", "600000.SH"],
        start_date="2022-01-01",
        end_date="2024-12-31",
    ).run()

    print(f"总收益率: {result.metrics['total_return']:.2%}")
    print(f"夏普比率: {result.metrics['sharpe']:.2f}")
    print(f"最大回撤: {result.metrics['max_drawdown']:.2%}")
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from strategies.base_strategy import BaseStrategy
from easyxt_backtest.engine import BacktestConfig, BacktestEngine, BacktestResult

log = logging.getLogger(__name__)


class StrategyRunner:
    """
    策略回测统一驱动器。

    负责：
      - 按参数实例化 BacktestEngine
      - 可选接入 RiskEngine 预交易风控
      - 可选接入 AuditTrail（backtest 模式，strategy_id 前缀 ``bt:``）
      - 调用 engine.run() 并返回 BacktestResult
    """

    def __init__(
        self,
        strategy: BaseStrategy,
        codes: List[str],
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "qfq",
        config: Optional[BacktestConfig] = None,
        duckdb_path: Optional[str] = None,
        enable_risk_engine: bool = True,
        enable_audit_trail: bool = False,
    ) -> None:
        """
        Args:
            strategy:           继承 BaseStrategy 的策略实例
            codes:              股票代码列表（如 ["000001.SZ"]）
            start_date:         回测开始日期（"YYYY-MM-DD"）
            end_date:           回测结束日期（"YYYY-MM-DD"）
            period:             K 线周期（"1d" / "60m" / "1m" 等）
            adjust:             复权类型（"qfq" / "hfq" / "none"）
            config:             BacktestConfig，不传使用默认值
            duckdb_path:        DuckDB 路径，不传走 resolve_duckdb_path 逻辑
            enable_risk_engine: 是否接入 RiskEngine 预交易风控
            enable_audit_trail: 是否写 AuditTrail（生产级合规要求时开启）
        """
        self.strategy = strategy
        self.codes = codes
        self.start_date = start_date
        self.end_date = end_date
        self.period = period
        self.adjust = adjust
        self.config = config or BacktestConfig()
        self.duckdb_path = duckdb_path
        self.enable_risk_engine = enable_risk_engine
        self.enable_audit_trail = enable_audit_trail

    def _build_risk_engine(self) -> Optional[Any]:
        if not self.enable_risk_engine:
            return None
        try:
            from core.risk_engine import RiskEngine

            return RiskEngine()
        except Exception:
            log.warning("RiskEngine 初始化失败，回测将跳过风控检查")
            return None

    def _build_audit_trail(self) -> Optional[Any]:
        if not self.enable_audit_trail:
            return None
        try:
            from core.audit_trail import AuditTrail

            at = AuditTrail(duckdb_path=self.duckdb_path)
            at.ensure_tables()
            return at
        except Exception:
            log.warning("AuditTrail 初始化失败，回测将跳过审计链写入")
            return None

    def run(self) -> BacktestResult:
        """执行回测，返回 :class:`~easyxt_backtest.engine.BacktestResult`。"""
        risk_engine = self._build_risk_engine()
        audit_trail = self._build_audit_trail()

        engine = BacktestEngine(
            config=self.config,
            duckdb_path=self.duckdb_path,
            risk_engine=risk_engine,
            audit_trail=audit_trail,
        )

        log.info(
            "回测启动: strategy=%s codes=%s %s~%s period=%s capital=%.0f",
            self.strategy.strategy_id,
            self.codes,
            self.start_date,
            self.end_date,
            self.period,
            self.config.initial_capital,
        )

        result = engine.run(
            strategy=self.strategy,
            codes=self.codes,
            start_date=self.start_date,
            end_date=self.end_date,
            period=self.period,
            adjust=self.adjust,
        )

        log.info(
            "回测完成: strategy=%s total_return=%.2f%% sharpe=%.2f max_dd=%.2f%% trades=%d",
            self.strategy.strategy_id,
            result.metrics.get("total_return", 0) * 100,
            result.metrics.get("sharpe", 0),
            result.metrics.get("max_drawdown", 0) * 100,
            result.metrics.get("trade_count", 0),
        )

        return result

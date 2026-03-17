"""
策略控制器 —— 统一编排入口

协调 StrategyManager（配置CRUD）→ strategy_factory（实例化）→
StrategyRegistry（运行时注册）→ StrategyRunner（回测执行）的完整管线。

用法::

    from strategies.strategy_controller import StrategyController

    ctrl = StrategyController()

    # 从已有配置运行回测
    result = ctrl.run_backtest(
        strategy_id="xxx-uuid",
        codes=["000001.SZ"],
        start_date="2023-01-01",
        end_date="2024-12-31",
    )

    # 从 strategy_type 直接运行（跳过 StrategyManager）
    result = ctrl.run_backtest_by_type(
        strategy_type="trend",
        codes=["000001.SZ"],
        start_date="2023-01-01",
        end_date="2024-12-31",
        params={"short_period": 5, "long_period": 20},
    )
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


class StrategyController:
    """
    策略全生命周期控制器。

    职责：
      1. 从 StrategyManager 加载配置
      2. 通过 strategy_factory 创建 BaseStrategy 实例
      3. 在 StrategyRegistry 注册/注销
      4. 通过 StrategyRunner 执行回测
      5. 回测结束后自动更新注册状态 + 可选快照
    """

    def __init__(
        self,
        config_dir: str = "strategies/configs",
        duckdb_path: Optional[str] = None,
        enable_risk_engine: bool = True,
        enable_audit_trail: bool = False,
        risk_config: Optional[Any] = None,
    ) -> None:
        self._config_dir = config_dir
        self._duckdb_path = duckdb_path
        self._enable_risk = enable_risk_engine
        self._enable_audit = enable_audit_trail
        self._risk_config = risk_config

        # 延迟初始化，避免循环导入
        self._manager = None
        self._registry = None

    @property
    def manager(self):
        if self._manager is None:
            from strategies.management.strategy_manager import StrategyManager
            self._manager = StrategyManager(config_dir=self._config_dir)
        return self._manager

    @property
    def registry(self):
        if self._registry is None:
            from strategies.registry import strategy_registry
            self._registry = strategy_registry
        return self._registry

    # ------------------------------------------------------------------
    # 核心 API
    # ------------------------------------------------------------------

    def run_backtest(
        self,
        strategy_id: str,
        codes: List[str],
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "qfq",
        param_overrides: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        从 StrategyManager 加载配置 → 创建策略 → 注册 → 回测 → 注销。

        Args:
            strategy_id: StrategyManager 中已有的策略 ID
            codes:       回测标的列表
            start_date:  回测开始日期
            end_date:    回测结束日期
            period:      K 线周期
            adjust:      复权类型
            param_overrides: 参数覆盖（可选，优先于配置文件）

        Returns:
            BacktestResult

        Raises:
            ValueError: 策略不存在或类型不支持
        """
        config = self.manager.get_strategy(strategy_id)
        if config is None:
            raise ValueError(f"策略 {strategy_id} 不存在")

        params = dict(config.parameters)
        if param_overrides:
            params.update(param_overrides)

        return self._execute(
            strategy_type=config.strategy_type,
            strategy_id=config.strategy_id,
            codes=codes or config.symbols,
            start_date=start_date,
            end_date=end_date,
            period=period or config.period,
            adjust=adjust,
            params=params,
            risk_controls=dict(config.risk_controls),
        )

    def run_backtest_by_type(
        self,
        strategy_type: str,
        codes: List[str],
        start_date: str,
        end_date: str,
        period: str = "1d",
        adjust: str = "qfq",
        params: Optional[Dict[str, Any]] = None,
        strategy_id: Optional[str] = None,
    ) -> Any:
        """
        直接按策略类型运行回测（无需预先在 StrategyManager 创建配置）。

        Args:
            strategy_type: "trend" | "reversion" | "grid" | "conditional" | "factor"
            codes:         回测标的列表
            start_date:    回测开始日期
            end_date:      回测结束日期
            params:        策略参数
            strategy_id:   可选自定义 ID，不传则自动生成

        Returns:
            BacktestResult
        """
        import uuid
        sid = strategy_id or f"{strategy_type}_{uuid.uuid4().hex[:8]}"
        return self._execute(
            strategy_type=strategy_type,
            strategy_id=sid,
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            period=period,
            adjust=adjust,
            params=params or {},
        )

    def list_strategies(self) -> List[Dict[str, Any]]:
        """列出 StrategyManager 中所有已保存的策略配置。"""
        return self.manager.list_strategies()

    def list_running(self) -> List[Dict[str, Any]]:
        """列出 StrategyRegistry 中运行中的策略。"""
        return [
            {
                "strategy_id": info.strategy_id,
                "account_id": info.account_id,
                "status": info.status,
                "tags": info.tags,
            }
            for info in self.registry.list_running()
        ]

    # ------------------------------------------------------------------
    # 内部编排
    # ------------------------------------------------------------------

    def _execute(
        self,
        strategy_type: str,
        strategy_id: str,
        codes: List[str],
        start_date: str,
        end_date: str,
        period: str,
        adjust: str,
        params: Dict[str, Any],
        risk_controls: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """工厂创建 → 注册 → 回测 → 注销。"""
        from strategies.strategy_factory import create_strategy_from_config

        # 构造轻量 config 供工厂
        class _Cfg:
            pass
        cfg = _Cfg()
        cfg.strategy_type = strategy_type  # type: ignore[attr-defined]
        cfg.strategy_id = strategy_id  # type: ignore[attr-defined]
        cfg.parameters = params  # type: ignore[attr-defined]

        strategy = create_strategy_from_config(cfg)

        # 注册到 Registry
        self.registry.register(
            strategy_id=strategy_id,
            strategy_obj=strategy,
            params=params,
            tags=[strategy_type, "backtest"],
        )

        try:
            from easyxt_backtest.strategy_runner import StrategyRunner
            from easyxt_backtest.engine import BacktestConfig

            bt_config = BacktestConfig()
            if risk_controls:
                # 将策略级风控参数传递给 risk_config
                rc = dict(self._risk_config or {}) if isinstance(self._risk_config, dict) else {}
                rc.update(risk_controls)
            else:
                rc = self._risk_config

            runner = StrategyRunner(
                strategy=strategy,
                codes=codes,
                start_date=start_date,
                end_date=end_date,
                period=period,
                adjust=adjust,
                config=bt_config,
                duckdb_path=self._duckdb_path,
                enable_risk_engine=self._enable_risk,
                enable_audit_trail=self._enable_audit,
                risk_config=rc,
            )

            result = runner.run()
            self.registry.unregister(strategy_id, status="stopped")
            log.info(
                "StrategyController 回测完成: %s type=%s codes=%s",
                strategy_id, strategy_type, codes,
            )
            return result

        except Exception:
            self.registry.unregister(strategy_id, status="error")
            raise

    # ------------------------------------------------------------------
    # Stage1 验收集成
    # ------------------------------------------------------------------

    def validate(
        self,
        strategy_name: str,
        symbol: str,
        start_date: str,
        end_date: str,
        oos_split: str,
        short_period: int = 5,
        long_period: int = 20,
        benchmark: str = "CSI300",
        dry_run: bool = False,
    ) -> Any:
        """
        通过 Stage1 四关验收流程评测策略。

        在正式回测前调用，确保策略通过数据验收、样本内外对比、参数敏感性等基线。

        Args:
            strategy_name: 策略名称（用于输出标识）
            symbol:        标的代码
            start_date:    评测起始日
            end_date:      评测结束日
            oos_split:     样本内外分割日
            short_period:  短期均线周期
            long_period:   长期均线周期
            benchmark:     基准指数 ("CSI300" / "CSI500" / "none")
            dry_run:       仅数据验收

        Returns:
            Stage1Result
        """
        from strategies.stage1_pipeline import Stage1Runner

        runner = Stage1Runner(
            strategy=strategy_name,
            symbol=symbol,
            start=start_date,
            end=end_date,
            oos_split=oos_split,
            short_period=short_period,
            long_period=long_period,
            benchmark=benchmark,
            dry_run=dry_run,
        )
        return runner.run()

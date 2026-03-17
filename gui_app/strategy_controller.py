"""
strategy_controller.py — 策略管理控制器（无 Qt 依赖）
======================================================

将 UI 层（StrategyGovernancePanel）与策略后端解耦。
所有业务判断集中于此，可 100% 单元测试。

核心职责：
1. 策略 CRUD（依赖 StrategyManager）
2. 触发回测执行（依赖 BacktestEngine）
3. 提取绩效指标（Sharpe / MaxDD / CAGR / Calmar / WinRate）
4. 交易记录查询 & 资金曲线提取
5. 回测历史管理
"""
from __future__ import annotations

import importlib
import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)


# ─── 延迟导入帮助函数 ──────────────────────────────────────────────────────


def _safe_import(module_path: str, class_name: Optional[str] = None) -> Any:
    """尝试导入模块或类，失败时返回 None。"""
    try:
        mod = importlib.import_module(module_path)
        if class_name:
            return getattr(mod, class_name, None)
        return mod
    except Exception as exc:
        log.debug("_safe_import(%s, %s) failed: %s", module_path, class_name, exc)
        return None


# ─── 控制器主类 ────────────────────────────────────────────────────────────


class StrategyController:
    """纯 Python 策略管理控制器，协调策略管理与回测子系统。

    支持依赖注入（便于单元测试）::

        ctrl = StrategyController(
            strategy_manager=MockManager(),
            backtest_engine=MockEngine(),
        )
    """

    def __init__(
        self,
        strategy_manager: Any = None,
        backtest_engine: Any = None,
        results_dir: Optional[str] = None,
    ) -> None:
        self._strategy_manager = strategy_manager
        self._backtest_engine = backtest_engine
        self._results_dir = Path(results_dir or "strategies/results")

    # ------------------------------------------------------------------
    # 内部：延迟获取依赖
    # ------------------------------------------------------------------

    def _get_strategy_manager(self) -> Any:
        if self._strategy_manager is not None:
            return self._strategy_manager
        sm_mod = _safe_import("strategies.management.strategy_manager")
        if sm_mod is not None:
            mgr = getattr(sm_mod, "strategy_manager", None)
            if mgr is not None:
                return mgr
        return None

    def _get_backtest_engine(self) -> Any:
        if self._backtest_engine is not None:
            return self._backtest_engine
        be_mod = _safe_import("strategies.management.backtest_engine")
        if be_mod is not None:
            eng = getattr(be_mod, "backtest_engine", None)
            if eng is not None:
                return eng
        return None

    # ------------------------------------------------------------------
    # 1. 策略 CRUD
    # ------------------------------------------------------------------

    def get_all_strategies(self) -> list[dict[str, Any]]:
        """返回所有策略配置的字典列表。

        每条记录包含::
            strategy_id, strategy_name, strategy_type, created_at,
            version, symbols_count, period, backtest_range
        """
        try:
            mgr = self._get_strategy_manager()
            if mgr is None:
                return []
            return mgr.list_strategies()
        except Exception as exc:
            log.warning("get_all_strategies failed: %s", exc)
            return []

    def get_strategy(self, strategy_id: str) -> Optional[dict[str, Any]]:
        """返回单条策略的完整配置字典，未找到时返回 None。"""
        try:
            mgr = self._get_strategy_manager()
            if mgr is None:
                return None
            cfg = mgr.get_strategy(strategy_id)
            if cfg is None:
                return None
            if hasattr(cfg, "model_dump"):
                return cfg.model_dump()
            return dict(cfg)
        except Exception as exc:
            log.warning("get_strategy(%s) failed: %s", strategy_id, exc)
            return None

    def create_strategy(self, config_data: dict[str, Any]) -> dict[str, Any]:
        """创建新策略。

        Returns::
            {"ok": True, "strategy_id": "xxx"}
            {"ok": False, "error": "..."}
        """
        try:
            mgr = self._get_strategy_manager()
            if mgr is None:
                return {"ok": False, "error": "策略管理器不可用"}
            strategy_id = mgr.create_strategy(config_data)
            return {"ok": True, "strategy_id": strategy_id}
        except Exception as exc:
            log.error("create_strategy failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    def delete_strategy(self, strategy_id: str) -> dict[str, Any]:
        """删除策略。

        Returns::
            {"ok": True}  或  {"ok": False, "error": "..."}
        """
        try:
            mgr = self._get_strategy_manager()
            if mgr is None:
                return {"ok": False, "error": "策略管理器不可用"}
            success = mgr.delete_strategy(strategy_id)
            if success:
                return {"ok": True}
            return {"ok": False, "error": "删除失败，策略不存在"}
        except Exception as exc:
            log.error("delete_strategy(%s) failed: %s", strategy_id, exc)
            return {"ok": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # 2. 回测执行
    # ------------------------------------------------------------------

    def run_backtest(self, strategy_id: str) -> dict[str, Any]:
        """对指定策略执行回测。

        Returns::
            {
                "ok": True,
                "backtest_id": "...",
                "performance_metrics": {...},
                "equity_curve": {"dates": [...], "values": [...]},
                "trades": [...],
                "elapsed_sec": 1.23,
            }  或  {"ok": False, "error": "..."}
        """
        t0 = time.perf_counter()
        try:
            mgr = self._get_strategy_manager()
            eng = self._get_backtest_engine()
            if mgr is None:
                return {"ok": False, "error": "策略管理器不可用"}
            if eng is None:
                return {"ok": False, "error": "回测引擎不可用"}

            strategy_config = mgr.get_strategy(strategy_id)
            if strategy_config is None:
                return {"ok": False, "error": f"策略 {strategy_id} 不存在"}

            result = eng.run_backtest(strategy_config)

            equity_curve = self._extract_equity_curve(result)
            trades = self._extract_trades(result)
            perf = result.performance_metrics if hasattr(result, "performance_metrics") else {}

            elapsed = round(time.perf_counter() - t0, 3)
            return {
                "ok": True,
                "backtest_id": getattr(result, "backtest_id", ""),
                "performance_metrics": dict(perf),
                "equity_curve": equity_curve,
                "trades": trades,
                "elapsed_sec": elapsed,
            }
        except Exception as exc:
            log.error("run_backtest(%s) failed: %s", strategy_id, exc)
            return {"ok": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # 3. 绩效指标
    # ------------------------------------------------------------------

    def get_performance_summary(
        self, performance_metrics: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """将 performance_metrics 字典转换为 UI 绩效指标卡片数据。

        Returns: 列表，每项::
            {"label": "夏普比率", "value": "1.23", "color": "#4CAF50"}
        """
        def _fmt_pct(v: Any, decimals: int = 2) -> str:
            try:
                return f"{float(v) * 100:.{decimals}f}%"
            except Exception:
                return "N/A"

        def _fmt_f(v: Any, decimals: int = 4) -> str:
            try:
                return f"{float(v):.{decimals}f}"
            except Exception:
                return "N/A"

        def _color_pct(v: Any, positive_good: bool = True) -> str:
            try:
                fv = float(v)
                if fv == 0:
                    return "#999999"
                if positive_good:
                    return "#4CAF50" if fv > 0 else "#F44336"
                else:
                    return "#F44336" if fv > 0 else "#4CAF50"
            except Exception:
                return "#999999"

        m = performance_metrics
        rows = [
            {
                "label": "总收益率",
                "value": _fmt_pct(m.get("total_return", 0)),
                "color": _color_pct(m.get("total_return", 0)),
                "key": "total_return",
            },
            {
                "label": "年化收益(CAGR)",
                "value": _fmt_pct(m.get("cagr", m.get("annualized_return", 0))),
                "color": _color_pct(m.get("cagr", m.get("annualized_return", 0))),
                "key": "cagr",
            },
            {
                "label": "夏普比率",
                "value": _fmt_f(m.get("sharpe_ratio", 0)),
                "color": "#4CAF50" if float(m.get("sharpe_ratio", 0) or 0) >= 1.0 else "#FF9800",
                "key": "sharpe_ratio",
            },
            {
                "label": "最大回撤",
                "value": _fmt_pct(m.get("max_drawdown", 0), 2),
                "color": "#F44336" if float(m.get("max_drawdown", 0) or 0) > 0.1 else "#FF9800",
                "key": "max_drawdown",
            },
            {
                "label": "Calmar 比率",
                "value": _fmt_f(m.get("calmar_ratio", 0), 2),
                "color": "#4CAF50" if float(m.get("calmar_ratio", 0) or 0) >= 0.5 else "#FF9800",
                "key": "calmar_ratio",
            },
            {
                "label": "胜率",
                "value": _fmt_pct(m.get("win_rate", 0)),
                "color": "#4CAF50" if float(m.get("win_rate", 0) or 0) >= 0.5 else "#FF9800",
                "key": "win_rate",
            },
            {
                "label": "交易次数",
                "value": str(int(m.get("trade_count", m.get("num_trades", 0)) or 0)),
                "color": "#555555",
                "key": "trade_count",
            },
            {
                "label": "盈亏比",
                "value": _fmt_f(m.get("profit_loss_ratio", m.get("profit_factor", 0)), 2),
                "color": "#4CAF50" if float(m.get("profit_loss_ratio", m.get("profit_factor", 0)) or 0) >= 1.5 else "#FF9800",
                "key": "profit_loss_ratio",
            },
        ]
        return rows

    # ------------------------------------------------------------------
    # 4. 回测历史
    # ------------------------------------------------------------------

    def get_backtest_history(self, strategy_id: str) -> list[dict[str, Any]]:
        """返回指定策略的历史回测记录列表。

        每条记录::
            {"backtest_id": "...", "created_at": "...", "total_return": 0.12, ...}
        """
        try:
            results_dir = self._results_dir
            history: list[dict[str, Any]] = []
            if not results_dir.exists():
                return history
            for fpath in sorted(results_dir.glob(f"{strategy_id}_*.json")):
                try:
                    with open(fpath, encoding="utf-8") as f:
                        data = json.load(f)
                    perf = data.get("performance_metrics", {})
                    history.append(
                        {
                            "backtest_id": data.get("backtest_id", fpath.stem),
                            "created_at": data.get("created_at", ""),
                            "total_return": perf.get("total_return", 0),
                            "sharpe_ratio": perf.get("sharpe_ratio", 0),
                            "max_drawdown": perf.get("max_drawdown", 0),
                            "trade_count": perf.get("trade_count", perf.get("num_trades", 0)),
                        }
                    )
                except Exception:
                    continue
            return sorted(history, key=lambda x: x.get("created_at", ""), reverse=True)
        except Exception as exc:
            log.warning("get_backtest_history(%s) failed: %s", strategy_id, exc)
            return []

    # ------------------------------------------------------------------
    # 5. 内部辅助：提取资金曲线 & 交易记录
    # ------------------------------------------------------------------

    def _extract_equity_curve(self, result: Any) -> dict[str, list]:
        """从 BacktestResult 提取资金曲线数据。"""
        try:
            import pandas as pd

            ec = getattr(result, "equity_curve", None)
            if ec is not None and not (hasattr(ec, "empty") and ec.empty):
                if hasattr(ec, "index") and hasattr(ec, "values"):
                    dates = [
                        str(d)[:10] if hasattr(d, "__str__") else str(d)
                        for d in ec.index
                    ]
                    values = [float(v) for v in ec.values]
                    return {"dates": dates, "values": values}
            # fallback: performance_metrics 中可能带 equity_curve 列表
            m = getattr(result, "performance_metrics", {}) or {}
            curve = m.get("equity_curve") or m.get("portfolio_curve")
            if isinstance(curve, dict):
                return {
                    "dates": list(curve.get("dates", [])),
                    "values": [float(v) for v in curve.get("values", [])],
                }
        except Exception as exc:
            log.debug("_extract_equity_curve failed: %s", exc)
        return {"dates": [], "values": []}

    def _extract_trades(self, result: Any) -> list[dict[str, Any]]:
        """从 BacktestResult 提取交易记录列表。"""
        try:
            trades = getattr(result, "trades", []) or []
            rows: list[dict[str, Any]] = []
            for t in trades:
                if isinstance(t, dict):
                    rows.append(t)
                elif hasattr(t, "__iter__") and not isinstance(t, str):
                    items = list(t)
                    rows.append(
                        {
                            "date": items[0] if len(items) > 0 else "",
                            "action": items[1] if len(items) > 1 else "",
                            "price": items[2] if len(items) > 2 else 0,
                            "volume": items[3] if len(items) > 3 else 0,
                            "value": items[4] if len(items) > 4 else 0,
                            "pnl": items[5] if len(items) > 5 else 0,
                        }
                    )
            return rows
        except Exception as exc:
            log.debug("_extract_trades failed: %s", exc)
        return []

    # ------------------------------------------------------------------
    # 6. 策略类型 & 周期常量（供 UI 下拉菜单使用）
    # ------------------------------------------------------------------

    @staticmethod
    def strategy_type_options() -> list[tuple[str, str]]:
        """返回 (显示文本, 枚举值) 列表。"""
        return [
            ("趋势跟踪", "trend"),
            ("均值回归", "reversion"),
            ("因子选股", "factor"),
            ("网格交易", "grid"),
            ("条件单", "conditional"),
            ("跨周期对冲", "hedge"),
        ]

    @staticmethod
    def period_options() -> list[str]:
        return ["1d", "1h", "30m", "15m", "5m", "1m"]

    @staticmethod
    def base_strategy_options() -> list[str]:
        return [
            "MA_Cross",
            "RSI_Reversion",
            "Momentum_Factor",
            "Grid_Basic",
            "Conditional_Break",
        ]

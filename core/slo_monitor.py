"""
SLO 聚合监控器（Phase 1）

聚合三路 SLO 指标：
  1. DuckDB 锁等待（来自 DuckDBConnectionManager.get_lock_metrics()）
  2. 风控触发频率（来自 RiskEngine.get_risk_stats()）
  3. 委托拒绝率（自行维护计数器）

SLO 目标（可在实例化时覆盖）：
  db_lock_failure_rate  < 0.001  (0.1%)
  db_lock_p95_ms        < 200 ms
  risk_halt_rate        < 0.01   (每 100 次检查不超过 1 次 HALT)
  order_reject_rate     < 0.05   (5%)

使用方式::

    monitor = SLOMonitor()
    monitor.attach_db_manager(db_mgr)
    monitor.attach_risk_engine(risk_engine)
    monitor.record_order(rejected=True)
    report = monitor.get_report()
    breaches = monitor.check_slo_breached()
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SLO Targets
# ---------------------------------------------------------------------------


@dataclass
class SLOTargets:
    db_lock_failure_rate: float = 0.001   # < 0.1%
    db_lock_p95_ms: float = 200.0         # ms
    risk_halt_rate: float = 0.01          # < 1% of all risk checks
    order_reject_rate: float = 0.05       # < 5%


# ---------------------------------------------------------------------------
# Error Budget
# ---------------------------------------------------------------------------


@dataclass
class ErrorBudget:
    """
    月度错误预算追踪。

    以 30 天为一个预算周期。SLO 标准（如 99.9%）意味着每月允许最多
    ``(1 - monthly_uptime_target) * 30 * 86400`` 秒的不可用时间。

    使用示例::

        budget = ErrorBudget(monthly_uptime_target=0.999)
        budget.record_downtime(120)        # 记录 120 秒不可用
        if budget.is_exhausted():
            freeze_non_critical_releases()
        print(f"剩余预算: {budget.remaining_seconds():.0f}s, 燃烧率: {budget.burn_rate():.2f}x")
    """

    monthly_uptime_target: float = 0.999   # 默认 99.9% SLO
    _budget_seconds: float = field(init=False, repr=False)
    _consumed_seconds: float = field(init=False, repr=False, default=0.0)
    _window_start: float = field(init=False, repr=False)

    MONTH_SECONDS: float = field(init=False, repr=False, default=30 * 86400)

    def __post_init__(self) -> None:
        self.MONTH_SECONDS = 30 * 86400  # 30 天
        self._budget_seconds = (1.0 - self.monthly_uptime_target) * self.MONTH_SECONDS
        self._consumed_seconds = 0.0
        self._window_start = time.time()

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def record_downtime(self, seconds: float) -> None:
        """累计下线/不可用时长（秒）。"""
        self._consumed_seconds += max(0.0, seconds)

    def reset(self) -> None:
        """开始新的月度预算周期（建议月初调用）。"""
        self._consumed_seconds = 0.0
        self._window_start = time.time()

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def remaining_seconds(self) -> float:
        """剩余可用预算（秒）。可为负值，表示已超支。"""
        return self._budget_seconds - self._consumed_seconds

    def is_exhausted(self) -> bool:
        """预算余额耗尽（已违背 SLO 承诺）。"""
        return self._consumed_seconds >= self._budget_seconds

    def burn_rate(self) -> float:
        """
        预算消耗燃烧率。

        定义：实际消耗速率 / 预算允许速率。

        * ``< 1.0`` — 低于预算速率，当前窗口 SLO 安全
        * ``= 1.0`` — 恰好以预算速率消耗
        * ``> 1.0`` — 超速消耗，若不采取措施将在 ``budget/(rate-1)`` 后耗尽
        """
        elapsed = time.time() - self._window_start
        if elapsed <= 0:
            return 0.0
        expected_consumed = (elapsed / self.MONTH_SECONDS) * self._budget_seconds
        return self._consumed_seconds / expected_consumed if expected_consumed > 0 else 0.0

    def consumed_ratio(self) -> float:
        """已消耗预算比例 [0.0, ∞)，>1.0 表示超支。"""
        if self._budget_seconds <= 0:
            return 0.0
        return self._consumed_seconds / self._budget_seconds

    def as_dict(self) -> dict:
        """序列化为字典，供报告 / 日志使用。"""
        return {
            "monthly_uptime_target": self.monthly_uptime_target,
            "budget_seconds": round(self._budget_seconds, 1),
            "consumed_seconds": round(self._consumed_seconds, 3),
            "remaining_seconds": round(self.remaining_seconds(), 1),
            "consumed_ratio": round(self.consumed_ratio(), 4),
            "burn_rate": round(self.burn_rate(), 3),
            "is_exhausted": self.is_exhausted(),
        }





class SLOMonitor:
    """统一 SLO 聚合监控器。线程安全（仅依赖 GIL，无显式锁）。"""

    def __init__(
        self,
        targets: Optional[SLOTargets] = None,
        error_budget: Optional[ErrorBudget] = None,
    ) -> None:
        self.targets = targets or SLOTargets()
        self.error_budget = error_budget or ErrorBudget()
        self._db_manager: Optional[Any] = None
        self._risk_engine: Optional[Any] = None
        # 委托统计
        self._order_total: int = 0
        self._order_rejected: int = 0
        # 启动时间戳
        self._started_at: float = time.time()

    # ------------------------------------------------------------------
    # Attach sources
    # ------------------------------------------------------------------

    def attach_db_manager(self, db_manager: Any) -> None:
        """绑定 DuckDBConnectionManager 实例，用于读取锁等待指标。"""
        self._db_manager = db_manager

    def attach_risk_engine(self, risk_engine: Any) -> None:
        """绑定 RiskEngine 实例，用于读取风控事件统计。"""
        self._risk_engine = risk_engine

    # ------------------------------------------------------------------
    # Record events
    # ------------------------------------------------------------------

    def record_order(self, rejected: bool = False) -> None:
        """记录一次委托提交，rejected=True 表示风控拒单或技术拒单。"""
        self._order_total += 1
        if rejected:
            self._order_rejected += 1

    def reset(self) -> None:
        """重置所有本地计数器（建议每日零点调用）。"""
        self._order_total = 0
        self._order_rejected = 0
        self._started_at = time.time()
        self.error_budget.reset()
        if self._db_manager is not None:
            try:
                self._db_manager.reset_lock_metrics()
            except Exception:
                log.exception("reset lock_metrics 失败")
        if self._risk_engine is not None:
            try:
                self._risk_engine.reset_risk_stats()
            except Exception:
                log.exception("reset risk_stats 失败")

    # ------------------------------------------------------------------
    # Report
    # ------------------------------------------------------------------

    def get_report(self) -> Dict[str, Any]:
        """
        返回完整 SLO 报告字典，包含实际值、目标值、是否达标。

        结构::

            {
              "db_lock": {
                "failure_rate":   float,
                "p95_wait_ms":    float,
                "total_attempts": int,
                "slo_ok": bool,
              },
              "risk": {
                "total_checks":  int,
                "halt_count":    int,
                "halt_rate":     float,
                "warn_count":    int,
                "limit_count":   int,
                "pass_count":    int,
                "slo_ok": bool,
              },
              "orders": {
                "total":         int,
                "rejected":      int,
                "reject_rate":   float,
                "slo_ok": bool,
              },
              "overall_ok": bool,
              "uptime_s": float,
            }
        """
        report: Dict[str, Any] = {
            "uptime_s": time.time() - self._started_at,
        }

        # -- DuckDB lock metrics --
        db_section: Dict[str, Any] = {
            "failure_rate": 0.0,
            "p95_wait_ms": 0.0,
            "total_attempts": 0,
            "slo_ok": True,
        }
        if self._db_manager is not None:
            try:
                m = self._db_manager.get_lock_metrics()
                db_section["failure_rate"] = m.get("failure_rate", 0.0)
                db_section["p95_wait_ms"] = m.get("p95_wait_ms", 0.0)
                db_section["total_attempts"] = m.get("total_attempts", 0)
                db_section["slo_ok"] = (
                    db_section["failure_rate"] <= self.targets.db_lock_failure_rate
                    and db_section["p95_wait_ms"] <= self.targets.db_lock_p95_ms
                )
            except Exception:
                log.exception("读取 db_manager lock_metrics 失败")
        report["db_lock"] = db_section

        # -- Risk engine stats --
        risk_section: Dict[str, Any] = {
            "total_checks": 0,
            "halt_count": 0,
            "halt_rate": 0.0,
            "warn_count": 0,
            "limit_count": 0,
            "pass_count": 0,
            "slo_ok": True,
        }
        if self._risk_engine is not None:
            try:
                all_stats = self._risk_engine.get_risk_stats()
                # all_stats: {account_id: {action: count}}
                totals: Dict[str, int] = {}
                for _acct, cnts in all_stats.items():
                    for action, cnt in cnts.items():
                        totals[action] = totals.get(action, 0) + cnt
                total_checks = sum(totals.values())
                halt_count = totals.get("halt", 0)
                risk_section["total_checks"] = total_checks
                risk_section["halt_count"] = halt_count
                risk_section["warn_count"] = totals.get("warn", 0)
                risk_section["limit_count"] = totals.get("limit", 0)
                risk_section["pass_count"] = totals.get("pass", 0)
                risk_section["halt_rate"] = (
                    halt_count / total_checks if total_checks > 0 else 0.0
                )
                risk_section["slo_ok"] = (
                    risk_section["halt_rate"] <= self.targets.risk_halt_rate
                )
            except Exception:
                log.exception("读取 risk_engine stats 失败")
        report["risk"] = risk_section

        # -- Order stats --
        reject_rate = (
            self._order_rejected / self._order_total
            if self._order_total > 0
            else 0.0
        )
        orders_section = {
            "total": self._order_total,
            "rejected": self._order_rejected,
            "reject_rate": reject_rate,
            "slo_ok": reject_rate <= self.targets.order_reject_rate,
        }
        report["orders"] = orders_section

        report["error_budget"] = self.error_budget.as_dict()

        report["overall_ok"] = all(
            report[k]["slo_ok"] for k in ("db_lock", "risk", "orders")
        ) and not self.error_budget.is_exhausted()
        return report

    def check_slo_breached(self) -> List[str]:
        """
        快速健康检查，返回所有已违反的 SLO 项名称列表。
        空列表表示全部达标。
        """
        report = self.get_report()
        breaches: List[str] = []
        if not report["db_lock"]["slo_ok"]:
            breaches.append(
                f"db_lock: failure_rate={report['db_lock']['failure_rate']:.4f} "
                f"p95={report['db_lock']['p95_wait_ms']:.0f}ms"
            )
        if not report["risk"]["slo_ok"]:
            breaches.append(
                f"risk_halt: rate={report['risk']['halt_rate']:.4f} "
                f"count={report['risk']['halt_count']}"
            )
        if not report["orders"]["slo_ok"]:
            breaches.append(
                f"order_reject: rate={report['orders']['reject_rate']:.4f} "
                f"count={report['orders']['rejected']}/{report['orders']['total']}"
            )
        if self.error_budget.is_exhausted():
            breaches.append(
                f"error_budget: exhausted "
                f"consumed={report['error_budget']['consumed_seconds']:.1f}s "
                f"budget={report['error_budget']['budget_seconds']:.1f}s"
            )
        if breaches:
            log.warning("SLO 违反: %s", "; ".join(breaches))
        return breaches

    def log_report(self) -> None:
        """将完整报告写入日志（INFO 级别）。"""
        report = self.get_report()
        status = "OK" if report["overall_ok"] else "BREACHED"
        eb = report["error_budget"]
        log.info(
            "SLO Report [%s] uptime=%.0fs | "
            "db_lock: failure=%.4f%% p95=%.0fms | "
            "risk: halt=%.4f%% total=%d | "
            "orders: reject=%.2f%% total=%d | "
            "error_budget: consumed=%.1fs remaining=%.1fs burn=%.2fx%s",
            status,
            report["uptime_s"],
            report["db_lock"]["failure_rate"] * 100,
            report["db_lock"]["p95_wait_ms"],
            report["risk"]["halt_rate"] * 100,
            report["risk"]["total_checks"],
            report["orders"]["reject_rate"] * 100,
            report["orders"]["total"],
            eb["consumed_seconds"],
            eb["remaining_seconds"],
            eb["burn_rate"],
            " [BUDGET EXHAUSTED]" if eb["is_exhausted"] else "",
        )

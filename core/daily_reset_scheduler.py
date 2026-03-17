"""
每日风控重置调度器（Phase 1）

职责：
  - 每个交易日开盘前（默认 09:25）重置 RiskEngine 的日内高点状态
  - 每日零点重置 RiskEngine 风控事件计数器和 SLOMonitor 统计
  - 支持手动触发重置（用于测试/紧急场景）

使用方式::

    from core.daily_reset_scheduler import DailyResetScheduler
    from core.risk_engine import RiskEngine
    from core.slo_monitor import SLOMonitor

    risk = RiskEngine()
    slo = SLOMonitor()
    slo.attach_risk_engine(risk)

    scheduler = DailyResetScheduler(risk_engine=risk, slo_monitor=slo)
    scheduler.start()   # 后台线程，进程退出自动停止

    # 手动重置（用于测试或强制场景）
    scheduler.force_reset_now()
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, List, Optional

log = logging.getLogger(__name__)


class DailyResetScheduler:
    """
    后台线程调度器：每日定时重置风控状态。

    使用守护线程（daemon=True），随主进程退出自动终止。

    时间节点：
      1. 每日 ``market_open_time``（默认 09:25:00）：重置 RiskEngine 日内高点
      2. 每日 ``midnight_reset_time``（默认 00:01:00）：重置风控统计计数器 + SLO Monitor
    """

    def __init__(
        self,
        risk_engine: Optional[Any] = None,
        slo_monitor: Optional[Any] = None,
        account_ids: Optional[List[str]] = None,
        market_open_time: str = "09:25:00",   # 本地时间 HH:MM:SS
        midnight_reset_time: str = "00:01:00",
        check_interval_s: float = 30.0,        # 检查周期（秒）
    ) -> None:
        """
        Args:
            risk_engine: RiskEngine 实例，为 None 时仍可启动（无操作）
            slo_monitor: SLOMonitor 实例，为 None 时跳过 SLO 重置
            account_ids: 需要重置的账户 ID 列表；为 None/空时重置全部
            market_open_time: 开盘重置时间（本地时间 HH:MM:SS）
            midnight_reset_time: 零点统计重置时间（本地时间 HH:MM:SS）
            check_interval_s: 调度循环检查间隔（秒）
        """
        self._risk_engine = risk_engine
        self._slo_monitor = slo_monitor
        self._account_ids: List[str] = account_ids or []
        self._market_open_time = market_open_time
        self._midnight_reset_time = midnight_reset_time
        self._check_interval = max(5.0, float(check_interval_s))

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # 记录上次执行日期，防止同一天重复触发
        self._last_open_reset_date: str = ""
        self._last_midnight_reset_date: str = ""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """启动后台调度线程（幂等，多次调用安全）。"""
        if self._thread is not None and self._thread.is_alive():
            log.warning("DailyResetScheduler 已在运行，忽略重复启动")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="DailyResetScheduler",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "DailyResetScheduler 启动 | 开盘重置=%s 零点重置=%s 间隔=%.0fs",
            self._market_open_time, self._midnight_reset_time, self._check_interval,
        )

    def stop(self) -> None:
        """停止后台调度线程（最长等待 check_interval_s × 2）。"""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=self._check_interval * 2)
        log.info("DailyResetScheduler 已停止")

    def force_reset_now(self, reset_type: str = "all") -> None:
        """
        手动立即执行重置（用于测试或紧急场景）。

        Args:
            reset_type: "open" = 仅重置日内高点；
                        "midnight" = 仅重置统计计数器；
                        "all" = 全部重置（默认）
        """
        log.info("DailyResetScheduler.force_reset_now type=%s", reset_type)
        if reset_type in ("open", "all"):
            self._do_open_reset()
        if reset_type in ("midnight", "all"):
            self._do_midnight_reset()

    # ------------------------------------------------------------------
    # Internal loop
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        """调度主循环。"""
        while not self._stop_event.is_set():
            try:
                self._check_and_reset()
            except Exception:
                log.exception("DailyResetScheduler 调度循环异常")
            self._stop_event.wait(timeout=self._check_interval)

    def _check_and_reset(self) -> None:
        now = datetime.now(tz=_SH)
        today = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M:%S")

        # ── 开盘重置（日内高点）────────────────────────────────────────
        if (
            current_time >= self._market_open_time
            and self._last_open_reset_date != today
        ):
            self._do_open_reset()
            self._last_open_reset_date = today

        # ── 零点重置（计数器 + SLO）────────────────────────────────────
        if (
            current_time >= self._midnight_reset_time
            and self._last_midnight_reset_date != today
        ):
            self._do_midnight_reset()
            self._last_midnight_reset_date = today

    def _do_open_reset(self) -> None:
        """开盘：重置所有账户的日内高点（_daily_high）。"""
        if self._risk_engine is None:
            return
        try:
            if self._account_ids:
                for aid in self._account_ids:
                    self._risk_engine.reset_daily_state(aid)
                log.info(
                    "DailyResetScheduler: 开盘日内高点重置完成 accounts=%s",
                    self._account_ids,
                )
            else:
                self._risk_engine.reset_daily_state(None)
                log.info("DailyResetScheduler: 开盘日内高点重置完成（全账户）")
        except Exception:
            log.exception("DailyResetScheduler: 开盘重置失败")

    def _do_midnight_reset(self) -> None:
        """零点：重置风控事件计数器 + SLO Monitor 统计。"""
        if self._risk_engine is not None:
            try:
                self._risk_engine.reset_risk_stats()
                log.info("DailyResetScheduler: 风控统计计数器已重置")
            except Exception:
                log.exception("DailyResetScheduler: 风控统计重置失败")

        if self._slo_monitor is not None:
            try:
                self._slo_monitor.reset()
                log.info("DailyResetScheduler: SLO Monitor 已重置")
            except Exception:
                log.exception("DailyResetScheduler: SLO Monitor 重置失败")

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        running = self._thread is not None and self._thread.is_alive()
        return (
            f"<DailyResetScheduler running={running} "
            f"open={self._market_open_time} midnight={self._midnight_reset_time}>"
        )

"""
chart_slo_monitor.py — 图表适配器运行时 SLO 监控

功能：
  - 追踪 set_data P95 / update_data P95 / watchdog P99 / exception_rate 四项 KPI
  - 每个评估窗口（默认 60s）统计一次
  - 连续 N 窗口（默认 3）违约才升级为 ALERT（避免一次抖动误报）
  - 通过 signal_bus 发布 CHART_SLO_ALERT 事件（或降级 print）
  - 支持 ChartSloMonitor.record_latency / record_exception 接口

典型接入方式（在 kline_chart_workspace 或 NativeLwcChartAdapter 中调用）：

    slo = ChartSloMonitor.get_instance()
    t0 = time.perf_counter()
    adapter.set_data(df)
    slo.record_latency("set_data", (time.perf_counter() - t0) * 1000)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

# ── 阈值常量（与 tests/benchmark_chart.py 保持一致）────────────────────────────

KPI_SET_DATA_P95_MS: float = float(os.environ.get("EASYXT_SLO_SET_DATA_P95_MS", "120"))
KPI_UPDATE_P95_MS: float = float(os.environ.get("EASYXT_SLO_UPDATE_P95_MS", "120"))
KPI_WATCHDOG_P99_RATIO: float = float(os.environ.get("EASYXT_SLO_WATCHDOG_RATIO", "1.2"))
KPI_EXCEPTION_RATE: float = 0.0
WINDOW_SECONDS: float = float(os.environ.get("EASYXT_SLO_WINDOW_S", "60"))
ALERT_CONSECUTIVE: int = int(os.environ.get("EASYXT_SLO_CONSECUTIVE", "3"))


@dataclass
class SloWindowResult:
    """单个窗口的 SLO 评估结果。"""
    window_id: int
    ts: str
    set_data_p95_ms: float
    update_p95_ms: float
    watchdog_p99_ms: float
    exception_rate: float
    violations: list[str] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        return len(self.violations) == 0


class ChartSloMonitor:
    """
    图表适配器运行时 SLO 滑动窗口监控。

    单例；通过 get_instance() 获取。
    """

    _instance: "ChartSloMonitor | None" = None
    _instance_lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "ChartSloMonitor":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # 原始延迟样本（毫秒），每窗口清空
        self._set_data_lats: list[float] = []
        self._update_lats: list[float] = []
        self._watchdog_lats: list[float] = []

        # 异常计数
        self._exception_count: int = 0
        self._op_count: int = 0

        # 窗口计时
        self._window_start: float = time.monotonic()
        self._window_id: int = 0

        # 历史窗口（保留最近 10 个）
        self._history: deque[SloWindowResult] = deque(maxlen=10)

        # 连续违约计数
        self._consecutive_violations: int = 0

        # watchdog 基线（首个 ok 窗口的 P99 作为基线）
        self._watchdog_p99_baseline_ms: float | None = None

        # 窗口评估定时器
        self._eval_timer = threading.Timer(WINDOW_SECONDS, self._eval_window)
        self._eval_timer.daemon = True
        self._eval_timer.start()

        log.debug(
            "ChartSloMonitor: 初始化完成 window=%.0fs alert_at=%d consecutive violations",
            WINDOW_SECONDS,
            ALERT_CONSECUTIVE,
        )

    # ── 数据记录接口（线程安全）──────────────────────────────────────────────

    def record_latency(self, op: str, ms: float) -> None:
        """
        记录一次操作延迟。

        op: "set_data" | "update_data" | "watchdog"
        ms: 耗时（毫秒）
        """
        with self._lock:
            self._op_count += 1
            if op == "set_data":
                self._set_data_lats.append(ms)
            elif op in ("update_data", "update_bar"):
                self._update_lats.append(ms)
            elif op == "watchdog":
                self._watchdog_lats.append(ms)

    def record_exception(self) -> None:
        """记录一次未捕获异常（用于 exception_rate KPI）。"""
        with self._lock:
            self._exception_count += 1
            self._op_count += 1

    # ── 窗口评估 ────────────────────────────────────────────────────────────

    def _eval_window(self) -> None:
        """每 WINDOW_SECONDS 秒执行一次窗口评估。"""
        try:
            self._do_eval()
        finally:
            # 重新调度（即使本次失败也继续）
            self._eval_timer = threading.Timer(WINDOW_SECONDS, self._eval_window)
            self._eval_timer.daemon = True
            self._eval_timer.start()

    def _do_eval(self) -> None:
        with self._lock:
            set_lats = list(self._set_data_lats)
            upd_lats = list(self._update_lats)
            wdg_lats = list(self._watchdog_lats)
            exc_count = self._exception_count
            op_count = max(self._op_count, 1)

            # 清空当前窗口数据
            self._set_data_lats.clear()
            self._update_lats.clear()
            self._watchdog_lats.clear()
            self._exception_count = 0
            self._op_count = 0
            self._window_id += 1
            wid = self._window_id

        set_p95 = _percentile(set_lats, 95) if set_lats else 0.0
        upd_p95 = _percentile(upd_lats, 95) if upd_lats else 0.0
        wdg_p99 = _percentile(wdg_lats, 99) if wdg_lats else 0.0
        exc_rate = exc_count / op_count

        violations: list[str] = []

        # KPI-1a: set_data P95
        if set_lats and set_p95 > KPI_SET_DATA_P95_MS:
            violations.append(
                f"set_data P95={set_p95:.1f}ms > {KPI_SET_DATA_P95_MS}ms (n={len(set_lats)})"
            )

        # KPI-1b: update P95
        if upd_lats and upd_p95 > KPI_UPDATE_P95_MS:
            violations.append(
                f"update_data P95={upd_p95:.1f}ms > {KPI_UPDATE_P95_MS}ms (n={len(upd_lats)})"
            )

        # KPI-2: watchdog P99 vs baseline
        if wdg_lats:
            if self._watchdog_p99_baseline_ms is None and wdg_p99 > 0:
                self._watchdog_p99_baseline_ms = wdg_p99  # 首个有效窗口作基线
                log.info("ChartSloMonitor: watchdog P99 基线设定 = %.1fms", wdg_p99)
            elif self._watchdog_p99_baseline_ms and wdg_p99 > 0:
                ratio = wdg_p99 / self._watchdog_p99_baseline_ms
                if ratio > KPI_WATCHDOG_P99_RATIO:
                    violations.append(
                        f"watchdog P99 劣化 ×{ratio:.2f} ({wdg_p99:.1f}ms vs 基线 "
                        f"{self._watchdog_p99_baseline_ms:.1f}ms)"
                    )

        # KPI-4: exception rate
        if exc_count > 0 and exc_rate > KPI_EXCEPTION_RATE:
            violations.append(f"exception_rate={exc_rate:.2%} (count={exc_count})")

        r = SloWindowResult(
            window_id=wid,
            ts=_now_str(),
            set_data_p95_ms=set_p95,
            update_p95_ms=upd_p95,
            watchdog_p99_ms=wdg_p99,
            exception_rate=exc_rate,
            violations=violations,
        )
        with self._lock:
            self._history.append(r)

        if violations:
            with self._lock:
                self._consecutive_violations += 1
                consec = self._consecutive_violations
        else:
            with self._lock:
                self._consecutive_violations = 0
                consec = 0

        self._maybe_alert(r, consec)

    def _maybe_alert(self, result: SloWindowResult, consecutive: int) -> None:
        """根据连续违约次数决定是否升级告警。"""
        if result.is_ok:
            if consecutive == 0:
                log.debug("ChartSloMonitor: 窗口 %d OK", result.window_id)
            return

        # 每次违约都记录 WARNING
        for v in result.violations:
            log.warning("ChartSloMonitor[W%d]: %s", result.window_id, v)

        # 连续 ALERT_CONSECUTIVE 次才升级为 ALERT（避免偶发抖动误报）
        if consecutive >= ALERT_CONSECUTIVE:
            self._emit_alert(result, consecutive)

    def _emit_alert(self, result: SloWindowResult, consecutive: int) -> None:
        """发布 CHART_SLO_ALERT 事件（通过 signal_bus 或 fallback log）。"""
        payload: dict[str, Any] = {
            "window_id": result.window_id,
            "ts": result.ts,
            "consecutive_violations": consecutive,
            "violations": result.violations,
            "set_data_p95_ms": result.set_data_p95_ms,
            "update_p95_ms": result.update_p95_ms,
            "watchdog_p99_ms": result.watchdog_p99_ms,
            "exception_rate": result.exception_rate,
        }
        log.error(
            "ChartSloMonitor[ALERT] 连续 %d 窗口 SLO 违约: %s",
            consecutive,
            "; ".join(result.violations),
        )
        try:
            from core.signal_bus import signal_bus
            from core.events import Events
            if hasattr(Events, "CHART_SLO_ALERT"):
                signal_bus.emit(Events.CHART_SLO_ALERT, payload=payload)
        except Exception:
            pass  # signal_bus 不可用时静默（降级为 log.error 已够用）

    # ── 查询接口 ─────────────────────────────────────────────────────────────

    def get_latest(self) -> SloWindowResult | None:
        """返回最近一个窗口的评估结果。"""
        with self._lock:
            return self._history[-1] if self._history else None

    def get_status(self) -> dict[str, Any]:
        """返回当前 SLO 整体状态（供状态栏/健康检查使用）。"""
        latest = self.get_latest()
        if latest is None:
            return {"status": "no_data", "consecutive_violations": 0}
        with self._lock:
            consec = self._consecutive_violations
        return {
            "status": "alert" if consec >= ALERT_CONSECUTIVE else (
                "warning" if consec > 0 else "ok"
            ),
            "window_id": latest.window_id,
            "ts": latest.ts,
            "consecutive_violations": consec,
            "violations": latest.violations,
            "set_data_p95_ms": latest.set_data_p95_ms,
            "update_p95_ms": latest.update_p95_ms,
        }

    def stop(self) -> None:
        """停止后台评估定时器（widget 销毁时调用）。"""
        self._eval_timer.cancel()


# ── 内部工具 ──────────────────────────────────────────────────────────────────

def _percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = int(len(s) * p / 100)
    idx = max(0, min(idx, len(s) - 1))
    return s[idx]


def _now_str() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

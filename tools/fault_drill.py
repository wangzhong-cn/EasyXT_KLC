"""
故障演练脚本（Phase 2 前置）

用途：在 staging/开发环境中验证各组件的容错能力。
所有演练均有明确的 PASS/FAIL 标准，可接入 CI 或每周计划任务。

演练项目：
  1. drill_risk_halt()          - 风控 HALT 场景：触发日内熔断，验证策略正确停止
  2. drill_audit_tamper()       - 审计篡改检测：修改审计数据库记录后验证 hash 报警
  3. drill_db_lock_metrics()    - DuckDB 锁指标：并发写入后验证指标可观测
  4. drill_calibrate_thresholds() - 阈值校准：输入 mock 收益率序列，验证校准输出

运行方式::

    python tools/fault_drill.py                  # 运行全部演练
    python tools/fault_drill.py risk_halt        # 运行单项
    python tools/fault_drill.py --list           # 列出所有演练

注意：演练使用内存 DuckDB（:memory:），不会影响真实数据库。
"""

from __future__ import annotations

import sys
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Callable, Dict, List


# ---------------------------------------------------------------------------
# Minimal in-memory DB manager for drills (no real file needed)
# ---------------------------------------------------------------------------

import duckdb


class _DrillDBManager:
    """仅供演练使用的内存 DuckDB 管理器。"""

    def __init__(self) -> None:
        self._con = duckdb.connect(":memory:")
        self._lock = threading.Lock()
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        import types
        # Reuse get_lock_metrics / reset_lock_metrics logic by composing
        self._lock_metrics: dict = {"attempts": 0, "failures": 0, "wait_times_ms": []}

    @contextmanager
    def get_write_connection(self):
        with self._lock:
            yield self._con

    @contextmanager
    def get_read_connection(self):
        yield self._con

    def get_lock_metrics(self) -> dict:
        times = self._lock_metrics["wait_times_ms"]
        p95 = sorted(times)[int(len(times) * 0.95)] if times else 0.0
        total = self._lock_metrics["attempts"]
        return {
            "failure_rate": self._lock_metrics["failures"] / total if total > 0 else 0.0,
            "p95_wait_ms": p95,
            "total_attempts": total,
            "failures": self._lock_metrics["failures"],
        }

    def reset_lock_metrics(self) -> None:
        self._lock_metrics = {"attempts": 0, "failures": 0, "wait_times_ms": []}


# ---------------------------------------------------------------------------
# Drill result
# ---------------------------------------------------------------------------


class DrillResult:
    def __init__(self, name: str) -> None:
        self.name = name
        self.passed = False
        self.details: List[str] = []
        self._start = time.monotonic()

    def ok(self, detail: str = "") -> None:
        self.passed = True
        if detail:
            self.details.append(f"  ✓ {detail}")

    def fail(self, detail: str = "") -> None:
        self.passed = False
        if detail:
            self.details.append(f"  ✗ {detail}")

    def check(self, condition: bool, ok_msg: str, fail_msg: str) -> bool:
        if condition:
            self.details.append(f"  ✓ {ok_msg}")
        else:
            self.passed = False
            self.details.append(f"  ✗ {fail_msg}")
        return condition

    def elapsed_ms(self) -> float:
        return (time.monotonic() - self._start) * 1000

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"[{status}] {self.name} ({self.elapsed_ms():.0f}ms)"]
        lines.extend(self.details)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Drills
# ---------------------------------------------------------------------------


def drill_risk_halt() -> DrillResult:
    """
    演练：风控 HALT 场景。
    验证：日内回撤超过熔断线时，check_pre_trade 正确返回 HALT 且计入统计。
    """
    from core.risk_engine import RiskAction, RiskEngine, RiskThresholds

    r = DrillResult("drill_risk_halt")
    r.passed = True

    engine = RiskEngine(RiskThresholds(intraday_drawdown_halt=0.03))
    engine.update_daily_high("acc_drill", 100_000.0)

    # 触发 HALT（回撤 5% > halt 3%）
    result = engine.check_pre_trade(
        account_id="acc_drill",
        code="000001.SZ",
        volume=100,
        price=10.0,
        direction="buy",
        positions={},
        nav=95_000.0,   # 5% drawdown
    )
    r.check(result.action == RiskAction.HALT, "HALT 正确触发", f"期望 HALT, 实际 {result.action}")

    # 验证统计被记录
    stats = engine.get_risk_stats("acc_drill")
    r.check(stats.get("halt", 0) == 1, "HALT 统计记录正确", f"统计 halt={stats.get('halt')}, 期望 1")
    r.check("日内回撤" in result.reason, "reason 包含回撤信息", f"reason={result.reason!r}")

    return r


def drill_audit_tamper() -> DrillResult:
    """
    演练：审计链路篡改检测。
    验证：直接 UPDATE 修改审计表后，verify_chain_integrity() 能发现篡改。
    """
    from core.audit_trail import AuditTrail

    r = DrillResult("drill_audit_tamper")
    r.passed = True

    db = _DrillDBManager()
    trail = AuditTrail(db_manager=db)

    # 写入一条真实信号
    sid = trail.record_signal("drill_strat", "000001.SZ", "buy", price_hint=10.0, volume_hint=100)

    # 验证完整链路：未篡改时应全部通过
    pre_result = trail.verify_chain_integrity()
    r.check(pre_result["ok"], "未篡改时完整性通过", f"未篡改就报告失败: {pre_result}")

    # 直接 UPDATE 模拟篡改（修改 price_hint，破坏哈希）
    with db.get_write_connection() as con:
        con.execute(
            "UPDATE audit_signals SET price_hint = 999.0 WHERE signal_id = ?",
            [sid],
        )

    # 篡改后应检测到
    post_result = trail.verify_chain_integrity()
    r.check(not post_result["ok"], "篡改后完整性报警", "篡改后未检测到异常")
    r.check(
        sid in post_result.get("signals", {}).get("tampered_ids", []),
        f"被篡改的 signal_id 在报告中: {sid[:8]}...",
        "篡改 signal_id 未出现在报告中",
    )

    return r


def drill_db_lock_metrics() -> DrillResult:
    """
    演练：锁指标可观测性。
    验证：并发写入后 get_lock_metrics() 返回结构完整且值合理。
    """
    import os
    import tempfile
    from data_manager.duckdb_connection_pool import DuckDBConnectionManager

    r = DrillResult("drill_db_lock_metrics")
    r.passed = True

    # 独立临时文件，避免单例污染
    with tempfile.NamedTemporaryFile(suffix=".ddb", delete=False) as f:
        db_path = f.name
    try:
        mgr = object.__new__(DuckDBConnectionManager)
        mgr._initialized = False
        mgr._instance_key = db_path
        mgr.__init__(db_path)

        with mgr.get_write_connection() as con:
            con.execute("CREATE TABLE t (n INTEGER)")

        errors: List[Exception] = []

        def write_row(i: int) -> None:
            try:
                with mgr.get_write_connection() as con:
                    con.execute("INSERT INTO t VALUES (?)", [i])
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_row, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        r.check(not errors, f"20 并发写入无异常", f"出现 {len(errors)} 个错误: {errors[:2]}")

        m = mgr.get_lock_metrics()
        r.check("failure_rate" in m, "get_lock_metrics 结构完整", str(m))
        r.check("p95_wait_ms" in m, "p95_wait_ms 字段存在", str(m))
        r.check(m["failure_rate"] >= 0.0, "failure_rate 非负", str(m))
    finally:
        os.unlink(db_path)

    return r


def drill_calibrate_thresholds() -> DrillResult:
    """
    演练：阈值校准。
    验证：给定 mock 历史收益率序列，calibrate_thresholds_from_returns() 返回合理阈值。
    """
    from core.risk_engine import RiskEngine

    r = DrillResult("drill_calibrate_thresholds")
    r.passed = True

    # Wave of returns: mostly small gains, some losses
    returns = (
        [0.01] * 20 + [-0.005] * 10 + [-0.02] * 5 + [0.008] * 15
    )

    t = RiskEngine.calibrate_thresholds_from_returns(returns, var95_safety_margin=1.5)

    r.check(0.0 < t.var95_limit <= 0.05, f"var95_limit={t.var95_limit:.4f} 在合理范围", "var95_limit 超出范围")
    r.check(t.intraday_drawdown_halt >= 0.04, f"halt_level={t.intraday_drawdown_halt:.4f} >= 0.04", "halt_level 过低")
    r.check(t.intraday_drawdown_warn < t.intraday_drawdown_halt, "warn < halt 通过", "warn >= halt，阈值不合理")
    r.check(t.concentration_limit == 0.30, "concentration_limit 保持默认", str(t))

    # 空序列应返回默认阈值（不报错）
    default_t = RiskEngine.calibrate_thresholds_from_returns([])
    r.check(default_t.intraday_drawdown_halt == 0.05, "空序列返回默认阈值", str(default_t))

    return r


# ---------------------------------------------------------------------------
# Registry & runner
# ---------------------------------------------------------------------------

_DRILLS: Dict[str, Callable[[], DrillResult]] = {
    "risk_halt": drill_risk_halt,
    "audit_tamper": drill_audit_tamper,
    "db_lock_metrics": drill_db_lock_metrics,
    "calibrate_thresholds": drill_calibrate_thresholds,
}


def run_all() -> bool:
    """运行全部演练，返回 True 表示全部通过。"""
    results = []
    for name, fn in _DRILLS.items():
        print(f"\n{'─' * 50}")
        try:
            result = fn()
        except Exception as exc:
            result = DrillResult(name)
            result.fail(f"演练执行异常: {exc}")
        results.append(result)
        print(result)

    passed = sum(1 for r in results if r.passed)
    print(f"\n{'═' * 50}")
    print(f"故障演练结果: {passed}/{len(results)} PASS")
    print("═" * 50)
    return passed == len(results)


def run_one(name: str) -> bool:
    fn = _DRILLS.get(name)
    if fn is None:
        print(f"未找到演练: {name!r}，可用: {list(_DRILLS)}")
        return False
    result = fn()
    print(result)
    return result.passed


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--list" in args:
        print("可用演练:")
        for name in _DRILLS:
            print(f"  {name}")
        sys.exit(0)

    if args:
        ok = all(run_one(name) for name in args)
    else:
        ok = run_all()

    sys.exit(0 if ok else 1)

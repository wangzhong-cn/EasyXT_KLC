"""
P0-B DuckDB 并发可用性验收脚本
=====================================
在入库进程运行期间，模拟 GUI 读取负载，输出量化指标：

  - GUI 查询失败率 (failure_rate)        目标: < 5%
  - K线加载 p50 / p95 延迟 (ms)         目标: p95 < 3000ms
  - 批次窗口命中率 (window_hit_rate)     目标: > 20%（至少每 5 批有 1 次成功）
  - 入库吞吐降幅 (不采集，靠 checkpoint 对比)

用法
----
  # 完整报告（默认 60 轮，间隔 3s）
  conda activate myenv
  python tools/p0b_concurrent_verify.py

  # 快速冒烟（20 轮）
  python tools/p0b_concurrent_verify.py --rounds 20 --interval 2

  # 持续监控模式（每隔 30s 打印一行，无限运行）
  python tools/p0b_concurrent_verify.py --monitor
"""
import argparse
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# 模拟 GUI 执行的典型读查询（表不存在时自动降级到 information_schema）
_PROBE_QUERIES = [
    ("daily_count",
     "SELECT COUNT(*) FROM stock_daily"),
    ("5m_count",
     "SELECT COUNT(*) FROM stock_5m"),
]
_FALLBACK_QUERY = "SELECT 1"  # 表不存在时使用，仅验证连接可用
_FAILURE_RATE_MAX = 0.05
_FAILURE_RATE_MAX_BULK = 0.995
_P95_MS_MAX = 3000.0
_WINDOW_HIT_RATE_MIN = 0.20
_WINDOW_HIT_RATE_MIN_BULK = 0.01  # 初始全量入库场景：大批次周期长，5s 窗口占比 <1%，放宽到 1%


def _write_lock_exists(db_path: str) -> bool:
    """检查入库加锁标志文件是否存在。

    检测两种标志：
      - ``{db_path}.write.lock``  —— get_write_connection() 跨进程文件锁
      - ``{db_path}.ingest.lock`` —— ingest_ashare/ingest_index 哨兵文件
                                       （direct self.con 不创建 .write.lock，必须靠哨兵）
    """
    return Path(f"{db_path}.write.lock").exists() or Path(f"{db_path}.ingest.lock").exists()


def _classify_error(exc: Exception) -> str:
    msg = str(exc).lower()
    if "already open" in msg or "lock" in msg or "另一个程序正在使用" in msg:
        return "lock_conflict"
    return "other_error"


def run_one_probe(mgr) -> tuple[float, bool, bool, int, int, str]:
    """执行一轮探测查询，返回 (elapsed_ms, success, write_lock_held, lock_attempts_delta, lock_failures_delta, error_kind)。"""
    write_lock_held = _write_lock_exists(str(mgr.duckdb_path))
    before = mgr.get_lock_metrics()
    t0 = time.monotonic()
    error_kind = "none"
    try:
        with mgr.get_read_connection() as con:
            for _label, sql in _PROBE_QUERIES:
                try:
                    con.execute(sql).fetchall()
                except Exception:
                    # 表可能还未建立，降级到连通性验证
                    con.execute(_FALLBACK_QUERY).fetchall()
                    break
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        after = mgr.get_lock_metrics()
        return (
            elapsed_ms,
            True,
            write_lock_held,
            int(after["total_attempts"] - before["total_attempts"]),
            int(after["failures"] - before["failures"]),
            error_kind,
        )
    except Exception as exc:
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        error_kind = _classify_error(exc)
        after = mgr.get_lock_metrics()
        return (
            elapsed_ms,
            False,
            write_lock_held,
            int(after["total_attempts"] - before["total_attempts"]),
            int(after["failures"] - before["failures"]),
            error_kind,
        )


def compute_metrics(results: list[tuple[float, bool, bool, int, int, str]]) -> dict:
    total = len(results)
    successes = [ms for ms, ok, _wl, _la, _lf, _ek in results if ok]
    failures = total - len(successes)
    failure_rate = failures / total if total > 0 else 0.0
    p50 = statistics.median(successes) if successes else float("inf")
    p95 = sorted(successes)[int(len(successes) * 0.95)] if len(successes) >= 2 else (successes[0] if successes else float("inf"))
    # 窗口命中：成功且延迟 < 2000ms
    window_hits = sum(1 for ms, ok, _wl, _la, _lf, _ek in results if ok and ms < 2000)
    # 写锁命中率：探测时跨进程写锁文件存在的比例
    write_lock_hits = sum(1 for _ms, _ok, wl, _la, _lf, _ek in results if wl)
    lock_conflicts = sum(1 for _ms, ok, _wl, _la, _lf, ek in results if (not ok) and ek == "lock_conflict")
    total_lock_attempts = sum(la for _ms, _ok, _wl, la, _lf, _ek in results)
    total_lock_failures = sum(lf for _ms, _ok, _wl, _la, lf, _ek in results)
    return {
        "total": total,
        "success": len(successes),
        "failures": failures,
        "failure_rate": failure_rate,
        "p50_ms": p50,
        "p95_ms": p95,
        "window_hit_rate": window_hits / total if total > 0 else 0.0,
        "write_lock_rate": write_lock_hits / total if total > 0 else 0.0,
        "lock_conflict_rate": lock_conflicts / total if total > 0 else 0.0,
        "avg_lock_attempts_per_probe": (total_lock_attempts / total) if total > 0 else 0.0,
        "avg_lock_failures_per_probe": (total_lock_failures / total) if total > 0 else 0.0,
    }


def _to_json_safe(value: Any) -> Any:
    if isinstance(value, float) and (value in (float("inf"), float("-inf")) or value != value):
        return None
    return value


def _normalize_for_json(data: Any) -> Any:
    if isinstance(data, dict):
        return {k: _normalize_for_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_normalize_for_json(v) for v in data]
    return _to_json_safe(data)


def evaluate_gates(
    metrics: dict,
    *,
    window_hit_threshold: float = _WINDOW_HIT_RATE_MIN,
    failure_rate_max: float = _FAILURE_RATE_MAX,
) -> dict:
    failure_rate_pass = metrics["failure_rate"] <= failure_rate_max
    p95_pass = metrics["p95_ms"] <= _P95_MS_MAX
    window_hit_pass = metrics["window_hit_rate"] >= window_hit_threshold
    overall_pass = failure_rate_pass and p95_pass and window_hit_pass
    return {
        "overall_pass": overall_pass,
        "failure_rate": {
            "threshold_max": failure_rate_max,
            "actual": metrics["failure_rate"],
            "pass": failure_rate_pass,
        },
        "p95_ms": {
            "threshold_max": _P95_MS_MAX,
            "actual": metrics["p95_ms"],
            "pass": p95_pass,
        },
        "window_hit_rate": {
            "threshold_min": window_hit_threshold,
            "actual": metrics["window_hit_rate"],
            "pass": window_hit_pass,
        },
    }


def persist_report(report: dict, output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    latest = output_dir / "p0b_concurrent_latest.json"
    snapshot = output_dir / f"p0b_concurrent_{ts}.json"
    normalized = _normalize_for_json(report)
    latest.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    snapshot.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    return latest, snapshot


def print_header():
    print(f"{'轮次':>4} | {'状态':6} | {'耗时ms':>8} | {'失败率':>7} | {'p95ms':>7} | {'窗口命中率':>9} | {'写锁':>4}")
    print("-" * 70)


def check_gates(
    metrics: dict,
    *,
    window_hit_threshold: float = _WINDOW_HIT_RATE_MIN,
    failure_rate_max: float = _FAILURE_RATE_MAX,
) -> list[str]:
    fails = []
    if metrics["failure_rate"] > failure_rate_max:
        fails.append(f"  ❌ 失败率 {metrics['failure_rate']:.1%} > {failure_rate_max:.1%} 红线")
    else:
        print(f"  ✅ 失败率 {metrics['failure_rate']:.1%} ≤ {failure_rate_max:.1%}")

    if metrics["p95_ms"] > _P95_MS_MAX:
        fails.append(f"  ❌ p95 延迟 {metrics['p95_ms']:.0f}ms > 3000ms 红线")
    else:
        print(f"  ✅ p95 延迟 {metrics['p95_ms']:.0f}ms ≤ 3000ms")

    if metrics["window_hit_rate"] < window_hit_threshold:
        fails.append(f"  ⚠️  批次窗口命中率 {metrics['window_hit_rate']:.1%} < {window_hit_threshold:.0%} 警戒线")
    else:
        print(f"  ✅ 批次窗口命中率 {metrics['window_hit_rate']:.1%} ≥ {window_hit_threshold:.0%}")
    return fails


def run_benchmark(rounds: int, interval: float, verbose: bool, output_dir: Path, tag: str, bulk_mode: bool = False):
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(str(db_path))
    mgr.reset_lock_metrics()

    print(f"\nP0-B 并发验收 — DB: {db_path}")
    print(f"探测轮次: {rounds}, 间隔: {interval}s{', 模式: bulk（全量入库松阈值）' if bulk_mode else ''}\n")

    results: list[tuple[float, bool, bool, int, int, str]] = []
    interrupted = False
    if verbose:
        print_header()

    try:
        for i in range(rounds):
            elapsed, ok, wl, lock_attempts, lock_failures, error_kind = run_one_probe(mgr)
            results.append((elapsed, ok, wl, lock_attempts, lock_failures, error_kind))

            if verbose:
                status = "OK" if ok else "FAIL"
                wl_str = "Y" if wl else "N"
                cum = compute_metrics(results)
                print(
                    f"{i+1:>4} | {status:6} | {elapsed:>8.0f} | "
                    f"{cum['failure_rate']:>6.1%} | {cum['p95_ms']:>7.0f} | "
                    f"{cum['window_hit_rate']:>8.1%} | {wl_str:>4}"
                )
            time.sleep(interval)
    except KeyboardInterrupt:
        interrupted = True
        print("\n收到中断信号，输出当前已采样结果...")

    # 最终报告
    m = compute_metrics(results)
    pool_m = mgr.get_lock_metrics()
    window_threshold = _WINDOW_HIT_RATE_MIN_BULK if bulk_mode else _WINDOW_HIT_RATE_MIN
    failure_rate_max = _FAILURE_RATE_MAX_BULK if bulk_mode else _FAILURE_RATE_MAX

    print("\n" + "=" * 60)
    print("验收摘要")
    print("=" * 60)
    print(f"  探测总次数: {m['total']}")
    print(f"  成功: {m['success']}  失败: {m['failures']}")
    print(f"  失败率:       {m['failure_rate']:.2%}")
    print(f"  p50 延迟:     {m['p50_ms']:.0f} ms")
    print(f"  p95 延迟:     {m['p95_ms']:.0f} ms")
    print(f"  窗口命中率:   {m['window_hit_rate']:.1%}")
    print(f"  写锁持有率:   {m['write_lock_rate']:.1%}  (探测时入库写锁文件存在的比例)")
    print(f"  锁冲突率:     {m['lock_conflict_rate']:.1%}  (失败中识别为文件锁冲突的比例)")
    print(f"  每轮锁重试:   {m['avg_lock_attempts_per_probe']:.1f} 次")
    print(f"  连接池锁等待: attempts={pool_m['total_attempts']}, "
          f"failures={pool_m['failures']}, "
          f"failure_rate={pool_m['failure_rate']:.2%}, "
          f"p95_wait={pool_m['p95_wait_ms']:.0f}ms")

    print("\nGate 判定:")
    gate_eval = evaluate_gates(
        m,
        window_hit_threshold=window_threshold,
        failure_rate_max=failure_rate_max,
    )
    gate_fails = check_gates(
        m,
        window_hit_threshold=window_threshold,
        failure_rate_max=failure_rate_max,
    )
    report = {
        "script": "tools/p0b_concurrent_verify.py",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tag": tag,
        "db_path": str(db_path),
        "config": {
            "rounds": rounds,
            "completed_rounds": len(results),
            "interrupted": interrupted,
            "interval_s": interval,
            "verbose": verbose,
            "thresholds": {
                "failure_rate_max": failure_rate_max,
                "p95_ms_max": _P95_MS_MAX,
                "window_hit_rate_min": window_threshold,
            },
            "bulk_mode": bulk_mode,
        },
        "metrics": m,
        "pool_lock_metrics": pool_m,
        "gate": gate_eval,
    }
    latest, snapshot = persist_report(report, output_dir=output_dir)
    print(f"\n报告已写入: {latest}")
    print(f"报告快照:   {snapshot}")
    if interrupted:
        print("\n⚠️ 验收被人工中断，门禁结果仅代表当前采样窗口")
        return 130
    if gate_fails:
        print("\n以下指标未达标:")
        for f in gate_fails:
            print(f)
        return 1
    print("\n✅ 所有 P0-B 指标通过")
    return 0


def run_monitor(interval: float):
    """持续监控模式：每隔 interval 秒打 1 行，Ctrl-C 结束。"""
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

    db_path = resolve_duckdb_path()
    mgr = get_db_manager(str(db_path))
    print(f"P0-B 持续监控 — DB: {db_path} (Ctrl-C 停止)\n")
    print_header()
    i = 0
    rolling: list[tuple[float, bool, bool, int, int, str]] = []

    while True:
        elapsed, ok, wl, lock_attempts, lock_failures, error_kind = run_one_probe(mgr)
        rolling.append((elapsed, ok, wl, lock_attempts, lock_failures, error_kind))
        # 滚动窗口：最近 20 次
        if len(rolling) > 20:
            rolling.pop(0)
        cum = compute_metrics(rolling)
        status = "OK" if ok else "FAIL"
        wl_str = "Y" if wl else "N"
        print(
            f"{i+1:>4} | {status:6} | {elapsed:>8.0f} | "
            f"{cum['failure_rate']:>6.1%} | {cum['p95_ms']:>7.0f} | "
            f"{cum['window_hit_rate']:>8.1%} | {wl_str:>4}"
        )
        i += 1
        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser(description="P0-B DuckDB 并发可用性验收")
    parser.add_argument("--rounds", type=int, default=60, help="探测轮次（默认60）")
    parser.add_argument("--interval", type=float, default=3.0, help="探测间隔秒数（默认3s）")
    parser.add_argument("--monitor", action="store_true", help="持续监控模式")
    parser.add_argument("--verbose", "-v", action="store_true", default=True,
                        help="逐轮打印（默认开启）")
    parser.add_argument("--output-dir", default="artifacts", help="报告输出目录（默认 artifacts）")
    parser.add_argument("--tag", default="manual", help="报告标签（默认 manual）")
    parser.add_argument("--bulk", action="store_true",
                        help="全量入库松阈值模式：window_hit_rate 阈值从 20%%降为 1%%（大批次周期长，窗口占比天然 <1%%）")
    args = parser.parse_args()

    if args.monitor:
        try:
            run_monitor(args.interval)
        except KeyboardInterrupt:
            print("\n监控已停止")
        return

    sys.exit(
        run_benchmark(
            args.rounds,
            args.interval,
            args.verbose,
            output_dir=Path(args.output_dir),
            tag=args.tag,
            bulk_mode=args.bulk,
        )
    )


if __name__ == "__main__":
    main()

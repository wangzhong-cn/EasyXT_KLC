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
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path


# 模拟 GUI 执行的典型读查询（表不存在时自动降级到 information_schema）
_PROBE_QUERIES = [
    ("daily_count",
     "SELECT COUNT(*) FROM stock_daily"),
    ("5m_count",
     "SELECT COUNT(*) FROM stock_5m"),
]
_FALLBACK_QUERY = "SELECT 1"  # 表不存在时使用，仅验证连接可用


def _write_lock_exists(db_path: str) -> bool:
    """检查跨进程写锁文件是否存在（写锁文件存在 = 入库进程正在写入）。"""
    return Path(f"{db_path}.write.lock").exists()


def run_one_probe(mgr) -> tuple[float, bool, bool]:
    """执行一轮探测查询，返回 (elapsed_ms, success, write_lock_held)。"""
    write_lock_held = _write_lock_exists(str(mgr.duckdb_path))
    t0 = time.monotonic()
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
        return elapsed_ms, True, write_lock_held
    except Exception:
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        return elapsed_ms, False, write_lock_held


def compute_metrics(results: list[tuple[float, bool, bool]]) -> dict:
    total = len(results)
    successes = [ms for ms, ok, _wl in results if ok]
    failures = total - len(successes)
    failure_rate = failures / total if total > 0 else 0.0
    p50 = statistics.median(successes) if successes else float("inf")
    p95 = sorted(successes)[int(len(successes) * 0.95)] if len(successes) >= 2 else (successes[0] if successes else float("inf"))
    # 窗口命中：成功且延迟 < 2000ms
    window_hits = sum(1 for ms, ok, _wl in results if ok and ms < 2000)
    # 写锁命中率：探测时跨进程写锁文件存在的比例
    write_lock_hits = sum(1 for _ms, _ok, wl in results if wl)
    return {
        "total": total,
        "success": len(successes),
        "failures": failures,
        "failure_rate": failure_rate,
        "p50_ms": p50,
        "p95_ms": p95,
        "window_hit_rate": window_hits / total if total > 0 else 0.0,
        "write_lock_rate": write_lock_hits / total if total > 0 else 0.0,
    }


def print_header():
    print(f"{'轮次':>4} | {'状态':6} | {'耗时ms':>8} | {'失败率':>7} | {'p95ms':>7} | {'窗口命中率':>9} | {'写锁':>4}")
    print("-" * 70)


def check_gates(metrics: dict) -> list[str]:
    fails = []
    if metrics["failure_rate"] > 0.05:
        fails.append(f"  ❌ 失败率 {metrics['failure_rate']:.1%} > 5% 红线")
    else:
        print(f"  ✅ 失败率 {metrics['failure_rate']:.1%} ≤ 5%")

    if metrics["p95_ms"] > 3000:
        fails.append(f"  ❌ p95 延迟 {metrics['p95_ms']:.0f}ms > 3000ms 红线")
    else:
        print(f"  ✅ p95 延迟 {metrics['p95_ms']:.0f}ms ≤ 3000ms")

    if metrics["window_hit_rate"] < 0.20:
        fails.append(f"  ⚠️  批次窗口命中率 {metrics['window_hit_rate']:.1%} < 20% 警戒线")
    else:
        print(f"  ✅ 批次窗口命中率 {metrics['window_hit_rate']:.1%} ≥ 20%")
    return fails


def run_benchmark(rounds: int, interval: float, verbose: bool):
    db_path = resolve_duckdb_path()
    mgr = get_db_manager(str(db_path))
    mgr.reset_lock_metrics()

    print(f"\nP0-B 并发验收 — DB: {db_path}")
    print(f"探测轮次: {rounds}, 间隔: {interval}s\n")

    results: list[tuple[float, bool, bool]] = []
    if verbose:
        print_header()

    for i in range(rounds):
        elapsed, ok, wl = run_one_probe(mgr)
        results.append((elapsed, ok, wl))

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

    # 最终报告
    m = compute_metrics(results)
    pool_m = mgr.get_lock_metrics()

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
    print(f"  连接池锁等待: attempts={pool_m['total_attempts']}, "
          f"failures={pool_m['failures']}, "
          f"failure_rate={pool_m['failure_rate']:.2%}, "
          f"p95_wait={pool_m['p95_wait_ms']:.0f}ms")

    print("\nGate 判定:")
    gate_fails = check_gates(m)
    if gate_fails:
        print("\n以下指标未达标:")
        for f in gate_fails:
            print(f)
        return 1
    print("\n✅ 所有 P0-B 指标通过")
    return 0


def run_monitor(interval: float):
    """持续监控模式：每隔 interval 秒打 1 行，Ctrl-C 结束。"""
    db_path = resolve_duckdb_path()
    mgr = get_db_manager(str(db_path))
    print(f"P0-B 持续监控 — DB: {db_path} (Ctrl-C 停止)\n")
    print_header()
    i = 0
    rolling: list[tuple[float, bool, bool]] = []

    while True:
        elapsed, ok, wl = run_one_probe(mgr)
        rolling.append((elapsed, ok, wl))
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
    args = parser.parse_args()

    if args.monitor:
        try:
            run_monitor(args.interval)
        except KeyboardInterrupt:
            print("\n监控已停止")
        return

    sys.exit(run_benchmark(args.rounds, args.interval, args.verbose))


if __name__ == "__main__":
    main()

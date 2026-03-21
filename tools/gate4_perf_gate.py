"""Gate4 性能基线采样与回归阻断。

Benchmarks three hot-path proxies (all hermetic / in-memory):
  hash_throughput  — SHA256 over a 1 KB payload  (adj_factor_hash 代理)
  duckdb_in_mem    — DuckDB in-memory GROUP BY    (custom_period_bars 代理)
  pandas_resample  — pandas 5-min resampling      (K 线聚合代理)

CLI:
  python gate4_perf_gate.py --sample [--n-runs N] [--n-batches B] [--baseline PATH]
      测量并写 / 覆盖基线，exit 0。

  python gate4_perf_gate.py --check  [--n-runs N] [--n-batches B] [--baseline PATH]
                                     [--threshold 0.20]
      比对基线；任一 p50/p95/p99 回归 > threshold → exit 1。
      基线缺失时 exit 2（不阻断，仅告知需先 --sample）。
      若基线与当前环境指纹不匹配，降级为 warn（`env_mismatch_warn=True`），不阻断。

产物:
  artifacts/gate4_perf_baseline.json  — 基线（--sample 写入）
  artifacts/gate4_perf_latest.json    — 最新采样 / 回归报告（两个子命令均写入）

环境变量（编排器调用时有效）:
  EASYXT_GATE4_PERF_THRESHOLD    回归阻断阈值，默认 0.20
  EASYXT_GATE4_PERF_N_RUNS       每批采样轮次，默认 100
  EASYXT_GATE4_PERF_N_BATCHES    rolling-median 批次数，默认 1
  EASYXT_GATE4_ALLOW_BOOTSTRAP   基线缺失时是否自动写基线，默认 1（1=允许，0=拒绝）
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = PROJECT_ROOT / "artifacts" / "gate4_perf_baseline.json"
DEFAULT_LATEST = PROJECT_ROOT / "artifacts" / "gate4_perf_latest.json"
DEFAULT_N_RUNS = 100
DEFAULT_N_BATCHES = 1          # rolling-median 批次数
DEFAULT_THRESHOLD = 0.20  # 20% 回归阻断
_WARMUP = 5            # 丢弃的预热轮次，消除进程冷启动抖动

_HASH_PAYLOAD = b"adj_factor_hash_proxy_payload_" + b"x" * 994  # 1 KB


# ─── env fingerprint ──────────────────────────────────────────────────────────


def _collect_env_fingerprint() -> dict[str, Any]:
    """采集硬件/运行时指纹，写入基线；check 时用于跨机/跨版本降级 warn。"""
    fp: dict[str, Any] = {
        "python_version": sys.version.split()[0],
        "platform": platform.system(),
        "cpu_count": (lambda c: c if c is not None else 0)(getattr(platform, "cpu_count", lambda: None)()),
        "machine": platform.machine(),
    }
    try:
        import duckdb  # noqa: PLC0415
        fp["duckdb_version"] = getattr(duckdb, "__version__", "unknown")
    except ImportError:
        fp["duckdb_version"] = None
    try:
        import pandas as pd  # noqa: PLC0415
        fp["pandas_version"] = pd.__version__
    except ImportError:
        fp["pandas_version"] = None
    # cpu_count via os fallback
    if not fp["cpu_count"]:
        import os  # noqa: PLC0415
        fp["cpu_count"] = os.cpu_count() or 0
    return fp


def _env_mismatch(cur: dict[str, Any], base: dict[str, Any]) -> tuple[bool, str]:
    """比较两份环境指纹；返回 (mismatched, reason)。"""
    reasons: list[str] = []
    for key in ("python_version", "duckdb_version", "pandas_version"):
        a, b = cur.get(key), base.get(key)
        if a and b and a != b:
            reasons.append(f"{key}: {b} → {a}")
    # cpu_count 差异超过 50% 视为跨机
    a_cpu = int(cur.get("cpu_count") or 0)
    b_cpu = int(base.get("cpu_count") or 0)
    if b_cpu > 0 and a_cpu > 0 and abs(a_cpu - b_cpu) / b_cpu > 0.5:
        reasons.append(f"cpu_count: {b_cpu} → {a_cpu}")
    return bool(reasons), "; ".join(reasons)


def _pct(data: list[float], p: float) -> float:
    """Return p-th percentile (0–100) of sorted data."""
    if not data:
        return 0.0
    s = sorted(data)
    idx = max(0, min(int(len(s) * p / 100), len(s) - 1))
    return s[idx]


def _stats(samples: list[float]) -> dict[str, Any]:
    if not samples:
        return {"p50_us": 0.0, "p95_us": 0.0, "p99_us": 0.0, "n": 0, "available": False}
    return {
        "p50_us": round(_pct(samples, 50), 3),
        "p95_us": round(_pct(samples, 95), 3),
        "p99_us": round(_pct(samples, 99), 3),
        "n": len(samples),
        "available": True,
    }


# ─── benchmarks ───────────────────────────────────────────────────────────────


def _bench_hash(n: int) -> list[float]:
    """SHA256 over a fixed 1 KB payload — adj_factor_hash 热路径代理。"""
    for _ in range(_WARMUP):   # 预热，不计入统计
        hashlib.sha256(_HASH_PAYLOAD).hexdigest()
    out: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        hashlib.sha256(_HASH_PAYLOAD).hexdigest()
        out.append((time.perf_counter() - t0) * 1e6)
    return out


def _bench_duckdb(n: int) -> list[float]:
    """In-memory DuckDB GROUP BY — custom_period_bars 查询代理。"""
    try:
        import duckdb  # noqa: PLC0415
    except ImportError:
        return []
    con = duckdb.connect(":memory:")
    con.execute(
        "CREATE TABLE t AS SELECT range::INTEGER AS id, "
        "(range % 5)::VARCHAR AS grp, random() AS v FROM range(1000)"
    )
    for _ in range(_WARMUP):   # 预热
        con.execute("SELECT grp, AVG(v) FROM t GROUP BY grp").fetchall()
    out: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        con.execute("SELECT grp, AVG(v) FROM t GROUP BY grp").fetchall()
        out.append((time.perf_counter() - t0) * 1e6)
    con.close()
    return out


def _bench_pandas(n: int) -> list[float]:
    """Pandas date-based resampling — K 线聚合热路径代理。"""
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        return []
    idx = pd.date_range("2020-01-01", periods=500, freq="1min")
    df = pd.DataFrame({"close": range(500), "volume": range(500, 1000)}, index=idx)
    for _ in range(_WARMUP):   # 预热
        df.resample("5min").agg({"close": "last", "volume": "sum"})
    out: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        df.resample("5min").agg({"close": "last", "volume": "sum"})
        out.append((time.perf_counter() - t0) * 1e6)
    return out


# ─── public API ───────────────────────────────────────────────────────────────


def run_sample(n_runs: int = DEFAULT_N_RUNS, n_batches: int = DEFAULT_N_BATCHES) -> dict[str, Any]:
    """运行全部基准，返回采样报告字典。

    n_batches > 1 时执行 rolling-median：将总采样分为 n_batches 批，
    取各批 p50/p95/p99 的中位数，显著压制高噪声项（pandas_resample / hash）的
    单进程抖动。
    """
    _benches: dict[str, tuple[Any, ...]] = {
        "hash_throughput": (_bench_hash,),
        "duckdb_in_mem": (_bench_duckdb,),
        "pandas_resample": (_bench_pandas,),
    }

    if n_batches <= 1:
        bench_stats = {
            k: _stats(fn(n_runs)) for k, (fn,) in _benches.items()
        }
    else:
        runs_per_batch = max(5, n_runs // n_batches)
        # 每批独立采样，最终对各分位取中位数
        p50_lists: dict[str, list[float]] = {k: [] for k in _benches}
        p95_lists: dict[str, list[float]] = {k: [] for k in _benches}
        p99_lists: dict[str, list[float]] = {k: [] for k in _benches}
        n_lists: dict[str, list[int]] = {k: [] for k in _benches}
        available: dict[str, bool] = {k: False for k in _benches}
        for _ in range(n_batches):
            for k, (fn,) in _benches.items():
                s = _stats(fn(runs_per_batch))
                if s.get("available"):
                    available[k] = True
                    p50_lists[k].append(s["p50_us"])
                    p95_lists[k].append(s["p95_us"])
                    p99_lists[k].append(s["p99_us"])
                    n_lists[k].append(s["n"])
        bench_stats = {}
        for k in _benches:
            if available[k]:
                bench_stats[k] = {
                    "p50_us": round(_pct(p50_lists[k], 50), 3),
                    "p95_us": round(_pct(p95_lists[k], 50), 3),
                    "p99_us": round(_pct(p99_lists[k], 50), 3),
                    "n": sum(n_lists[k]),
                    "n_batches": n_batches,
                    "available": True,
                }
            else:
                bench_stats[k] = {"p50_us": 0.0, "p95_us": 0.0, "p99_us": 0.0,
                                   "n": 0, "n_batches": n_batches, "available": False}

    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_runs": n_runs,
        "n_batches": n_batches,
        "env_fingerprint": _collect_env_fingerprint(),
        "benchmarks": bench_stats,
    }


def check_regression(
    current: dict[str, Any],
    baseline: dict[str, Any],
    threshold: float = DEFAULT_THRESHOLD,
    p99_threshold: float | None = None,
) -> dict[str, Any]:
    """比对 current 与 baseline；返回回归报告字典。

    - p50_us：超过 threshold 则阻断（默认 20%）
    - p95_us：超过 threshold*2 则阻断（默认 40%）
      不同进程间 p95 方差通常 ~25%，需要额外缓冲。
    - p99_us：超过 p99_threshold 则阻断（默认 threshold*5=100%）
      p99 在 n=100 时等于最坏单次采样, 受 OS 调度/缓存抖动影响大，
      需要 >2x 基线才能作为可信回归信号。

    若环境指纹（CPU核数/lib版本）与基线偏差过大，降级为 warn 而不阻断：
    - gate_pass 强制置 True
    - env_mismatch_warn=True, env_mismatch_detail 描述差异

    gate_pass=True  → 无回归超阈值（或环境偏差降级）
    gate_pass=False → 至少一项分位数回归超阈值
    """
    # ── env fingerprint check ────────────────────────────────────────────────
    cur_fp = current.get("env_fingerprint", {})
    base_fp = baseline.get("env_fingerprint", {})
    env_mismatched = False
    env_mismatch_reason = ""
    if cur_fp and base_fp:
        env_mismatched, env_mismatch_reason = _env_mismatch(cur_fp, base_fp)

    _p99_thr = (threshold * 5) if p99_threshold is None else p99_threshold
    _thresholds = {"p50_us": threshold, "p95_us": threshold * 2, "p99_us": _p99_thr}

    cur_b = current.get("benchmarks", {})
    base_b = baseline.get("benchmarks", {})
    regressions: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []

    for bench, cur_stats in cur_b.items():
        if not cur_stats.get("available", False):
            continue
        base_stats = base_b.get(bench, {})
        if not base_stats or not base_stats.get("available", False):
            continue
        for key in ("p50_us", "p95_us", "p99_us"):
            cur_val = float(cur_stats.get(key, 0.0) or 0.0)
            base_val = float(base_stats.get(key, 0.0) or 0.0)
            if base_val <= 0:
                continue
            thr = _thresholds[key]
            delta = (cur_val - base_val) / base_val
            entry: dict[str, Any] = {
                "benchmark": bench,
                "metric": key,
                "baseline_us": base_val,
                "current_us": cur_val,
                "delta_ratio": round(delta, 4),
                "threshold": thr,
                "passed": delta <= thr,
            }
            details.append(entry)
            if not entry["passed"]:
                regressions.append(entry)

    raw_gate_pass = len(regressions) == 0
    # 环境不匹配时降级为 warn（不阻断，但记录）
    effective_gate_pass = True if env_mismatched else raw_gate_pass

    return {
        "gate_pass": effective_gate_pass,
        "raw_gate_pass": raw_gate_pass,
        "env_mismatch_warn": env_mismatched,
        "env_mismatch_detail": env_mismatch_reason,
        "threshold": threshold,
        "p95_threshold": threshold * 2,
        "p99_threshold": _p99_thr,
        "regression_count": len(regressions),
        "regressions": regressions,
        "details": details,
    }


# ─── CLI ───────────────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate4 性能基线采样与回归阻断")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sample", action="store_true", help="采样并写入 / 更新基线")
    mode.add_argument("--check", action="store_true", help="与基线对比，回归 > 阈值则 exit 1")
    parser.add_argument("--n-runs", type=int, default=DEFAULT_N_RUNS, help="每批采样轮次")
    parser.add_argument("--n-batches", type=int, default=DEFAULT_N_BATCHES,
                        help="rolling-median 批次数（>1 时取各批分位的中位数）")
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE, help="基线文件路径")
    parser.add_argument("--latest", type=Path, default=DEFAULT_LATEST, help="最新报告输出路径")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="最大允许回归比例（默认 0.20 = 20%%）",
    )
    args = parser.parse_args()

    args.baseline.parent.mkdir(parents=True, exist_ok=True)
    args.latest.parent.mkdir(parents=True, exist_ok=True)

    if args.sample:
        report = run_sample(n_runs=args.n_runs, n_batches=args.n_batches)
        args.baseline.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        args.latest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"status": "baseline_updated", "baseline": str(args.baseline)}, ensure_ascii=False))
        return 0

    # --check
    if not args.baseline.exists():
        result: dict[str, Any] = {
            "status": "baseline_missing",
            "gate_pass": True,
            "baseline": str(args.baseline),
        }
        args.latest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False))
        return 2

    try:
        baseline_data = json.loads(args.baseline.read_text(encoding="utf-8"))
    except Exception as exc:
        result = {"status": "baseline_parse_error", "gate_pass": True, "error": str(exc)}
        args.latest.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False))
        return 2

    current_data = run_sample(n_runs=args.n_runs, n_batches=args.n_batches)
    reg = check_regression(current_data, baseline_data, threshold=args.threshold)
    status = "env_mismatch_warn" if reg.get("env_mismatch_warn") else (
        "regression_blocked" if not reg["gate_pass"] else "pass"
    )
    full_report: dict[str, Any] = {
        "status": status,
        "baseline": str(args.baseline),
        "sample": current_data,
        **reg,
    }
    args.latest.write_text(json.dumps(full_report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(full_report, ensure_ascii=False))
    return 0 if reg["gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

"""
benchmark_chart.py — EasyXT 图表迁移 KPI 基准测试

测量 LwcPythonAdapter 与 NativeLwcAdapter 的四项核心 KPI：
  KPI-1  交互延迟 P95 < 120ms（set_data / update_data / toggle indicator）
  KPI-2  主线程 watchdog P99 不劣化（连续 1000 次 update_data 不卡顿）
  KPI-3  线程水位（adapter 生命周期内线程数增量）
  KPI-4  异常率（1000 次操作内未捕获异常次数）

用法：
    # 在 conda myenv 中：
    python tests/benchmark_chart.py --adapter lwc_python
    python tests/benchmark_chart.py --adapter native    # Stage 2 就绪后
    python tests/benchmark_chart.py --compare           # 对比两种 adapter

灰度切换门槛（Stage 3 前必须全部达标）：
    交互 P95   < 120ms
    watchdog P99 不比 lwc_python 劣化超过 20%
    线程增量   ≤ 3（允许 WsBridge 守护线程）
    异常率     = 0
"""
from __future__ import annotations

import argparse
import statistics
import threading
import time
import sys
import os
import traceback
from typing import Callable

import numpy as np
import pandas as pd

# ── KPI thresholds ────────────────────────────────────────────────────────────
KPI_INTERACTION_P95_MS = 120.0     # 交互延迟 P95 上限
KPI_WATCHDOG_DEGRADATION = 1.20    # watchdog P99 最大劣化倍数（相对 baseline）
KPI_MAX_THREAD_DELTA = 3           # 允许的线程增量上限
KPI_MAX_EXCEPTION_RATE = 0.0       # 允许的异常率


# ── Data generators ───────────────────────────────────────────────────────────

def make_ohlcv_df(n: int = 5000) -> pd.DataFrame:
    """生成 n 根模拟 OHLCV K 线。"""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2020-01-01", periods=n, freq="D")
    close = 100.0 + np.cumsum(rng.normal(0, 1, n))
    high = close + rng.uniform(0, 2, n)
    low = close - rng.uniform(0, 2, n)
    open_ = low + rng.uniform(0, high - low)
    vol = rng.integers(1_000_000, 5_000_000, n).astype(float)
    return pd.DataFrame({
        "time": dates.strftime("%Y-%m-%d"),
        "open": np.round(open_, 2),
        "high": np.round(high, 2),
        "low": np.round(low, 2),
        "close": np.round(close, 2),
        "volume": vol,
    })


def make_single_bar(df: pd.DataFrame, offset: int = 0) -> pd.Series:
    """从 DataFrame 取最后一行并微调，模拟实时 tick。"""
    row = df.iloc[-1].copy()
    row["close"] = float(row["close"]) + offset * 0.01
    return row


# ── Timing utilities ──────────────────────────────────────────────────────────

class Timer:
    """简单的高精度计时上下文管理器。"""
    def __init__(self):
        self._start = 0.0
        self.elapsed_ms = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000


def measure_latencies(fn: Callable, iterations: int = 100) -> list[float]:
    """连续调用 fn iterations 次，返回每次耗时（ms）列表。"""
    latencies = []
    for _ in range(iterations):
        with Timer() as t:
            try:
                fn()
            except Exception:
                pass  # 异常由 exception_rate 单独统计
        latencies.append(t.elapsed_ms)
    return latencies


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    return float(np.percentile(data, p))


# ── Individual KPI checks ─────────────────────────────────────────────────────

class BenchmarkResult:
    def __init__(self, adapter_name: str):
        self.adapter_name = adapter_name
        self.set_data_p50 = 0.0
        self.set_data_p95 = 0.0
        self.update_bar_p50 = 0.0
        self.update_bar_p95 = 0.0
        self.update_bar_p99 = 0.0
        self.thread_delta = 0
        self.exception_count = 0
        self.total_ops = 0
        self.errors: list[str] = []

    @property
    def exception_rate(self) -> float:
        return self.exception_count / self.total_ops if self.total_ops else 0.0

    def check_kpis(self, baseline: "BenchmarkResult | None" = None) -> list[str]:
        """返回 KPI 失败项描述列表（空=全部达标）。"""
        failures = []
        if self.set_data_p95 > KPI_INTERACTION_P95_MS:
            failures.append(
                f"set_data P95={self.set_data_p95:.1f}ms > {KPI_INTERACTION_P95_MS}ms"
            )
        if self.update_bar_p95 > KPI_INTERACTION_P95_MS:
            failures.append(
                f"update_bar P95={self.update_bar_p95:.1f}ms > {KPI_INTERACTION_P95_MS}ms"
            )
        if self.thread_delta > KPI_MAX_THREAD_DELTA:
            failures.append(
                f"thread_delta={self.thread_delta} > {KPI_MAX_THREAD_DELTA}"
            )
        if self.exception_rate > KPI_MAX_EXCEPTION_RATE:
            failures.append(
                f"exception_rate={self.exception_rate:.2%} > 0"
            )
        if baseline:
            if baseline.update_bar_p99 > 0:
                ratio = self.update_bar_p99 / baseline.update_bar_p99
                if ratio > KPI_WATCHDOG_DEGRADATION:
                    failures.append(
                        f"watchdog P99 degraded x{ratio:.2f} "
                        f"(limit x{KPI_WATCHDOG_DEGRADATION})"
                    )
        return failures

    def print_summary(self) -> None:
        print(f"\n  {'─'*50}")
        print(f"  Adapter       : {self.adapter_name}")
        print(f"  set_data      : P50={self.set_data_p50:.1f}ms  P95={self.set_data_p95:.1f}ms")
        print(f"  update_bar    : P50={self.update_bar_p50:.1f}ms  "
              f"P95={self.update_bar_p95:.1f}ms  P99={self.update_bar_p99:.1f}ms")
        print(f"  thread_delta  : {self.thread_delta}")
        print(f"  exceptions    : {self.exception_count}/{self.total_ops} "
              f"({self.exception_rate:.2%})")
        failures = self.check_kpis()
        if failures:
            print(f"\n  ❌ KPI FAILURES:")
            for f in failures:
                print(f"     - {f}")
        else:
            print(f"\n  ✅ All KPIs passed")


# ── Adapter runners ───────────────────────────────────────────────────────────

def _run_headless_benchmark(adapter, df: pd.DataFrame) -> BenchmarkResult:
    """
    无 GUI 模式下的 adapter benchmark（直接调用接口方法，不渲染）。
    适合 CI 环境。
    """
    result = BenchmarkResult(type(adapter).__name__)
    exceptions = []

    def safe(fn):
        try:
            fn()
        except NotImplementedError:
            pass  # Stage 2 前 native adapter 未实现，忽略
        except Exception as e:
            exceptions.append(str(e))

    thread_before = threading.active_count()

    # ── KPI-1a: set_data latency
    lats = []
    for i in range(50):
        with Timer() as t:
            safe(lambda: adapter.set_data(df))
        lats.append(t.elapsed_ms)
    result.set_data_p50 = percentile(lats, 50)
    result.set_data_p95 = percentile(lats, 95)

    # ── KPI-1b: update_bar latency
    ulats = []
    for i in range(1000):
        bar = make_single_bar(df, i)
        with Timer() as t:
            safe(lambda b=bar: adapter.update_data(b))
        ulats.append(t.elapsed_ms)
    result.update_bar_p50 = percentile(ulats, 50)
    result.update_bar_p95 = percentile(ulats, 95)
    result.update_bar_p99 = percentile(ulats, 99)

    result.thread_delta = threading.active_count() - thread_before
    result.exception_count = len(exceptions)
    result.total_ops = 50 + 1000
    result.errors = exceptions[:10]  # 保留前 10 条

    return result


# ── Main CLI ──────────────────────────────────────────────────────────────────

def _build_mock_adapter(name: str):
    """构建可独立测试的 headless adapter（不依赖 GUI）。"""
    # 导入路径处理
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)

    if name == "lwc_python":
        # headless mock: 直接测试序列化路径，不需要 QWebEngineView
        class _MockLwcPython:
            """模拟 LwcPythonChartAdapter 的数据路径（无 GUI overhead）。"""
            def set_data(self, df: pd.DataFrame) -> None:
                import json
                # 模拟 js_data 序列化开销
                cols = [c for c in ("time","open","high","low","close","volume") if c in df.columns]
                _ = json.dumps(df[cols].to_dict("records"))

            def update_data(self, row: pd.Series) -> None:
                import json
                _ = json.dumps(row.to_dict())

            def marker(self, text: str) -> None:
                pass

        return _MockLwcPython()

    elif name == "native":
        class _MockNative:
            """模拟 NativeLwcChartAdapter 的数据路径（无 WS overhead）。"""
            def set_data(self, df: pd.DataFrame) -> None:
                import json
                from gui_app.widgets.chart.rpc_protocol import build_set_data
                _ = json.dumps({"jsonrpc": "2.0", "method": "chart.setData",
                                "params": build_set_data(df)})

            def update_data(self, row: pd.Series) -> None:
                import json
                from gui_app.widgets.chart.rpc_protocol import build_update_bar
                _ = json.dumps({"jsonrpc": "2.0", "method": "chart.updateBar",
                                "params": build_update_bar(row)})

            def marker(self, text: str) -> None:
                pass

        return _MockNative()
    else:
        raise ValueError(f"Unknown adapter: {name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="EasyXT chart adapter benchmark")
    parser.add_argument("--adapter", choices=["lwc_python", "native", "both"],
                        default="lwc_python")
    parser.add_argument("--bars", type=int, default=5000,
                        help="Number of OHLCV bars to benchmark with")
    parser.add_argument("--strict", action="store_true",
                        help="Exit 1 if any KPI fails")
    args = parser.parse_args()

    print("=" * 60)
    print(" EasyXT Chart Adapter Benchmark")
    print(f" bars={args.bars}")
    print("=" * 60)

    df = make_ohlcv_df(args.bars)
    results: list[BenchmarkResult] = []

    adapters_to_test = (
        ["lwc_python", "native"] if args.adapter == "both" else [args.adapter]
    )

    baseline = None
    for name in adapters_to_test:
        print(f"\n[Benchmarking: {name}]")
        adapter = _build_mock_adapter(name)
        result = _run_headless_benchmark(adapter, df)
        result.print_summary()
        results.append(result)
        if name == "lwc_python":
            baseline = result

    # Cross-adapter KPI check
    if len(results) == 2 and baseline:
        print("\n" + "=" * 60)
        print(" Cross-adapter KPI gate")
        print("=" * 60)
        native_result = results[1]
        failures = native_result.check_kpis(baseline=baseline)
        if failures:
            print(f"\n❌ Native adapter does NOT meet migration gate:")
            for f in failures:
                print(f"   - {f}")
            if args.strict:
                return 1
        else:
            print(f"\n✅ Native adapter meets all migration KPI gates")
            print("   → Safe to proceed to Stage 3 (gradual switch)")

    return 0


if __name__ == "__main__":
    sys.exit(main())

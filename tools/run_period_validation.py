#!/usr/bin/env python3
"""
run_period_validation.py — 独立运维入口：触发多周期重建并校验 period_validation_report.jsonl

使用说明
--------
    python tools/run_period_validation.py                # 自动选 1 只股票，回溯 3 天
    python tools/run_period_validation.py --limit 3      # 抽样 3 只
    python tools/run_period_validation.py --lookback-days 7
    python tools/run_period_validation.py --json         # 机器可读输出

退出码
------
    0  — 所有周期校验通过
    1  — 校验失败（有 is_valid=false 的记录）
    2  — 重建过程本身出错（非校验失败）

常见误操作与正确用法
--------------------
    ❌  governance_jobs.py --job period_validation   # 不支持此 job，会报错
    ✅  run_period_validation.py                     # 正确入口

内部流程
--------
    1. 调用 governance_jobs.py --job rebuild（触发 PeriodBarBuilder.cross_validate）
    2. 从 artifacts/period_validation_report.jsonl 读取结果
    3. 统计 is_valid=false 条数，非零则失败
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ARTIFACTS_DIR = Path("artifacts")
REPORT_FILE = ARTIFACTS_DIR / "period_validation_report.jsonl"
GOVERNANCE_SCRIPT = Path("tools") / "governance_jobs.py"


def _run_rebuild(limit: int, lookback_days: int) -> dict[str, Any]:
    cmd = [
        sys.executable,
        str(GOVERNANCE_SCRIPT),
        "--job", "rebuild",
        "--rebuild-auto-symbol-limit", str(limit),
        "--rebuild-lookback-days", str(lookback_days),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"governance_jobs rebuild 失败 (exit={result.returncode}):\n"
            f"{result.stderr or result.stdout}"
        )
    # governance_jobs.py 输出前有 xtdata 连接信息等非 JSON 行，找最后一个 JSON 对象
    for line in reversed(result.stdout.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return dict(json.loads(line))
            except json.JSONDecodeError:
                continue
    raise RuntimeError(
        f"rebuild 输出中未找到 JSON 结果 (exit={result.returncode})\nraw: {result.stdout[:500]}"
    )


def _read_report() -> list[dict[str, Any]]:
    if not REPORT_FILE.exists():
        return []
    entries: list[dict[str, Any]] = []
    with open(REPORT_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return entries


def main() -> None:
    parser = argparse.ArgumentParser(
        description="触发多周期重建并生成/校验 period_validation_report.jsonl",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="注意: governance_jobs.py --job period_validation 不支持；请改用本脚本。",
    )
    parser.add_argument("--limit", type=int, default=1, metavar="N",
                        help="自动抽样股票数量（默认 1）")
    parser.add_argument("--lookback-days", type=int, default=3, metavar="DAYS",
                        help="回溯天数（默认 3）")
    parser.add_argument("--json", dest="json_out", action="store_true",
                        help="以 JSON 格式输出结果（适合 CI/机器解析）")
    args = parser.parse_args()

    # ── 1. 触发重建 ──────────────────────────────────────────────────────────
    try:
        rebuild_output = _run_rebuild(limit=args.limit, lookback_days=args.lookback_days)
    except RuntimeError as exc:
        if args.json_out:
            print(json.dumps({"status": "error", "passed": False, "error": str(exc)},
                             ensure_ascii=False))
        else:
            print(f"[period_validation] ERROR: {exc}", file=sys.stderr)
        sys.exit(2)

    rebuild_ok = rebuild_output.get("multiperiod_rebuild", {}).get("ok", False)
    if not rebuild_ok:
        msg = "multiperiod_rebuild.ok=false，检查 artifacts/ 日志"
        if args.json_out:
            print(json.dumps({"status": "rebuild_failed", "passed": False, "error": msg},
                             ensure_ascii=False))
        else:
            print(f"[period_validation] FAIL: {msg}", file=sys.stderr)
        sys.exit(2)

    # ── 2. 读取校验报告 ──────────────────────────────────────────────────────
    entries = _read_report()
    if not entries:
        msg = f"{REPORT_FILE} 未生成或为空"
        if args.json_out:
            print(json.dumps({"status": "missing", "passed": False, "error": msg},
                             ensure_ascii=False))
        else:
            print(f"[period_validation] FAIL: {msg}", file=sys.stderr)
        sys.exit(1)

    # ── 3. 统计结果 ──────────────────────────────────────────────────────────
    failed = [e for e in entries if not e.get("is_valid", True)]
    passed = len(failed) == 0

    if args.json_out:
        print(json.dumps(
            {
                "status": "pass" if passed else "fail",
                "passed": passed,
                "total_entries": len(entries),
                "failed_items": len(failed),
                "report_file": str(REPORT_FILE),
                "failed_periods": [e.get("period") for e in failed],
            },
            ensure_ascii=False,
            indent=2,
        ))
    else:
        status_tag = "PASS" if passed else "FAIL"
        print(
            f"[period_validation] {status_tag}: "
            f"{len(entries)} 条记录，{len(failed)} 条失败"
        )
        if failed:
            for e in failed:
                print(f"  ✗ period={e.get('period')} errors={e.get('errors', [])}")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

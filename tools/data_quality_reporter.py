"""
数据质量报告生成器
用途：扫描 data_export/ 下所有 feather 文件，输出结构化质量报告。

运行:
    python tools/data_quality_reporter.py
    python tools/data_quality_reporter.py --data-dir data_export --output data_export/quality_report.json

输出格式 (data_export/quality_report.json):
    {
      "generated_at": "2026-03-09T10:00:00",
      "trading_calendar_size": 1465,
      "summary": { "total_files": 36, "total_rows": 1133434, "pass": 30, "warn": 4, "fail": 2 },
      "files": {
        "IF/IF01_1d.feather": {
          "rows": 1465, "period": "1d",
          "date_range": ["2020-01-02", "2026-03-07"],
          "missing_rate": 0.0, "duplicate_rate": 0.0,
          "nan_rate": 0.0, "zero_price_count": 0,
          "gap_count": 0, "trading_day_align_rate": 1.0,
          "issues": [], "status": "pass"
        }
      }
    }

质量指标说明:
  missing_rate            — [1d 专用] 交易日历中应有但缺失的比例
  trading_day_align_rate  — 1 - missing_rate，越接近 1 越好
  duplicate_rate          — 时间戳重复行占比
  nan_rate                — 数值字段 NaN 占比
  zero_price_count        — close <= 0 的行数
  gap_count               — [1m 专用] 连续时间戳之间超过预期间隔的空洞数

交易日历来源:
  优先使用 IF01.IF_1d.feather 的 time 列（期货指数每交易日必有数据），
  兜底使用 000300.SH_1d.feather。
  若两者均不存在，日线对齐率检查跳过。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# 项目根
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data_export"
DEFAULT_OUTPUT = DEFAULT_DATA_DIR / "quality_report.json"

# 1d 缺失率警告阈值
MISSING_RATE_WARN = 0.02     # >2% 触发 warn
MISSING_RATE_FAIL = 0.10     # >10% 触发 fail
NAN_RATE_WARN = 0.001
DUPLICATE_WARN = 0           # 任何重复均 warn
# 1m 间隔过大阈值（超过 2 分钟视为空洞）
GAP_1M_SECONDS = 120
# 期货日盘有1次午休（约90min），股指有1次，商品期货含夜盘最多3段；
# 每段边界各计 1 次。正常品种每日约 2-3 个"空洞"。
# 校年：1465 交易日 × 3 = ~4400，只要 >总行数5% 才 warn
GAP_1M_WARN_RATIO = 0.06   # 空洞数 > feather行数 × 6‰ 才 warn（非交易间隙预期值约3-5‰）


def _load_feather(path: Path) -> pd.DataFrame:
    try:
        return pd.read_feather(path)
    except Exception:
        return pd.DataFrame()


def _build_trading_calendar(data_dir: Path) -> set[date]:
    """
    从 IF01.IF_1d 或 000300.SH_1d 推导交易日历（返回 date 集合）。
    """
    candidates = [
        data_dir / "IF" / "IF01_IF_1d.feather",
        data_dir / "IF" / "IF01.IF_1d.feather",
        data_dir / "SH" / "000300_SH_1d.feather",
    ]
    # 也尝试 glob 找第一个 1d 期货文件
    for path in list(data_dir.glob("IF/*_1d.feather"))[:3]:
        candidates.append(path)

    for path in candidates:
        if not path.exists():
            continue
        df = _load_feather(path)
        time_col = _find_time_col(df)
        if time_col and len(df) > 100:
            times = pd.to_datetime(df[time_col], errors="coerce").dropna()
            cal = {t.date() for t in times}
            if len(cal) > 100:
                return cal
    return set()


def _find_time_col(df: pd.DataFrame) -> str | None:
    for col in ["date", "time", "datetime", "index"]:
        if col in df.columns:
            return col
    # feather reset_index 后可能是 "date" 或 "index"
    if len(df.columns) > 0:
        first = df.columns[0]
        sample = df[first].iloc[0] if len(df) > 0 else None
        if sample is not None:
            try:
                pd.to_datetime(sample)
                return first
            except Exception:
                pass
    return None


def _check_file(path: Path, data_dir: Path, trading_days: set[date]) -> dict[str, Any]:
    rel = str(path.relative_to(data_dir)).replace("\\", "/")
    df = _load_feather(path)
    rows = len(df)

    if rows == 0:
        return {
            "rows": 0, "period": _period_from_name(path.stem),
            "issues": ["empty_file"], "status": "fail",
        }

    period = _period_from_name(path.stem)
    result: dict[str, Any] = {
        "rows": rows,
        "period": period,
        "issues": [],
    }

    time_col = _find_time_col(df)
    if time_col is None:
        result["issues"].append("no_time_column")
        result["status"] = "warn"
        return result

    times = pd.to_datetime(df[time_col], errors="coerce")
    valid_times = times.dropna()

    if len(valid_times) < rows:
        bad = rows - len(valid_times)
        result["issues"].append(f"unparseable_time:{bad}")

    result["date_range"] = [
        str(valid_times.min().date()),
        str(valid_times.max().date()),
    ] if len(valid_times) > 0 else [None, None]

    # ── 重复率 ────────────────────────────────────────────────────────────────
    dup_count = int(times.duplicated().sum())
    result["duplicate_rate"] = round(dup_count / rows, 6)
    if dup_count > DUPLICATE_WARN:
        result["issues"].append(f"duplicates:{dup_count}")

    # ── NaN 率 ────────────────────────────────────────────────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if numeric_cols:
        nan_count = int(df[numeric_cols].isna().sum().sum())
        total_cells = rows * len(numeric_cols)
        nan_rate = nan_count / max(total_cells, 1)
        result["nan_rate"] = round(nan_rate, 6)
        if nan_rate > NAN_RATE_WARN:
            result["issues"].append(f"high_nan_rate:{nan_rate:.4%}")
    else:
        result["nan_rate"] = 0.0

    # ── close <= 0 检查 ───────────────────────────────────────────────────────
    if "close" in df.columns:
        zero_count = int((pd.to_numeric(df["close"], errors="coerce").fillna(0) <= 0).sum())
        result["zero_price_count"] = zero_count
        if zero_count > 0:
            result["issues"].append(f"zero_price:{zero_count}")
    else:
        result["zero_price_count"] = 0

    # ── 1d 交易日对齐率 ──────────────────────────────────────────────────────
    if period == "1d" and trading_days:
        start_d = valid_times.min().date()
        end_d = valid_times.max().date()
        expected_cal = {d for d in trading_days if start_d <= d <= end_d}
        actual_days = {t.date() for t in valid_times}
        missing_days = expected_cal - actual_days
        missing_rate = len(missing_days) / max(len(expected_cal), 1)
        result["missing_rate"] = round(missing_rate, 6)
        result["trading_day_align_rate"] = round(1.0 - missing_rate, 6)
        result["gap_count"] = len(missing_days)
        if missing_rate > MISSING_RATE_FAIL:
            result["issues"].append(f"high_missing_rate:{missing_rate:.2%}")
        elif missing_rate > MISSING_RATE_WARN:
            result["issues"].append(f"missing_rate:{missing_rate:.2%}")
    elif period == "1d":
        # 无日历时做简单连续性检查（非周末）
        result["missing_rate"] = None
        result["trading_day_align_rate"] = None
        result["gap_count"] = None

    # ── 1m 时间间隔空洞检查 ──────────────────────────────────────────────────
    if period == "1m" and len(valid_times) > 1:
        sorted_t = valid_times.sort_values()
        # 仅在同一交易日内计算间隔（跨日不统计）
        day_groups = sorted_t.groupby(sorted_t.dt.date)
        gap_count = 0
        for _, grp in day_groups:
            if len(grp) < 2:
                continue
            diffs = grp.diff().dt.total_seconds().dropna()
            gap_count += int((diffs > GAP_1M_SECONDS).sum())
        result["gap_count"] = gap_count
        if gap_count > GAP_1M_WARN_RATIO * rows:
            result["issues"].append(f"intraday_gaps:{gap_count}")

    # ── 最终状态 ──────────────────────────────────────────────────────────────
    if not result["issues"]:
        result["status"] = "pass"
    else:
        # 判断 warn vs fail
        fail_keywords = ["empty_file", "zero_price", "high_missing_rate", "high_nan_rate"]
        is_fail = any(any(kw in issue for kw in fail_keywords) for issue in result["issues"])
        result["status"] = "fail" if is_fail else "warn"

    return result


def _period_from_name(stem: str) -> str:
    """从文件名提取周期，如 'IF01_IF_1d' → '1d'，'IF01_IF_1m' → '1m'。"""
    parts = stem.split("_")
    for part in reversed(parts):
        if part in ("1d", "1m", "5m", "15m", "30m", "60m", "1w", "1M"):
            return part
    return "unknown"


def run(data_dir: Path, output: Path) -> dict:
    print(f"[质量报告] 扫描目录: {data_dir}")

    trading_days = _build_trading_calendar(data_dir)
    print(f"  交易日历: {len(trading_days)} 个交易日", end="")
    if trading_days:
        sorted_days = sorted(trading_days)
        print(f"（{sorted_days[0]} ~ {sorted_days[-1]}）")
    else:
        print("（未找到，日线对齐检查将跳过）")

    all_feathers = sorted(data_dir.rglob("*.feather"))
    # 排除 tick_snapshot（非 OHLCV）
    feathers = [f for f in all_feathers if "tick_snapshot" not in f.name]
    print(f"  待检查文件: {len(feathers)} 个（已排除 tick_snapshot）")

    files_result: dict[str, Any] = {}
    summary = {"total_files": 0, "total_rows": 0, "pass": 0, "warn": 0, "fail": 0}

    for path in feathers:
        rel = str(path.relative_to(data_dir)).replace("\\", "/")
        info = _check_file(path, data_dir, trading_days)
        files_result[rel] = info
        summary["total_files"] += 1
        summary["total_rows"] += info.get("rows", 0)
        st = info.get("status", "fail")
        summary[st] = summary.get(st, 0) + 1

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "trading_calendar_size": len(trading_days),
        "summary": summary,
        "files": files_result,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # ── 打印报告摘要 ──────────────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  质量报告摘要")
    print(f"{'='*65}")
    print(f"  总文件: {summary['total_files']:>4}  |  总行数: {summary['total_rows']:>10,}")
    print(f"  ✅ pass: {summary['pass']:>4}  |  ⚠️  warn: {summary['warn']:>4}  |  ❌ fail: {summary['fail']:>4}")
    print(f"{'='*65}")

    warn_fail = [(rel, info) for rel, info in files_result.items()
                 if info.get("status") in ("warn", "fail")]
    if warn_fail:
        print(f"\n  需关注的文件（{len(warn_fail)} 个）:")
        for rel, info in sorted(warn_fail, key=lambda x: x[1].get("status", "") == "fail", reverse=True):
            icon = "❌" if info.get("status") == "fail" else "⚠️ "
            issues = ", ".join(info.get("issues", []))
            rows = info.get("rows", 0)
            print(f"  {icon} {rel:<50} rows={rows:>8,}  [{issues}]")
    else:
        print("\n  🎉 所有文件均通过质量检查！")

    print(f"\n  报告已输出: {output}")
    return report


def main():
    parser = argparse.ArgumentParser(description="EasyXT 数据质量报告生成器")
    parser.add_argument(
        "--data-dir", default=str(DEFAULT_DATA_DIR),
        help=f"feather 数据目录（默认: {DEFAULT_DATA_DIR}）"
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"报告输出路径（默认: {DEFAULT_OUTPUT}）"
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output = Path(args.output)

    if not data_dir.exists():
        print(f"[ERROR] 数据目录不存在: {data_dir}")
        sys.exit(1)

    run(data_dir, output)


if __name__ == "__main__":
    main()

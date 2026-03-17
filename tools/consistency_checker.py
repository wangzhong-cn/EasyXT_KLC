"""
回测一致性抽样校验器
用途：抽取 N 个同时拥有 1m + 1d feather 的品种，
      将 1m 数据聚合为日线，与已存 1d 数据对比，
      发现时区/夜盘归属/节假日错位等问题。

运行:
    python tools/consistency_checker.py --sample 5
    python tools/consistency_checker.py --sample 10 --data-dir data_export

核心检验逻辑:
    1. 加载品种的 1d feather 和 1m feather
    2. 对 1m 数据按"交易日"分组（夜盘 18:00+ 归属下一交易日）
    3. 聚合 OHLCV：open=first, high=max, low=min, close=last, volume=sum
    4. 与 1d bar 逐字段对比，允许浮点误差 tol=1e-6
    5. 统计匹配率、最大偏差、偏差最大的日期

输出:
    data_export/consistency_report.json
    控制台摘要表
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "data_export"
DEFAULT_OUTPUT = DEFAULT_DATA_DIR / "consistency_report.json"

# 浮点容差
# QMT 集合竞价处理说明（经实证验证）：
#   A股/指数：QMT 将 9:15-9:25 集合竞价的成交量吸收进 09:30 复合K线，
#             因此 1D.volume ≈ sum(1m.volume)，open 也应严格匹配，用 REL_TOL_STRICT。
#   股指期货：2014年后无独立集合竞价，09:30 开盘，全部严格校验。
#   商品期货：有夜盘（18:00/21:00-23:00/01:00），1m 首根 21:00 K线经常缺失，
#             导致 volume/open 有 0.1-17% 系统性偏差，须放宽容差。
REL_TOL_STRICT = 1e-4   # 0.01%，股票/指数/股指期货的所有字段
REL_TOL_OPEN   = 5e-3   # 0.5%，仅用于商品期货 open（夜盘首根缺失）
REL_TOL_VOLUME = 2e-2   # 2.0%，仅用于商品期货 volume（夜盘边界偏差）
ABS_TOL = 0.001         # abs 兜底
CORRUPT_MIN_1D_VOLUME = 1000.0
THIN_TRADE_OHLC_TOL = 1e-6  # 用于判定合约薄交易 O=H=L=C 的浮点容差

# 夜盘归属切换时间（UTC+8）：18:00 之后的 1m bar 归属"下一交易日"
NIGHT_SESSION_CUTOFF = dtime(18, 0, 0)


def _load_feather(path: Path) -> pd.DataFrame:
    try:
        return pd.read_feather(path)
    except Exception:
        return pd.DataFrame()


def _detect_type(symbol: str) -> str:
    """
    根据品种代码判断类型，用于选择校验容差策略。
    返回：'stock' / 'index' / 'stock_index_futures' / 'commodity_futures' / 'unknown'
    """
    if "." not in symbol:
        return "unknown"
    code, mkt = symbol.rsplit(".", 1)
    mkt = mkt.upper()
    # 商品期货市场后缀
    if mkt in ("SF", "DF", "ZF", "NF", "GF", "INE", "GFEX"):
        return "commodity_futures"
    # 股指/国债期货市场后缀
    if mkt == "IF":
        return "stock_index_futures"
    # 上交所：6xxxxx/5xxxxx 为股票，其余为指数/ETF
    if mkt == "SH":
        return "stock" if (code.startswith("6") or code.startswith("5")) else "index"
    # 深交所：399xxx 为指数，其余为股票
    if mkt == "SZ":
        return "index" if code.startswith("399") else "stock"
    # 北交所
    if mkt == "BJ":
        return "stock"
    return "unknown"


def _check_zero_vol_days(
    df_1m: pd.DataFrame, time_col: str, instr_type: str = "unknown",
) -> tuple[dict[str, float], dict[str, int]]:
    """
    检测数据腐败：统计各交易日中零成交量K线占比 > 50% 的日期。
    自动排除期货品种的"合约薄交易"日（O=H=L=C vol=0，合约生命周期正常现象）。

    返回 (corrupt_days, thin_trade_days):
      corrupt_days: {date_str: zero_fraction}  真正的数据腐败
      thin_trade_days: {date_str: bar_count}   合约薄交易（正常，不报警）
    """
    df = df_1m.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    if df.empty or "volume" not in df.columns:
        return {}, {}
    df["_date"] = pd.to_datetime(df[time_col], errors="coerce").map(lambda x: x.date())
    is_futures = instr_type in ("commodity_futures", "stock_index_futures")
    corrupt: dict[str, float] = {}
    thin_trade: dict[str, int] = {}
    for d, grp in df.groupby("_date"):
        n = len(grp)
        zero_frac = (grp["volume"] == 0).sum() / max(n, 1)
        if zero_frac > 0.5:
            # 期货品种：检测是否为合约薄交易（O=H=L=C，价格无波动）
            if is_futures and _is_thin_trading(grp):
                thin_trade[str(d)] = n
            else:
                corrupt[str(d)] = round(float(zero_frac), 4)
    return corrupt, thin_trade


def _is_thin_trading(grp: pd.DataFrame) -> bool:
    """判断一天的 1m 数据是否为期货合约薄交易特征（O=H=L=C vol=0）。"""
    ohlc = ["open", "high", "low", "close"]
    if not all(c in grp.columns for c in ohlc) or "volume" not in grp.columns:
        return False
    n = len(grp)
    zero_vol = (grp["volume"] == 0).sum()
    if zero_vol < n * 0.5:
        return False
    spread = np.asarray(grp["high"], dtype=float) - np.asarray(grp["low"], dtype=float)
    flat_bars = (np.abs(spread) < THIN_TRADE_OHLC_TOL).sum()
    return flat_bars >= n * 0.8


def _detect_corrupt_days(
    df_1d: pd.DataFrame,
    agg_1d: pd.DataFrame,
    common_dates: pd.Index,
    instr_type: str,
) -> dict[str, float]:
    corrupt: dict[str, float] = {}
    if "volume" not in df_1d.columns or "volume" not in agg_1d.columns:
        return corrupt
    for d in common_dates:
        v1d = float(np.asarray(df_1d.loc[d, "volume"]).reshape(-1)[0])
        v1m = float(np.asarray(agg_1d.loc[d, "volume"]).reshape(-1)[0])
        if v1d <= 0:
            continue
        if v1m <= 0 and v1d >= CORRUPT_MIN_1D_VOLUME:
            corrupt[str(d.date())] = 1.0
            continue
        rel_gap = abs(v1d - v1m) / max(v1d, 1.0)
        if instr_type in ("stock", "index", "stock_index_futures") and rel_gap > 0.2:
            corrupt[str(d.date())] = round(rel_gap, 4)
    return corrupt


def _find_time_col(df: pd.DataFrame) -> str | None:
    for col in ["date", "time", "datetime"]:
        if col in df.columns:
            return col
    if len(df.columns) > 0:
        first = df.columns[0]
        try:
            pd.to_datetime(df[first].iloc[0])
            return first
        except Exception:
            pass
    return None


def _make_assign_fn(trading_day_set: set[pd.Timestamp]):
    """
    返回一个 assign_trade_date 闭包，夜盘 18:00+ 时归属到下一个"真实交易日"，
    而不是下一个自然日（避免周五夜盘被归到周六这类非交易日）。

    trading_day_set: 从 1d feather 中提取的交易日集合（normalize 后的 Timestamp）。
    """
    sorted_days = sorted(trading_day_set)   # 升序列表，用于 bisect

    import bisect

    def _next_trading_day(base_date: pd.Timestamp) -> pd.Timestamp:
        """返回 base_date 之后的第一个交易日（不含 base_date 本身）。"""
        idx = bisect.bisect_right(sorted_days, base_date)
        if idx < len(sorted_days):
            return sorted_days[idx]
        # 超出已知范围：回退到 base_date 本身（不会漏数据）
        return base_date

    def _assign(ts: pd.Timestamp) -> pd.Timestamp:
        t = ts.time()
        if t >= NIGHT_SESSION_CUTOFF:
            cur_date = pd.Timestamp(ts.date())
            return _next_trading_day(cur_date)
        return pd.Timestamp(ts.date())

    return _assign


def _aggregate_1m_to_1d(
    df_1m: pd.DataFrame,
    time_col: str,
    trading_day_set: set[pd.Timestamp] | None = None,
) -> pd.DataFrame:
    """
    将 1m DataFrame 聚合为 1d，考虑夜盘归属。
    返回 DataFrame，index=trade_date，columns=[open, high, low, close, volume]

    trading_day_set: 传入后夜盘会归属到正确的下一个交易日（跳过周末/节假日）。
    """
    df = df_1m.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])

    if trading_day_set:
        _assign = _make_assign_fn(trading_day_set)
    else:
        def _assign(ts: pd.Timestamp) -> pd.Timestamp:  # type: ignore[misc]
            if ts.time() >= NIGHT_SESSION_CUTOFF:
                return pd.Timestamp((ts + timedelta(days=1)).date())
            return pd.Timestamp(ts.date())

    df["_trade_date"] = df[time_col].map(_assign)

    agg_dict: dict[str, Any] = {}
    for field, fn in [("open", "first"), ("high", "max"), ("low", "min"),
                      ("close", "last"), ("volume", "sum"), ("amount", "sum")]:
        if field in df.columns:
            agg_dict[field] = fn

    if not agg_dict:
        return pd.DataFrame()

    result = df.groupby("_trade_date").agg(agg_dict)
    result.index.name = "date"
    return result


def _near_equal(a: float, b: float, rel_tol: float = REL_TOL_STRICT) -> bool:
    """相对误差 + 绝对误差双重容差比较。"""
    if a == b:
        return True
    if np.isnan(a) and np.isnan(b):
        return True
    if np.isnan(a) or np.isnan(b):
        return False
    denom = max(abs(a), abs(b), 1e-10)
    return abs(a - b) / denom <= rel_tol or abs(a - b) <= ABS_TOL


def _compare_symbol(
    symbol: str,
    path_1d: Path,
    path_1m: Path,
) -> dict[str, Any]:
    """
    对单个品种做 1m→1d 聚合一致性校验。
    返回校验结果字典。
    """
    result: dict[str, Any] = {
        "symbol": symbol,
        "path_1d": str(path_1d.relative_to(DEFAULT_DATA_DIR)),
        "path_1m": str(path_1m.relative_to(DEFAULT_DATA_DIR)),
        "issues": [],
        "status": "pass",
    }

    instr_type = _detect_type(symbol)
    result["instr_type"] = instr_type

    df_1d = _load_feather(path_1d)
    df_1m = _load_feather(path_1m)

    if df_1d.empty:
        result["issues"].append("empty_1d_file")
        result["status"] = "fail"
        return result
    if df_1m.empty:
        result["issues"].append("empty_1m_file")
        result["status"] = "fail"
        return result

    tc_1d = _find_time_col(df_1d)
    tc_1m = _find_time_col(df_1m)
    if not tc_1d or not tc_1m:
        result["issues"].append("no_time_column")
        result["status"] = "skip"
        return result

    zero_vol_days, thin_trade_days = _check_zero_vol_days(df_1m, tc_1m, instr_type)
    if zero_vol_days:
        result["zero_vol_days"] = zero_vol_days
    if thin_trade_days:
        result["thin_trade_days"] = thin_trade_days
        result["thin_trade_days_count"] = len(thin_trade_days)

    # align 1d
    df_1d = df_1d.copy()
    df_1d[tc_1d] = pd.to_datetime(df_1d[tc_1d], errors="coerce")
    df_1d = df_1d.dropna(subset=[tc_1d]).set_index(tc_1d)
    df_1d.index = pd.DatetimeIndex(df_1d.index).normalize()  # 去掉时分秒

    # 从本品种 1d 数据中提取交易日集合，用于夜盘正确归属
    trading_day_set = set(df_1d.index)

    # aggregate 1m → 1d（传入交易日集合，夜盘归属到正确的下一交易日）
    agg_1d = _aggregate_1m_to_1d(df_1m, tc_1m, trading_day_set)
    if agg_1d.empty:
        result["issues"].append("aggregation_failed")
        result["status"] = "skip"
        return result

    # overlap dates
    common_dates = df_1d.index.intersection(agg_1d.index)
    if len(common_dates) == 0:
        result["issues"].append("no_common_dates")
        result["status"] = "skip"
        return result

    result["common_days"] = len(common_dates)
    result["1d_days"] = len(df_1d)
    result["1m_agg_days"] = len(agg_1d)

    corrupt_days = _detect_corrupt_days(df_1d, agg_1d, common_dates, instr_type)
    if corrupt_days:
        result["corrupt_days"] = corrupt_days
        result["issues"].append(f"volume_corruption:{len(corrupt_days)}days")

    # per-field comparison
    fields = ["open", "high", "low", "close", "volume"]
    field_stats: dict[str, Any] = {}
    any_mismatch = False

    for field in fields:
        if field not in df_1d.columns or field not in agg_1d.columns:
            continue

        v1d = df_1d.loc[common_dates, field].values.astype(float)
        vagg = agg_1d.loc[common_dates, field].values.astype(float)

        mismatches = []
        max_rel_err = 0.0
        worst_date = None

        # 按字段 + 品种类型选择容忍度：
        # 商品期货夜盘首根常缺失，open/volume 需放宽；其他品种全部严格。
        is_commodity = (instr_type == "commodity_futures")
        if field == "open" and is_commodity:
            tol = REL_TOL_OPEN
        elif field == "volume" and is_commodity:
            tol = REL_TOL_VOLUME
        else:
            tol = REL_TOL_STRICT

        for i, (a, b) in enumerate(zip(v1d, vagg)):
            if not _near_equal(a, b, tol):
                mismatches.append(common_dates[i])
                if max(abs(a), abs(b), 1e-10) > 0:
                    rel_err = abs(a - b) / max(abs(a), abs(b), 1e-10)
                    if rel_err > max_rel_err:
                        max_rel_err = rel_err
                        worst_date = str(common_dates[i].date())

        match_rate = 1.0 - len(mismatches) / max(len(common_dates), 1)
        field_stats[field] = {
            "match_rate": round(match_rate, 6),
            "mismatch_count": len(mismatches),
            "max_rel_err": round(max_rel_err, 6),
            "worst_date": worst_date,
        }

        if len(mismatches) > 0:
            any_mismatch = True
            # fail 阈值：
            # close：严格 95%（最关键）
            # high/low：0.90
            # open/volume 商品期货：0.80（夜盘首根糟糟缺失，已宽容处理）
            # open/volume 其他品种：0.90（应严格匹配）
            if field == "close":
                fail_threshold = 0.95
            elif field in ("high", "low"):
                fail_threshold = 0.90
            elif is_commodity:
                fail_threshold = 0.80  # open / volume 商品期货
            else:
                fail_threshold = 0.90  # open / volume 股票/指数/股指期货
            if match_rate < fail_threshold:
                result["issues"].append(
                    f"{field}_low_match:{match_rate:.2%}"
                )
            else:
                result["issues"].append(
                    f"{field}_mismatch:{len(mismatches)}"
                )

    result["field_stats"] = field_stats

    if not result["issues"]:
        result["status"] = "pass"
    elif any(
        "low_match" in issue or "volume_corruption" in issue
        for issue in result["issues"]
    ):
        result["status"] = "fail"
    else:
        result["status"] = "warn"

    return result


def _find_pairs(data_dir: Path) -> list[tuple[str, Path, Path]]:
    """
    扫描 data_dir，找出同时有 1d 和 1m feather 的品种。
    返回 [(symbol_key, path_1d, path_1m), ...]
    """
    pairs = []
    for path_1d in sorted(data_dir.rglob("*_1d.feather")):
        if "tick_snapshot" in path_1d.name:
            continue
        stem = path_1d.stem  # e.g. "IF01_IF_1d"
        base = stem[:-3]     # e.g. "IF01_IF"
        path_1m = path_1d.parent / f"{base}_1m.feather"
        if path_1m.exists():
            symbol_key = base.replace("_", ".", 1)  # "IF01.IF"
            pairs.append((symbol_key, path_1d, path_1m))
    return pairs


def run(data_dir: Path, output: Path, sample_n: int) -> dict:
    print(f"[一致性校验] 数据目录: {data_dir}")

    all_pairs = _find_pairs(data_dir)
    print(f"  找到同时有 1d+1m 的品种: {len(all_pairs)} 个")

    if not all_pairs:
        print("  [SKIP] 无可校验品种（需先运行 download 获取 1m 数据）")
        return {"summary": {"skipped": True}, "symbols": []}

    # 抽样
    if sample_n > 0 and sample_n < len(all_pairs):
        # 优先抽股指期货（quality signal），再随机补充
        priority = [p for p in all_pairs if "IF01" in p[0] or "IC01" in p[0]
                    or "000300" in p[0] or "cu01" in p[0]]
        rest = [p for p in all_pairs if p not in priority]
        selected = priority[:min(2, len(priority))]
        need = sample_n - len(selected)
        if need > 0 and rest:
            selected += random.sample(rest, min(need, len(rest)))
    else:
        selected = all_pairs

    print(f"  本次校验: {len(selected)} 个品种")
    for sym, p1d, p1m in selected:
        print(f"    {sym}")

    results = []
    for symbol, path_1d, path_1m in selected:
        res = _compare_symbol(symbol, path_1d, path_1m)
        results.append(res)

    # 汇总
    summary = {
        "total": len(results),
        "pass": sum(1 for r in results if r["status"] == "pass"),
        "warn": sum(1 for r in results if r["status"] == "warn"),
        "fail": sum(1 for r in results if r["status"] == "fail"),
        "skip": sum(1 for r in results if r["status"] == "skip"),
    }

    corrupt_symbols = sorted(
        r["symbol"] for r in results if r.get("corrupt_days")
    )

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": str(data_dir),
        "sample_n": sample_n,
        "summary": summary,
        "corrupt_symbols": corrupt_symbols,
        "symbols": results,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # ── 打印摘要 ──────────────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  1m→1d 聚合一致性校验结果")
    print(f"{'='*70}")
    print(f"  ✅ pass: {summary['pass']}  |  ⚠️  warn: {summary['warn']}  "
          f"|  ❌ fail: {summary['fail']}  |  ⏭  skip: {summary['skip']}")
    print(f"{'='*70}")
    print(f"  {'品种':<15} {'状态':<6} {'公共天数':<8} "
          f"{'close匹配率':<14} {'最大偏差日':<14} {'问题'}")
    print(f"  {'-'*67}")

    for r in results:
        sym = r["symbol"][:14]
        st = r["status"]
        icon = {"pass": "✅", "warn": "⚠️ ", "fail": "❌", "skip": "⏭ "}.get(st, "?")
        common = r.get("common_days", "-")
        fs = r.get("field_stats", {}).get("close", {})
        mr = f"{fs.get('match_rate', 0)*100:.2f}%" if fs else "-"
        wd = fs.get("worst_date", "-") or "-"
        issues = ", ".join(r.get("issues", [])) or "OK"
        print(f"  {icon} {sym:<15} {common:<8} {mr:<14} {wd:<14} {issues}")

    print(f"\n  报告已输出: {output}")
    if corrupt_symbols:
        print(f"  [腐败品种] {len(corrupt_symbols)} 个: {', '.join(corrupt_symbols)}")
    return report


def main():
    parser = argparse.ArgumentParser(description="EasyXT 1m→1d 聚合一致性校验器")
    parser.add_argument(
        "--data-dir", default=str(DEFAULT_DATA_DIR),
        help=f"feather 数据目录（默认: {DEFAULT_DATA_DIR}）"
    )
    parser.add_argument(
        "--output", default=str(DEFAULT_OUTPUT),
        help=f"报告输出路径（默认: {DEFAULT_OUTPUT}）"
    )
    parser.add_argument(
        "--sample", type=int, default=5,
        help="随机抽样品种数（0=全量，默认5）"
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="随机种子（保证可重现，默认42）"
    )
    parser.add_argument(
        "--strict", action="store_true",
        help="严格模式：若存在 fail/腐败品种则返回非0退出码"
    )
    parser.add_argument(
        "--fail-on-warn", action="store_true",
        help="严格模式扩展：warn 也视为失败（需配合 --strict）"
    )
    args = parser.parse_args()

    random.seed(args.seed)
    data_dir = Path(args.data_dir)
    output = Path(args.output)

    if not data_dir.exists():
        print(f"[ERROR] 数据目录不存在: {data_dir}")
        sys.exit(1)

    report = run(data_dir, output, args.sample)
    if args.strict:
        summary = report.get("summary", {})
        has_fail = int(summary.get("fail", 0)) > 0
        has_warn = int(summary.get("warn", 0)) > 0
        has_corrupt = len(report.get("corrupt_symbols", [])) > 0
        if has_fail or has_corrupt or (args.fail_on_warn and has_warn):
            sys.exit(2)


if __name__ == "__main__":
    main()

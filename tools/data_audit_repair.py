"""
EasyXT 数据终极审计 & 修复管线
==========================================================
Phase 1: 全量灾情评估（离线，无需 QMT）
Phase 2: 靶向重下载（需 QMT 在线）
Phase 3: 修复后全量重验证（离线）

运行:
  python tools/data_audit_repair.py --phase diagnose     # Phase 1: 评估
  python tools/data_audit_repair.py --phase repair        # Phase 2: 重下载
  python tools/data_audit_repair.py --phase validate      # Phase 3: 重验证
  python tools/data_audit_repair.py --phase all           # 全流程
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import sys
import time
from datetime import date, time as dtime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"
REPORT_DIR = DATA_DIR / "audit_reports"
NIGHT_CUTOFF = dtime(18, 0, 0)

# 合约生命周期：主力合约切换时，旧合约会出现"薄交易"特征
# （O=H=L=C 且 volume=0），这不是数据腐败，不应触发修复。
THIN_TRADE_OHLC_TOL = 1e-6  # OHLC 全相等的浮点容差

# ═══════════════════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════════════════

def load(p: Path) -> pd.DataFrame:
    try:
        return pd.read_feather(p)
    except Exception:
        return pd.DataFrame()


def detect_type(sym: str) -> str:
    if "." not in sym:
        return "unknown"
    code, mkt = sym.rsplit(".", 1)
    mkt = mkt.upper()
    if mkt in ("SF", "DF", "ZF", "NF", "GF", "INE", "GFEX"):
        return "commodity_futures"
    if mkt == "IF":
        return "stock_index_futures"
    if mkt == "SH":
        return "stock" if (code.startswith("6") or code.startswith("5")) else "index"
    if mkt == "SZ":
        return "index" if code.startswith("399") else "stock"
    if mkt == "BJ":
        return "stock"
    return "unknown"


def find_timecol(df: pd.DataFrame) -> str | None:
    for c in ("date", "time", "datetime"):
        if c in df.columns:
            return c
    return None


def make_assign_fn(trading_days: set):
    days_sorted = sorted(trading_days)
    def _next(d: pd.Timestamp) -> pd.Timestamp:
        idx = bisect.bisect_right(days_sorted, d)
        return days_sorted[idx] if idx < len(days_sorted) else d
    def _assign(ts: pd.Timestamp) -> pd.Timestamp:
        if ts.time() >= NIGHT_CUTOFF:
            return _next(pd.Timestamp(ts.date()))
        return pd.Timestamp(ts.date())
    return _assign


def agg_1m_to_1d(df_1m: pd.DataFrame, tc: str, tdays: set) -> pd.DataFrame:
    df = df_1m.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    fn = make_assign_fn(tdays)
    df["_td"] = df[tc].map(fn)
    agg = {}
    for f, op in [("open", "first"), ("high", "max"), ("low", "min"),
                  ("close", "last"), ("volume", "sum"), ("amount", "sum")]:
        if f in df.columns:
            agg[f] = op
    if not agg:
        return pd.DataFrame()
    return df.groupby("_td").agg(agg)


def is_thin_trading_day(grp: pd.DataFrame) -> bool:
    """
    判断某一天的 1m K线是否为"合约薄交易"特征。
    特征：大部分 bar 的 O=H=L=C（价格无波动）且 volume=0。
    这是期货主力合约切换后旧合约的正常现象，不是数据腐败。
    """
    if grp.empty:
        return False
    ohlc = ["open", "high", "low", "close"]
    has_ohlc = all(c in grp.columns for c in ohlc)
    has_vol = "volume" in grp.columns

    if not has_ohlc or not has_vol:
        return False

    n = len(grp)
    # volume 全部为 0
    zero_vol = (grp["volume"] == 0).sum()
    if zero_vol < n * 0.5:
        return False

    # OHLC 无波动：每根 bar 的 O=H=L=C
    spread = np.asarray(grp["high"], dtype=float) - np.asarray(grp["low"], dtype=float)
    flat_bars = (np.abs(spread) < THIN_TRADE_OHLC_TOL).sum()
    return flat_bars >= n * 0.8


# ═══════════════════════════════════════════════════════════════════════
# Phase 1: 全量灾情评估
# ═══════════════════════════════════════════════════════════════════════

def _find_all_pairs() -> list[tuple[str, Path, Path]]:
    pairs = []
    for p1d in sorted(DATA_DIR.rglob("*_1d.feather")):
        if "tick" in p1d.name.lower():
            continue
        stem = p1d.stem
        base = stem[:-3]
        p1m = p1d.parent / f"{base}_1m.feather"
        sym = base.replace("_", ".", 1)
        pairs.append((sym, p1d, p1m))
    return pairs


def _diagnose_1d(sym: str, p1d: Path) -> dict:
    """1D 基础健康检查。"""
    df = load(p1d)
    if df.empty:
        return {"status": "empty", "rows": 0}
    tc = find_timecol(df)
    if not tc:
        return {"status": "no_timecol", "rows": len(df)}
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])

    result = {
        "status": "ok",
        "rows": len(df),
        "date_range": f"{pd.Timestamp(df[tc].min()).date()} → {pd.Timestamp(df[tc].max()).date()}",
        "zero_vol_days": 0,
        "zero_close_days": 0,
        "nan_days": 0,
    }
    if "volume" in df.columns:
        result["zero_vol_days"] = int((df["volume"] == 0).sum())
    if "close" in df.columns:
        result["zero_close_days"] = int((df["close"] == 0).sum())
        result["nan_days"] = int(df["close"].isna().sum())
    return result


def _diagnose_1m(sym: str, p1m: Path) -> dict:
    """1m 全面灾情评估。"""
    if not p1m.exists():
        return {"status": "missing", "rows": 0, "corrupt_days": [], "total_days": 0}

    df = load(p1m)
    if df.empty:
        return {"status": "empty", "rows": 0, "corrupt_days": [], "total_days": 0}

    tc = find_timecol(df)
    if not tc:
        return {"status": "no_timecol", "rows": len(df), "corrupt_days": [], "total_days": 0}

    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    df["_date"] = pd.to_datetime(df[tc], errors="coerce").map(lambda x: x.date())

    kind = detect_type(sym)
    # A股/指数正常日应有 241 bars（09:30-15:00，每分钟一根）
    # 股指期货 270 bars（09:30-15:15）
    # 商品期货各异
    expected_bars = {"stock": 241, "index": 241, "stock_index_futures": 270}

    corrupt_days = []      # volume 全0/多0（真正的数据腐败）
    thin_trade_days = []   # 合约薄交易（O=H=L=C vol=0，正常现象）
    short_days = []        # bar数不足
    total_days_set = set()
    is_futures = kind in ("commodity_futures", "stock_index_futures")

    for d, grp in df.groupby("_date"):
        total_days_set.add(str(d))
        n = len(grp)
        vol_col = grp["volume"] if "volume" in grp.columns else pd.Series(dtype=float)
        zero_frac = (vol_col == 0).sum() / max(n, 1) if len(vol_col) > 0 else 0

        if zero_frac > 0.5:
            # 期货品种：检查是否为合约薄交易（生命周期末期）
            if is_futures and is_thin_trading_day(grp):
                thin_trade_days.append({"date": str(d), "bars": n})
            else:
                corrupt_days.append({
                    "date": str(d),
                    "bars": n,
                    "zero_vol_bars": int((vol_col == 0).sum()),
                    "zero_frac": round(float(zero_frac), 4),
                    "vol_sum": float(vol_col.sum()) if len(vol_col) > 0 else 0,
                })

        exp = expected_bars.get(kind)
        if exp and 0 < n < int(exp * 0.9):
            short_days.append({"date": str(d), "bars": n, "expected": exp})

    # 首根K线时间统计（判断是否有集合竞价独立K线）
    first_bar_times = df.groupby("_date")[tc].min().map(lambda x: x.time() if pd.notna(x) else None)
    has_auction_bars = any(
        dtime(9, 15) <= t < dtime(9, 30) for t in first_bar_times
    )

    return {
        "status": "ok" if not corrupt_days else "corrupted",
        "rows": len(df),
        "total_days": len(total_days_set),
        "corrupt_days": corrupt_days,
        "corrupt_days_count": len(corrupt_days),
        "thin_trade_days_count": len(thin_trade_days),
        "short_days": short_days,
        "short_days_count": len(short_days),
        "has_auction_bars": has_auction_bars,
        "first_bar_min": str(first_bar_times.min()) if len(first_bar_times) > 0 else None,
        "first_bar_max": str(first_bar_times.max()) if len(first_bar_times) > 0 else None,
    }


def _diagnose_cross(sym: str, p1d: Path, p1m: Path) -> dict:
    """1D vs 1m 交叉校验。"""
    if not p1m.exists():
        return {"status": "no_1m"}

    df1d = load(p1d)
    df1m = load(p1m)
    if df1d.empty or df1m.empty:
        return {"status": "empty"}

    tc1d = find_timecol(df1d)
    tc1m = find_timecol(df1m)
    if not tc1d or not tc1m:
        return {"status": "no_timecol"}

    df1d = df1d.copy()
    df1d[tc1d] = pd.to_datetime(df1d[tc1d], errors="coerce")
    df1d = df1d.dropna(subset=[tc1d]).set_index(tc1d)
    df1d.index = pd.DatetimeIndex(df1d.index).normalize()
    tdays = set(df1d.index)

    agg = agg_1m_to_1d(df1m, tc1m, tdays)
    if agg.empty:
        return {"status": "agg_empty"}

    common = df1d.index.intersection(agg.index)
    if len(common) == 0:
        return {"status": "no_overlap"}

    # 逐日成交量残差
    vol_1d = df1d.loc[common, "volume"].values.astype(float) if "volume" in df1d.columns else None
    vol_agg = agg.loc[common, "volume"].values.astype(float) if "volume" in agg.columns else None

    if vol_1d is None or vol_agg is None:
        return {"status": "no_volume"}

    residuals = []
    day_details = []
    for i, d in enumerate(common):
        v1 = vol_1d[i]
        va = vol_agg[i]
        res = (v1 - va) / v1 if v1 > 0 else np.nan
        residuals.append(res)

        # 逐字段对比
        detail = {"date": str(d.date()), "vol_residual_pct": round(float(res * 100), 4) if not np.isnan(res) else None}
        for field in ["open", "high", "low", "close"]:
            if field in df1d.columns and field in agg.columns:
                a_raw = df1d.loc[d, field]
                b_raw = agg.loc[d, field]
                a = float(np.asarray(a_raw).reshape(-1)[0])
                b = float(np.asarray(b_raw).reshape(-1)[0])
                if abs(a) > 0:
                    err = abs(a - b) / abs(a)
                    detail[f"{field}_err_pct"] = round(float(err * 100), 6)
                    detail[f"{field}_match"] = err < 1e-4
        day_details.append(detail)

    residuals = np.array(residuals, dtype=float)
    valid = residuals[~np.isnan(residuals)]

    return {
        "status": "ok",
        "common_days": len(common),
        "1d_days": len(df1d),
        "1m_agg_days": len(agg),
        "vol_residual_median_pct": round(float(np.median(valid) * 100), 6) if len(valid) > 0 else None,
        "vol_residual_mean_pct": round(float(np.mean(valid) * 100), 6) if len(valid) > 0 else None,
        "vol_residual_std_pct": round(float(np.std(valid) * 100), 6) if len(valid) > 0 else None,
        "vol_residual_max_pct": round(float(np.max(np.abs(valid)) * 100), 6) if len(valid) > 0 else None,
        "open_match_rate": None,  # 下面算
        "close_match_rate": None,
        "day_details": day_details,
    }


def _diagnose_tick(tick_path: Path) -> dict:
    """Tick snapshot 交叉验证。"""
    if not tick_path.exists():
        return {"status": "no_tick"}
    df = load(tick_path)
    if df.empty:
        return {"status": "empty"}

    results = {}
    for _, row in df.iterrows():
        code = row.get("code", "")
        results[code] = {
            "tick_volume": int(row["volume"]) if "volume" in row.index else None,
            "tick_amount": float(row["amount"]) if "amount" in row.index else None,
            "tick_time": str(row["timetag"]) if "timetag" in row.index else None,
        }
    return {"status": "ok", "symbols": results, "count": len(results)}


def phase_diagnose():
    """Phase 1: 全量灾情评估。"""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 72)
    print("  Phase 1: 数据终极灾情评估")
    print("=" * 72)

    all_pairs = _find_all_pairs()
    print(f"  发现品种: {len(all_pairs)} 个\n")

    audit = {}
    needs_repair = []

    for sym, p1d, p1m in all_pairs:
        kind = detect_type(sym)
        print(f"  检查 {sym:<14} ({kind}) ...", end="")

        diag_1d = _diagnose_1d(sym, p1d)
        diag_1m = _diagnose_1m(sym, p1m)
        diag_cross = _diagnose_cross(sym, p1d, p1m)

        # 判定是否需要修复
        repair_reasons = []
        severity = "ok"

        # 1m 数据腐败（排除合约薄交易后的真正腐败）
        if diag_1m.get("corrupt_days_count", 0) > 0:
            n = diag_1m["corrupt_days_count"]
            total = diag_1m.get("total_days", 1)
            pct = n / max(total, 1) * 100
            if pct > 30:
                severity = "critical"
                repair_reasons.append(f"1m零成交量腐败{n}天({pct:.0f}%)(严重)")
            elif pct > 5:
                severity = "major" if severity != "critical" else severity
                repair_reasons.append(f"1m零成交量腐败{n}天({pct:.0f}%)")
            else:
                severity = "minor" if severity == "ok" else severity
                repair_reasons.append(f"1m零成交量腐败{n}天({pct:.0f}%)(轻微)")

        # 合约薄交易提示（不影响 severity）
        thin_n = diag_1m.get("thin_trade_days_count", 0)
        if thin_n > 0:
            repair_reasons.append(f"合约薄交易{thin_n}天(正常，非腐败)")

        # 1m 文件缺失
        if diag_1m.get("status") == "missing":
            severity = "critical"
            repair_reasons.append("1m文件不存在")

        # 1m 文件空
        if diag_1m.get("status") == "empty":
            severity = "critical"
            repair_reasons.append("1m文件为空")

        # 1D 数据异常（期货品种的零成交量日通常是合约切换，不视为严重问题）
        zv_1d = diag_1d.get("zero_vol_days", 0)
        if zv_1d > 0:
            if kind in ("commodity_futures", "stock_index_futures"):
                # 期货零成交量日属于合约生命周期正常现象
                repair_reasons.append(f"1D有{zv_1d}天零成交量(合约切换,正常)")
            else:
                severity = "major" if severity == "ok" else severity
                repair_reasons.append(f"1D有{zv_1d}天零成交量")

        # 1D 空文件
        if diag_1d.get("status") == "empty":
            severity = "critical"
            repair_reasons.append("1D文件为空")

        # 缺盘中数据（bar数不足）
        if diag_1m.get("short_days_count", 0) > 0:
            severity = "minor" if severity == "ok" else severity
            repair_reasons.append(f"1m有{diag_1m['short_days_count']}天bar数不足")

        status_icon = {
            "ok": "✅",
            "minor": "🟡",
            "major": "🟠",
            "critical": "🔴"
        }.get(severity, "?")

        print(f" {status_icon} {severity}")
        if repair_reasons:
            for r in repair_reasons:
                print(f"    → {r}")
            needs_repair.append({
                "sym": sym,
                "type": kind,
                "severity": severity,
                "reasons": repair_reasons,
                "corrupt_days": diag_1m.get("corrupt_days", []),
            })

        audit[sym] = {
            "type": kind,
            "severity": severity,
            "repair_reasons": repair_reasons,
            "diag_1d": diag_1d,
            "diag_1m": {k: v for k, v in diag_1m.items() if k != "corrupt_days"},
            "diag_1m_corrupt_count": diag_1m.get("corrupt_days_count", 0),
            "diag_1m_thin_trade_count": diag_1m.get("thin_trade_days_count", 0),
            "diag_cross": {k: v for k, v in diag_cross.items() if k != "day_details"},
        }

    # ── Tick 交叉 ──────────────────────────────────────────────────────────
    tick_info = _diagnose_tick(DATA_DIR / "tick_snapshot.feather")

    # ── 汇总 ──────────────────────────────────────────────────────────────
    summary = {
        "total_symbols": len(all_pairs),
        "ok": sum(1 for s in audit.values() if s["severity"] == "ok"),
        "minor": sum(1 for s in audit.values() if s["severity"] == "minor"),
        "major": sum(1 for s in audit.values() if s["severity"] == "major"),
        "critical": sum(1 for s in audit.values() if s["severity"] == "critical"),
        "needs_repair": len(needs_repair),
    }

    print(f"\n{'='*72}")
    print(f"  灾情汇总")
    print(f"{'='*72}")
    print(f"  总品种    : {summary['total_symbols']}")
    print(f"  ✅ 正常   : {summary['ok']}")
    print(f"  🟡 轻微   : {summary['minor']}")
    print(f"  🟠 严重   : {summary['major']}")
    print(f"  🔴 危急   : {summary['critical']}")
    print(f"  需修复    : {summary['needs_repair']} 个品种")

    if needs_repair:
        print(f"\n  ┌── 修复清单 ──────────────────────────────────────────┐")
        for item in sorted(needs_repair, key=lambda x: {"critical": 0, "major": 1, "minor": 2}.get(x["severity"], 3)):
            sev = item["severity"]
            icon = "🔴" if sev == "critical" else ("🟠" if sev == "major" else "🟡")
            print(f"  │ {icon} {item['sym']:<14} {sev:<10} {item['type']}")
            for r in item["reasons"]:
                print(f"  │    → {r}")
        print(f"  └──────────────────────────────────────────────────────┘")

    # ── 集合竞价专项 ──────────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  集合竞价处理方式验证（A股/指数专项）")
    print(f"{'='*72}")
    for sym, info in audit.items():
        if info["type"] in ("stock", "index"):
            cross = info.get("diag_cross", {})
            res_med = cross.get("vol_residual_median_pct")
            has_auction = info.get("diag_1m", {}).get("has_auction_bars", False)
            first_bar = info.get("diag_1m", {}).get("first_bar_min")
            if res_med is not None:
                verdict = "吸收(09:30复合K线)" if abs(res_med) < 0.01 else f"异常(残差{res_med:.4f}%)"
                print(f"  {sym:<14} 残差中位={res_med:+.4f}%  首根={first_bar}  "
                      f"有竞价K线={has_auction}  → {verdict}")

    # ── 商品期货夜盘专项 ──────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  商品期货夜盘首根缺失分析")
    print(f"{'='*72}")
    for sym, info in audit.items():
        if info["type"] == "commodity_futures":
            cross = info.get("diag_cross", {})
            res_med = cross.get("vol_residual_median_pct")
            if res_med is not None:
                # 区分腐败导致的偏差 vs 真实夜盘首根缺失
                corrupt_n = info.get("diag_1m_corrupt_count", 0)
                thin_n = info.get("diag_1m_thin_trade_count", 0)
                total_d = info.get("diag_1m", {}).get("total_days", 0)
                clean_pct = (total_d - corrupt_n) / max(total_d, 1) * 100
                print(f"  {sym:<14} 残差中位={res_med:+.4f}%  "
                      f"干净天占比={clean_pct:.0f}%  腐败天={corrupt_n}  薄交易={thin_n}")

    # ── 保存报告 ──────────────────────────────────────────────────────────
    report = {
        "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
        "summary": summary,
        "needs_repair": needs_repair,
        "tick_info": tick_info,
        "symbols": audit,
    }
    report_file = REPORT_DIR / "diagnosis_report.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  完整报告: {report_file}")

    # 生成修复脚本参数（供 Phase 2 使用）
    repair_symbols = [item["sym"] for item in needs_repair]
    repair_file = REPORT_DIR / "repair_list.json"
    with open(repair_file, "w", encoding="utf-8") as f:
        json.dump({
            "symbols": repair_symbols,
            "details": needs_repair,
        }, f, ensure_ascii=False, indent=2, default=str)
    print(f"  修复清单: {repair_file}")

    return summary, needs_repair


# ═══════════════════════════════════════════════════════════════════════
# Phase 2: 靶向重下载修复
# ═══════════════════════════════════════════════════════════════════════

def phase_repair():
    """Phase 2: 根据修复清单，靶向重新下载损坏数据。"""
    repair_file = REPORT_DIR / "repair_list.json"
    if not repair_file.exists():
        print("  [ERROR] 修复清单不存在，请先运行 --phase diagnose")
        return

    with open(repair_file, "r", encoding="utf-8") as f:
        repair_data = json.load(f)

    symbols = repair_data.get("symbols", [])
    if not symbols:
        print("  无需修复的品种！✅")
        return

    print("=" * 72)
    print(f"  Phase 2: 靶向数据修复（{len(symbols)} 个品种）")
    print("=" * 72)

    # 检查 QMT 连接
    try:
        import xtquant.xtdata as xtdata
        try:
            xtdata.enable_hello = False
        except Exception:
            pass
        print("  xtdata 模块已加载")
    except ImportError:
        print("  [ERROR] 无法导入 xtquant.xtdata，请确保 QMT 环境可用")
        return

    # 尝试连接测试
    try:
        test_data = xtdata.get_market_data(["close"], ["000001.SZ"], "1d", start_time="20260301")
        if not test_data or "close" not in test_data:
            print("  [WARN] QMT 数据接口返回空，可能未连接。继续尝试...")
    except Exception as e:
        print(f"  [WARN] QMT 连接测试异常: {e}")

    START_DATE = "20200101"
    OUT_DIR = str(DATA_DIR)
    FIELDS = ["open", "high", "low", "close", "volume", "amount"]

    ok_count = 0
    fail_count = 0

    for sym in symbols:
        print(f"\n  修复 {sym}:")

        for period in ["1d", "1m"]:
            print(f"    {period}: ", end="")
            try:
                # 先触发下载
                xtdata.download_history_data2([sym], period, START_DATE, "", None, True)
                time.sleep(0.5)

                # 获取数据
                data = xtdata.get_market_data(FIELDS, [sym], period, start_time=START_DATE)
                if not data or "close" not in data:
                    print(f"无数据 ❌")
                    fail_count += 1
                    continue

                close_df = data["close"]
                if not hasattr(close_df, "shape") or sym not in close_df.index:
                    print(f"品种不在返回中 ❌")
                    fail_count += 1
                    continue

                frames = {}
                for field in FIELDS:
                    if field in data and sym in data[field].index:
                        frames[field] = data[field].loc[sym]

                if not frames:
                    print(f"无字段 ❌")
                    fail_count += 1
                    continue

                df = pd.DataFrame(frames)
                df.index.name = "date"
                df = df.reset_index()

                # 写入
                mkt = sym.split(".")[-1] if "." in sym else "UNK"
                mkt_dir = DATA_DIR / mkt
                mkt_dir.mkdir(parents=True, exist_ok=True)
                safe_code = sym.replace(".", "_")
                fpath = mkt_dir / f"{safe_code}_{period}.feather"

                df.to_feather(fpath)
                print(f"{len(df)} 行 → {fpath.name} ✅")
                ok_count += 1

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"异常: {e} ❌")
                fail_count += 1

    print(f"\n  修复完成: 成功 {ok_count}, 失败 {fail_count}")
    return ok_count, fail_count


# ═══════════════════════════════════════════════════════════════════════
# Phase 3: 修复后全量验证
# ═══════════════════════════════════════════════════════════════════════

def phase_validate():
    """Phase 3: 全量验证（同 Phase 1 但标记为 post-repair）。"""
    print("=" * 72)
    print("  Phase 3: 修复后全量验证")
    print("=" * 72)
    summary, repairs = phase_diagnose()

    # 终审判定
    print(f"\n{'='*72}")
    print(f"  ┌── 终审判定 ──────────────────────────────────────────────┐")
    if summary["critical"] == 0 and summary["major"] == 0:
        print(f"  │  ✅ 数据质量合格，可以继续推进项目计划              │")
    elif summary["critical"] == 0:
        print(f"  │  🟡 存在轻微问题，建议修复后再推进                  │")
    else:
        print(f"  │  🔴 仍有严重问题，禁止推进！继续修复！              │")
    print(f"  └──────────────────────────────────────────────────────────┘")

    return summary


# ═══════════════════════════════════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="EasyXT 数据终极审计 & 修复管线")
    parser.add_argument(
        "--phase", default="diagnose",
        choices=["diagnose", "repair", "validate", "all"],
        help="运行阶段: diagnose(评估) | repair(修复) | validate(验证) | all(全流程)",
    )
    args = parser.parse_args()

    if args.phase == "diagnose":
        phase_diagnose()
    elif args.phase == "repair":
        phase_repair()
    elif args.phase == "validate":
        phase_validate()
    elif args.phase == "all":
        print("\n" + "█" * 72)
        print("  EasyXT 数据修复全流程")
        print("█" * 72)
        phase_diagnose()
        print("\n")
        phase_repair()
        print("\n")
        phase_validate()


if __name__ == "__main__":
    main()

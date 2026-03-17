"""
终极全量多源交叉验证 v3-fast — 5500+品种 × 批量读取 × 极速铁证
======================================================================
使用 qmt311 环境 (Python 3.11 + xtquant) 运行
采用 get_local_data 批量读取本地数据, 无需网络

验证矩阵:
  S1: QMT 本地 1D (黄金标准)
  S2: QMT 本地 1m → 聚合1D → 对比 S1 (volume精确匹配)
  S3: QMT 本地 5m → 聚合1D → 对比 S1 (5m可用时)
  S4: akshare 独立源 → 对比 S1 (A股抽样30+)
  S5: Feather 1D → 对比 S1 (已有导出时)

品种覆盖: SH+SZ+SF+DF+IF+ZF ≈ 5500+品种
"""
import time, sys, json, bisect, os, traceback
from pathlib import Path
from datetime import time as dtime, timedelta, datetime
from collections import defaultdict, Counter
from typing import Any
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data_export"
QMT_BASE = Path(r"D:\申万宏源策略量化交易终端\userdata_mini\datadir")
START = "20240101"

import xtquant.xtdata as xtdata
xtdata.enable_hello = False

# ═══════════════════════════════════════════
# 品种分类 (与v3一致)
# ═══════════════════════════════════════════
NIGHT_CODES = {
    "cu", "al", "zn", "pb", "ni", "sn", "au", "ag", "rb", "hc",
    "bu", "fu", "sp", "ss", "ao", "br", "ec", "lu", "nr", "sc",
    "bc", "si",
    "c", "cs", "a", "b", "m", "y", "p", "jd", "l", "v", "pp",
    "j", "jm", "i", "fb", "bb", "eg", "eb", "pg", "rr", "lh",
    "cf", "sr", "ta", "ma", "fg", "oi", "rm", "zc", "jr", "lr",
    "wh", "pm", "ri", "rs", "sf", "sm", "cy", "ap", "cj", "ur",
    "sa", "pf", "pk", "sh",
}

def classify_symbol(code):
    if "." not in code:
        return "unknown"
    name, mkt = code.rsplit(".", 1)
    if mkt == "SH":
        if name.startswith(("000", "880")):
            return "index"
        elif name.startswith(("51", "56", "58")):
            return "etf"
        elif name.startswith(("60", "68")):
            return "stock"
        elif name.startswith("11"):
            return "bond"
        return "other_sh"
    elif mkt == "SZ":
        if name.startswith(("399", "980")):
            return "index"
        elif name.startswith(("15", "16")):
            return "etf"
        elif name.startswith(("00", "30")):
            return "stock"
        elif name.startswith("12"):
            return "bond"
        return "other_sz"
    elif mkt == "IF":
        return "index_futures"
    elif mkt in ("SF", "DF", "ZF"):
        base = ''.join(c for c in name if c.isalpha()).lower()
        return "commodity_night" if base in NIGHT_CODES else "commodity_day"
    elif mkt == "HK":
        return "hk_stock"
    return "unknown"


def has_night_session(kind):
    return "night" in kind


# ═══════════════════════════════════════════
# 构建品种列表
# ═══════════════════════════════════════════
def build_symbol_list():
    symbols = {}
    for mkt in ["SH", "SZ", "SF", "DF", "IF", "ZF", "HK"]:
        p1m = QMT_BASE / mkt / "60"
        if not p1m.exists():
            continue
        for dat in p1m.glob("*.DAT"):
            code = f"{dat.stem}.{mkt}"
            symbols[code] = classify_symbol(code)
    return symbols


# ═══════════════════════════════════════════
# 批量读取本地数据
# ═══════════════════════════════════════════
FIELDS = ["open", "high", "low", "close", "volume", "amount"]

SKIP_MARKETS = {"IF"}  # IF market crashes xtquant bson layer

def batch_read_local(sym_list, period, start=START, batch_size=100):
    """批量读取本地数据，按市场分组，返回 {sym: DataFrame}"""
    # 按市场分组
    by_mkt = defaultdict(list)
    for sym in sym_list:
        mkt = sym.split(".")[-1] if "." in sym else "UNK"
        if mkt not in SKIP_MARKETS:
            by_mkt[mkt].append(sym)

    all_data = {}
    for mkt, syms in by_mkt.items():
        for i in range(0, len(syms), batch_size):
            batch = syms[i:i+batch_size]
            try:
                raw = xtdata.get_local_data(field_list=FIELDS, stock_list=batch, period=period, start_time=start)
                for sym in batch:
                    if sym in raw and raw[sym].shape[0] > 0:
                        df = _parse_raw(raw[sym])
                        if df is not None:
                            all_data[sym] = df
            except Exception:
                # 批量失败时逐个读取
                for sym in batch:
                    try:
                        raw = xtdata.get_local_data(field_list=FIELDS, stock_list=[sym], period=period, start_time=start)
                        if sym in raw and raw[sym].shape[0] > 0:
                            df = _parse_raw(raw[sym])
                            if df is not None:
                                all_data[sym] = df
                    except Exception:
                        pass

    skipped = sum(len(v) for k, v in by_mkt.items() if k in SKIP_MARKETS)
    if skipped:
        print(f"    (skipped {skipped} symbols from {SKIP_MARKETS} due to xtquant crash)")
    return all_data


def _parse_raw(raw_df):
    """解析xtdata返回的单品种DataFrame"""
    df = raw_df.copy()
    df.index.name = "date"
    df = df.reset_index()
    sample = str(df["date"].iloc[0])
    fmt = "%Y%m%d" if len(sample) <= 8 else "%Y%m%d%H%M%S"
    df["date"] = pd.to_datetime(df["date"], format=fmt, errors="coerce")
    df = df.dropna(subset=["date"])
    if "volume" in df.columns:
        df = df[df["volume"] > 0]
    return df if len(df) > 0 else None


# ═══════════════════════════════════════════
# 交易日与聚合
# ═══════════════════════════════════════════
def assign_trading_day(ts, sorted_tdays, night):
    if night and ts.time() >= dtime(18, 0):
        cur = pd.Timestamp(ts.date())
        idx = bisect.bisect_right(sorted_tdays, cur)
        return sorted_tdays[idx] if idx < len(sorted_tdays) else cur
    return pd.Timestamp(ts.date())


def agg_to_1d(df, sorted_tdays=None, night=False):
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    if sorted_tdays and night:
        df["_td"] = df["date"].apply(lambda ts: assign_trading_day(ts, sorted_tdays, True))
    else:
        df["_td"] = df["date"].dt.normalize()

    agg = {}
    for f, op in [("open", "first"), ("high", "max"), ("low", "min"),
                  ("close", "last"), ("volume", "sum"), ("amount", "sum")]:
        if f in df.columns:
            agg[f] = op
    if not agg:
        return pd.DataFrame()
    result = df.groupby("_td").agg(agg)
    if "volume" in result.columns:
        result = result[result["volume"] > 0]
    return result


# ═══════════════════════════════════════════
# 比对
# ═══════════════════════════════════════════
def compare_1d(ref_idx, test_idx, trim_edges=True):
    """对比两个DatetimeIndex索引的1D数据
    返回: (common_days, vol_residual_median_pct, vol_exact_pct)"""
    if ref_idx.empty or test_idx.empty:
        return (0, None, None)

    common = ref_idx.index.intersection(test_idx.index)
    if trim_edges and len(common) > 10:
        common = common[1:-1]
    if len(common) < 3:
        return (len(common), None, None)

    if "volume" not in ref_idx.columns or "volume" not in test_idx.columns:
        return (len(common), None, None)

    rv = ref_idx.loc[common, "volume"].values.astype(float)
    tv = test_idx.loc[common, "volume"].values.astype(float)

    mask = rv > 0
    if mask.sum() == 0:
        return (len(common), None, None)

    residual = (rv[mask] - tv[mask]) / rv[mask]
    tol = max(np.abs(rv).max() * 1e-6, 0.01)
    exact = np.sum(np.abs(rv - tv) < tol)

    return (
        len(common),
        round(float(np.median(residual) * 100), 6),
        round(float(exact / len(common) * 100), 2)
    )


# ═══════════════════════════════════════════
# 首bar分析
# ═══════════════════════════════════════════
def check_first_bar(df, kind):
    """返回: {day_first, has_auction_bar, night_first, has_night_auction}"""
    if df.empty or len(df) < 10:
        return {}
    df = df.copy()
    df["_t"] = df["date"].dt.time
    df["_d"] = df["date"].dt.date

    result = {}

    # 日盘首bar
    if kind in ("stock", "index", "etf", "index_futures", "bond"):
        day_df = df[(df["_t"] >= dtime(9, 0)) & (df["_t"] <= dtime(10, 0))]
    else:
        day_df = df[(df["_t"] >= dtime(8, 50)) & (df["_t"] <= dtime(10, 0))]

    if not day_df.empty:
        first_times = day_df.groupby("_d")["_t"].min()
        if len(first_times) > 0:
            result["day_first"] = str(first_times.mode().iloc[0])
            if kind in ("stock", "index", "etf", "index_futures", "bond"):
                result["has_auction_bar"] = bool(any(dtime(9, 25) <= t < dtime(9, 30) for t in first_times))
            else:
                result["has_auction_bar"] = bool(any(dtime(8, 55) <= t < dtime(9, 0) for t in first_times))

    # 夜盘首bar
    if has_night_session(kind):
        night = df[(df["_t"] >= dtime(20, 50)) & (df["_t"] <= dtime(22, 0))]
        if not night.empty:
            nf_times = night.groupby("_d")["_t"].min()
            if len(nf_times) > 0:
                result["night_first"] = str(nf_times.mode().iloc[0])
                result["has_night_auction"] = bool(any(dtime(20, 55) <= t < dtime(21, 0) for t in nf_times))

    return result


def count_daily_bars(df):
    if df.empty:
        return {}
    counts = df.groupby(df["date"].dt.date).size()
    return {
        "median": int(counts.median()),
        "min": int(counts.min()),
        "max": int(counts.max()),
        "mode": int(counts.mode().iloc[0]) if len(counts) > 0 else 0,
        "total_days": int(len(counts)),
    }


# ═══════════════════════════════════════════
# akshare独立源
# ═══════════════════════════════════════════
def get_akshare_daily(code, start_date="20240101"):
    try:
        import akshare as ak
        name, mkt = code.rsplit(".", 1)
        if mkt not in ("SH", "SZ"):
            return pd.DataFrame()
        kind = classify_symbol(code)
        if kind not in ("stock", "etf"):
            return pd.DataFrame()
        df = ak.stock_zh_a_hist(
            symbol=name, period="daily",
            start_date=start_date, end_date="20261231", adjust=""
        )
        if df.empty:
            return df
        df = df.rename(columns={
            "日期": "date", "开盘": "open", "最高": "high",
            "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"
        })
        df["date"] = pd.to_datetime(df["date"])
        return df[["date", "open", "high", "low", "close", "volume", "amount"]]
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════
# Feather加载
# ═══════════════════════════════════════════
def load_feather(sym, period):
    mkt = sym.split(".")[-1] if "." in sym else "UNK"
    safe = sym.replace(".", "_")
    p = DATA_DIR / mkt / f"{safe}_{period}.feather"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_feather(p)
        tc = next((c for c in ["date", "time", "datetime"] if c in df.columns), None)
        if tc:
            df[tc] = pd.to_datetime(df[tc], errors="coerce")
            if tc != "date":
                df = df.rename(columns={tc: "date"})
        return df
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════
# 主验证流程
# ═══════════════════════════════════════════
def run():
    t0 = time.time()
    print("=" * 80)
    print("  终极全量多源交叉验证 v3-fast — 批量读取 × 极速铁证")
    print("=" * 80)

    # ── Step 1: 构建品种列表 ──
    symbols = build_symbol_list()
    sym_list = sorted(symbols.keys())
    kind_counts = Counter(symbols.values())
    print(f"\n  总品种数: {len(sym_list)}")
    for k, c in sorted(kind_counts.items()):
        print(f"    {k:20s}: {c}")

    # ── Step 2: 批量读取所有1D数据 ──
    print(f"\n  [Phase 1] 批量读取1D数据...")
    t1 = time.time()
    data_1d = batch_read_local(sym_list, "1d")
    print(f"    1D: {len(data_1d)} symbols loaded in {time.time()-t1:.1f}s")

    # ── Step 3: 批量读取所有1m数据 ──
    print(f"  [Phase 2] 批量读取1m数据...")
    t2 = time.time()
    data_1m = batch_read_local(sym_list, "1m")
    print(f"    1m: {len(data_1m)} symbols loaded in {time.time()-t2:.1f}s")

    # ── Step 4: 批量读取5m数据 (有DAT文件的品种) ──
    syms_with_5m = []
    for sym in sym_list:
        mkt = sym.split(".")[-1]
        p5m = QMT_BASE / mkt / "300" / f"{sym.split('.')[0]}.DAT"
        if p5m.exists():
            syms_with_5m.append(sym)

    data_5m = {}
    if syms_with_5m:
        print(f"  [Phase 3] 批量读取5m数据 ({len(syms_with_5m)} symbols)...")
        t3 = time.time()
        data_5m = batch_read_local(syms_with_5m, "5m")
        print(f"    5m: {len(data_5m)} symbols loaded in {time.time()-t3:.1f}s")

    print(f"\n  数据加载完成: 1d={len(data_1d)}, 1m={len(data_1m)}, 5m={len(data_5m)}, 耗时={time.time()-t0:.1f}s")

    # ── Step 5: 逐品种验证 ──
    print(f"\n  [Phase 4] 逐品种验证...")
    results = []
    first_bar_stats = []
    bar_count_stats = []
    errors = []
    akshare_results = []
    group_id = 0
    processed = 0
    skipped = 0
    t4 = time.time()

    # akshare 抽样: 股票30+, ETF 5+, 指数5+
    ak_sample = set()
    for code, kind in sorted(symbols.items()):
        if kind == "stock" and sum(1 for c in ak_sample if symbols[c] == "stock") < 50:
            ak_sample.add(code)
        elif kind == "etf" and sum(1 for c in ak_sample if symbols[c] == "etf") < 10:
            ak_sample.add(code)
        elif kind == "index" and sum(1 for c in ak_sample if symbols[c] == "index") < 5:
            ak_sample.add(code)

    for sym in sym_list:
        processed += 1
        kind = symbols[sym]
        night = has_night_session(kind)

        if processed % 500 == 0:
            elapsed = time.time() - t4
            rate = processed / max(elapsed, 0.1)
            eta = (len(sym_list) - processed) / max(rate, 0.1)
            print(f"    [{processed}/{len(sym_list)}] {rate:.0f}/s ETA={eta:.0f}s groups={group_id}")

        try:
            # 1D黄金标准
            if sym not in data_1d:
                skipped += 1
                continue
            df_1d = data_1d[sym]
            if len(df_1d) < 5:
                skipped += 1
                continue

            ref = df_1d.set_index("date").copy()
            ref.index = ref.index.normalize()
            sorted_tdays = sorted(set(ref.index))

            # ── S2: 1m → agg 1D ──
            if sym in data_1m:
                df_1m = data_1m[sym]
                if len(df_1m) > 10:
                    # 首bar分析
                    fb = check_first_bar(df_1m, kind)
                    if fb:
                        fb["sym"] = sym
                        fb["kind"] = kind
                        first_bar_stats.append(fb)

                    # 每日bar数
                    bc = count_daily_bars(df_1m)
                    if bc:
                        bc["sym"] = sym
                        bc["kind"] = kind
                        bar_count_stats.append(bc)

                    # 聚合对比
                    agg = agg_to_1d(df_1m, sorted_tdays, night)
                    common, vr_med, vr_pct = compare_1d(ref, agg)
                    if common >= 3:
                        group_id += 1
                        results.append({
                            "group": group_id, "sym": sym, "kind": kind,
                            "source": "S2:1m→1d", "common_days": common,
                            "vol_residual_pct": vr_med, "vol_exact_pct": vr_pct,
                            "day_first": fb.get("day_first"),
                            "bars_per_day_mode": bc.get("mode"),
                        })

            # ── S3: 5m → agg 1D ──
            if sym in data_5m:
                df_5m = data_5m[sym]
                if len(df_5m) > 5:
                    agg5 = agg_to_1d(df_5m, sorted_tdays, night)
                    common5, vr5_med, vr5_pct = compare_1d(ref, agg5)
                    if common5 >= 3:
                        group_id += 1
                        results.append({
                            "group": group_id, "sym": sym, "kind": kind,
                            "source": "S3:5m→1d", "common_days": common5,
                            "vol_residual_pct": vr5_med, "vol_exact_pct": vr5_pct,
                        })

            # ── S5: Feather 1D ──
            feath = load_feather(sym, "1d")
            if not feath.empty and "date" in feath.columns:
                fi = feath.set_index("date")
                fi.index = pd.to_datetime(fi.index).normalize()
                common_f, vr_f, vr_f_pct = compare_1d(ref, fi)
                if common_f >= 3:
                    group_id += 1
                    results.append({
                        "group": group_id, "sym": sym, "kind": kind,
                        "source": "S5:Feather_1d", "common_days": common_f,
                        "vol_residual_pct": vr_f, "vol_exact_pct": vr_f_pct,
                    })

        except Exception as e:
            errors.append({"sym": sym, "kind": kind, "error": str(e), "tb": traceback.format_exc()})

    print(f"\n    品种验证完成: {processed} processed, {skipped} skipped, {group_id} groups, {len(errors)} errors")

    # ── Step 6: akshare 独立源抽样验证 ──
    print(f"\n  [Phase 5] akshare独立源验证 ({len(ak_sample)} symbols)...")
    t5 = time.time()
    for sym in sorted(ak_sample):
        try:
            ak_df = get_akshare_daily(sym)
            if ak_df.empty:
                continue
            if sym not in data_1d:
                continue
            ref = data_1d[sym].set_index("date").copy()
            ref.index = pd.DatetimeIndex(ref.index).normalize()
            ak_idx = ak_df.set_index("date")
            ak_idx.index = pd.DatetimeIndex(ak_idx.index).normalize()

            # akshare volume is in 手 (lots), QMT is in 股 (shares)
            # need to check unit difference
            common_idx = ref.index.intersection(ak_idx.index)
            if len(common_idx) > 5:
                # Check if akshare volume needs *100 conversion
                rv = np.asarray(ref.loc[common_idx[:10], "volume"], dtype=float)
                av = np.asarray(ak_idx.loc[common_idx[:10], "volume"], dtype=float)
                if av.sum() > 0:
                    ratio = np.median(rv / av)
                    if 90 < ratio < 110:
                        ak_idx["volume"] = ak_idx["volume"] * 100  # 手→股

            common, vr, vr_pct = compare_1d(ref, ak_idx)
            if common >= 3:
                group_id += 1
                r = {
                    "group": group_id, "sym": sym, "kind": symbols[sym],
                    "source": "S4:akshare_1d", "common_days": common,
                    "vol_residual_pct": vr, "vol_exact_pct": vr_pct,
                }
                results.append(r)
                akshare_results.append(r)
            time.sleep(0.2)
        except Exception as e:
            errors.append({"sym": sym, "kind": symbols.get(sym, "?"), "source": "akshare", "error": str(e)})

    print(f"    akshare: {len(akshare_results)} symbols verified in {time.time()-t5:.1f}s")

    # ═══════════════════════════════════════════
    # 汇总报告
    # ═══════════════════════════════════════════
    elapsed = time.time() - t0
    print(f"\n\n{'='*80}")
    print(f"  FINAL REPORT — {group_id} groups verified in {elapsed:.0f}s")
    print(f"  {processed} symbols processed, {skipped} skipped (no 1d data), {len(errors)} errors")
    print(f"{'='*80}")

    # ── Volume Residual Summary ──
    print(f"\n### Volume Residual Summary ###")
    vr_by_cat = defaultdict(list)
    for r in results:
        if r.get("vol_residual_pct") is not None:
            vr_by_cat[(r["kind"], r["source"])].append(r["vol_residual_pct"])

    print(f"  {'Kind':<22s} {'Source':<20s} {'N':>5s} {'Med%':>12s} {'MaxAbs%':>12s} {'Zero%':>8s}")
    print(f"  {'-'*80}")
    all_s2_residuals = []
    for (k, s), vals in sorted(vr_by_cat.items()):
        v = np.array(vals)
        n_zero = np.sum(np.abs(v) < 1e-6)
        print(f"  {k:<22s} {s:<20s} {len(v):5d} {np.median(v):+12.6f} {np.max(np.abs(v)):12.6f} {n_zero/len(v)*100:7.1f}%")
        if "S2" in s:
            all_s2_residuals.extend(vals)

    if all_s2_residuals:
        v = np.array(all_s2_residuals)
        n_zero = np.sum(np.abs(v) < 1e-6)
        n_tiny = np.sum(np.abs(v) < 0.01)
        print(f"\n  S2 GLOBAL: N={len(v)}, median={np.median(v):+.8f}%, max_abs={np.max(np.abs(v)):.6f}%")
        print(f"  S2 Zero-residual: {n_zero}/{len(v)} ({n_zero/len(v)*100:.1f}%)")
        print(f"  S2 Near-zero (<0.01%): {n_tiny}/{len(v)} ({n_tiny/len(v)*100:.1f}%)")

    # ── First Bar Evidence ──
    print(f"\n### First Bar Evidence (Auction Absorption) ###")
    fb_summary: dict[str, dict[str, Any]] = {}
    for fb in first_bar_stats:
        k = fb["kind"]
        if k not in fb_summary:
            fb_summary[k] = {
                "count": 0,
                "day_first": Counter(),
                "auction": 0,
                "night_first": Counter(),
                "night_auction": 0,
            }
        fb_summary[k]["count"] += 1
        if fb.get("day_first"):
            fb_summary[k]["day_first"][fb["day_first"]] += 1
        if fb.get("has_auction_bar"):
            fb_summary[k]["auction"] += 1
        if fb.get("night_first"):
            fb_summary[k]["night_first"][fb["night_first"]] += 1
        if fb.get("has_night_auction"):
            fb_summary[k]["night_auction"] += 1

    for k, info in sorted(fb_summary.items()):
        count = int(info["count"])
        day_first_counter = info["day_first"]
        auction_count = int(info["auction"])
        night_first_counter = info["night_first"]
        night_auction_count = int(info["night_auction"])
        print(f"\n  [{k}] n={count}")
        print(f"    Day first bar: {day_first_counter.most_common(5)}")
        print(f"    Has auction bar: {auction_count}/{count} ({auction_count/max(count,1)*100:.1f}%)")
        if night_first_counter:
            print(f"    Night first bar: {night_first_counter.most_common(5)}")
            print(f"    Has night auction: {night_auction_count}/{count}")

    # ── Bar Count Stats ──
    print(f"\n### Daily Bar Count Statistics ###")
    bc_by_kind = defaultdict(list)
    for bc in bar_count_stats:
        if bc.get("mode"):
            bc_by_kind[bc["kind"]].append(bc["mode"])
    for k, modes in sorted(bc_by_kind.items()):
        m = Counter(modes).most_common(5)
        print(f"  {k:<22s}: n={len(modes):5d}  mode_dist={m}")

    # ── akshare Results ──
    if akshare_results:
        print(f"\n### akshare Independent Source Validation ({len(akshare_results)} symbols) ###")
        for r in sorted(akshare_results, key=lambda x: x["sym"]):
            status = "✅" if r.get("vol_exact_pct") and r["vol_exact_pct"] > 95 else "⚠️"
            print(f"  {status} {r['sym']:14s} days={r['common_days']:3d}  vol_res={r.get('vol_residual_pct','?'):>10}%  match={r.get('vol_exact_pct','?'):>6}%")

    # ── Error Summary ──
    if errors:
        print(f"\n### Errors ({len(errors)}) ###")
        err_types = Counter(e.get("error", "?")[:60] for e in errors)
        for msg, cnt in err_types.most_common(10):
            print(f"  [{cnt:4d}] {msg}")

    # ═══════════════════════════════════════════
    # 保存结果
    # ═══════════════════════════════════════════
    out_dir = DATA_DIR / "audit_reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "meta": {
            "version": "v3-fast",
            "timestamp": datetime.now().isoformat(),
            "python": sys.version,
            "total_symbols": len(symbols),
            "symbols_with_1d": len(data_1d),
            "symbols_with_1m": len(data_1m),
            "symbols_with_5m": len(data_5m),
            "total_groups": group_id,
            "elapsed_seconds": round(elapsed, 1),
            "errors": len(errors),
            "skipped": skipped,
        },
        "volume_residual_summary": {},
        "first_bar_summary": {},
        "bar_count_summary": {},
        "akshare_summary": {},
    }

    for (k, s), vals in sorted(vr_by_cat.items()):
        v = np.array(vals)
        report["volume_residual_summary"][f"{k}|{s}"] = {
            "n": len(v),
            "median_pct": round(float(np.median(v)), 8),
            "mean_pct": round(float(np.mean(v)), 8),
            "max_abs_pct": round(float(np.max(np.abs(v))), 6),
            "zero_pct": round(float(np.sum(np.abs(v) < 1e-6) / len(v) * 100), 2),
            "near_zero_pct": round(float(np.sum(np.abs(v) < 0.01) / len(v) * 100), 2),
        }

    for k, info in sorted(fb_summary.items()):
        day_first_counter = info["day_first"]
        night_first_counter = info["night_first"]
        report["first_bar_summary"][k] = {
            "count": info["count"],
            "day_first_mode": day_first_counter.most_common(1)[0][0] if day_first_counter else None,
            "day_first_dist": dict(day_first_counter.most_common(5)),
            "auction_bar_found": info["auction"],
            "night_first_mode": night_first_counter.most_common(1)[0][0] if night_first_counter else None,
            "night_first_dist": dict(night_first_counter.most_common(5)) if night_first_counter else {},
            "night_auction_found": info["night_auction"],
        }

    for k, modes in sorted(bc_by_kind.items()):
        m = Counter(modes)
        report["bar_count_summary"][k] = {
            "n": len(modes),
            "mode_dist": m.most_common(5),
        }

    if akshare_results:
        vr_ak = [r["vol_residual_pct"] for r in akshare_results if r.get("vol_residual_pct") is not None]
        vp_ak = [r["vol_exact_pct"] for r in akshare_results if r.get("vol_exact_pct") is not None]
        report["akshare_summary"] = {
            "n": len(akshare_results),
            "vol_residual_median": round(float(np.median(vr_ak)), 6) if vr_ak else None,
            "vol_exact_median": round(float(np.median(vp_ak)), 2) if vp_ak else None,
            "details": akshare_results,
        }

    with open(out_dir / "ultimate_crossval_v3.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v3_all_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v3_first_bars.json", "w", encoding="utf-8") as f:
        json.dump(first_bar_stats, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v3_bar_counts.json", "w", encoding="utf-8") as f:
        json.dump(bar_count_stats, f, ensure_ascii=False, indent=2)

    if errors:
        with open(out_dir / "v3_errors.json", "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)

    print(f"\n  Results saved to {out_dir}/")
    print(f"    ultimate_crossval_v3.json (summary)")
    print(f"    v3_all_results.json ({len(results)} groups)")
    print(f"    v3_first_bars.json ({len(first_bar_stats)} symbols)")
    print(f"    v3_bar_counts.json ({len(bar_count_stats)} symbols)")
    if errors:
        print(f"    v3_errors.json ({len(errors)} errors)")

    # ── 铁律判定 ──
    print(f"\n{'='*80}")
    print("  铁律判定")
    print(f"{'='*80}")

    if all_s2_residuals:
        v = np.array(all_s2_residuals)
        perfect = np.sum(np.abs(v) < 1e-6)
        near = np.sum(np.abs(v) < 0.01)
        print(f"\n  [铁律1] sum(1m_volume) == 1D_volume:")
        print(f"    验证组数:  {len(v)}")
        print(f"    精确匹配:  {perfect}/{len(v)} ({perfect/len(v)*100:.2f}%)")
        print(f"    近似匹配:  {near}/{len(v)} ({near/len(v)*100:.2f}%)")
        print(f"    最大残差:  {np.max(np.abs(v)):.8f}%")
        if perfect / len(v) > 0.99:
            print(f"    ✅ 铁律成立: 1分钟线聚合volume == 日线volume (含竞价)")
        else:
            print(f"    ⚠️ 需进一步分析残差来源")

    if fb_summary:
        print(f"\n  [铁律2] 集合竞价吸收进首根分钟线:")
        for k, info in sorted(fb_summary.items()):
            day_first_counter = info["day_first"]
            night_first_counter = info["night_first"]
            if day_first_counter:
                mode = day_first_counter.most_common(1)[0][0]
                print(f"    {k}: 日盘首bar={mode}, 独立竞价bar={info['auction']}/{info['count']}")
            if night_first_counter:
                mode = night_first_counter.most_common(1)[0][0]
                print(f"    {k}: 夜盘首bar={mode}, 独立竞价bar={info['night_auction']}/{info['count']}")

    print(f"\n  总耗时: {elapsed:.1f}s")
    print("  验证完毕。")


if __name__ == "__main__":
    run()

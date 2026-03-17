"""
终极全量多源交叉验证 v4 — DAT直读版 × 5500+品种 × 铁证如山
======================================================================
完全不依赖 xtquant API — 直接解码 QMT DAT 二进制文件

DAT格式 (已逆向验证):
  文件头:  8 字节
  记录:    64 字节/条
    [0-3]   uint32  timestamp (epoch seconds)
    [4-7]   uint32  open * 1000
    [8-11]  uint32  high * 1000
    [12-15] uint32  low * 1000
    [16-19] uint32  close * 1000
    [20-23] uint32  (padding, always 0)
    [24-27] uint32  volume (手/lots)
    [28-63] metadata (preClose, openInterest, etc.)

验证矩阵:
  V1: sum(1m_volume_per_day) vs 1d_volume  → 精确匹配 = 竞价已含
  V2: 首bar时间分析 → 确认竞价吸收时间点
  V3: 每日bar数统计 → 确认交易时段结构
  V4: akshare独立源交叉验证 → 第三方数据一致性
  V5: 5m聚合验证 → 多周期一致性
"""
import struct, time, json, sys, bisect, os, traceback
from pathlib import Path
from datetime import datetime, time as dtime, timedelta
from collections import defaultdict, Counter
from typing import Any
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"
QMT_BASE = Path(r"D:\申万宏源策略量化交易终端\userdata_mini\datadir")

RECORD_SIZE = 64
HEADER_SIZE = 8
MIN_TIMESTAMP = 1577836800   # 2020-01-01 epoch
MAX_TIMESTAMP = 1798761600   # 2027-01-01 epoch

# ═══════════════════════════════════════════
# DAT 文件直读
# ═══════════════════════════════════════════
def read_dat(dat_path, start_epoch=MIN_TIMESTAMP):
    """读取单个DAT文件，返回DataFrame(date, open, high, low, close, volume)"""
    fsize = dat_path.stat().st_size
    if fsize <= HEADER_SIZE:
        return pd.DataFrame()

    n_records = (fsize - HEADER_SIZE) // RECORD_SIZE
    if n_records < 1:
        return pd.DataFrame()

    rows = []
    with open(dat_path, 'rb') as f:
        f.seek(HEADER_SIZE)
        for _ in range(n_records):
            rec = f.read(RECORD_SIZE)
            if len(rec) < RECORD_SIZE:
                break
            ts = struct.unpack_from('<I', rec, 0)[0]
            if ts < start_epoch or ts > MAX_TIMESTAMP:
                continue
            o = struct.unpack_from('<I', rec, 4)[0] / 1000
            h = struct.unpack_from('<I', rec, 8)[0] / 1000
            l = struct.unpack_from('<I', rec, 12)[0] / 1000
            c = struct.unpack_from('<I', rec, 16)[0] / 1000
            v = struct.unpack_from('<I', rec, 24)[0]
            if v == 0 and o == 0:
                continue
            rows.append((datetime.fromtimestamp(ts), o, h, l, c, v))

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    return df


def read_dat_fast(dat_path, start_epoch=MIN_TIMESTAMP):
    """读取DAT文件 - numpy向量化版本 (大文件更快)"""
    fsize = dat_path.stat().st_size
    if fsize <= HEADER_SIZE:
        return pd.DataFrame()

    n_records = (fsize - HEADER_SIZE) // RECORD_SIZE
    if n_records < 1:
        return pd.DataFrame()

    with open(dat_path, 'rb') as f:
        f.seek(HEADER_SIZE)
        data = f.read(n_records * RECORD_SIZE)

    # 解析为结构化numpy数组
    dt = np.dtype([
        ('ts', '<u4'), ('open', '<u4'), ('high', '<u4'), ('low', '<u4'),
        ('close', '<u4'), ('pad', '<u4'), ('volume', '<u4'),
        ('rest', 'V36')  # 剩余36字节跳过
    ])
    arr = np.frombuffer(data[:n_records * RECORD_SIZE], dtype=dt)

    # 过滤有效记录
    mask = (arr['ts'] >= start_epoch) & (arr['ts'] <= MAX_TIMESTAMP) & (arr['volume'] > 0)
    valid = arr[mask]

    if len(valid) == 0:
        return pd.DataFrame()

    # +28800 将UTC epoch转换为北京时间 (UTC+8), 确保日期归属正确
    df = pd.DataFrame({
        'date': pd.to_datetime(valid['ts'].astype(np.int64) + 28800, unit='s'),
        'open': valid['open'].astype(np.float64) / 1000,
        'high': valid['high'].astype(np.float64) / 1000,
        'low': valid['low'].astype(np.float64) / 1000,
        'close': valid['close'].astype(np.float64) / 1000,
        'volume': valid['volume'].astype(np.int64),
    })
    return df


# ═══════════════════════════════════════════
# 品种分类
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
        if name.startswith(("000", "880")): return "index"
        if name.startswith(("51", "56", "58")): return "etf"
        if name.startswith(("60", "68")): return "stock"
        if name.startswith("11"): return "bond"
        return "other_sh"
    elif mkt == "SZ":
        if name.startswith(("399", "980")): return "index"
        if name.startswith(("15", "16")): return "etf"
        if name.startswith(("00", "30")): return "stock"
        if name.startswith("12"): return "bond"
        return "other_sz"
    elif mkt == "IF":
        return "index_futures"
    elif mkt in ("SF", "DF", "ZF"):
        base = ''.join(c for c in name if c.isalpha()).lower()
        return "commodity_night" if base in NIGHT_CODES else "commodity_day"
    elif mkt == "HK":
        return "hk_stock"
    return "unknown"


def has_night(kind):
    return "night" in kind


# ═══════════════════════════════════════════
# 构建品种列表 (从1m DAT目录)
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
# 核心验证逻辑
# ═══════════════════════════════════════════
def agg_1m_to_1d(df_1m, sorted_tdays=None, night_session=False):
    """将1m数据聚合为日线"""
    if df_1m.empty:
        return pd.DataFrame()

    df = df_1m.copy()
    if night_session and sorted_tdays:
        def _assign_td(ts):
            if ts.time() >= dtime(18, 0):
                cur = pd.Timestamp(ts.date())
                idx = bisect.bisect_right(sorted_tdays, cur)
                return sorted_tdays[idx] if idx < len(sorted_tdays) else cur
            return pd.Timestamp(ts.date())
        df["_td"] = df["date"].apply(_assign_td)
    else:
        df["_td"] = df["date"].dt.normalize()

    agg = df.groupby("_td").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    )
    return agg[agg["volume"] > 0]


def compare_volume(ref_1d, agg_1d):
    """比较日线volume与聚合volume
    返回: (common_days, residual_median_pct, exact_match_pct, max_abs_residual_pct)
    """
    if ref_1d.empty or agg_1d.empty:
        return (0, None, None, None)

    # 对齐索引
    r = ref_1d.copy()
    t = agg_1d.copy()

    if not isinstance(r.index, pd.DatetimeIndex):
        r.index = pd.to_datetime(r.index)
    if not isinstance(t.index, pd.DatetimeIndex):
        t.index = pd.to_datetime(t.index)

    r.index = r.index.normalize()
    t.index = t.index.normalize()

    common = r.index.intersection(t.index)
    # 去掉首尾各1天 (可能不完整)
    if len(common) > 10:
        common = common[1:-1]
    if len(common) < 3:
        return (len(common), None, None, None)

    rv = r.loc[common, "volume"].values.astype(np.float64)
    tv = t.loc[common, "volume"].values.astype(np.float64)

    mask = rv > 0
    if mask.sum() == 0:
        return (len(common), None, None, None)

    # 残差 = (日线 - 聚合) / 日线
    residual = np.zeros(len(rv))
    residual[mask] = (rv[mask] - tv[mask]) / rv[mask] * 100
    residual[~mask] = 0

    tol = 0.01  # 容差: 0.01手
    exact = np.sum(np.abs(rv - tv) < tol)

    return (
        len(common),
        round(float(np.median(residual[mask])), 8),
        round(float(exact / len(common) * 100), 2),
        round(float(np.max(np.abs(residual[mask]))), 6),
    )


def check_first_bar(df_1m, kind):
    """首bar时间分析"""
    if df_1m.empty or len(df_1m) < 10:
        return {}

    df = df_1m.copy()
    df["_t"] = df["date"].dt.time
    df["_d"] = df["date"].dt.date

    result = {}

    # 日盘
    if kind in ("stock", "index", "etf", "index_futures", "bond", "other_sh", "other_sz"):
        day = df[(df["_t"] >= dtime(9, 0)) & (df["_t"] <= dtime(10, 0))]
    else:
        day = df[(df["_t"] >= dtime(8, 50)) & (df["_t"] <= dtime(10, 0))]

    if not day.empty:
        firsts = day.groupby("_d")["_t"].min()
        if len(firsts) > 0:
            result["day_first"] = str(firsts.mode().iloc[0])
            if kind in ("stock", "index", "etf", "index_futures", "bond", "other_sh", "other_sz"):
                result["has_auction_bar"] = bool(any(dtime(9, 25) <= t < dtime(9, 30) for t in firsts))
            else:
                result["has_auction_bar"] = bool(any(dtime(8, 55) <= t < dtime(9, 0) for t in firsts))

    # 夜盘
    if has_night(kind):
        night = df[(df["_t"] >= dtime(20, 50)) & (df["_t"] <= dtime(22, 0))]
        if not night.empty:
            nf = night.groupby("_d")["_t"].min()
            if len(nf) > 0:
                result["night_first"] = str(nf.mode().iloc[0])
                result["has_night_auction"] = bool(any(dtime(20, 55) <= t < dtime(21, 0) for t in nf))

    return result


def count_daily_bars(df_1m):
    if df_1m.empty:
        return {}
    counts = df_1m.groupby(df_1m["date"].dt.date).size()
    return {
        "median": int(counts.median()),
        "min": int(counts.min()),
        "max": int(counts.max()),
        "mode": int(counts.mode().iloc[0]) if len(counts) > 0 else 0,
        "total_days": int(len(counts)),
    }


# ═══════════════════════════════════════════
# akshare 独立源
# ═══════════════════════════════════════════
def get_akshare_daily(code):
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
            start_date="20240101", end_date="20261231", adjust=""
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
# 主验证流程
# ═══════════════════════════════════════════
def run():
    t0 = time.time()
    print("=" * 80)
    print("  终极全量验证 v4 (DAT直读) — 5500+品种 × 铁证如山")
    print("  完全不依赖xtquant API — 直接解码QMT二进制数据文件")
    print("=" * 80)

    # ── Step 1: 构建品种列表 ──
    symbols = build_symbol_list()
    sym_list = sorted(symbols.keys())
    kind_counts = Counter(symbols.values())
    print(f"\n  总品种数: {len(sym_list)}")
    for k, c in sorted(kind_counts.items()):
        print(f"    {k:20s}: {c}")

    # ── Step 2: 检查数据可用性 ──
    has_1d = has_1m = has_5m = 0
    for sym in sym_list:
        mkt = sym.split(".")[-1]
        name = sym.split(".")[0]
        if (QMT_BASE / mkt / "86400" / f"{name}.DAT").exists():
            has_1d += 1
        if (QMT_BASE / mkt / "60" / f"{name}.DAT").exists():
            has_1m += 1
        if (QMT_BASE / mkt / "300" / f"{name}.DAT").exists():
            has_5m += 1
    print(f"\n  数据可用性: 1d={has_1d}, 1m={has_1m}, 5m={has_5m}")

    # ── Step 3: 逐品种验证 ──
    results = []
    first_bar_stats = []
    bar_count_stats = []
    errors = []
    group_id = 0
    processed = 0
    skipped = 0
    t1 = time.time()

    # akshare抽样 (股票50, ETF 10, 指数5)
    ak_sample = set()
    stock_list = [c for c, k in sorted(symbols.items()) if k == "stock"]
    etf_list = [c for c, k in sorted(symbols.items()) if k == "etf"]
    idx_list = [c for c, k in sorted(symbols.items()) if k == "index"]
    # 均匀抽样
    if stock_list:
        step = max(1, len(stock_list) // 50)
        ak_sample.update(stock_list[::step][:50])
    if etf_list:
        ak_sample.update(etf_list[:10])
    if idx_list:
        ak_sample.update(idx_list[:5])

    for sym in sym_list:
        processed += 1
        kind = symbols[sym]
        mkt = sym.split(".")[-1]
        name = sym.split(".")[0]
        is_night = has_night(kind)

        if processed % 500 == 0:
            elapsed = time.time() - t1
            rate = processed / max(elapsed, 0.1)
            eta = (len(sym_list) - processed) / max(rate, 0.1)
            print(f"  [{processed:5d}/{len(sym_list)}] {rate:.0f}/s ETA={eta:.0f}s groups={group_id}")

        try:
            # 读取1D DAT
            dat_1d_path = QMT_BASE / mkt / "86400" / f"{name}.DAT"
            if not dat_1d_path.exists():
                skipped += 1
                continue

            df_1d = read_dat_fast(dat_1d_path)
            if df_1d.empty or len(df_1d) < 5:
                skipped += 1
                continue

            ref_1d = df_1d.set_index("date")
            ref_1d.index = pd.DatetimeIndex(ref_1d.index).normalize()
            sorted_tdays = sorted(set(ref_1d.index))

            # ── V1: 1m → agg 对比 1D volume ──
            dat_1m_path = QMT_BASE / mkt / "60" / f"{name}.DAT"
            if dat_1m_path.exists():
                df_1m = read_dat_fast(dat_1m_path)
                if not df_1m.empty and len(df_1m) > 10:
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
                    agg_1d = agg_1m_to_1d(df_1m, sorted_tdays, is_night)
                    common, vr_med, exact_pct, max_abs = compare_volume(ref_1d, agg_1d)
                    if common >= 3:
                        group_id += 1
                        results.append({
                            "group": group_id, "sym": sym, "kind": kind,
                            "source": "V1:1m→1d",
                            "common_days": common,
                            "vol_residual_pct": vr_med,
                            "vol_exact_pct": exact_pct,
                            "max_abs_residual": max_abs,
                            "day_first": fb.get("day_first"),
                            "has_auction": fb.get("has_auction_bar"),
                            "bars_mode": bc.get("mode"),
                        })

            # ── V5: 5m → agg 对比 1D ──
            dat_5m_path = QMT_BASE / mkt / "300" / f"{name}.DAT"
            if dat_5m_path.exists():
                df_5m = read_dat_fast(dat_5m_path)
                if not df_5m.empty and len(df_5m) > 5:
                    agg_5m = agg_1m_to_1d(df_5m, sorted_tdays, is_night)
                    c5, vr5, ex5, max5 = compare_volume(ref_1d, agg_5m)
                    if c5 >= 3:
                        group_id += 1
                        results.append({
                            "group": group_id, "sym": sym, "kind": kind,
                            "source": "V5:5m→1d",
                            "common_days": c5,
                            "vol_residual_pct": vr5,
                            "vol_exact_pct": ex5,
                            "max_abs_residual": max5,
                        })

        except Exception as e:
            errors.append({"sym": sym, "kind": kind, "error": str(e)})

    elapsed_main = time.time() - t1
    print(f"\n  品种验证完成: {processed} processed, {skipped} skipped, {group_id} groups, {len(errors)} errors, {elapsed_main:.1f}s")

    # ── Step 4: akshare独立源验证 ──
    akshare_results = []
    print(f"\n  [akshare] 独立源验证 ({len(ak_sample)} symbols)...")
    t_ak = time.time()
    for sym in sorted(ak_sample):
        try:
            mkt = sym.split(".")[-1]
            name_part = sym.split(".")[0]
            dat_1d_path = QMT_BASE / mkt / "86400" / f"{name_part}.DAT"
            if not dat_1d_path.exists():
                continue
            df_1d = read_dat_fast(dat_1d_path)
            if df_1d.empty:
                continue

            ak_df = get_akshare_daily(sym)
            if ak_df.empty:
                continue

            ref = df_1d.set_index("date")
            ref.index = pd.DatetimeIndex(ref.index).normalize()
            ak_idx = ak_df.set_index("date")
            ak_idx.index = pd.DatetimeIndex(ak_idx.index).normalize()

            # 检查 volume 单位 (akshare可能是手)
            common_idx = ref.index.intersection(ak_idx.index)
            if len(common_idx) > 5:
                rv = np.asarray(ref.loc[common_idx[:10], "volume"], dtype=float)
                av = np.asarray(ak_idx.loc[common_idx[:10], "volume"], dtype=float)
                if av.sum() > 0 and rv.sum() > 0:
                    mask = (rv > 0) & (av > 0)
                    ratio = np.median(rv[mask] / av[mask]) if np.any(mask) else 1
                    if 90 < ratio < 110:
                        # akshare volume和DAT 1:1 (both in 手)
                        pass
                    elif 0.009 < ratio < 0.011:
                        # DAT in 手, akshare in 股 → convert
                        ak_idx["volume"] = ak_idx["volume"] / 100

            common, vr, ex, mx = compare_volume(ref, ak_idx)
            if common >= 3:
                group_id += 1
                r = {
                    "group": group_id, "sym": sym, "kind": symbols[sym],
                    "source": "V4:akshare",
                    "common_days": common,
                    "vol_residual_pct": vr,
                    "vol_exact_pct": ex,
                    "max_abs_residual": mx,
                }
                results.append(r)
                akshare_results.append(r)
            time.sleep(0.2)
        except Exception as e:
            errors.append({"sym": sym, "kind": symbols.get(sym, "?"), "source": "akshare", "error": str(e)})

    print(f"    akshare: {len(akshare_results)} symbols verified in {time.time()-t_ak:.1f}s")

    # ═══════════════════════════════════════════
    # 汇总报告
    # ═══════════════════════════════════════════
    elapsed = time.time() - t0
    print(f"\n\n{'='*80}")
    print(f"  FINAL REPORT — {group_id} groups | {elapsed:.0f}s")
    print(f"  {processed} symbols, {skipped} skipped, {len(errors)} errors")
    print(f"{'='*80}")

    # === Volume Residual ===
    print(f"\n### Volume Residual (1m→1d & 5m→1d) ###")
    vr_by_cat = defaultdict(list)
    for r in results:
        if r.get("vol_residual_pct") is not None:
            vr_by_cat[(r["kind"], r["source"])].append(r["vol_residual_pct"])

    print(f"  {'Kind':<22s} {'Source':<16s} {'N':>6s} {'Median%':>12s} {'MaxAbs%':>12s} {'ZeroRes%':>10s}")
    print(f"  {'-'*85}")
    all_v1_residuals = []
    for (k, s), vals in sorted(vr_by_cat.items()):
        v = np.array(vals)
        n_zero = np.sum(np.abs(v) < 1e-6)
        print(f"  {k:<22s} {s:<16s} {len(v):6d} {np.median(v):+12.8f} {np.max(np.abs(v)):12.6f} {n_zero/len(v)*100:9.1f}%")
        if "V1" in s:
            all_v1_residuals.extend(vals)

    if all_v1_residuals:
        v = np.array(all_v1_residuals)
        perfect = np.sum(np.abs(v) < 1e-6)
        near = np.sum(np.abs(v) < 0.01)
        print(f"\n  *** V1 (1m→1d) GLOBAL ***")
        print(f"  Total groups:     {len(v)}")
        print(f"  Perfect match:    {perfect}/{len(v)} ({perfect/len(v)*100:.2f}%)")
        print(f"  Near-zero <0.01%: {near}/{len(v)} ({near/len(v)*100:.2f}%)")
        print(f"  Median residual:  {np.median(v):+.10f}%")
        print(f"  Max abs residual: {np.max(np.abs(v)):.8f}%")

    # === First Bar ===
    print(f"\n### First Bar (Auction Absorption Evidence) ###")
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
        print(f"    Auction bar found: {auction_count}/{count} ({auction_count/max(count,1)*100:.1f}%)")
        if night_first_counter:
            print(f"    Night first bar: {night_first_counter.most_common(5)}")
            print(f"    Night auction: {night_auction_count}/{count}")

    # === Bar Counts ===
    print(f"\n### Daily Bar Count ###")
    bc_by_kind = defaultdict(list)
    for bc in bar_count_stats:
        if bc.get("mode"):
            bc_by_kind[bc["kind"]].append(bc["mode"])
    for k, modes in sorted(bc_by_kind.items()):
        m = Counter(modes).most_common(5)
        print(f"  {k:<22s}: n={len(modes):5d}  mode_dist={m}")

    # === akshare ===
    if akshare_results:
        print(f"\n### akshare Independent Source ({len(akshare_results)}) ###")
        for r in sorted(akshare_results, key=lambda x: x["sym"])[:30]:
            status = "PASS" if r.get("vol_exact_pct") and r["vol_exact_pct"] > 95 else "WARN"
            print(f"  [{status}] {r['sym']:14s} days={r['common_days']:3d} res={r.get('vol_residual_pct','?'):>12}% match={r.get('vol_exact_pct','?'):>6}%")
        if len(akshare_results) > 30:
            print(f"  ... and {len(akshare_results)-30} more")

    # === Errors ===
    if errors:
        print(f"\n### Errors ({len(errors)}) ###")
        et = Counter(e.get("error", "?")[:80] for e in errors)
        for msg, cnt in et.most_common(10):
            print(f"  [{cnt:4d}] {msg}")

    # ═══════════════════════════════════════════
    # 保存结果
    # ═══════════════════════════════════════════
    out_dir = DATA_DIR / "audit_reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "meta": {
            "version": "v4-dat-direct",
            "timestamp": datetime.now().isoformat(),
            "python": sys.version,
            "total_symbols": len(symbols),
            "has_1d": has_1d, "has_1m": has_1m, "has_5m": has_5m,
            "total_groups": group_id,
            "elapsed_seconds": round(elapsed, 1),
            "errors": len(errors),
            "skipped": skipped,
            "method": "Direct binary DAT file reading (no xtquant API dependency)",
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
            "median_pct": round(float(np.median(v)), 10),
            "mean_pct": round(float(np.mean(v)), 10),
            "max_abs_pct": round(float(np.max(np.abs(v))), 8),
            "zero_pct": round(float(np.sum(np.abs(v) < 1e-6) / len(v) * 100), 2),
            "near_zero_pct": round(float(np.sum(np.abs(v) < 0.01) / len(v) * 100), 2),
        }

    for k, info in sorted(fb_summary.items()):
        day_first_counter = info["day_first"]
        night_first_counter = info["night_first"]
        report["first_bar_summary"][k] = {
            "count": info["count"],
            "day_first_mode": day_first_counter.most_common(1)[0][0] if day_first_counter else None,
            "day_first_dist": dict(day_first_counter.most_common(10)),
            "auction_bar_found": info["auction"],
            "night_first_mode": night_first_counter.most_common(1)[0][0] if night_first_counter else None,
            "night_first_dist": dict(night_first_counter.most_common(10)) if night_first_counter else {},
            "night_auction_found": info["night_auction"],
        }

    for k, modes in sorted(bc_by_kind.items()):
        report["bar_count_summary"][k] = {
            "n": len(modes),
            "mode_dist": Counter(modes).most_common(10),
        }

    if akshare_results:
        vr_ak = [r["vol_residual_pct"] for r in akshare_results if r.get("vol_residual_pct") is not None]
        report["akshare_summary"] = {
            "n": len(akshare_results),
            "vol_residual_median": round(float(np.median(vr_ak)), 8) if vr_ak else None,
            "details": akshare_results,
        }

    with open(out_dir / "ultimate_crossval_v4.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v4_all_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v4_first_bars.json", "w", encoding="utf-8") as f:
        json.dump(first_bar_stats, f, ensure_ascii=False, indent=2)

    with open(out_dir / "v4_bar_counts.json", "w", encoding="utf-8") as f:
        json.dump(bar_count_stats, f, ensure_ascii=False, indent=2)

    if errors:
        with open(out_dir / "v4_errors.json", "w", encoding="utf-8") as f:
            json.dump(errors, f, ensure_ascii=False, indent=2)

    print(f"\n  Results saved to {out_dir}/")
    print(f"    ultimate_crossval_v4.json (summary)")
    print(f"    v4_all_results.json ({len(results)} groups)")
    print(f"    v4_first_bars.json ({len(first_bar_stats)} symbols)")
    print(f"    v4_bar_counts.json ({len(bar_count_stats)} symbols)")

    # === 铁律判定 ===
    print(f"\n{'='*80}")
    print("  铁律判定 (Iron Rules Verdict)")
    print(f"{'='*80}")

    if all_v1_residuals:
        v = np.array(all_v1_residuals)
        perfect = np.sum(np.abs(v) < 1e-6)
        near = np.sum(np.abs(v) < 0.01)
        print(f"\n  [铁律1] sum(1m_volume) == 1D_volume")
        print(f"    验证品种: {len(v)} 组 (DAT二进制直读)")
        print(f"    精确匹配: {perfect}/{len(v)} ({perfect/len(v)*100:.2f}%)")
        print(f"    最大残差: {np.max(np.abs(v)):.10f}%")
        if perfect / len(v) > 0.99:
            print(f"    >>> 铁律成立: 1分钟线volume聚合 == 日线volume")
            print(f"    >>> 含义: 集合竞价数据已被吸收进首根分钟线")
        elif near / len(v) > 0.95:
            print(f"    >>> 基本成立 (>95% 近似匹配), 微小残差可能来自数据边界")
        else:
            print(f"    >>> 需进一步分析")

    if fb_summary:
        print(f"\n  [铁律2] 集合竞价无独立bar — 被吸收进第一根分钟线")
        for k, info in sorted(fb_summary.items()):
            n = info["count"]
            auc = info["auction"]
            day_first_counter = info["day_first"]
            night_first_counter = info["night_first"]
            if day_first_counter:
                mode = day_first_counter.most_common(1)[0][0]
                print(f"    {k:20s}: 日盘首bar={mode}, 独立竞价bar={auc}/{n} ({auc/max(n,1)*100:.1f}%)")
            if night_first_counter:
                mode = night_first_counter.most_common(1)[0][0]
                print(f"    {k:20s}: 夜盘首bar={mode}, 夜盘竞价bar={info['night_auction']}/{n}")

    print(f"\n  总耗时: {elapsed:.1f}s")
    print("  " + "=" * 40)
    print("  验证完毕 — DAT二进制直读, 绝无API中间环节")
    print("  " + "=" * 40)


if __name__ == "__main__":
    run()

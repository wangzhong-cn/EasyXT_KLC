"""
终极多源数据交叉验证 v2 — 黄金标准1D收敛 + 集合竞价铁证
=========================================================
数据源:
  S1: QMT API 1D（黄金标准）
  S2: QMT API 1m → 正确归属交易日 → 聚合1D
  S3: QMT API 5m → 正确归属交易日 → 聚合1D
  S4: 已导出 feather 1D
  S5: 已导出 feather 1m → 聚合1D

关键改进: 期货夜盘bar正确归属到下一个交易日
"""
import time, sys, json, bisect
from pathlib import Path
from datetime import time as dtime, timedelta
from collections import defaultdict
from typing import Any
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"

try:
    import xtquant.xtdata as xtdata
    xtdata.enable_hello = False
    QMT_OK = True
except ImportError:
    QMT_OK = False
    print("[WARN] xtdata not available")

FIELDS = ["open", "high", "low", "close", "volume", "amount"]
START = "20240101"

# ─── 品种池 (41 个, 目标 50+ 组验证) ──────────────────────────────
SYMBOLS = {
    # A股上交所 (10)
    "600000.SH": "stock", "600519.SH": "stock", "601318.SH": "stock",
    "600036.SH": "stock", "601398.SH": "stock", "600887.SH": "stock",
    "600276.SH": "stock", "601888.SH": "stock", "600809.SH": "stock",
    "601012.SH": "stock",
    # A股深交所 (5)
    "000001.SZ": "stock", "000858.SZ": "stock", "000333.SZ": "stock",
    "002594.SZ": "stock", "000651.SZ": "stock",
    # 指数 (6)
    "000001.SH": "index", "000300.SH": "index", "000016.SH": "index",
    "000905.SH": "index", "399001.SZ": "index", "399006.SZ": "index",
    # ETF (4)
    "510300.SH": "etf", "510050.SH": "etf", "510500.SH": "etf",
    "159915.SZ": "etf",
    # 股指期货 (4)
    "IF01.IF": "index_futures", "IC01.IF": "index_futures",
    "IH01.IF": "index_futures", "IM01.IF": "index_futures",
    # 商品期货-有夜盘 (10)
    "cu01.SF": "commodity_night", "al01.SF": "commodity_night",
    "rb01.SF": "commodity_night", "au01.SF": "commodity_night",
    "ag01.SF": "commodity_night", "c01.DF": "commodity_night",
    "m01.DF": "commodity_night", "p01.DF": "commodity_night",
    "i01.DF": "commodity_night", "TA01.ZF": "commodity_night",
    # 商品期货-仅日盘 (1)
    "v01.DF": "commodity_day",
    # 科创板 (1)
    "688981.SH": "stock",
}

# ─── QMT API 数据获取 ──────────────────────────────────────────────

def get_api_data(sym, period, start=START):
    if not QMT_OK:
        return pd.DataFrame()
    try:
        xtdata.download_history_data2([sym], period, start, "", None, True)
        time.sleep(0.12)
        data = xtdata.get_market_data(FIELDS, [sym], period, start_time=start)
        if not data or "close" not in data or sym not in data["close"].index:
            return pd.DataFrame()
        frames = {}
        for f in FIELDS:
            if f in data and sym in data[f].index:
                frames[f] = data[f].loc[sym]
        if not frames:
            return pd.DataFrame()
        df = pd.DataFrame(frames)
        df.index.name = "date"
        df = df.reset_index()
        sample = str(df["date"].iloc[0])
        fmt = "%Y%m%d" if len(sample) == 8 else "%Y%m%d%H%M%S"
        df["date"] = pd.to_datetime(df["date"], format=fmt, errors="coerce")
        df = df.dropna(subset=["date"])
        if "volume" in df.columns:
            df = df[df["volume"] > 0]
        return df
    except Exception as e:
        print(f"    [ERR] {sym} {period}: {e}")
        return pd.DataFrame()


def load_feather(sym, period):
    mkt = sym.split(".")[-1] if "." in sym else "UNK"
    safe = sym.replace(".", "_")
    p = DATA_DIR / mkt / f"{safe}_{period}.feather"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_feather(p)
    tc = next((c for c in ["date", "time", "datetime"] if c in df.columns), None)
    if tc:
        df[tc] = pd.to_datetime(df[tc], errors="coerce")
    return df


# ─── 交易日归属 (期货夜盘关键) ─────────────────────────────────────

def build_trading_day_map(api_1d_dates):
    """从1D数据的日期构建交易日列表(sorted)"""
    return sorted(set(pd.to_datetime(d).normalize() for d in api_1d_dates))


def assign_trading_day(ts, sorted_tdays, has_night=False):
    """将分钟bar的时间戳分配到正确的交易日

    规则:
    - A股/ETF/指数/股指期货: bar_date = trading_day (无夜盘)
    - 有夜盘商品: >=18:00 的bar归属到 sorted_tdays 中的下一个交易日
    """
    if has_night and ts.time() >= dtime(18, 0):
        # 夜盘: 找 > 当前日期的下一个交易日
        cur = pd.Timestamp(ts.date())
        idx = bisect.bisect_right(sorted_tdays, cur)
        if idx < len(sorted_tdays):
            return sorted_tdays[idx]
        return cur  # fallback
    return pd.Timestamp(ts.date())


def agg_to_1d(df, tc="date", sorted_tdays=None, has_night=False):
    """将分钟级数据聚合到交易日级"""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])

    if sorted_tdays and has_night:
        df["_td"] = df[tc].apply(lambda ts: assign_trading_day(ts, sorted_tdays, True))
    else:
        df["_td"] = df[tc].dt.normalize()

    agg = {}
    for f, op in [("open", "first"), ("high", "max"), ("low", "min"),
                  ("close", "last"), ("volume", "sum"), ("amount", "sum")]:
        if f in df.columns:
            agg[f] = op
    if not agg:
        return pd.DataFrame()
    result = df.groupby("_td").agg(agg)
    result = result[result["volume"] > 0] if "volume" in result.columns else result
    return result


# ─── 对比 ──────────────────────────────────────────────────────────

def compare_1d(ref, test, label):
    """对比 test 与 ref(黄金标准)"""
    if ref.empty or test.empty:
        return {"label": label, "status": "no_data", "common": 0}

    r = ref.copy()
    t = test.copy()

    for df in [r, t]:
        if not isinstance(df.index, pd.DatetimeIndex):
            if "date" in df.columns:
                df.set_index("date", inplace=True)
            df.index = pd.to_datetime(df.index, errors="coerce")
        df.index = df.index.normalize()

    common = r.index.intersection(t.index)
    # 去掉第一天和最后一天(边界可能不完整)
    if len(common) > 10:
        common = common[1:-1]

    if len(common) < 3:
        return {"label": label, "status": "insufficient", "common": len(common)}

    result = {"label": label, "status": "ok", "common": len(common)}

    for field in ["open", "high", "low", "close", "volume"]:
        if field not in r.columns or field not in t.columns:
            continue
        rv = r.loc[common, field].values.astype(float)
        tv = t.loc[common, field].values.astype(float)

        tol = max(np.abs(rv).max() * 1e-6, 0.01) if len(rv) > 0 else 0.01
        exact = np.sum(np.abs(rv - tv) < tol)
        result[f"{field}_exact"] = int(exact)
        result[f"{field}_match_pct"] = round(exact / len(common) * 100, 2)

        mask = np.abs(rv) > 1e-10
        if mask.sum() > 0:
            rel_err = np.abs(rv[mask] - tv[mask]) / np.abs(rv[mask])
            result[f"{field}_median_err_pct"] = round(float(np.median(rel_err) * 100), 6)
            result[f"{field}_max_err_pct"] = round(float(np.max(rel_err) * 100), 4)

    # volume 残差特别分析
    if "volume" in r.columns and "volume" in t.columns:
        rv = r.loc[common, "volume"].values.astype(float)
        tv = t.loc[common, "volume"].values.astype(float)
        mask = rv > 0
        if mask.sum() > 0:
            residual = (rv[mask] - tv[mask]) / rv[mask]
            result["vol_residual_median_pct"] = round(float(np.median(residual) * 100), 6)
            result["vol_residual_max_pct"] = round(float(np.max(np.abs(residual)) * 100), 4)
            result["vol_zero_residual_count"] = int(np.sum(np.abs(residual) < 1e-10))

    return result


# ─── 首根bar分析 ──────────────────────────────────────────────────

def analyze_first_bars(df, tc="date", kind="stock"):
    """分析每天首根bar的时间、volume"""
    if df.empty:
        return {}
    df = df.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    df["_t"] = df[tc].dt.time
    df["_d"] = df[tc].dt.date

    info = {}

    # 日盘第一根bar
    if kind in ("stock", "index", "etf"):
        day_df = df[(df["_t"] >= dtime(9, 0)) & (df["_t"] <= dtime(10, 0))]
    elif kind == "index_futures":
        day_df = df[(df["_t"] >= dtime(9, 0)) & (df["_t"] <= dtime(10, 0))]
    else:
        day_df = df[(df["_t"] >= dtime(8, 50)) & (df["_t"] <= dtime(10, 0))]

    if not day_df.empty:
        first = day_df.groupby("_d").first()
        first_times = day_df.groupby("_d")["_t"].min()
        info["day_first_mode"] = str(first_times.mode().iloc[0]) if len(first_times) > 0 else None
        info["day_first_unique"] = sorted(set(str(t) for t in first_times))
        info["day_total_days"] = len(first_times)

        # 集合竞价证据: 首根bar是否存在独立的09:25-09:30(A股)或08:55-09:00(期货)bar
        if kind in ("stock", "index", "etf"):
            info["has_auction_bar_0925"] = bool(any(dtime(9, 25) <= t < dtime(9, 30) for t in first_times))
            info["has_bar_0930"] = bool(any(t == dtime(9, 30) for t in first_times))
            info["has_bar_0931"] = bool(any(t == dtime(9, 31) for t in first_times))
        else:
            info["has_auction_bar_0855"] = bool(any(dtime(8, 55) <= t < dtime(9, 0) for t in first_times))
            info["has_bar_0900"] = bool(any(t == dtime(9, 0) for t in first_times))
            info["has_bar_0901"] = bool(any(t == dtime(9, 1) for t in first_times))

        # 首根bar volume (均值)
        if "volume" in first.columns:
            info["day_first_bar_vol_mean"] = round(float(first["volume"].mean()), 1)

    # 夜盘首根bar
    if "night" in kind:
        night = df[(df["_t"] >= dtime(20, 50)) & (df["_t"] <= dtime(22, 0))]
        if not night.empty:
            nf_times = night.groupby("_d")["_t"].min()
            info["night_first_mode"] = str(nf_times.mode().iloc[0]) if len(nf_times) > 0 else None
            info["has_night_auction_2055"] = bool(any(dtime(20, 55) <= t < dtime(21, 0) for t in nf_times))
            info["has_night_bar_2100"] = bool(any(t == dtime(21, 0) for t in nf_times))
            info["has_night_bar_2101"] = bool(any(t == dtime(21, 1) for t in nf_times))
            info["night_days"] = len(nf_times)

            # 首根夜盘bar volume
            nf = night.groupby("_d").first()
            if "volume" in nf.columns:
                info["night_first_bar_vol_mean"] = round(float(nf["volume"].mean()), 1)

    return info


# ─── 主验证循环 ─────────────────────────────────────────────────────

def run():
    print("=" * 80)
    print("  终极多源数据交叉验证 v2 — 黄金标准1D收敛")
    print(f"  品种: {len(SYMBOLS)} | 开始: {START}")
    print("=" * 80)

    all_results = []
    group_id = 0
    auction_evidence = []

    for sym, kind in sorted(SYMBOLS.items()):
        print(f"\n{'─'*60}")
        print(f"  [{sym}] type={kind}")

        has_night = "night" in kind

        # ── S1: API 1D (黄金标准) ──
        api_1d = get_api_data(sym, "1d")
        if api_1d.empty:
            print("    S1(1D): 无数据, skip")
            continue

        api_1d_idx = api_1d.set_index("date")
        api_1d_idx.index = api_1d_idx.index.normalize()
        sorted_tdays = build_trading_day_map(api_1d_idx.index)
        n1d = len(api_1d)
        print(f"    S1(1D): {n1d} days  [{api_1d['date'].min().date()} → {api_1d['date'].max().date()}]")

        # ── S2: API 1m → agg ──
        api_1m = get_api_data(sym, "1m")
        if not api_1m.empty:
            fb = analyze_first_bars(api_1m, "date", kind)
            agg = agg_to_1d(api_1m, "date", sorted_tdays, has_night)
            cmp = compare_1d(api_1d_idx, agg, "S2:API_1m→1d")
            group_id += 1
            cmp.update({"group": group_id, "sym": sym, "kind": kind, "first_bar": fb})
            all_results.append(cmp)

            vr = cmp.get("vol_residual_median_pct", "?")
            vm = cmp.get("volume_match_pct", "?")
            print(f"    S2(1m→1d): {cmp['common']}d, vol_res={vr}%, vol_match={vm}%")
            print(f"      首bar={fb.get('day_first_mode','?')}")

            # 集合竞价证据
            ae = {"sym": sym, "kind": kind}
            ae.update(fb)
            auction_evidence.append(ae)

            if has_night and fb.get("night_first_mode"):
                print(f"      夜盘首bar={fb.get('night_first_mode')}, "
                      f"auction_2055={fb.get('has_night_auction_2055')}")
        else:
            print("    S2(1m): 无数据")

        # ── S3: API 5m → agg ──
        api_5m = get_api_data(sym, "5m")
        if not api_5m.empty:
            agg5 = agg_to_1d(api_5m, "date", sorted_tdays, has_night)
            cmp5 = compare_1d(api_1d_idx, agg5, "S3:API_5m→1d")
            group_id += 1
            cmp5.update({"group": group_id, "sym": sym, "kind": kind})
            all_results.append(cmp5)

            fb5 = analyze_first_bars(api_5m, "date", kind)
            vr5 = cmp5.get("vol_residual_median_pct", "?")
            print(f"    S3(5m→1d): {cmp5['common']}d, vol_res={vr5}%, "
                  f"首bar_5m={fb5.get('day_first_mode','?')}")
        else:
            print("    S3(5m): 无数据")

        # ── S4: Feather 1D ──
        fth_1d = load_feather(sym, "1d")
        if not fth_1d.empty:
            cmp_f = compare_1d(api_1d_idx, fth_1d, "S4:Feather_1d")
            group_id += 1
            cmp_f.update({"group": group_id, "sym": sym, "kind": kind})
            all_results.append(cmp_f)
            print(f"    S4(Fth 1d): {cmp_f['common']}d, vol_res={cmp_f.get('vol_residual_median_pct','?')}%")

        # ── S5: Feather 1m → agg ──
        fth_1m = load_feather(sym, "1m")
        if not fth_1m.empty:
            agg_f = agg_to_1d(fth_1m, "date", sorted_tdays, has_night)
            cmp_fm = compare_1d(api_1d_idx, agg_f, "S5:Feather_1m→1d")
            group_id += 1
            cmp_fm.update({"group": group_id, "sym": sym, "kind": kind})
            all_results.append(cmp_fm)
            print(f"    S5(Fth 1m→1d): {cmp_fm['common']}d, vol_res={cmp_fm.get('vol_residual_median_pct','?')}%")

    # ─── 汇总报告 ───────────────────────────────────────────────────
    print(f"\n\n{'='*80}")
    print(f"  ▓▓▓  汇总报告  ▓▓▓  总验证组: {group_id}")
    print(f"{'='*80}")

    # 按 (kind, source) 统计
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {"vol_res": [], "vol_match": [], "close_match": [], "n": 0})
    for r in all_results:
        if r.get("status") != "ok":
            continue
        key = f"{r.get('kind','?')}|{r['label']}"
        stats[key]["n"] += 1
        vr = r.get("vol_residual_median_pct")
        if vr is not None:
            stats[key]["vol_res"].append(vr)
        vm = r.get("volume_match_pct")
        if vm is not None:
            stats[key]["vol_match"].append(vm)
        cm = r.get("close_match_pct")
        if cm is not None:
            stats[key]["close_match"].append(cm)

    print(f"\n  {'类型|来源':<36} {'组':>3} {'vol残差中位%':>14} {'vol匹配%':>10}")
    print(f"  {'-'*67}")
    for key in sorted(stats):
        s = stats[key]
        vr = f"{np.median(s['vol_res']):+.6f}" if s["vol_res"] else "N/A"
        vm = f"{np.mean(s['vol_match']):.2f}" if s["vol_match"] else "N/A"
        print(f"  {key:<36} {s['n']:>3} {vr:>14} {vm:>10}")

    # ─── 集合竞价专项铁证 ────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"  ▓▓▓  集合竞价首根Bar铁证  ▓▓▓")
    print(f"{'='*80}")
    print(f"\n  {'品种':<14} {'类型':<16} {'日盘首bar':>10} {'独立竞价bar':>10} "
          f"{'夜盘首bar':>10} {'夜盘竞价bar':>10}")
    print(f"  {'-'*75}")

    for ae in sorted(auction_evidence, key=lambda x: x.get("kind", "")):
        sym = ae.get("sym", "?")
        kind = ae.get("kind", "?")
        dfm = ae.get("day_first_mode", "?")

        if kind in ("stock", "index", "etf"):
            has_auc = ae.get("has_auction_bar_0925", False)
        else:
            has_auc = ae.get("has_auction_bar_0855", False)

        nfm = ae.get("night_first_mode", "-")
        has_nauc = ae.get("has_night_auction_2055", False)

        print(f"  {sym:<14} {kind:<16} {str(dfm):>10} {'YES❌' if has_auc else 'NO✅':>10} "
              f"{str(nfm):>10} {'YES❌' if has_nauc else 'NO✅':>10}")

    # 保存
    out_dir = DATA_DIR / "audit_reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "ultimate_crossval_v2.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)

    with open(out_dir / "auction_evidence.json", "w", encoding="utf-8") as f:
        json.dump(auction_evidence, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n  结果已保存: {out_dir}/")
    print(f"  总验证组: {group_id}")

    return all_results, auction_evidence, group_id


if __name__ == "__main__":
    results, evidence, n_groups = run()
    if n_groups >= 50:
        print(f"\n  ✅ 达成 {n_groups} 组验证 (目标 50+)")
    else:
        print(f"\n  ⚠️ 完成 {n_groups} 组 (目标 50+)")

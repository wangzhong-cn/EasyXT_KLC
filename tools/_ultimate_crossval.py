"""
终极多源数据交叉验证 — 黄金标准1D收敛
=========================================
数据源：
  S1: QMT API 1D（黄金标准）
  S2: QMT API 1m → 聚合1D
  S3: QMT API 5m → 聚合1D
  S4: QMT API Tick → 聚合1D（仅部分品种有）
  S5: 已导出 feather 1D
  S6: 已导出 feather 1m → 聚合1D

目标：50+ 组交叉验证，覆盖  A股/指数/ETF/股指期货/商品期货（有夜盘/无夜盘）
重点验证：集合竞价成交量归属、首根bar时间、夜盘双竞价
"""
import struct, time, bisect, sys, os
from pathlib import Path
from datetime import date, time as dtime, timedelta, datetime
import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"
NIGHT_CUTOFF = dtime(18, 0, 0)
RESULTS = []

# ─── QMT API ────────────────────────────────────────────────────────
try:
    import xtquant.xtdata as xtdata
    try:
        xtdata.enable_hello = False
    except Exception:
        pass
    QMT_OK = True
except ImportError:
    QMT_OK = False
    print("[WARN] xtdata not available, using feather only")

FIELDS = ["open", "high", "low", "close", "volume", "amount"]

# ─── 品种池 ────────────────────────────────────────────────────────
# 覆盖：主板蓝筹/中小盘/科创板/ETF/宽基指数/股指期货/有夜盘商品/无夜盘商品
SYMBOLS = {
    # A股（上交所）
    "600000.SH": "stock",    # 浦发银行
    "600519.SH": "stock",    # 贵州茅台
    "601318.SH": "stock",    # 中国平安
    "600036.SH": "stock",    # 招商银行
    "601398.SH": "stock",    # 工商银行
    "600887.SH": "stock",    # 伊利股份
    "600276.SH": "stock",    # 恒瑞医药
    "601888.SH": "stock",    # 中国中免
    "600809.SH": "stock",    # 山西汾酒
    "601012.SH": "stock",    # 隆基绿能
    # A股（深交所）—通过API获取
    "000001.SZ": "stock",    # 平安银行
    "000858.SZ": "stock",    # 五粮液
    "000333.SZ": "stock",    # 美的集团
    "002594.SZ": "stock",    # 比亚迪
    "000651.SZ": "stock",    # 格力电器
    # 指数
    "000001.SH": "index",    # 上证综指
    "000300.SH": "index",    # 沪深300
    "000016.SH": "index",    # 上证50
    "000905.SH": "index",    # 中证500
    "399001.SZ": "index",    # 深证成指
    "399006.SZ": "index",    # 创业板指
    # ETF
    "510300.SH": "etf",      # 沪深300ETF
    "510050.SH": "etf",      # 上证50ETF
    "510500.SH": "etf",      # 中证500ETF
    "159915.SZ": "etf",      # 创业板ETF
    # 股指期货
    "IF01.IF": "index_futures",
    "IC01.IF": "index_futures",
    "IH01.IF": "index_futures",
    "IM01.IF": "index_futures",
    # 商品期货（有夜盘 - 21:00开盘）
    "cu01.SF": "commodity_night",   # 铜
    "al01.SF": "commodity_night",   # 铝
    "rb01.SF": "commodity_night",   # 螺纹钢
    "au01.SF": "commodity_night",   # 黄金
    "ag01.SF": "commodity_night",   # 白银
    "c01.DF": "commodity_night",    # 玉米
    "m01.DF": "commodity_night",    # 豆粕
    "p01.DF": "commodity_night",    # 棕榈油
    "i01.DF": "commodity_night",    # 铁矿石
    "TA01.ZF": "commodity_night",   # PTA
    # 商品期货（仅日盘 - 09:00开盘, 无夜盘）
    "v01.DF": "commodity_day",      # PVC
    # 科创板
    "688981.SH": "stock",    # 中芯国际
}

# ─── 工具函数 ───────────────────────────────────────────────────────

def get_api_data(sym, period, start="20240101"):
    """通过 QMT API 获取数据

    QMT get_market_data 返回: {field: DataFrame(rows=codes, cols=date_strings)}
    1D 列名: "20240102"；1m/5m 列名: "20241107095900"
    """
    if not QMT_OK:
        return pd.DataFrame()
    try:
        xtdata.download_history_data2([sym], period, start, "", None, True)
        time.sleep(0.15)
        data = xtdata.get_market_data(FIELDS, [sym], period, start_time=start)
        if not data or "close" not in data or sym not in data["close"].index:
            return pd.DataFrame()
        # 每个field取该股票那一行(loc[sym])→Series(index=date_string)
        frames = {}
        for f in FIELDS:
            if f in data:
                df_f = data[f]
                if sym in df_f.index:
                    frames[f] = df_f.loc[sym]
        if not frames:
            return pd.DataFrame()
        df = pd.DataFrame(frames)
        df.index.name = "date"
        df = df.reset_index()
        # 解析日期列  1D="20240102"(8位)  1m="20241107095900"(14位)
        sample = str(df["date"].iloc[0])
        fmt = "%Y%m%d" if len(sample) == 8 else "%Y%m%d%H%M%S"
        df["date"] = pd.to_datetime(df["date"], format=fmt, errors="coerce")
        df = df.dropna(subset=["date"])
        # 过滤全零行
        df = df[df["volume"] > 0]
        return df
    except Exception as e:
        print(f"    [API ERR] {sym} {period}: {e}")
        return pd.DataFrame()


def load_feather(sym, period):
    """加载已导出的 feather 文件"""
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
        return df
    except Exception:
        return pd.DataFrame()


def make_assign_fn(tdays):
    sdays = sorted(tdays)
    def _assign(ts):
        if ts.time() >= NIGHT_CUTOFF:
            idx = bisect.bisect_right(sdays, pd.Timestamp(ts.date()))
            return sdays[idx] if idx < len(sdays) else pd.Timestamp(ts.date())
        return pd.Timestamp(ts.date())
    return _assign


def agg_intraday_to_1d(df, tc="date", tdays=None):
    """将分钟级数据聚合到日级"""
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    if tdays:
        fn = make_assign_fn(tdays)
        df["_td"] = df[tc].map(fn)
    else:
        df["_td"] = df[tc].dt.normalize()
    agg = {}
    for f, op in [("open","first"),("high","max"),("low","min"),
                  ("close","last"),("volume","sum"),("amount","sum")]:
        if f in df.columns:
            agg[f] = op
    if not agg:
        return pd.DataFrame()
    return df.groupby("_td").agg(agg)


def compare_1d(ref_1d, test_1d, label):
    """将 test_1d 与 ref_1d (黄金标准) 对比"""
    if ref_1d.empty or test_1d.empty:
        return {"label": label, "status": "no_data", "common": 0}

    # 对齐索引
    ref = ref_1d.copy()
    tst = test_1d.copy()

    # 确保索引是日期
    if not isinstance(ref.index, pd.DatetimeIndex):
        if "date" in ref.columns:
            ref = ref.set_index("date")
        ref.index = pd.to_datetime(ref.index, errors="coerce")
    ref.index = ref.index.normalize()

    if not isinstance(tst.index, pd.DatetimeIndex):
        if "date" in tst.columns:
            tst = tst.set_index("date")
        tst.index = pd.to_datetime(tst.index, errors="coerce")
    tst.index = tst.index.normalize()

    common = ref.index.intersection(tst.index)
    if len(common) < 5:
        return {"label": label, "status": "insufficient", "common": len(common)}

    result = {"label": label, "status": "ok", "common": len(common)}

    for field in ["open", "high", "low", "close", "volume"]:
        if field not in ref.columns or field not in tst.columns:
            continue
        rv = ref.loc[common, field].values.astype(float)
        tv = tst.loc[common, field].values.astype(float)

        # 完全匹配率（容差1e-6）
        exact = np.sum(np.abs(rv - tv) < max(np.abs(rv).max() * 1e-6, 0.01)) / len(common)

        # 相对误差
        mask = np.abs(rv) > 1e-10
        if mask.sum() > 0:
            rel_err = np.abs(rv[mask] - tv[mask]) / np.abs(rv[mask])
            result[f"{field}_match"] = round(float(exact * 100), 2)
            result[f"{field}_median_err"] = round(float(np.median(rel_err) * 100), 6)
            result[f"{field}_max_err"] = round(float(np.max(rel_err) * 100), 4)
        else:
            result[f"{field}_match"] = 0.0

    # volume 残差（特别关注）
    if "volume" in ref.columns and "volume" in tst.columns:
        rv = ref.loc[common, "volume"].values.astype(float)
        tv = tst.loc[common, "volume"].values.astype(float)
        mask = rv > 0
        if mask.sum() > 0:
            residual = (rv[mask] - tv[mask]) / rv[mask]
            result["vol_residual_median_pct"] = round(float(np.median(residual) * 100), 6)
            result["vol_residual_mean_pct"] = round(float(np.mean(residual) * 100), 6)

    return result


def get_first_bar_info(df, tc="date"):
    """获取每天首根bar的时间信息"""
    if df.empty:
        return {}
    df = df.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    df["_d"] = df[tc].dt.date
    df["_t"] = df[tc].dt.time
    first_times = df.groupby("_d")["_t"].min()
    return {
        "earliest": str(first_times.min()),
        "latest": str(first_times.max()),
        "mode": str(first_times.mode().iloc[0]) if len(first_times) > 0 else None,
        "has_pre_open": bool(any(dtime(9,15) <= t < dtime(9,30) for t in first_times)),
        "has_pre_0900": bool(any(dtime(8,55) <= t < dtime(9,0) for t in first_times)),
        "has_pre_2100": bool(any(dtime(20,55) <= t < dtime(21,0) for t in first_times)),
        "total_days": len(first_times),
    }


def get_night_session_info(df, tc="date"):
    """检查夜盘首根bar信息"""
    if df.empty:
        return {}
    df = df.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    df["_t"] = df[tc].dt.time
    night = df[(df["_t"] >= dtime(20,0)) | (df["_t"] <= dtime(3,0))]
    if night.empty:
        return {"has_night": False}
    night_first = night.groupby(night[tc].dt.date)["_t"].min()
    return {
        "has_night": True,
        "night_first_min": str(night_first.min()),
        "night_first_mode": str(night_first.mode().iloc[0]) if len(night_first) > 0 else None,
        "has_auction_2055": bool(any(dtime(20,55) <= t < dtime(21,0) for t in night_first)),
        "night_days": len(night_first),
    }


# ─── 主验证循环 ─────────────────────────────────────────────────────

def run_verification():
    print("=" * 80)
    print("  终极多源数据交叉验证 — 黄金标准1D收敛")
    print("  品种池: {} 个".format(len(SYMBOLS)))
    print("=" * 80)

    all_results = []
    group_id = 0

    for sym, kind in sorted(SYMBOLS.items()):
        print(f"\n{'─'*60}")
        print(f"  [{sym}] type={kind}")

        # ── S1: QMT API 1D（黄金标准）──
        api_1d = get_api_data(sym, "1d")
        if api_1d.empty:
            print(f"    S1(API 1D): 无数据, 跳过")
            continue
        api_1d_indexed = api_1d.set_index("date")
        api_1d_indexed.index = api_1d_indexed.index.normalize()
        tdays = set(api_1d_indexed.index)
        n1d = len(api_1d)
        print(f"    S1(API 1D): {n1d} days  [{api_1d['date'].min().date()} → {api_1d['date'].max().date()}]")

        # ── S2: QMT API 1m → agg ──
        api_1m = get_api_data(sym, "1m")
        if not api_1m.empty:
            first_info = get_first_bar_info(api_1m)
            night_info = get_night_session_info(api_1m) if "night" in kind else {}
            agg_1m = agg_intraday_to_1d(api_1m, "date", tdays)
            cmp_1m = compare_1d(api_1d_indexed, agg_1m, f"S2: API_1m_agg vs API_1d")
            group_id += 1
            cmp_1m["group"] = group_id
            cmp_1m["sym"] = sym
            cmp_1m["kind"] = kind
            cmp_1m["first_bar"] = first_info
            cmp_1m["night_bar"] = night_info
            all_results.append(cmp_1m)
            vol_res = cmp_1m.get("vol_residual_median_pct", "N/A")
            vol_match = cmp_1m.get("volume_match", "N/A")
            print(f"    S2(API 1m→1d): {cmp_1m['common']} days, "
                  f"vol_res={vol_res}%, vol_match={vol_match}%"
                  f"  first_bar={first_info.get('mode','?')}")
            if night_info.get("has_night"):
                print(f"      夜盘: first={night_info.get('night_first_mode','?')}, "
                      f"has_auction_2055={night_info.get('has_auction_2055')}")
        else:
            print(f"    S2(API 1m): 无数据")

        # ── S3: QMT API 5m → agg ──
        api_5m = get_api_data(sym, "5m")
        if not api_5m.empty:
            first_5m = get_first_bar_info(api_5m)
            agg_5m = agg_intraday_to_1d(api_5m, "date", tdays)
            cmp_5m = compare_1d(api_1d_indexed, agg_5m, f"S3: API_5m_agg vs API_1d")
            group_id += 1
            cmp_5m["group"] = group_id
            cmp_5m["sym"] = sym
            cmp_5m["kind"] = kind
            cmp_5m["first_bar_5m"] = first_5m
            all_results.append(cmp_5m)
            vol_res = cmp_5m.get("vol_residual_median_pct", "N/A")
            print(f"    S3(API 5m→1d): {cmp_5m['common']} days, "
                  f"vol_res={vol_res}%  first_bar_5m={first_5m.get('mode','?')}")
        else:
            print(f"    S3(API 5m): 无数据")

        # ── S4: Tick → agg（仅尝试）──
        if QMT_OK:
            try:
                tick = xtdata.get_full_tick([sym])
                if tick and sym in tick and tick[sym]:
                    td = tick[sym]
                    tvol = td.get("lastVol", 0) or td.get("volume", 0)
                    tprice = td.get("lastPrice", 0)
                    if tvol > 0:
                        group_id += 1
                        tr = {
                            "group": group_id, "sym": sym, "kind": kind,
                            "label": "S4: Tick snapshot",
                            "status": "snapshot_only",
                            "tick_vol": int(tvol),
                            "tick_price": float(tprice),
                        }
                        all_results.append(tr)
                        print(f"    S4(Tick): vol={int(tvol)}, price={tprice}")
            except Exception:
                pass

        # ── S5: Feather 1D vs API 1D ──
        fth_1d = load_feather(sym, "1d")
        if not fth_1d.empty:
            cmp_fth = compare_1d(api_1d_indexed, fth_1d, "S5: Feather_1d vs API_1d")
            group_id += 1
            cmp_fth["group"] = group_id
            cmp_fth["sym"] = sym
            cmp_fth["kind"] = kind
            all_results.append(cmp_fth)
            vol_res = cmp_fth.get("vol_residual_median_pct", "N/A")
            print(f"    S5(Feather 1d): {cmp_fth['common']} days, vol_res={vol_res}%")

        # ── S6: Feather 1m → agg vs API 1D ──
        fth_1m = load_feather(sym, "1m")
        if not fth_1m.empty:
            agg_fth = agg_intraday_to_1d(fth_1m, "date", tdays)
            cmp_fth_1m = compare_1d(api_1d_indexed, agg_fth, "S6: Feather_1m_agg vs API_1d")
            group_id += 1
            cmp_fth_1m["group"] = group_id
            cmp_fth_1m["sym"] = sym
            cmp_fth_1m["kind"] = kind
            all_results.append(cmp_fth_1m)
            vol_res = cmp_fth_1m.get("vol_residual_median_pct", "N/A")
            print(f"    S6(Feather 1m→1d): {cmp_fth_1m['common']} days, vol_res={vol_res}%")

    # ─── 汇总 ───────────────────────────────────────────────────────
    print(f"\n{'='*80}")
    print(f"  总验证组数: {group_id}")
    print(f"{'='*80}")

    # 按类型统计
    type_stats = {}
    for r in all_results:
        if r.get("status") not in ("ok",):
            continue
        k = r.get("kind", "unknown")
        lbl = r.get("label", "")
        key = f"{k}|{lbl.split(':')[0]}"
        if key not in type_stats:
            type_stats[key] = {"vol_res": [], "vol_match": [], "close_match": [], "count": 0}
        type_stats[key]["count"] += 1
        vr = r.get("vol_residual_median_pct")
        if vr is not None:
            type_stats[key]["vol_res"].append(vr)
        vm = r.get("volume_match")
        if vm is not None:
            type_stats[key]["vol_match"].append(vm)
        cm = r.get("close_match")
        if cm is not None:
            type_stats[key]["close_match"].append(cm)

    print(f"\n  类型汇总:")
    print(f"  {'类型':<30} {'组数':>4} {'vol残差中位%':>12} {'close匹配%':>12}")
    print(f"  {'-'*62}")
    for key in sorted(type_stats.keys()):
        st = type_stats[key]
        vr = f"{np.median(st['vol_res']):+.6f}" if st["vol_res"] else "N/A"
        cm = f"{np.mean(st['close_match']):.2f}" if st["close_match"] else "N/A"
        print(f"  {key:<30} {st['count']:>4} {vr:>12} {cm:>12}")

    # 集合竞价专项汇总
    print(f"\n{'='*80}")
    print(f"  集合竞价首根Bar时间汇总")
    print(f"{'='*80}")
    for r in all_results:
        fb = r.get("first_bar", {})
        nb = r.get("night_bar", {})
        if fb:
            sym = r.get("sym", "?")
            kind = r.get("kind", "?")
            print(f"  {sym:<14} ({kind:<16}) "
                  f"first_bar={fb.get('mode','?'):<10} "
                  f"pre_open={fb.get('has_pre_open',False)!s:<6} "
                  f"pre_0900={fb.get('has_pre_0900',False)!s:<6} "
                  f"pre_2100={nb.get('has_auction_2055',False) if nb else 'N/A'!s:<6}")

    # 保存详细结果
    import json
    out = DATA_DIR / "audit_reports" / "ultimate_crossval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  详细结果: {out}")
    print(f"  总验证组: {group_id}")
    return all_results, group_id


if __name__ == "__main__":
    results, n_groups = run_verification()

    if n_groups < 50:
        print(f"\n[WARN] 仅完成 {n_groups} 组, 目标 50+")
    else:
        print(f"\n[OK] 完成 {n_groups} 组验证 (目标 50+) ✅")

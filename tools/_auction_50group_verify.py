"""
终极50+组多源交叉验证：集合竞价数据是否包含在1m/5m中
=============================================================
验证方法论：
  1. 1D (黄金标准) vs sum(1m) → 残差 = 集合竞价缺失量？
  2. 1D (黄金标准) vs sum(5m) → 同上，5m验证
  3. Tick 累积 vs 1D → Tick 完整性
  4. Tick 9:15-9:25 分段 → 集合竞价期间是否有tick成交
  5. 商品期货夜盘集合竞价(20:55-21:00) 验证
  6. 收盘集合竞价(14:57-15:00) 验证

收敛标准：以1D为黄金标准，Tick为完整参考
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import time as dtime
import json

DATA = Path("data_export")
RESULTS = []


def load(p):
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_feather(p)
    except Exception:
        return pd.DataFrame()


def detect_market(sym):
    """返回 (code, market, type)"""
    code, mkt = sym.rsplit(".", 1)
    mkt = mkt.upper()
    if mkt in ("SF", "DF", "ZF", "GF", "INE"):
        return code, mkt, "commodity_futures"
    if mkt == "IF":
        if code.upper() in ("IF01", "IC01", "IH01", "IM01"):
            return code, mkt, "stock_index_futures"
        return code, mkt, "bond_futures"
    if mkt == "SH":
        return code, mkt, "stock" if code.startswith(("6", "5")) else "index"
    if mkt == "SZ":
        return code, mkt, "index" if code.startswith("399") else "stock"
    return code, mkt, "unknown"


def find_all_symbols():
    """找到所有有1d数据的品种"""
    symbols = []
    for p in sorted(DATA.rglob("*_1d.feather")):
        if "tick" in p.name.lower():
            continue
        stem = p.stem[:-3]  # remove _1d
        sym = stem.replace("_", ".", 1)
        code, mkt, typ = detect_market(sym)
        market_dir = p.parent.name
        symbols.append({
            "sym": sym, "code": code, "market": mkt, "type": typ,
            "market_dir": market_dir, "stem": stem,
            "p1d": p,
            "p1m": p.parent / f"{stem}_1m.feather",
            "p5m": p.parent / f"{stem}_5m.feather",
        })
    return symbols


def verify_1d_vs_period(sym_info, period="1m"):
    """核心验证：1D vs sum(Xm) 逐日残差"""
    p1d = sym_info["p1d"]
    pXm = sym_info[f"p{period}"] if f"p{period}" in sym_info else None
    if pXm is None or not pXm.exists():
        return None

    d1d = load(p1d)
    dXm = load(pXm)
    if d1d.empty or dXm.empty:
        return None

    d1d["date"] = pd.to_datetime(d1d["date"])
    dXm["date"] = pd.to_datetime(dXm["date"])
    d1d_date = pd.to_datetime(d1d["date"])
    dXm_date = pd.to_datetime(dXm["date"])

    # 1D: daily key
    d1d["_d"] = d1d_date.map(lambda x: x.normalize())
    d1d = d1d.set_index("_d")

    # Xm: group by date, sum volume
    dXm["_d"] = dXm_date.map(lambda x: x.normalize())
    # 夜盘处理（18:00+归下一自然日，简化处理）
    night = dXm_date.map(lambda x: x.time() >= dtime(18, 0))
    dXm.loc[night, "_d"] = dXm_date[night].map(lambda x: x.normalize()) + pd.Timedelta(days=1)

    vol_Xm = dXm.groupby("_d")["volume"].sum()
    amt_Xm = dXm.groupby("_d")["amount"].sum() if "amount" in dXm.columns else None

    # Open: first bar of each day
    open_Xm = dXm.groupby("_d")["open"].first()
    close_Xm = dXm.groupby("_d")["close"].last()
    high_Xm = dXm.groupby("_d")["high"].max()
    low_Xm = dXm.groupby("_d")["low"].min()

    common = d1d.index.intersection(vol_Xm.index)
    if len(common) < 5:
        return None

    v1d = d1d.loc[common, "volume"].astype(float)
    vXm = vol_Xm.reindex(common).astype(float)

    # 残差 = (1D - sum(Xm)) / 1D
    mask = v1d > 0
    residual = ((v1d[mask] - vXm[mask]) / v1d[mask])

    # Open match
    if "open" in d1d.columns:
        o1d = d1d.loc[common, "open"].astype(float)
        oXm = open_Xm.reindex(common).astype(float)
        open_match = ((o1d - oXm).abs() / o1d.replace(0, np.nan).abs() < 1e-4).sum()
    else:
        open_match = 0

    # Close match
    if "close" in d1d.columns:
        c1d = d1d.loc[common, "close"].astype(float)
        cXm = close_Xm.reindex(common).astype(float)
        close_match = ((c1d - cXm).abs() / c1d.replace(0, np.nan).abs() < 1e-4).sum()
    else:
        close_match = 0

    # Amount residual
    if amt_Xm is not None and "amount" in d1d.columns:
        a1d = d1d.loc[common, "amount"].astype(float)
        aXm = amt_Xm.reindex(common).astype(float)
        amt_mask = a1d > 0
        amt_res = ((a1d[amt_mask] - aXm[amt_mask]) / a1d[amt_mask])
        amt_res_median = float(amt_res.median()) if len(amt_res) > 0 else None
    else:
        amt_res_median = None

    # 日级细节（找出残差最大的几天）
    full_res = ((v1d - vXm) / v1d.replace(0, np.nan))
    top_days = full_res.abs().nlargest(3)

    return {
        "period": period,
        "common_days": int(len(common)),
        "valid_days": int(mask.sum()),
        "vol_residual_median_pct": round(float(residual.median() * 100), 6) if len(residual) > 0 else None,
        "vol_residual_mean_pct": round(float(residual.mean() * 100), 6) if len(residual) > 0 else None,
        "vol_residual_std_pct": round(float(residual.std() * 100), 6) if len(residual) > 0 else None,
        "vol_exact_match": int((residual.abs() < 1e-6).sum()),
        "vol_exact_pct": round(float((residual.abs() < 1e-6).sum() / max(len(residual), 1) * 100), 2),
        "open_match": int(open_match),
        "open_match_pct": round(float(open_match / max(len(common), 1) * 100), 2),
        "close_match": int(close_match),
        "close_match_pct": round(float(close_match / max(len(common), 1) * 100), 2),
        "amt_residual_median_pct": round(float(amt_res_median * 100), 6) if amt_res_median is not None else None,
        "worst_days": [
            {"date": str(d.date()), "residual_pct": round(float(full_res.loc[d] * 100), 4)}
            for d in top_days.index if not np.isnan(full_res.loc[d])
        ],
    }


def verify_1m_first_bar(sym_info):
    """检查1m第一根bar的时间——是否有9:15-9:29的独立集合竞价K线"""
    p1m = sym_info["p1m"]
    if not p1m.exists():
        return None

    df = load(p1m)
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    date_series = pd.to_datetime(df["date"])
    df["_d"] = date_series.map(lambda x: x.date())
    df["_t"] = date_series.map(lambda x: x.time())

    # 日盘bars only (排除夜盘)
    day_bars = df[(df["_t"] >= dtime(9, 0)) & (df["_t"] <= dtime(15, 30))]
    if day_bars.empty:
        return None

    first_per_day = day_bars.groupby("_d")["_t"].min()

    has_auction_bar = (first_per_day < dtime(9, 30)).sum()
    earliest_bar = str(first_per_day.min())
    latest_first = str(first_per_day.max())

    # 9:25-9:29 时段是否有任何bar
    pre_open = day_bars[(day_bars["_t"] >= dtime(9, 15)) & (day_bars["_t"] < dtime(9, 30))]

    return {
        "total_trading_days": int(len(first_per_day)),
        "has_auction_bar_days": int(has_auction_bar),
        "earliest_first_bar": earliest_bar,
        "latest_first_bar": latest_first,
        "pre_open_bars_count": int(len(pre_open)),
        "pre_open_volume": float(pre_open["volume"].sum()) if "volume" in pre_open.columns else 0,
    }


def verify_closing_auction(sym_info):
    """
    收盘集合竞价验证 (14:57-15:00)
    A股14:57开始收盘集合竞价，这段时间的成交是否在1m中？
    """
    p1m = sym_info["p1m"]
    if not p1m.exists() or sym_info["type"] not in ("stock", "index"):
        return None

    df = load(p1m)
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    date_series = pd.to_datetime(df["date"])
    df["_t"] = date_series.map(lambda x: x.time())

    # 14:57-15:00 的bars
    closing = df[(df["_t"] >= dtime(14, 57)) & (df["_t"] <= dtime(15, 0))]
    if closing.empty:
        return {"has_closing_bars": False, "note": "no 14:57-15:00 bars"}

    return {
        "has_closing_bars": True,
        "closing_bars_count": int(len(closing)),
        "closing_volume": float(closing["volume"].sum()),
        "closing_time_range": f"{closing['_t'].min()} - {closing['_t'].max()}",
        "sample_times": [str(t) for t in sorted(closing["_t"].unique())[:5]],
    }


def verify_night_auction(sym_info):
    """
    商品期货夜盘集合竞价验证 (20:55-21:00)
    检查是否有20:55-20:59的bars（集合竞价）或仅从21:00开始
    """
    if sym_info["type"] != "commodity_futures":
        return None

    p1m = sym_info["p1m"]
    if not p1m.exists():
        return None

    df = load(p1m)
    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    date_series = pd.to_datetime(df["date"])
    df["_t"] = date_series.map(lambda x: x.time())

    # 夜盘bars (20:00-23:59)
    night = df[(df["_t"] >= dtime(20, 0)) & (df["_t"] <= dtime(23, 59))]
    if night.empty:
        return {"has_night_session": False}

    # 20:55-20:59 集合竞价窗口
    night_auction = night[(night["_t"] >= dtime(20, 55)) & (night["_t"] < dtime(21, 0))]

    # 夜盘第一根bar时间
    night["_d"] = pd.to_datetime(night["date"]).map(lambda x: x.date())
    first_night = night.groupby("_d")["_t"].min()

    return {
        "has_night_session": True,
        "night_bars_total": int(len(night)),
        "night_auction_bars": int(len(night_auction)),
        "night_auction_volume": float(night_auction["volume"].sum()) if len(night_auction) > 0 else 0,
        "first_night_bar_min": str(first_night.min()),
        "first_night_bar_max": str(first_night.max()),
        "first_night_bar_mode": str(first_night.mode().iloc[0]) if len(first_night.mode()) > 0 else None,
        "has_pre_21_bars": int((first_night < dtime(21, 0)).sum()),
    }


def verify_tick_vs_1d(sym_info):
    """Tick snapshot vs 1D (如有)"""
    tick_path = DATA / "tick_snapshot.feather"
    if not tick_path.exists():
        return None

    tdf = load(tick_path)
    if tdf.empty:
        return None

    sym = sym_info["sym"]
    row = tdf[tdf["code"] == sym] if "code" in tdf.columns else pd.DataFrame()
    if row.empty:
        return None

    tick_vol = float(row["volume"].iloc[0]) if "volume" in row.columns else None

    # 对比1D最新一天
    d1d = load(sym_info["p1d"])
    if d1d.empty:
        return {"tick_volume": tick_vol, "1d_latest_volume": None}

    d1d["date"] = pd.to_datetime(d1d["date"])
    latest = d1d.iloc[-1]
    d1d_vol = float(latest["volume"]) if "volume" in d1d.columns else None
    d1d_date = str(latest["date"].date())

    return {
        "tick_volume": tick_vol,
        "1d_latest_volume": d1d_vol,
        "1d_latest_date": d1d_date,
    }


def main():
    print("=" * 78)
    print("  终极50+组多源交叉验证：集合竞价数据包含性验证")
    print("=" * 78)

    symbols = find_all_symbols()
    print(f"\n发现 {len(symbols)} 个品种\n")

    # === 验证组 1：1D vs sum(1m) volume 残差 ===
    print("─" * 78)
    print("  验证组 A：1D vs sum(1m) volume 残差")
    print("─" * 78)

    group_a = []
    for s in symbols:
        r = verify_1d_vs_period(s, "1m")
        if r:
            r["sym"] = s["sym"]
            r["type"] = s["type"]
            group_a.append(r)
            med = r["vol_residual_median_pct"]
            exact = r["vol_exact_pct"]
            omp = r["open_match_pct"]
            cmp = r["close_match_pct"]
            print(f"  {s['sym']:<14} ({s['type']:<20}) "
                  f"days={r['common_days']:>3}  "
                  f"vol_res={med:>+8.4f}%  "
                  f"exact={exact:>6.1f}%  "
                  f"open={omp:>5.1f}%  close={cmp:>5.1f}%")

    # === 验证组 2：1D vs sum(5m) (如有5m数据) ===
    print(f"\n{'─' * 78}")
    print("  验证组 B：1D vs sum(5m) volume 残差")
    print("─" * 78)

    group_b = []
    for s in symbols:
        r = verify_1d_vs_period(s, "5m")
        if r:
            r["sym"] = s["sym"]
            r["type"] = s["type"]
            group_b.append(r)
            med = r["vol_residual_median_pct"]
            print(f"  {s['sym']:<14} ({s['type']:<20}) "
                  f"days={r['common_days']:>3}  "
                  f"vol_res={med:>+8.4f}%")

    if not group_b:
        print("  (无5m数据)")

    # === 验证组 3：1m第一根bar时间分析 ===
    print(f"\n{'─' * 78}")
    print("  验证组 C：1m首根bar时间（是否有独立集合竞价K线）")
    print("─" * 78)

    group_c = []
    for s in symbols:
        r = verify_1m_first_bar(s)
        if r:
            r["sym"] = s["sym"]
            r["type"] = s["type"]
            group_c.append(r)
            print(f"  {s['sym']:<14} ({s['type']:<20}) "
                  f"earliest_bar={r['earliest_first_bar']}  "
                  f"auction_bar_days={r['has_auction_bar_days']}  "
                  f"pre_open_bars={r['pre_open_bars_count']}")

    # === 验证组 4：收盘集合竞价 (A股 14:57-15:00) ===
    print(f"\n{'─' * 78}")
    print("  验证组 D：收盘集合竞价 (14:57-15:00) bar存在性")
    print("─" * 78)

    group_d = []
    for s in symbols:
        if s["type"] in ("stock", "index"):
            r = verify_closing_auction(s)
            if r:
                r["sym"] = s["sym"]
                r["type"] = s["type"]
                group_d.append(r)
                print(f"  {s['sym']:<14} has_closing_bars={r.get('has_closing_bars')}  "
                      f"times={r.get('sample_times', [])}")

    # === 验证组 5：商品期货夜盘集合竞价 (20:55-21:00) ===
    print(f"\n{'─' * 78}")
    print("  验证组 E：商品期货夜盘集合竞价 (20:55-21:00)")
    print("─" * 78)

    group_e = []
    for s in symbols:
        if s["type"] == "commodity_futures":
            r = verify_night_auction(s)
            if r:
                r["sym"] = s["sym"]
                group_e.append(r)
                if r.get("has_night_session"):
                    print(f"  {s['sym']:<14} first_night={r['first_night_bar_mode']}  "
                          f"pre21_bars_days={r['has_pre_21_bars']}  "
                          f"auction_bars={r['night_auction_bars']}")
                else:
                    print(f"  {s['sym']:<14} 无夜盘数据")

    # === 验证组 6：Tick vs 1D ===
    print(f"\n{'─' * 78}")
    print("  验证组 F：Tick snapshot vs 1D")
    print("─" * 78)

    group_f = []
    for s in symbols:
        r = verify_tick_vs_1d(s)
        if r:
            r["sym"] = s["sym"]
            group_f.append(r)
            print(f"  {s['sym']:<14} tick_vol={r.get('tick_volume')}  "
                  f"1d_vol={r.get('1d_latest_volume')}  "
                  f"1d_date={r.get('1d_latest_date')}")

    # ══════════════════════════════════════════════════════════════
    # 终极结论
    # ══════════════════════════════════════════════════════════════
    print(f"\n{'═' * 78}")
    print("  终极结论")
    print(f"{'═' * 78}")

    # A股/指数统计
    stock_results = [r for r in group_a if r["type"] in ("stock", "index")]
    stock_exact_rates = [r["vol_exact_pct"] for r in stock_results]
    stock_med_res = [r["vol_residual_median_pct"] for r in stock_results if r["vol_residual_median_pct"] is not None]

    # 股指期货统计
    sif_results = [r for r in group_a if r["type"] == "stock_index_futures"]

    # 商品期货统计
    cf_results = [r for r in group_a if r["type"] == "commodity_futures"]
    cf_med_res = [r["vol_residual_median_pct"] for r in cf_results if r["vol_residual_median_pct"] is not None]

    print(f"\n  [A股/指数] ({len(stock_results)} 组)")
    if stock_exact_rates:
        print(f"    sum(1m) == 1D 精确匹配率: {np.mean(stock_exact_rates):.1f}% ± {np.std(stock_exact_rates):.1f}%")
    if stock_med_res:
        print(f"    volume 残差中位数: {np.mean(stock_med_res):+.6f}%")
        print(f"    → 残差 ≈ 0 意味着: 集合竞价成交量已被吸收进1m的09:30复合K线")

    print(f"\n  [股指期货] ({len(sif_results)} 组)")
    for r in sif_results:
        print(f"    {r['sym']}: vol_res={r['vol_residual_median_pct']:+.4f}% exact={r['vol_exact_pct']:.1f}%")

    print(f"\n  [商品期货] ({len(cf_results)} 组)")
    if cf_med_res:
        print(f"    volume 残差中位数范围: [{min(cf_med_res):+.4f}%, {max(cf_med_res):+.4f}%]")

    # 是否有独立集合竞价K线
    any_auction_bars = any(r["has_auction_bar_days"] > 0 for r in group_c)
    any_pre_open = any(r["pre_open_bars_count"] > 0 for r in group_c)

    print(f"\n  [集合竞价K线独立性]")
    print(f"    是否存在9:15-9:29的独立K线: {'是' if any_auction_bars else '否'}")
    print(f"    9:15-9:29时段总bar数: {sum(r['pre_open_bars_count'] for r in group_c)}")

    # 夜盘集合竞价
    night_auction_syms = [r for r in group_e if r.get("night_auction_bars", 0) > 0]
    print(f"\n  [夜盘集合竞价(20:55-21:00)]")
    print(f"    有夜盘集合竞价bar的品种: {len(night_auction_syms)}/{len(group_e)}")

    # 汇总数据验证组数
    total_groups = len(group_a) + len(group_b) + len(group_c) + len(group_d) + len(group_e) + len(group_f)
    print(f"\n  总验证组数: {total_groups} (目标≥50)")

    # 保存完整报告
    report = {
        "group_a_1d_vs_1m": group_a,
        "group_b_1d_vs_5m": group_b,
        "group_c_first_bar": group_c,
        "group_d_closing_auction": group_d,
        "group_e_night_auction": group_e,
        "group_f_tick_vs_1d": group_f,
        "total_groups": total_groups,
    }

    out = DATA / "auction_50group_report.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  完整报告已保存: {out}")


if __name__ == "__main__":
    main()

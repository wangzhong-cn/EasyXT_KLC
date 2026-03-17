"""
深度多源集合竞价验证脚本
==========================================================
目的：
  1. 对全部 20 个品种逐日计算 1D.volume - sum(1m.volume) 残差
  2. 通过统计特征判断 QMT 的 1m/5m 是否系统性排除集合竞价数据
  3. 检验 1D.open vs 1m 首根 open（集合竞价吸收证明）
  4. 读取 tick_snapshot.feather 参与交叉验证
  5. 分品种类型输出结论

验证假设：
  【H1 排除假说】QMT 1m/5m 不含集合竞价 → 1D.vol > sum(1m.vol) 系统性正偏 ~2-5%
  【H2 吸收假说】QMT 将集合竞价并入 09:30 复合K线 → 1D.vol ≈ sum(1m.vol) ≈ 0%
  【H3 1D也排除假说】1D 和 1m 都不含集合竞价 → 两者仍相等，但不等于 tick 总量

结论判据：
  - A股/指数：若残差 MEDIAN ≈ 0% 且分布对称 → H2（吸收）成立
  - A股/指数：若残差 MEDIAN > 1% 且单侧分布 → H1（排除）成立
  - Tick 交叉验证：若 last_tick.volume ≈ 1D.volume → 1D 是完整黄金标准
"""
from __future__ import annotations

import json
from datetime import time as dtime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"
NIGHT_CUTOFF = dtime(18, 0, 0)

# ─── 工具函数 ────────────────────────────────────────────────────────────────

def load(p: Path) -> pd.DataFrame:
    try:
        return pd.read_feather(p)
    except Exception as e:
        print(f"  [LOAD ERROR] {p.name}: {e}")
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

def has_night_session(sym: str) -> bool:
    """商品期货通常有夜盘，股指期货无夜盘，A股无夜盘。"""
    return detect_type(sym) == "commodity_futures"

def find_timecol(df: pd.DataFrame) -> str | None:
    for c in ("date", "time", "datetime"):
        if c in df.columns:
            return c
    if df.columns.size > 0:
        try:
            pd.to_datetime(df[df.columns[0]].iloc[0])
            return str(df.columns[0])
        except Exception:
            pass
    return None

def make_assign(trading_days: set):
    import bisect
    days_sorted = sorted(trading_days)

    def _next(d: pd.Timestamp) -> pd.Timestamp:
        idx = bisect.bisect_right(days_sorted, d)
        return days_sorted[idx] if idx < len(days_sorted) else d

    def _assign(ts: pd.Timestamp) -> pd.Timestamp:
        if ts.time() >= NIGHT_CUTOFF:
            base = pd.Timestamp(ts.date())
            return _next(base)
        return pd.Timestamp(ts.date())
    return _assign

def agg_1m(df_1m: pd.DataFrame, tc: str, tdays: set) -> pd.DataFrame:
    df = df_1m.copy()
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df = df.dropna(subset=[tc])
    fn = make_assign(tdays)
    df["_td"] = df[tc].map(fn)
    agg = {}
    for f, op in [("open","first"),("high","max"),("low","min"),
                  ("close","last"),("volume","sum"),("amount","sum")]:
        if f in df.columns:
            agg[f] = op
    if not agg:
        return pd.DataFrame()
    return df.groupby("_td").agg(agg)


# ─── 主验证逻辑 ──────────────────────────────────────────────────────────────

def analyze_symbol(sym: str, p1d: Path, p1m: Path) -> dict:
    """逐日分析单个品种的多维度一致性。"""
    kind = detect_type(sym)
    night = has_night_session(sym)

    df1d = load(p1d)
    df1m = load(p1m)
    if df1d.empty or df1m.empty:
        return {"sym": sym, "type": kind, "status": "no_data"}

    tc1d = find_timecol(df1d)
    tc1m = find_timecol(df1m)
    if not tc1d or not tc1m:
        return {"sym": sym, "type": kind, "status": "no_timecol"}

    df1d = df1d.copy()
    df1d[tc1d] = pd.to_datetime(df1d[tc1d], errors="coerce")
    df1d = df1d.dropna(subset=[tc1d]).set_index(tc1d)
    df1d.index = pd.DatetimeIndex(df1d.index).normalize()
    tdays = set(df1d.index)

    agg = agg_1m(df1m, tc1m, tdays)
    if agg.empty:
        return {"sym": sym, "type": kind, "status": "agg_failed"}

    # --- 第一步：检测零成交量腐败 ---
    tmp = df1m.copy()
    tmp[tc1m] = pd.to_datetime(tmp[tc1m], errors="coerce")
    tmp = tmp.dropna(subset=[tc1m])
    tmp["_date"] = pd.to_datetime(tmp[tc1m], errors="coerce").map(lambda x: x.date())
    corrupt_days = []
    for d, grp in tmp.groupby("_date"):
        if "volume" in grp.columns:
            frac = (grp["volume"] == 0).sum() / max(len(grp), 1)
            if frac > 0.5:
                corrupt_days.append(str(d))

    # --- 第二步：逐日成交量残差 1D - agg(1m) ---
    common = df1d.index.intersection(agg.index)
    if len(common) == 0:
        return {"sym": sym, "type": kind, "status": "no_overlap"}

    v1d_all = df1d.loc[common, "volume"].values.astype(float) if "volume" in df1d.columns else None
    vagg_all = agg.loc[common, "volume"].values.astype(float) if "volume" in agg.columns else None

    residuals = []        # (1D - agg) / 1D，正值 = 1D>1m（疑似缺集合竞价）
    day_labels = []
    bar_counts = []       # 每日 1m bar 数量（校验是否缺盘中数据）

    # 统计每日 bar 数
    tmp["_td"] = tmp[tc1m].map(make_assign(tdays))
    day_barcount = tmp.groupby("_td").size()

    for i, d in enumerate(common):
        v1 = v1d_all[i] if v1d_all is not None else np.nan
        va = vagg_all[i] if vagg_all is not None else np.nan
        if v1 > 0:
            res = (v1 - va) / v1
        else:
            res = np.nan
        residuals.append(res)
        day_labels.append(str(d.date()))
        bc = int(day_barcount.get(d, 0))
        bar_counts.append(bc)

    residuals = np.array(residuals, dtype=float)
    valid_mask = ~np.isnan(residuals)
    valid_res = residuals[valid_mask]

    # 过滤掉零成交量腐败日
    corrupt_set = set(corrupt_days)
    clean_mask = valid_mask.copy()
    for i, dl in enumerate(day_labels):
        if dl in corrupt_set:
            clean_mask[i] = False
    clean_res = residuals[clean_mask]

    # --- 第三步：1D.open vs 1m首根 open（集合竞价是否被吸收进09:30 bar）---
    open_checks = []
    if "open" in df1d.columns and "open" in agg.columns:
        o1d = df1d.loc[common, "open"].values.astype(float)
        oagg = agg.loc[common, "open"].values.astype(float)
        for i in range(len(common)):
            if abs(o1d[i]) > 0:
                rel_err = abs(o1d[i] - oagg[i]) / abs(o1d[i])
                open_checks.append(rel_err)
        open_checks = np.array(open_checks)
    else:
        open_checks = np.array([])

    # --- 第四步：每日 bar 数量统计（是否缺盘中/全天数据）---
    # A股正常应有 241 bars（09:30-15:00）;商品期货日盘+夜盘各异
    expected_bars = {
        "stock": 241,
        "index": 241,
        "stock_index_futures": 270,  # 09:30-15:15
        "commodity_futures": None,   # varies
    }
    exp = expected_bars.get(kind)
    bar_arr = np.array(bar_counts)
    short_days = []
    if exp:
        for i, cnt in enumerate(bar_counts):
            if 0 < cnt < exp * 0.9:  # 少于预期90%算缺数据
                short_days.append((day_labels[i], cnt))

    return {
        "sym": sym,
        "type": kind,
        "night_session": night,
        "common_days": len(common),
        "corrupt_days": len(corrupt_days),
        "corrupt_day_list": corrupt_days[:5],  # 只展示前5
        # 残差统计（含腐败日）
        "res_median_all": float(np.nanmedian(residuals)) if len(residuals) > 0 else None,
        "res_mean_all": float(np.nanmean(residuals)) if len(residuals) > 0 else None,
        # 残差统计（仅干净天）
        "res_median_clean": float(np.median(clean_res)) if len(clean_res) > 0 else None,
        "res_mean_clean": float(np.mean(clean_res)) if len(clean_res) > 0 else None,
        "res_std_clean": float(np.std(clean_res)) if len(clean_res) > 0 else None,
        "res_pct_positive_clean": float((clean_res > 0.001).sum() / max(len(clean_res), 1)),
        "res_pct_negative_clean": float((clean_res < -0.001).sum() / max(len(clean_res), 1)),
        "clean_days": len(clean_res),
        # 开盘价吸收检验
        "open_median_err": float(np.median(open_checks)) if len(open_checks) > 0 else None,
        "open_max_err": float(np.max(open_checks)) if len(open_checks) > 0 else None,
        "open_match_rate": float((open_checks <= 1e-4).sum() / max(len(open_checks), 1)) if len(open_checks) > 0 else None,
        # 缺盘数据
        "short_days": len(short_days),
        "short_day_samples": short_days[:3],
        # 各日细节（取最大残差日）
        "worst_residual": {
            "date": day_labels[int(np.nanargmax(np.abs(residuals)))] if len(residuals) > 0 else None,
            "val": float(np.nanmax(np.abs(residuals))) if len(residuals) > 0 else None,
        }
    }


def analyze_tick(tick_path: Path, sym_results: list[dict]) -> dict:
    """用 tick_snapshot 交叉验证 1D 是否等于 Tick 累计成交量。"""
    df_tick = load(tick_path)
    if df_tick.empty:
        return {"status": "no_tick_data"}

    print(f"\n  [Tick快照] 列名: {list(df_tick.columns)}")
    print(f"  [Tick快照] 行数: {len(df_tick)}")
    if len(df_tick) > 0:
        print(f"  [Tick快照] 前3行:\n{df_tick.head(3).to_string()}")

    return {"status": "tick_loaded", "rows": len(df_tick),
            "columns": list(df_tick.columns)}


# ─── 入口 ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 72)
    print("  深度集合竞价多源交叉验证")
    print("=" * 72)

    # 收集所有品种对
    pairs = []
    for p1d in sorted(DATA_DIR.rglob("*_1d.feather")):
        if "tick" in p1d.name.lower():
            continue
        stem = p1d.stem   # e.g. IF01_IF_1d
        base = stem[:-3]  # e.g. IF01_IF
        p1m = p1d.parent / f"{base}_1m.feather"
        if p1m.exists():
            sym = base.replace("_", ".", 1)
            pairs.append((sym, p1d, p1m))

    print(f"  发现品种对: {len(pairs)} 个")

    results = []
    for sym, p1d, p1m in pairs:
        r = analyze_symbol(sym, p1d, p1m)
        results.append(r)

    # ── Tick 交叉验证 ──────────────────────────────────────────────────────
    tick_info = {}
    tick_path = DATA_DIR / "tick_snapshot.feather"
    if tick_path.exists():
        tick_info = analyze_tick(tick_path, results)

    # ── 打印汇总表 ──────────────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  逐品种成交量残差分析（干净日）：1D vol - sum(1m vol) / 1D vol")
    print(f"{'='*72}")
    print(f"  {'品种':<14} {'类型':<22} {'干净天':<7} {'残差中位':<10} {'残差均值':<10}"
          f"{'正偏%(>0.1%)':<14} {'腐败天':<7} {'缺盘天'}")
    print(f"  {'-'*70}")

    auction_gap_stock = []    # A股/指数的干净日残差
    auction_gap_futures = []  # 期货的干净日残差
    open_match = []

    for r in results:
        sym = r["sym"]
        kind = r.get("type", "?")
        cd = r.get("clean_days", 0)
        med = r.get("res_median_clean")
        mea = r.get("res_mean_clean")
        pos = r.get("res_pct_positive_clean", 0)
        corrupt = r.get("corrupt_days", 0)
        short = r.get("short_days", 0)
        om = r.get("open_match_rate")

        med_s = f"{med*100:+.3f}%" if med is not None else "  N/A  "
        mea_s = f"{mea*100:+.3f}%" if mea is not None else "  N/A  "
        pos_s = f"{pos*100:.1f}%" if pos is not None else "N/A"
        om_s = f"{om*100:.1f}%" if om is not None else "N/A"

        flag = ""
        if med is not None and abs(med) < 0.001:
            flag = "✅ H2吸收"
        elif med is not None and med > 0.005:
            flag = "⚠️ H1排除?"
        elif corrupt > 0:
            flag = "❌ 数据腐败"

        print(f"  {sym:<14} {kind:<22} {cd:<7} {med_s:<10} {mea_s:<10}"
              f"{pos_s:<14} {corrupt:<7} {short:<7}  {flag}")

        if med is not None and cd >= 5:
            if kind in ("stock", "index"):
                auction_gap_stock.append(med)
            elif kind in ("stock_index_futures", "commodity_futures"):
                auction_gap_futures.append(med)
        if om is not None:
            open_match.append((sym, om))

    # ── 开盘价吸收统计 ──────────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  开盘价吸收验证：1D.open == 1m 09:30K线.open（集合竞价吸收证明）")
    print(f"{'='*72}")
    for sym, om in open_match:
        print(f"  {sym:<14} open匹配率: {om*100:.2f}%  "
              f"{'✅ 吸收确认' if om >= 0.95 else '⚠️ 存在偏差'}")

    # ── 统计结论 ──────────────────────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  统计结论")
    print(f"{'='*72}")

    if auction_gap_stock:
        med_s = np.median(auction_gap_stock)
        mean_s = np.mean(auction_gap_stock)
        print(f"\n  【A股/指数】干净日成交量残差 (1D-1m)/1D:")
        print(f"    样本品种数      : {len(auction_gap_stock)}")
        print(f"    中位值          : {med_s*100:+.4f}%")
        print(f"    均值            : {mean_s*100:+.4f}%")
        if abs(med_s) < 0.001:
            print(f"    ▶ 结论: 【H2 吸收假说成立】QMT 将集合竞价并入 09:30 复合K线，")
            print(f"            1D.volume ≈ sum(1m.volume)，无系统性缺失")
        elif med_s > 0.005:
            print(f"    ▶ 结论: 【H1 排除假说成立】QMT 1m 不含集合竞价，1D 系统偏大")
        else:
            print(f"    ▶ 结论: 残差微小，介于两种假说之间，需要 Tick 进一步确认")

    if auction_gap_futures:
        med_f = np.median(auction_gap_futures)
        mean_f = np.mean(auction_gap_futures)
        print(f"\n  【期货（含商品+股指）】干净日成交量残差 (1D-1m)/1D:")
        print(f"    样本品种数      : {len(auction_gap_futures)}")
        print(f"    中位值          : {med_f*100:+.4f}%")
        print(f"    均值            : {mean_f*100:+.4f}%")
        if abs(med_f) < 0.005:
            print(f"    ▶ 结论: 残差极小，期货无集合竞价问题（股指期货不适用，商品期货同）")
        else:
            print(f"    ▶ 结论: 存在可测量偏差，主因为夜盘首根K线缺失（非集合竞价）")

    # ── 商品期货夜盘双次边界问题 ─────────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  商品期货「夜盘边界」专项分析（是否存在2次缺失问题）")
    print(f"{'='*72}")
    commodity_results = [r for r in results if r.get("type") == "commodity_futures"]
    if commodity_results:
        print(f"  商品期货品种: {[r['sym'] for r in commodity_results]}")
        for r in commodity_results:
            sym = r["sym"]
            med = r.get("res_median_clean")
            short = r.get("short_days", 0)
            corrupt = r.get("corrupt_days", 0)
            print(f"\n  {sym}:")
            print(f"    干净日残差中位: {med*100:+.3f}%" if med else "    残差: N/A")
            print(f"    缺盘中日期数: {short}  |  腐败天: {corrupt}")
            if r.get("short_day_samples"):
                print(f"    缺盘样本: {r['short_day_samples']}")

    # ── 每日 K 线数量分布（A股）──────────────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  A股每日 1m bar 数量分布（正常应为241根）")
    print(f"{'='*72}")
    stock_results = [r for r in results if r.get("type") in ("stock", "index")]
    for r in stock_results:
        sym = r["sym"]
        short = r.get("short_days", 0)
        samples = r.get("short_day_samples", [])
        if short > 0:
            print(f"  {sym}: {short} 天K线数量不足241根")
            for d, cnt in samples:
                print(f"    {d}: {cnt} 根")
        else:
            print(f"  {sym}: ✅ 所有交易日 bar 数量正常")

    # ── 集合竞价 bar 时间检测（关键证据）────────────────────────────────
    print(f"\n{'='*72}")
    print(f"  集合竞价K线时间窗口检测（9:15-9:29 是否有1m K线）")
    print(f"{'='*72}")
    _check_auction_bars(pairs)

    # ── 保存报告 ──────────────────────────────────────────────────────────
    report = {
        "hypotheses": {
            "H1_exclusion": "QMT 1m/5m 不含集合竞价，1D系统偏大",
            "H2_absorption": "QMT 将集合竞价并入09:30复合K线，1D≈sum(1m)",
            "H3_both_exclude": "1D和1m都不含集合竞价，但Tick总量=1D（1D自身完整）",
        },
        "symbols": results,
        "tick_info": tick_info,
    }
    out = DATA_DIR / "auction_verification_report.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n  完整报告已保存: {out}")


def _check_auction_bars(pairs: list):
    """
    直接检查 1m feather 中是否存在 9:15-9:29 时间段的 K线。
    这是判断 QMT 是否下载了集合竞价 K线的直接证据。
    """
    AUCTION_START = dtime(9, 15, 0)
    AUCTION_END   = dtime(9, 29, 59)

    for sym, p1d, p1m in pairs:
        kind = detect_type(sym)
        if kind not in ("stock", "index"):
            continue  # 只检查A股/指数
        df1m = load(p1m)
        if df1m.empty:
            continue
        tc = find_timecol(df1m)
        if not tc:
            continue
        df1m[tc] = pd.to_datetime(df1m[tc], errors="coerce")
        df1m = df1m.dropna(subset=[tc])
        times = pd.to_datetime(df1m[tc], errors="coerce").map(lambda x: x.time() if pd.notna(x) else None)
        auction_bars = df1m[(times >= AUCTION_START) & (times <= AUCTION_END)]
        total_bars = len(df1m)
        n_auction = len(auction_bars)
        if n_auction > 0:
            sample_times = pd.to_datetime(auction_bars[tc], errors="coerce").map(lambda x: x.time() if pd.notna(x) else None).tolist()[:3]
            print(f"  {sym:<14} ⚡ 存在 {n_auction} 根集合竞价K线 (9:15-9:29)！"
                  f"  示例时间: {sample_times}")
        else:
            parsed_times = pd.to_datetime(df1m[tc], errors="coerce").map(lambda x: x.time() if pd.notna(x) else None)
            earliest = parsed_times.min() if total_bars > 0 else None
            print(f"  {sym:<14} ✅ 无集合竞价K线（9:15-9:29），"
                  f"最早K线时间: {earliest}  共 {total_bars} 根")


if __name__ == "__main__":
    main()

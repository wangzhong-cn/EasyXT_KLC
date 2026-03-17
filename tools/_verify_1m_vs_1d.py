"""验证 1m sum(volume) vs 1D volume，判断零成交量是否为合约薄交易还是数据腐败"""
import pandas as pd
from pathlib import Path

DATA = Path("data_export")

def verify_symbol(market, sym_file):
    p1d = DATA / market / f"{sym_file}_1d.feather"
    p1m = DATA / market / f"{sym_file}_1m.feather"
    if not p1d.exists() or not p1m.exists():
        print(f"  {sym_file}: 文件缺失, skip")
        return

    d1d = pd.read_feather(p1d)
    d1m = pd.read_feather(p1m)
    d1d["date"] = pd.to_datetime(d1d["date"])
    d1m["date"] = pd.to_datetime(d1m["date"])
    d1d["date_day"] = pd.DatetimeIndex(d1d["date"]).normalize()
    d1m["date_day"] = pd.DatetimeIndex(d1m["date"]).normalize()

    d1d_daily = d1d.set_index("date_day")
    vol_1m = d1m.groupby("date_day")["volume"].sum()
    vol_1d = d1d_daily["volume"]

    common = vol_1m.index.intersection(vol_1d.index)
    if len(common) == 0:
        print(f"  {sym_file}: 无重叠日期, skip")
        return

    m = pd.DataFrame({"v1d": vol_1d.reindex(common), "v1m": vol_1m.reindex(common)})
    m["diff"] = m["v1d"] - m["v1m"]
    m["diff_pct"] = (m["diff"] / m["v1d"].replace(0, float("nan")) * 100).round(2)

    exact = (m["diff"].abs() < 1).sum()
    missing = (m["diff"] > 1).sum()
    excess = (m["diff"] < -1).sum()
    median_pct = m["diff_pct"].median()

    print(f"  {sym_file}: {len(common)} days | "
          f"exact={exact} missing={missing} excess={excess} | "
          f"median_diff={median_pct:.2f}%")

    # 对"腐败"月份采样
    for month in ["2025-03", "2025-06", "2025-10", "2026-01", "2026-03"]:
        mask = [str(d).startswith(month) for d in m.index]
        sub = m.loc[mask]
        if len(sub) > 0:
            ex = (sub["diff"].abs() < 1).sum()
            med = sub["diff_pct"].median()
            print(f"    {month}: {len(sub)} days, exact={ex}/{len(sub)}, median_diff={med:.2f}%")


if __name__ == "__main__":
    # 商品期货（重灾区）
    print("=== 商品期货 1m vs 1D volume ===")
    for market, sym in [
        ("SF", "al01_SF"), ("SF", "cu01_SF"), ("SF", "rb01_SF"),
        ("SF", "ag01_SF"), ("SF", "au01_SF"),
        ("DF", "c01_DF"), ("DF", "p01_DF"), ("DF", "i01_DF"),
        ("DF", "v01_DF"), ("DF", "m01_DF"),
        ("ZF", "TA01_ZF"),
    ]:
        verify_symbol(market, sym)

    print()
    print("=== 股指期货 1m vs 1D volume ===")
    for sym in ["IF01_IF", "IC01_IF", "IH01_IF", "IM01_IF"]:
        verify_symbol("IF", sym)

    print()
    print("=== A股/指数 1m vs 1D volume ===")
    for market, sym in [
        ("SH", "600000_SH"), ("SZ", "000001_SZ"),
        ("SH", "000300_SH"), ("SH", "000905_SH"), ("SZ", "399001_SZ"),
    ]:
        verify_symbol(market, sym)

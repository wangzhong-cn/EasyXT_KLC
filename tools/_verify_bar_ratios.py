"""首根bar volume比率验证"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import time as dtime

DATA = Path("data_export")

print("=== A股 09:30首根bar vs 09:31 volume比值 ===")
for mkt, stem in [("SZ", "000001_SZ"), ("SH", "600000_SH"), ("SH", "000300_SH")]:
    df = pd.read_feather(DATA / mkt / f"{stem}_1m.feather")
    df["date"] = pd.to_datetime(df["date"])
    date_series = pd.to_datetime(df["date"])
    df["_d"] = date_series.map(lambda x: x.date())
    df["_t"] = date_series.map(lambda x: x.time())

    bar_0930 = df[df["_t"] == dtime(9, 30)].copy()
    bar_0931 = df[df["_t"] == dtime(9, 31)].copy()
    bar_0930["_dk"] = pd.to_datetime(bar_0930["date"]).map(lambda x: x.date())
    bar_0931["_dk"] = pd.to_datetime(bar_0931["date"]).map(lambda x: x.date())
    bar_0930 = bar_0930.set_index("_dk")
    bar_0931 = bar_0931.set_index("_dk")

    common = bar_0930.index.intersection(bar_0931.index)
    if len(common) > 0:
        v30 = np.asarray(bar_0930.loc[common, "volume"], dtype=float)
        v31 = np.asarray(bar_0931.loc[common, "volume"], dtype=float)
        ratio = v30 / (v31 + 1e-10)
        print(f"  {stem}: 09:30/09:31 ratio median={pd.Series(ratio).median():.2f}x, "
              f"mean={pd.Series(ratio).mean():.2f}x, days={len(common)}")

print()
print("=== 商品期货 09:01/09:02 和 21:01/21:02 volume比值 ===")
for mkt, stem in [("SF", "cu01_SF"), ("DF", "c01_DF"), ("SF", "rb01_SF")]:
    df = pd.read_feather(DATA / mkt / f"{stem}_1m.feather")
    df["date"] = pd.to_datetime(df["date"])
    date_series = pd.to_datetime(df["date"])
    df["_d"] = date_series.map(lambda x: x.date())
    df["_t"] = date_series.map(lambda x: x.time())

    for t1, t2, label in [(dtime(9,1), dtime(9,2), "09:01/09:02"),
                           (dtime(21,1), dtime(21,2), "21:01/21:02")]:
        b1 = df[df["_t"] == t1].copy()
        b2 = df[df["_t"] == t2].copy()
        b1["_dk"] = pd.to_datetime(b1["date"]).map(lambda x: x.date())
        b2["_dk"] = pd.to_datetime(b2["date"]).map(lambda x: x.date())
        b1 = b1.set_index("_dk")
        b2 = b2.set_index("_dk")
        common = b1.index.intersection(b2.index)
        if len(common) > 5:
            v1_vals = np.asarray(b1.loc[common, "volume"], dtype=float)
            v2_vals = np.asarray(b2.loc[common, "volume"], dtype=float)
            mask = v2_vals > 0
            if mask.sum() > 0:
                ratio = v1_vals[mask] / v2_vals[mask]
                print(f"  {stem} {label}: ratio median={pd.Series(ratio).median():.2f}x, "
                      f"mean={pd.Series(ratio).mean():.2f}x, days={mask.sum()}")

print()
print("=== A股收盘集合竞价 14:56-15:00 volume分布 ===")
for mkt, stem in [("SZ", "000001_SZ"), ("SH", "600000_SH")]:
    df = pd.read_feather(DATA / mkt / f"{stem}_1m.feather")
    df["date"] = pd.to_datetime(df["date"])
    df["_t"] = pd.to_datetime(df["date"]).map(lambda x: x.time())
    print(f"  {stem}:")
    for t in [dtime(14,56), dtime(14,57), dtime(14,58), dtime(14,59), dtime(15,0)]:
        bars = df[df["_t"] == t]
        if not bars.empty:
            med = bars["volume"].median()
            print(f"    {t}: median_vol={med:.0f}, days={len(bars)}")

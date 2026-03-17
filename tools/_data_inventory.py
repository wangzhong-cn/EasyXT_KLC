"""
Comprehensive inventory of ALL available data sources for cross-validation.
"""
import os, sys, json
from pathlib import Path
from collections import defaultdict

print("=" * 80)
print("  COMPREHENSIVE DATA SOURCE INVENTORY")
print("=" * 80)

# === 1. QMT Local DAT Files ===
QMT_BASE = Path(r"D:\申万宏源策略量化交易终端\userdata_mini\datadir")
PERIOD_MAP = {"0": "tick", "60": "1m", "300": "5m", "86400": "1d"}
MARKETS = ["SH", "SZ", "SF", "DF", "IF", "ZF", "HK"]

print("\n### 1. QMT Local DAT Files ###")
qmt_inventory = {}
total_dat = 0
for mkt in MARKETS:
    mkt_dir = QMT_BASE / mkt
    if not mkt_dir.exists():
        continue
    for period_dir in sorted(mkt_dir.iterdir()):
        if not period_dir.is_dir():
            continue
        pname = PERIOD_MAP.get(period_dir.name, period_dir.name)
        dats = list(period_dir.glob("*.DAT"))
        if not dats:
            continue
        codes = [d.stem for d in dats]
        key = f"{mkt}/{pname}"
        qmt_inventory[key] = {
            "count": len(codes),
            "size_mb": sum(d.stat().st_size for d in dats) / 1024 / 1024,
            "samples": sorted(codes)[:10],
        }
        total_dat += len(codes)
        print(f"  {key:20s}: {len(codes):5d} files  ({qmt_inventory[key]['size_mb']:.1f} MB)  samples: {sorted(codes)[:5]}")

print(f"\n  TOTAL QMT DAT files: {total_dat}")

# === 2. Feather Exports ===
FEATHER_DIR = Path(r"D:\EasyXT_KLC\data_export")
print("\n### 2. Feather Export Files ###")
feather_inv = defaultdict(lambda: defaultdict(int))
total_feather = 0
if FEATHER_DIR.exists():
    for mkt_dir in sorted(FEATHER_DIR.iterdir()):
        if not mkt_dir.is_dir():
            continue
        mkt = mkt_dir.name
        feathers = list(mkt_dir.glob("*.feather"))
        for f in feathers:
            parts = f.stem.rsplit("_", 1)
            if len(parts) == 2:
                period = parts[1]
                feather_inv[mkt][period] += 1
                total_feather += 1
        if feathers:
            print(f"  {mkt}: {len(feathers)} files — periods: {dict(feather_inv[mkt])}")

print(f"\n  TOTAL Feather files: {total_feather}")

# === 3. QMT API — Check connection and list available symbols ===
print("\n### 3. QMT API Connection ###")
try:
    import xtquant.xtdata as xtdata
    xtdata.enable_hello = False

    # Get all sector stock lists
    sectors = {
        "沪深A股": "沪深A股",
        "上证A股": "上证A股",
        "深证A股": "深证A股",
        "科创板": "科创板",
        "创业板": "创业板",
        "北交所": "北交所",
        "沪深300": "沪深300",
        "中证500": "中证500",
        "中证1000": "中证1000",
    }

    for name, sector in sectors.items():
        try:
            stocks = xtdata.get_stock_list_in_sector(sector)
            print(f"  {name}: {len(stocks)} symbols")
        except:
            print(f"  {name}: FAILED")

    # Futures
    for fut_sector in ["上海期货", "郑州期货", "大连期货", "中金期货"]:
        try:
            f = xtdata.get_stock_list_in_sector(fut_sector)
            print(f"  {fut_sector}: {len(f)} contracts  samples: {sorted(f)[:5]}")
        except:
            print(f"  {fut_sector}: FAILED")

    # ETF
    for etf_sector in ["沪市ETF", "深市ETF"]:
        try:
            e = xtdata.get_stock_list_in_sector(etf_sector)
            print(f"  {etf_sector}: {len(e)} funds")
        except:
            print(f"  {etf_sector}: FAILED")

    QMT_OK = True
except Exception as e:
    print(f"  QMT FAILED: {e}")
    QMT_OK = False

# === 4. akshare availability ===
print("\n### 4. akshare Module ###")
try:
    import akshare as ak
    print(f"  akshare version: {ak.__version__}")
    print("  Available: YES")
except ImportError:
    print("  akshare: NOT INSTALLED")

# === 5. qstock availability ===
print("\n### 5. qstock Module ###")
try:
    import qstock
    print(f"  qstock: AVAILABLE")
except ImportError:
    print("  qstock: NOT INSTALLED")

# === 6. QMT Local DAT — Full symbol list for 1m and 1d ===
print("\n### 6. Full QMT 1m Symbol List by Market ###")
for mkt in MARKETS:
    p1m = QMT_BASE / mkt / "60"
    p1d = QMT_BASE / mkt / "86400"
    if p1m.exists():
        codes_1m = sorted(d.stem for d in p1m.glob("*.DAT"))
        print(f"  {mkt}/1m: {len(codes_1m)} symbols")
    if p1d.exists():
        codes_1d = sorted(d.stem for d in p1d.glob("*.DAT"))
        print(f"  {mkt}/1d: {len(codes_1d)} symbols")
    # Tick
    ptick = QMT_BASE / mkt / "0"
    if ptick.exists():
        codes_tick = sorted(d.stem for d in ptick.glob("*.DAT"))
        print(f"  {mkt}/tick: {len(codes_tick)} symbols")

# === 7. Summary ===
print("\n" + "=" * 80)
print("  SUMMARY")
print("=" * 80)
print(f"  QMT DAT files total: {total_dat}")
print(f"  Feather files total: {total_feather}")
print(f"  DuckDB: EMPTY (schema only)")
print(f"  QMT API: {'CONNECTED' if QMT_OK else 'DISCONNECTED'}")

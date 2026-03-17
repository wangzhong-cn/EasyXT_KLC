"""Diagnose volume residual issue in v4"""
import struct, sys
from pathlib import Path
from datetime import datetime
import numpy as np
import pandas as pd

QMT_BASE = Path(r"D:\申万宏源策略量化交易终端\userdata_mini\datadir")
RECORD_SIZE = 64
HEADER_SIZE = 8

def read_dat_raw(dat_path, max_records=None):
    """Read DAT with all records, returning raw data"""
    fsize = dat_path.stat().st_size
    n_records = (fsize - HEADER_SIZE) // RECORD_SIZE
    if max_records:
        n_records = min(n_records, max_records)

    with open(dat_path, 'rb') as f:
        f.seek(HEADER_SIZE)
        data = f.read(n_records * RECORD_SIZE)

    dt = np.dtype([
        ('ts', '<u4'), ('open', '<u4'), ('high', '<u4'), ('low', '<u4'),
        ('close', '<u4'), ('pad', '<u4'), ('volume', '<u4'),
        ('rest', 'V36')
    ])
    return np.frombuffer(data[:n_records * RECORD_SIZE], dtype=dt)

# === Test with 600519.SH ===
sym = "600519.SH"
mkt, name = "SH", "600519"

# Read 1d
arr_1d = read_dat_raw(QMT_BASE / mkt / "86400" / f"{name}.DAT")
print(f"=== {sym} 1D ===")
print(f"  Total records: {len(arr_1d)}")

# Check timestamp format
last5_1d = arr_1d[-5:]
print("  Last 5 days:")
for r in last5_1d:
    ts_utc = datetime.utcfromtimestamp(r['ts'])
    ts_local = datetime.fromtimestamp(r['ts'])
    print(f"    ts={r['ts']}  UTC={ts_utc}  local={ts_local}  vol={r['volume']}")

# Read 1m
arr_1m = read_dat_raw(QMT_BASE / mkt / "60" / f"{name}.DAT")
print(f"\n=== {sym} 1M ===")
print(f"  Total records: {len(arr_1m)}")

# Get bars for 2026-03-06 (1d timestamp = 1772726400)
target_date = datetime(2026, 3, 6).date()
print(f"\n  Bars on {target_date}:")
bars_today = []
for r in arr_1m:
    if r['volume'] == 0 and r['open'] == 0:
        continue
    ts_local = datetime.fromtimestamp(r['ts'])
    if ts_local.date() == target_date:
        bars_today.append(r)
        if len(bars_today) <= 5 or ts_local.hour >= 14:
            print(f"    {ts_local.strftime('%H:%M:%S')} O={r['open']/1000:.3f} H={r['high']/1000:.3f} "
                  f"L={r['low']/1000:.3f} C={r['close']/1000:.3f} vol={r['volume']}")

print(f"  Total bars: {len(bars_today)}")
total_vol = sum(r['volume'] for r in bars_today)
print(f"  Sum 1m volume: {total_vol}")

# 1d volume for same day
for r in arr_1d:
    ts_local = datetime.fromtimestamp(r['ts'])
    if ts_local.date() == target_date:
        print(f"  1D volume:     {r['volume']}")
        print(f"  Difference:    {total_vol - r['volume']} ({(total_vol - r['volume'])/r['volume']*100:.4f}%)")
        break

# Now check: what does pd.to_datetime give us? (UTC vs local)
print(f"\n=== Timezone check ===")
test_ts = arr_1m[-1]['ts']
print(f"  Raw timestamp: {test_ts}")
print(f"  datetime.fromtimestamp: {datetime.fromtimestamp(test_ts)}")
print(f"  datetime.utcfromtimestamp: {datetime.utcfromtimestamp(test_ts)}")
print(f"  pd.to_datetime(unit='s'): {pd.to_datetime(test_ts, unit='s')}")
print(f"  pd.to_datetime with UTC+8: {pd.to_datetime(test_ts + 8*3600, unit='s')}")

# Check how many bars fall on 2026-03-06 using pd.to_datetime
dates_pd = pd.to_datetime(arr_1m['ts'][arr_1m['volume'] > 0].astype(np.int64), unit='s')
mask_pd = dates_pd.normalize() == pd.Timestamp('2026-03-06')
print(f"\n  pd.to_datetime (UTC) bars on 2026-03-06: {mask_pd.sum()}")

dates_local = pd.to_datetime((arr_1m['ts'][arr_1m['volume'] > 0].astype(np.int64) + 8*3600), unit='s')
mask_local = dates_local.normalize() == pd.Timestamp('2026-03-06')
print(f"  pd.to_datetime (UTC+8) bars on 2026-03-06: {mask_local.sum()}")

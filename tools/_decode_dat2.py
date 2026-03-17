"""Decode and compare DAT vs API for volume verification"""
import struct, sys
sys.path.insert(0, r'D:\EasyXT_KLC')
import xtquant.xtdata as xt
xt.enable_hello = False
from datetime import datetime

# === Get API data ===
d = xt.get_local_data(field_list=[], stock_list=['600519.SH'], period='1d', start_time='20260301')
api = d['600519.SH']
print("=== API Volume & Amount ===")
for idx, row in api.iterrows():
    print(f"  {idx}: vol={row['volume']:.0f}  amt={row['amount']:.0f}")

# === Read DAT volume ===
print("\n=== DAT Volume (offset 24-27 as uint32) ===")
dat_path = r'D:\申万宏源策略量化交易终端\userdata_mini\datadir\SH\86400\600519.DAT'
with open(dat_path, 'rb') as f:
    raw = f.read()

n_records = (len(raw) - 8) // 64
for i in range(5):
    idx = n_records - 5 + i
    offset = 8 + idx * 64
    rec = raw[offset:offset+64]
    ts = struct.unpack('<I', rec[0:4])[0]
    vol = struct.unpack('<I', rec[24:28])[0]
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d')
    print(f"  {dt}: vol_dat={vol}")

# === Check 1m DAT format ===
print("\n=== 1m DAT Exploration ===")
dat_1m = r'D:\申万宏源策略量化交易终端\userdata_mini\datadir\SH\60\600519.DAT'
with open(dat_1m, 'rb') as f:
    raw_1m = f.read()

print(f"  1m file size: {len(raw_1m)} bytes")
# Try 64-byte records
if (len(raw_1m) - 8) % 64 == 0:
    n = (len(raw_1m) - 8) // 64
    print(f"  64-byte records: {n} (header=8)")
# Try other sizes
for rs in [32, 36, 40, 44, 48, 52, 56, 60, 64, 68]:
    for hdr in [0, 4, 8, 12, 16]:
        if (len(raw_1m) - hdr) % rs == 0:
            n = (len(raw_1m) - hdr) // rs
            print(f"  rs={rs} hdr={hdr}: {n} records")

# Check last few 64-byte "records" of 1m
print("\n=== 1m last 3 records (assuming 64-byte) ===")
n = (len(raw_1m) - 8) // 64
for i in range(3):
    idx = n - 3 + i
    offset = 8 + idx * 64
    rec = raw_1m[offset:offset+64]
    ts = struct.unpack('<I', rec[0:4])[0]
    o = struct.unpack('<I', rec[4:8])[0]
    h = struct.unpack('<I', rec[8:12])[0]
    l = struct.unpack('<I', rec[12:16])[0]
    c = struct.unpack('<I', rec[16:20])[0]
    vol = struct.unpack('<I', rec[24:28])[0]

    dt = datetime.fromtimestamp(ts)
    print(f"  {dt} O={o/1000:.3f} H={h/1000:.3f} L={l/1000:.3f} C={c/1000:.3f} vol={vol}")

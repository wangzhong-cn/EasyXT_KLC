"""Quick DAT format decoder using API data as reference"""
import struct, sys
sys.path.insert(0, r'D:\EasyXT_KLC')
import xtquant.xtdata as xt
xt.enable_hello = False
from datetime import datetime

# Get API data for reference
d = xt.get_local_data(field_list=[], stock_list=['600519.SH'], period='1d', start_time='20260301')
api = d['600519.SH']
print("API last 5 days:")
print(api[['open','high','low','close','volume','amount','openInterest','preClose','suspendFlag']])
print()

# Read DAT file
dat_path = r'D:\申万宏源策略量化交易终端\userdata_mini\datadir\SH\86400\600519.DAT'
with open(dat_path, 'rb') as f:
    raw = f.read()

file_size = len(raw)
n_records = (file_size - 8) // 64
print(f"File: {file_size} bytes, header=8, records={n_records}, remainder={(file_size-8)%64}")
print()

# Read last 5 records
for i in range(5):
    idx = n_records - 5 + i
    offset = 8 + idx * 64
    rec = raw[offset:offset+64]

    ts = struct.unpack('<I', rec[0:4])[0]
    o = struct.unpack('<I', rec[4:8])[0] / 1000
    h = struct.unpack('<I', rec[8:12])[0] / 1000
    l = struct.unpack('<I', rec[12:16])[0] / 1000
    c = struct.unpack('<I', rec[16:20])[0] / 1000

    # Try all remaining fields
    vals = []
    for j in range(20, 64, 4):
        u = struct.unpack('<I', rec[j:j+4])[0]
        f = struct.unpack('<f', rec[j:j+4])[0]
        vals.append((j, u, f))

    dt = datetime.fromtimestamp(ts)

    print(f"Rec {idx}: {dt.strftime('%Y%m%d')} O={o:.3f} H={h:.3f} L={l:.3f} C={c:.3f}")
    for j, u, f in vals:
        note = ""
        # volume might be int64
        if j == 20:
            v64 = struct.unpack('<q', rec[20:28])[0]
            note = f" (i64={v64})"
        if j == 24:
            a64 = struct.unpack('<q', rec[24:32])[0]
            note = f" (i64_amt?={a64})"
        if j == 28:
            a64 = struct.unpack('<q', rec[28:36])[0]
            note = f" (i64_28-35={a64})"
        if 100 < abs(f) < 100000:
            note += f" FLOAT={f:.4f}"
        if 1300000 < u < 1500000:
            note += f" PRICE={u/1000:.3f}"
        print(f"  [{j:2d}] u32={u:15d} f32={f:18.6f}{note}")
    print()

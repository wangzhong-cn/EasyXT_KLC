"""临时验证脚本：确认A股1m数据缺少集合竞价，期货/指数不缺失"""
import pandas as pd
import numpy as np
from pathlib import Path

DATA = Path("data_export")

def check_symbol(sym_label, path_1m, path_1d):
    df1m = pd.read_feather(path_1m)
    df1d = pd.read_feather(path_1d)
    df1m['date'] = pd.to_datetime(df1m['date'].astype(str))
    df1d['date'] = pd.to_datetime(df1d['date'].astype(str))
    df1d = df1d.set_index('date')
    df1d.index = pd.DatetimeIndex(df1d.index).normalize()

    # 取最近5个交易日
    recent_days = sorted(pd.to_datetime(df1m['date'], errors='coerce').map(lambda x: x.date()).unique())[-5:]
    print(f"\n{'='*60}")
    print(f"品种: {sym_label}")
    print(f"{'日期':12s} {'bars':>5} {'first_bar':>8} {'last_bar':>8} {'1D_vol':>12} {'1m_sum':>12} {'diff':>10} {'auction%':>9}")
    for d in recent_days:
        d_ts = pd.Timestamp(d)
        day = df1m[pd.to_datetime(df1m['date'], errors='coerce').map(lambda x: x.date()) == d].sort_values('date')
        if len(day) == 0:
            continue
        first_t = day.iloc[0]['date'].strftime('%H:%M')
        last_t = day.iloc[-1]['date'].strftime('%H:%M')
        sum_1m = day['volume'].sum()
        v1d_raw = df1d.loc[d_ts, 'volume'] if d_ts in df1d.index else None
        v1d = float(np.asarray(v1d_raw).reshape(-1)[0]) if v1d_raw is not None else None
        if v1d and v1d > 0:
            diff = v1d - sum_1m
            pct = diff / v1d * 100
            print(f"{str(d):12s} {len(day):>5} {first_t:>8} {last_t:>8} {int(v1d):>12,} {int(sum_1m):>12,} {int(diff):>10,} {pct:>8.1f}%")

# A股
check_symbol("600000.SH (浦发银行)", DATA/"SH/600000_SH_1m.feather", DATA/"SH/600000_SH_1d.feather")
check_symbol("000001.SZ (平安银行)", DATA/"SZ/000001_SZ_1m.feather", DATA/"SZ/000001_SZ_1d.feather")

# 沪深300指数
check_symbol("000300.SH (沪深300指数)", DATA/"SH/000300_SH_1m.feather", DATA/"SH/000300_SH_1d.feather")

# 股指期货
check_symbol("IF01.IF (沪深300股指期货)", DATA/"IF/IF01_IF_1m.feather", DATA/"IF/IF01_IF_1d.feather")

# 商品期货
check_symbol("cu01.SF (铜期货)", DATA/"SF/cu01_SF_1m.feather", DATA/"SF/cu01_SF_1d.feather")

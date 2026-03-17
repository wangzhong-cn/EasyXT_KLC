from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data_export"
REPORT_PATH = DATA_DIR / "final_gold_report.json"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _pick_symbols(qdm: Any, xtdata: Any, target_count: int) -> list[str]:
    symbols: list[str] = []
    symbols.extend(qdm.STOCK_SAMPLES)
    symbols.extend(qdm.FUTURES_SAMPLES)
    dynamic = max(target_count // 6, 20)
    for mkt, limit in [
        ("SH", dynamic),
        ("SZ", dynamic),
        ("BJ", max(dynamic // 3, 10)),
        ("IF", max(dynamic // 2, 10)),
        ("SF", max(dynamic // 2, 10)),
        ("DF", max(dynamic // 2, 10)),
        ("ZF", max(dynamic // 3, 8)),
        ("INE", max(dynamic // 4, 6)),
        ("GF", max(dynamic // 4, 6)),
    ]:
        try:
            lst = xtdata.get_stock_list_in_sector(mkt) or []
            symbols.extend(lst[:limit])
        except Exception:
            continue
    uniq: list[str] = []
    seen = set()
    for s in symbols:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq[: max(target_count, 50)]


def _safe_read(path: Path) -> pd.DataFrame:
    try:
        return pd.read_feather(path)
    except Exception:
        return pd.DataFrame()


def _find_time_col(df: pd.DataFrame) -> str | None:
    for col in ("date", "time", "datetime"):
        if col in df.columns:
            return col
    return None


def _aggregate_to_daily(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    out = out.dropna(subset=[time_col])
    if out.empty:
        return pd.DataFrame()
    out["_date"] = pd.DatetimeIndex(out[time_col]).normalize()
    agg = {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    cols = {k: v for k, v in agg.items() if k in out.columns}
    if not cols:
        return pd.DataFrame()
    return out.groupby("_date").agg(cols)


def _cmp_daily(df1d: pd.DataFrame, dfx: pd.DataFrame) -> dict[str, float]:
    if df1d.empty or dfx.empty:
        return {}
    t1 = _find_time_col(df1d)
    if not t1:
        return {}
    a = df1d.copy()
    a[t1] = pd.to_datetime(a[t1], errors="coerce")
    a = a.dropna(subset=[t1]).set_index(t1)
    a.index = pd.DatetimeIndex(a.index).normalize()
    common = a.index.intersection(dfx.index)
    if len(common) == 0:
        return {}
    out: dict[str, float] = {}
    for f in ("open", "high", "low", "close", "volume"):
        if f not in a.columns or f not in dfx.columns:
            continue
        v1 = a.loc[common, f].astype(float)
        v2 = dfx.loc[common, f].astype(float)
        rel = ((v1 - v2).abs() / v1.abs().clip(lower=1.0)).fillna(0.0)
        out[f"{f}_median_rel_err"] = float(rel.median())
    out["common_days"] = float(len(common))
    return out


def _auction_presence(df: pd.DataFrame, time_col: str, windows: list[tuple[dtime, dtime]]) -> dict[str, int]:
    x = df.copy()
    x[time_col] = pd.to_datetime(x[time_col], errors="coerce")
    x = x.dropna(subset=[time_col])
    ts = pd.to_datetime(x[time_col], errors="coerce").map(lambda v: v.time() if pd.notna(v) else None)
    out: dict[str, int] = {}
    for idx, (st, ed) in enumerate(windows, 1):
        c = int(((ts >= st) & (ts <= ed)).sum())
        out[f"window_{idx}"] = c
    return out


def run(sample_count: int) -> dict[str, Any]:
    qdm = _load_module(PROJECT_ROOT / "tools" / "qmt_data_manager.py", "qdm")
    cc = _load_module(PROJECT_ROOT / "tools" / "consistency_checker.py", "cc")
    import xtquant.xtdata as xtdata

    symbols = _pick_symbols(qdm, xtdata, sample_count)
    out_dir = str(DATA_DIR)
    batch_size = 40
    for period in ("1d", "1m", "5m"):
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i : i + batch_size]
            qdm._save_market_data(xtdata, batch, period, "20200101", out_dir, incremental=True)

    report_1m = cc.run(DATA_DIR, DATA_DIR / "consistency_report_1m_final.json", 0)

    symbol_results: list[dict[str, Any]] = []
    for sym in symbols:
        safe = sym.replace(".", "_")
        mkt = sym.split(".")[-1]
        base = DATA_DIR / mkt
        p1d = base / f"{safe}_1d.feather"
        p1m = base / f"{safe}_1m.feather"
        p5m = base / f"{safe}_5m.feather"
        d1 = _safe_read(p1d)
        m1 = _safe_read(p1m)
        m5 = _safe_read(p5m)
        tc1 = _find_time_col(m1)
        tc5 = _find_time_col(m5)
        cmp_1m = _cmp_daily(d1, _aggregate_to_daily(m1, tc1)) if tc1 else {}
        cmp_5m = _cmp_daily(d1, _aggregate_to_daily(m5, tc5)) if tc5 else {}
        auction_a = _auction_presence(m1, tc1, [(dtime(9, 15), dtime(9, 29, 59))]) if tc1 else {}
        auction_f = _auction_presence(
            m1,
            tc1,
            [(dtime(8, 55), dtime(8, 59, 59)), (dtime(20, 55), dtime(20, 59, 59))],
        ) if tc1 else {}
        symbol_results.append(
            {
                "symbol": sym,
                "has_1d": not d1.empty,
                "has_1m": not m1.empty,
                "has_5m": not m5.empty,
                "cmp_1m_vs_1d": cmp_1m,
                "cmp_5m_vs_1d": cmp_5m,
                "auction_window_a_share": auction_a,
                "auction_window_futures": auction_f,
            }
        )

    tick_path = DATA_DIR / "tick_snapshot.feather"
    tick_rows = 0
    if tick_path.exists():
        try:
            tick_rows = int(len(pd.read_feather(tick_path)))
        except Exception:
            tick_rows = 0

    has_1d = sum(1 for x in symbol_results if x["has_1d"])
    has_1m = sum(1 for x in symbol_results if x["has_1m"])
    has_5m = sum(1 for x in symbol_results if x["has_5m"])

    sources = {"qmt": False, "qstock": False, "akshare": False}
    try:
        tsm = _load_module(PROJECT_ROOT / "easy_xt" / "triple_source_manager.py", "tsm")
        mgr = tsm.TripleSourceDataManager()
        sources = dict(mgr.sources)
    except Exception:
        pass

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "sample_count_requested": sample_count,
        "sample_count_actual": len(symbols),
        "coverage_1d": has_1d,
        "coverage_1m": has_1m,
        "coverage_5m": has_5m,
        "consistency_1m_summary": report_1m.get("summary", {}),
        "corrupt_symbols_1m": report_1m.get("corrupt_symbols", []),
        "tick_snapshot_rows": tick_rows,
        "multi_source_availability": sources,
        "release_gate": {
            "strict_pass": int(report_1m.get("summary", {}).get("fail", 0)) == 0,
            "tick_history_ready": tick_rows >= len(symbols),
        },
    }

    final = {
        "summary": summary,
        "symbols": symbol_results,
        "gold_rules": [
            "1D 作为结算黄金标准",
            "Tick 仅在完整历史可得时作为最终审计标准",
            "1m/5m 作为结构层标准，不可替代 1D",
            "A股/指数与期货必须分规则校验",
            "strict 门禁不过，不允许策略推进",
        ],
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"final_report={REPORT_PATH}")
    print(f"samples={len(symbols)} tick_rows={tick_rows}")
    print(f"summary={summary['consistency_1m_summary']}")
    return final


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-count", type=int, default=50)
    args = parser.parse_args()
    run(args.sample_count)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_manager.data_contract_validator import DataContractValidator
from data_manager.unified_data_interface import UnifiedDataInterface


def _default_config() -> dict[str, Any]:
    return {
        "symbols": ["000001.SZ", "000300.SH", "510300.SH"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "period": "1d",
        "thresholds": {
            "close_rel_p95_max": 0.02,
            "volume_rel_p95_max": 0.35,
            "overlap_ratio_min": 0.90,
        },
    }


def _load_config(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return _default_config()
    return json.loads(p.read_text(encoding="utf-8"))


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "datetime" in out.columns:
        out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
        out = out[out["datetime"].notna()]
        out = out.set_index("datetime", drop=False)
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[out.index.notna()].sort_index()
    if "datetime" not in out.columns:
        out["datetime"] = out.index
    return out


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lower_map = {str(c).lower(): str(c) for c in df.columns}
    for n in names:
        if n.lower() in lower_map:
            return lower_map[n.lower()]
    return None


def _source_metrics(
    symbol: str,
    source_name: str,
    df: pd.DataFrame,
    validator: DataContractValidator,
) -> dict[str, Any]:
    norm = _ensure_datetime_index(df)
    result = validator.validate(norm, symbol=symbol, source=source_name)
    return {
        "source": source_name,
        "rows": int(len(norm)),
        "contract": result.to_dict(),
        "date_min": None if norm.empty else str(norm.index.min()),
        "date_max": None if norm.empty else str(norm.index.max()),
    }


def _reconcile_pair(
    symbol: str,
    qmt_df: pd.DataFrame,
    ak_df: pd.DataFrame,
    thresholds: dict[str, float],
) -> dict[str, Any]:
    qmt = _ensure_datetime_index(qmt_df)
    ak = _ensure_datetime_index(ak_df)

    q_close = _pick_col(qmt, ["close"])
    a_close = _pick_col(ak, ["close"])
    q_vol = _pick_col(qmt, ["volume", "vol"])
    a_vol = _pick_col(ak, ["volume", "vol"])

    overlap = qmt.join(ak, how="inner", lsuffix="_qmt", rsuffix="_ak")
    overlap_rows = int(len(overlap))
    base_rows = int(max(1, min(len(qmt), len(ak))))
    overlap_ratio = overlap_rows / base_rows

    close_rel_p95 = None
    close_rel_max = None
    volume_rel_p95 = None

    if overlap_rows > 0 and q_close and a_close:
        left = overlap[f"{q_close}_qmt"] if f"{q_close}_qmt" in overlap.columns else overlap[q_close]
        right = overlap[f"{a_close}_ak"] if f"{a_close}_ak" in overlap.columns else overlap[a_close]
        denom = left.abs().clip(lower=1e-12)
        close_rel = (left - right).abs() / denom
        close_rel_p95 = float(close_rel.quantile(0.95))
        close_rel_max = float(close_rel.max())

    if overlap_rows > 0 and q_vol and a_vol:
        left_v = overlap[f"{q_vol}_qmt"] if f"{q_vol}_qmt" in overlap.columns else overlap[q_vol]
        right_v = overlap[f"{a_vol}_ak"] if f"{a_vol}_ak" in overlap.columns else overlap[a_vol]
        denom_v = left_v.abs().clip(lower=1.0)
        volume_rel = (left_v - right_v).abs() / denom_v
        volume_rel_p95 = float(volume_rel.quantile(0.95))

    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "both_sources_non_empty",
            "pass": (not qmt.empty) and (not ak.empty),
            "value": {"qmt_rows": int(len(qmt)), "ak_rows": int(len(ak))},
        }
    )
    checks.append(
        {
            "name": "overlap_ratio",
            "pass": overlap_ratio >= float(thresholds["overlap_ratio_min"]),
            "value": round(overlap_ratio, 4),
            "threshold": thresholds["overlap_ratio_min"],
        }
    )
    if close_rel_p95 is not None:
        checks.append(
            {
                "name": "close_rel_p95",
                "pass": close_rel_p95 <= float(thresholds["close_rel_p95_max"]),
                "value": round(close_rel_p95, 6),
                "threshold": thresholds["close_rel_p95_max"],
            }
        )
    if volume_rel_p95 is not None:
        checks.append(
            {
                "name": "volume_rel_p95",
                "pass": volume_rel_p95 <= float(thresholds["volume_rel_p95_max"]),
                "value": round(volume_rel_p95, 6),
                "threshold": thresholds["volume_rel_p95_max"],
            }
        )

    failed = [c for c in checks if not c["pass"]]
    return {
        "symbol": symbol,
        "qmt_rows": int(len(qmt)),
        "ak_rows": int(len(ak)),
        "overlap_rows": overlap_rows,
        "overlap_ratio": round(overlap_ratio, 6),
        "close_rel_p95": close_rel_p95,
        "close_rel_max": close_rel_max,
        "volume_rel_p95": volume_rel_p95,
        "checks": checks,
        "pass_reconciliation": len(failed) == 0,
        "failed_checks": failed,
    }


def _write_markdown(report: dict[str, Any], out_path: Path) -> None:
    lines: list[str] = []
    lines.append("# 双源离线对账周报")
    lines.append("")
    lines.append(f"- 生成时间: {report['generated_at']}")
    lines.append(f"- 区间: {report['start_date']} ~ {report['end_date']}")
    lines.append(f"- 周期: {report['period']}")
    lines.append(f"- 总标的: {report['summary']['total_symbols']}")
    lines.append(f"- 通过标的: {report['summary']['passed_symbols']}")
    lines.append(f"- 失败标的: {report['summary']['failed_symbols']}")
    lines.append("")
    lines.append("## 标的明细")
    lines.append("")
    lines.append("| Symbol | QMT Rows | AK Rows | Overlap | Close P95 | Volume P95 | Pass |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    for item in report["results"]:
        close_p95 = "-" if item["close_rel_p95"] is None else f"{item['close_rel_p95']:.6f}"
        vol_p95 = "-" if item["volume_rel_p95"] is None else f"{item['volume_rel_p95']:.6f}"
        lines.append(
            f"| {item['symbol']} | {item['qmt_rows']} | {item['ak_rows']} | "
            f"{item['overlap_ratio']:.4f} | {close_p95} | {vol_p95} | "
            f"{'PASS' if item['pass_reconciliation'] else 'FAIL'} |"
        )
    lines.append("")
    lines.append("## 失败项")
    lines.append("")
    failures = [x for x in report["results"] if not x["pass_reconciliation"]]
    if not failures:
        lines.append("本期无失败项。")
    else:
        for item in failures:
            lines.append(f"### {item['symbol']}")
            for chk in item["failed_checks"]:
                lines.append(
                    f"- {chk['name']}: value={chk.get('value')} threshold={chk.get('threshold')}"
                )
            lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/data_reconciliation_audit.json")
    parser.add_argument("--out-dir", default="artifacts")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    cfg = _load_config(args.config)
    symbols = list(cfg.get("symbols", []))
    start_date = str(cfg.get("start_date"))
    end_date = str(cfg.get("end_date"))
    period = str(cfg.get("period", "1d"))
    thresholds = cfg.get("thresholds", {})
    thresholds = {
        "close_rel_p95_max": float(thresholds.get("close_rel_p95_max", 0.02)),
        "volume_rel_p95_max": float(thresholds.get("volume_rel_p95_max", 0.35)),
        "overlap_ratio_min": float(thresholds.get("overlap_ratio_min", 0.90)),
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    udi = UnifiedDataInterface(silent_init=True)
    udi._check_qmt()
    udi._check_akshare()
    validator = DataContractValidator()

    results: list[dict[str, Any]] = []
    source_validations: list[dict[str, Any]] = []

    for symbol in symbols:
        qmt_df = pd.DataFrame()
        ak_df = pd.DataFrame()
        if udi.qmt_available:
            try:
                _q = udi._read_from_qmt(symbol, start_date, end_date, period)
                if _q is not None and not _q.empty:
                    qmt_df = _q
            except Exception as exc:
                print(f"[WARN] QMT读取失败 {symbol}: {exc}")
                qmt_df = pd.DataFrame()
        if udi.akshare_available:
            try:
                _a = udi._read_from_akshare(symbol, start_date, end_date, period)
                if _a is not None and not _a.empty:
                    ak_df = _a
            except Exception as exc:
                print(f"[WARN] AKShare读取失败 {symbol}: {exc}")
                ak_df = pd.DataFrame()

        source_validations.append(_source_metrics(symbol, "qmt", qmt_df, validator))
        source_validations.append(_source_metrics(symbol, "akshare", ak_df, validator))
        results.append(_reconcile_pair(symbol, qmt_df, ak_df, thresholds))

    passed = sum(1 for r in results if r["pass_reconciliation"])
    failed = len(results) - passed

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "start_date": start_date,
        "end_date": end_date,
        "period": period,
        "thresholds": thresholds,
        "qmt_available": bool(udi.qmt_available),
        "akshare_available": bool(udi.akshare_available),
        "summary": {
            "total_symbols": len(results),
            "passed_symbols": passed,
            "failed_symbols": failed,
        },
        "results": results,
        "source_validations": source_validations,
    }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"source_reconciliation_{ts}.json"
    md_path = out_dir / f"source_reconciliation_{ts}.md"
    latest_json = out_dir / "source_reconciliation_latest.json"
    latest_md = out_dir / "source_reconciliation_latest.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(report, md_path)
    latest_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_markdown(report, latest_md)

    print(f"[OK] 写入: {json_path}")
    print(f"[OK] 写入: {md_path}")
    print(f"[OK] 刷新: {latest_json}")
    print(f"[OK] 刷新: {latest_md}")
    print(
        f"[SUMMARY] total={len(results)} pass={passed} fail={failed} "
        f"qmt_available={udi.qmt_available} akshare_available={udi.akshare_available}"
    )

    if args.strict and failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

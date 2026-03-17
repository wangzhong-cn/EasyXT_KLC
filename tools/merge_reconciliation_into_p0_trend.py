#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _parse_iso(ts: str) -> datetime:
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _extract_recon(payload: dict[str, Any]) -> dict[str, Any]:
    if "summary" in payload:
        summary = payload.get("summary", {})
        total = int(summary.get("total_symbols", 0) or 0)
        passed = int(summary.get("passed_symbols", 0) or 0)
        failed = int(summary.get("failed_symbols", 0) or 0)
        ts = str(payload.get("generated_at") or "")
        qmt = bool(payload.get("qmt_available", False))
        ak = bool(payload.get("akshare_available", False))
    else:
        total = int(payload.get("total_symbols", 0) or 0)
        passed = int(payload.get("passed_symbols", 0) or 0)
        failed = int(payload.get("failed_symbols", 0) or 0)
        ts = str(payload.get("ts") or "")
        qmt = bool(payload.get("qmt_available", False))
        ak = bool(payload.get("akshare_available", False))
    if not ts:
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pass_rate = 0.0 if total <= 0 else passed / total
    status = "pass" if (failed == 0 and qmt and ak) else "fail"
    return {
        "ts": ts,
        "total_symbols": total,
        "passed_symbols": passed,
        "failed_symbols": failed,
        "pass_rate": pass_rate,
        "qmt_available": qmt,
        "akshare_available": ak,
        "status": status,
    }


def _merge(history: list[dict[str, Any]], recon: dict[str, Any]) -> list[dict[str, Any]]:
    recon_date = recon["ts"][:10]
    target_idx = -1
    for i in range(len(history) - 1, -1, -1):
        row = history[i]
        if str(row.get("ts", ""))[:10] == recon_date:
            target_idx = i
            break

    if target_idx >= 0:
        row = history[target_idx]
    elif history:
        row = deepcopy(history[-1])
        row["ts"] = recon["ts"]
        history.append(row)
        target_idx = len(history) - 1
    else:
        row = {
            "ts": recon["ts"],
            "strict_gate_pass": False,
            "P0_open": -1,
            "ach": -1,
            "checks": {},
        }
        history.append(row)
        target_idx = 0

    row["reconciliation"] = {
        "total_symbols": recon["total_symbols"],
        "passed_symbols": recon["passed_symbols"],
        "failed_symbols": recon["failed_symbols"],
        "pass_rate": round(recon["pass_rate"], 4),
        "qmt_available": recon["qmt_available"],
        "akshare_available": recon["akshare_available"],
    }
    checks = row.get("checks") or {}
    checks["source_reconciliation"] = {
        "status": recon["status"],
        "violations": recon["failed_symbols"],
    }
    row["checks"] = checks
    history[target_idx] = row
    history.sort(key=lambda x: _parse_iso(str(x.get("ts", ""))))
    return history


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recon-json", default="artifacts/governance_source_reconciliation_latest.json")
    parser.add_argument("--trend-json", default="artifacts/p0_trend_history.json")
    args = parser.parse_args()

    recon_path = Path(args.recon_json)
    trend_path = Path(args.trend_json)
    trend_path.parent.mkdir(parents=True, exist_ok=True)

    recon_payload = _load_json(recon_path, {})
    if not recon_payload:
        print(f"[SKIP] recon payload not found: {recon_path}")
        return 0
    recon = _extract_recon(recon_payload)
    history = _load_json(trend_path, [])
    if not isinstance(history, list):
        history = []
    merged = _merge(history, recon)
    trend_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] merged reconciliation into trend: {trend_path}")
    print(
        f"[SUMMARY] ts={recon['ts']} status={recon['status']} "
        f"total={recon['total_symbols']} pass={recon['passed_symbols']} fail={recon['failed_symbols']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

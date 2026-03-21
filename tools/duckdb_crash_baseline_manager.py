from __future__ import annotations

import argparse
import getpass
import json
import pathlib
from datetime import datetime, timezone
from typing import Any

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
DEFAULT_REPORT = ARTIFACTS_DIR / "duckdb_crash_gate_latest.json"
DEFAULT_BASELINE = ARTIFACTS_DIR / "duckdb_crash_baseline.json"
DEFAULT_LEDGER = ARTIFACTS_DIR / "duckdb_crash_baseline_ledger.jsonl"


def _load_json(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _normalize_baseline(raw: dict[str, Any]) -> dict[str, Any]:
    baseline = dict(raw or {})
    baseline.setdefault("ignore_signatures", [])
    baseline.setdefault("ignore_patterns", [])
    if not isinstance(baseline["ignore_signatures"], list):
        baseline["ignore_signatures"] = []
    if not isinstance(baseline["ignore_patterns"], list):
        baseline["ignore_patterns"] = []
    return baseline


def update_baseline_from_report(
    report: dict[str, Any],
    baseline: dict[str, Any],
    mode: str = "append",
) -> tuple[dict[str, Any], list[str]]:
    src_hits = list(report.get("hits", []) or [])
    signatures: list[str] = []
    for row in src_hits:
        if not isinstance(row, dict):
            continue
        sig = str(row.get("signature_id", "") or "").strip()
        if sig:
            signatures.append(sig)
    signatures = sorted(set(signatures))
    cur = _normalize_baseline(baseline)
    existing = {str(x) for x in cur.get("ignore_signatures", [])}
    if mode == "replace":
        new_set = set(signatures)
    else:
        new_set = existing | set(signatures)
    added = sorted(new_set - existing)
    cur["ignore_signatures"] = sorted(new_set)
    meta = dict(cur.get("_meta", {}) if isinstance(cur.get("_meta", {}), dict) else {})
    meta["updated_at"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    meta["source_report"] = str(report.get("_source_report", ""))
    meta["last_mode"] = mode
    cur["_meta"] = meta
    return cur, added


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=str, default=str(DEFAULT_REPORT))
    parser.add_argument("--baseline", type=str, default=str(DEFAULT_BASELINE))
    parser.add_argument("--ledger", type=str, default=str(DEFAULT_LEDGER))
    parser.add_argument("--mode", choices=["append", "replace"], default="append")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    report_path = pathlib.Path(args.report)
    if not report_path.is_absolute():
        report_path = PROJECT_ROOT / report_path
    baseline_path = pathlib.Path(args.baseline)
    if not baseline_path.is_absolute():
        baseline_path = PROJECT_ROOT / baseline_path
    ledger_path = pathlib.Path(args.ledger)
    if not ledger_path.is_absolute():
        ledger_path = PROJECT_ROOT / ledger_path

    report = _load_json(report_path)
    if not report:
        print(f"[FAIL] report not found or invalid: {report_path}")
        return 1
    report["_source_report"] = str(report_path)
    baseline = _load_json(baseline_path)
    updated, added = update_baseline_from_report(report, baseline, mode=str(args.mode))

    if args.dry_run:
        print(
            json.dumps(
                {
                    "mode": args.mode,
                    "report": str(report_path),
                    "baseline": str(baseline_path),
                    "added_count": len(added),
                    "added_signatures": added,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text(json.dumps(updated, ensure_ascii=False, indent=2), encoding="utf-8")
    entry = {
        "ts": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "operator": getpass.getuser(),
        "report": str(report_path),
        "baseline": str(baseline_path),
        "mode": args.mode,
        "hit_count": int(report.get("hit_count", 0) or 0),
        "added_count": len(added),
        "added_signatures": added,
    }
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    print(f"[OK] baseline updated: {baseline_path}")
    print(f"[OK] added signatures: {len(added)}")
    print(f"[OK] ledger appended: {ledger_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

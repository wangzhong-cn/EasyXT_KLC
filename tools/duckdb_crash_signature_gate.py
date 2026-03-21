from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
from datetime import datetime, timezone
from typing import Any

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
DEFAULT_OUTPUT = ARTIFACTS_DIR / "duckdb_crash_gate_latest.json"

CRASH_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"access violation", re.IGNORECASE),
    re.compile(r"segmentation fault", re.IGNORECASE),
    re.compile(r"duckdb.*fatal", re.IGNORECASE),
    re.compile(r"duckdb.*checkpoint.*fail", re.IGNORECASE),
    re.compile(r"checkpoint thread.*crash", re.IGNORECASE),
    # GUI/Qt 致命信号：线程在运行时被销毁
    re.compile(r"QThread.*Destroyed while thread is still running", re.IGNORECASE),
    # MongoDB/BSON 断言（进程级 abort）
    re.compile(r"bsonobj\.cpp.*assertion|assertion.*bsonobj", re.IGNORECASE),
)

DEFAULT_PATHS = (
    PROJECT_ROOT / "reg_latest.txt",
    ARTIFACTS_DIR / "reg_latest.txt",
    ARTIFACTS_DIR / "gui_cold_start_stress_latest.json",
)


def _iter_scan_files(extra_paths: list[str] | None = None, include_defaults: bool = True) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    seen: set[str] = set()
    if include_defaults:
        for p in DEFAULT_PATHS:
            key = str(p.resolve()) if p.exists() else str(p)
            if key not in seen:
                seen.add(key)
                files.append(p)
        for candidate in ARTIFACTS_DIR.glob("*.log"):
            key = str(candidate.resolve())
            if key not in seen:
                seen.add(key)
                files.append(candidate)
    if extra_paths:
        for raw in extra_paths:
            p = pathlib.Path(raw)
            if not p.is_absolute():
                p = PROJECT_ROOT / p
            key = str(p.resolve()) if p.exists() else str(p)
            if key not in seen:
                seen.add(key)
                files.append(p)
    return files


def _make_signature_id(hit: dict[str, Any]) -> str:
    payload = f"{hit.get('pattern','')}|{hit.get('message','')}"
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _load_baseline(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    p = pathlib.Path(path)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _is_ignored(hit: dict[str, Any], baseline: dict[str, Any]) -> bool:
    sig = str(hit.get("signature_id", "") or "")
    pattern = str(hit.get("pattern", "") or "")
    msg = str(hit.get("message", "") or "")
    ignore_ids = {str(x) for x in list(baseline.get("ignore_signatures", []) or [])}
    if sig and sig in ignore_ids:
        return True
    for p in list(baseline.get("ignore_patterns", []) or []):
        try:
            if re.search(str(p), pattern, flags=re.IGNORECASE) or re.search(str(p), msg, flags=re.IGNORECASE):
                return True
        except re.error:
            continue
    return False


def scan_duckdb_crash_signatures(
    extra_paths: list[str] | None = None,
    include_defaults: bool = True,
    max_age_hours: float | None = None,
    baseline_path: str | None = None,
) -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    files_scanned = 0
    files_skipped_by_age = 0
    now_ts = datetime.now(tz=timezone.utc).timestamp()
    baseline = _load_baseline(baseline_path)
    ignored_count = 0
    for path in _iter_scan_files(extra_paths, include_defaults=include_defaults):
        if not path.exists() or not path.is_file():
            continue
        if max_age_hours is not None:
            try:
                age_h = max(0.0, (now_ts - float(path.stat().st_mtime)) / 3600.0)
                if age_h > float(max_age_hours):
                    files_skipped_by_age += 1
                    continue
            except Exception:
                pass
        files_scanned += 1
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        lines = text.splitlines()
        for idx, line in enumerate(lines, start=1):
            line_lower = line.lower()
            if (
                "duckdb" not in line_lower
                and "checkpoint" not in line_lower
                and "access violation" not in line_lower
                and "qthread" not in line_lower
                and "bsonobj" not in line_lower
                and "assertion failed" not in line_lower
            ):
                continue
            matched = next((pat.pattern for pat in CRASH_PATTERNS if pat.search(line)), "")
            if matched:
                hit = {
                    "file": str(path),
                    "line": idx,
                    "pattern": matched,
                    "message": line.strip()[:400],
                }
                hit["signature_id"] = _make_signature_id(hit)
                if _is_ignored(hit, baseline):
                    ignored_count += 1
                    continue
                hits.append(hit)
    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files_scanned": files_scanned,
        "files_skipped_by_age": files_skipped_by_age,
        "max_age_hours": max_age_hours,
        "hit_count": len(hits),
        "ignored_count": ignored_count,
        "status": "fail" if hits else "pass",
        "hits": hits,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--path", action="append", default=[])
    parser.add_argument("--max-age-hours", type=float, default=None)
    parser.add_argument("--baseline", type=str, default="")
    args = parser.parse_args()
    extra_paths = list(args.path or [])
    scan_files = _iter_scan_files(extra_paths=extra_paths, include_defaults=True)
    report = scan_duckdb_crash_signatures(
        extra_paths=extra_paths,
        max_age_hours=args.max_age_hours,
        baseline_path=str(args.baseline or ""),
    )
    out = pathlib.Path(args.output)
    if not out.is_absolute():
        out = PROJECT_ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"[duckdb_crash_gate] status={report['status']} hits={report['hit_count']} files={report['files_scanned']}")
        print(f"[duckdb_crash_gate] written={out}")
        if args.verbose:
            existing = [str(p) for p in scan_files if p.exists() and p.is_file()]
            print(f"[duckdb_crash_gate] scan_files_total={len(scan_files)} existing={len(existing)}")
            for p in existing:
                print(f"[duckdb_crash_gate] file={p}")
    if args.strict and report["status"] == "fail":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

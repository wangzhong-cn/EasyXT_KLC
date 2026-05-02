from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_state_root(state_root: str | Path | None = None) -> Path:
    if state_root is not None:
        return Path(state_root)
    return Path(os.environ.get("EASYXT_STATE_ROOT") or (_project_root() / "runtime" / "state"))


def resolve_state_backup_root(backup_root: str | Path | None = None) -> Path:
    if backup_root is not None:
        return Path(backup_root)
    return Path(
        os.environ.get("EASYXT_STATE_BACKUP_ROOT")
        or (_project_root() / "runtime" / "state_backups")
    )


def resolve_federation_attach_budget(default: int = 8) -> int:
    try:
        return max(1, int(os.environ.get("EASYXT_FEDERATION_ATTACH_BUDGET", str(default))))
    except ValueError:
        return default


@dataclass(frozen=True, slots=True)
class SystemStateSnapshot:
    state_root: str
    catalog_path: str | None
    sqlite_logical_seq: int
    active_shard_id: str | None
    active_shard_count: int
    duckdb_shadow_version: str | None
    sync_status: str
    last_good_version: str | None
    shadow_failed_stage: str | None
    shadow_error: str | None
    backup_last_success_at: str | None
    shadow_manifest_path: str | None
    federation_attach_budget: int
    federation_executor_ready: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def get_system_state_snapshot(
    *,
    state_root: str | Path | None = None,
    backup_root: str | Path | None = None,
    federation_attach_budget: int | None = None,
) -> SystemStateSnapshot:
    resolved_state_root = resolve_state_root(state_root)
    resolved_backup_root = resolve_state_backup_root(backup_root)
    resolved_budget = federation_attach_budget or resolve_federation_attach_budget()

    catalog_path = resolved_state_root / "catalog" / "shard_catalog.db"
    sqlite_logical_seq = 0
    active_shard_id: str | None = None
    active_shard_count = 0

    if catalog_path.exists():
        sqlite_logical_seq, active_shard_id, active_shard_count = _read_catalog_status(catalog_path)

    shadow_manifest_path = resolved_state_root / "duckdb_shadow" / "current" / "manifest.json"
    shadow_payload = _load_json_file(shadow_manifest_path) if shadow_manifest_path.exists() else None
    duckdb_shadow_version = _coerce_optional_str(shadow_payload.get("version_id")) if shadow_payload else None
    sync_status = _coerce_optional_str(shadow_payload.get("status")) if shadow_payload else None
    last_good_version = _coerce_optional_str(shadow_payload.get("last_good_version")) if shadow_payload else None
    shadow_failed_stage = _coerce_optional_str(
        shadow_payload.get("failed_stage")
        or shadow_payload.get("error_stage")
        or shadow_payload.get("last_error_stage")
        if shadow_payload
        else None
    )
    shadow_error = _coerce_optional_str(shadow_payload.get("error")) if shadow_payload else None
    if not sync_status:
        sync_status = "unconfigured"

    backup_last_success_at = _latest_backup_created_at(resolved_backup_root)
    federation_executor_ready = _duckdb_available()

    return SystemStateSnapshot(
        state_root=str(resolved_state_root),
        catalog_path=str(catalog_path) if catalog_path.exists() else None,
        sqlite_logical_seq=sqlite_logical_seq,
        active_shard_id=active_shard_id,
        active_shard_count=active_shard_count,
        duckdb_shadow_version=duckdb_shadow_version,
        sync_status=sync_status,
        last_good_version=last_good_version,
        shadow_failed_stage=shadow_failed_stage,
        shadow_error=shadow_error,
        backup_last_success_at=backup_last_success_at,
        shadow_manifest_path=str(shadow_manifest_path) if shadow_manifest_path.exists() else None,
        federation_attach_budget=resolved_budget,
        federation_executor_ready=federation_executor_ready,
    )


def _read_catalog_status(catalog_path: Path) -> tuple[int, str | None, int]:
    con = sqlite3.connect(catalog_path)
    con.row_factory = sqlite3.Row
    try:
        next_row = con.execute(
            "SELECT value FROM catalog_meta WHERE key = 'next_logical_seq'"
        ).fetchone()
        sqlite_logical_seq = max(0, int(next_row["value"]) - 1) if next_row else 0
        count_row = con.execute(
            "SELECT COUNT(*) AS count FROM shards WHERE status != 'archived'"
        ).fetchone()
        active_shard_count = int(count_row["count"] or 0) if count_row else 0
        active_row = con.execute(
            """
            SELECT shard_id
              FROM shards
             WHERE status != 'archived'
             ORDER BY COALESCE(logical_seq_end, 0) DESC, updated_at DESC, shard_id DESC
             LIMIT 1
            """
        ).fetchone()
        active_shard_id = str(active_row["shard_id"]) if active_row else None
        return sqlite_logical_seq, active_shard_id, active_shard_count
    finally:
        con.close()


def _load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _latest_backup_created_at(backup_root: Path) -> str | None:
    if not backup_root.exists():
        return None
    latest: str | None = None
    for manifest_path in backup_root.glob("*/manifest.json"):
        try:
            payload = _load_json_file(manifest_path)
        except Exception:
            continue
        created_at = _coerce_optional_str(payload.get("created_at"))
        if created_at and (latest is None or created_at > latest):
            latest = created_at
    return latest


def _coerce_optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401

        return True
    except Exception:
        return False
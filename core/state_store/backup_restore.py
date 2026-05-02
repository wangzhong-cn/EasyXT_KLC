from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .shard_catalog import ShardCatalog, ShardRef


def _utcnow_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    try:
        with path.open("a+b") as handle:
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        # Windows 某些文件句柄在 backup 后可能拒绝 fsync；这里保持 best-effort。
        # 核心一致性仍由 SQLite backup API + close/commit 保证。
        return


def _sqlite_backup(source_path: Path, destination_path: Path) -> None:
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source_path) as source, sqlite3.connect(destination_path) as destination:
        source.backup(destination)
        destination.commit()
    _fsync_file(destination_path)


def _sqlite_integrity_ok(path: Path) -> bool:
    with sqlite3.connect(path) as con:
        row = con.execute("PRAGMA integrity_check").fetchone()
    if row is None:
        return False
    return str(row[0]).lower() == "ok"


@dataclass(frozen=True, slots=True)
class BackupFileRecord:
    role: str
    relative_path: str
    family_name: str | None
    shard_id: str | None
    size_bytes: int
    sha256: str
    logical_seq_end: int | None
    row_count: int | None


@dataclass(frozen=True, slots=True)
class BackupManifest:
    format_version: int
    backup_id: str
    created_at: str
    source_root: str
    logical_seq_watermark: int
    catalog_relative_path: str
    file_count: int
    files: list[BackupFileRecord]

    def to_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "backup_id": self.backup_id,
            "created_at": self.created_at,
            "source_root": self.source_root,
            "logical_seq_watermark": self.logical_seq_watermark,
            "catalog_relative_path": self.catalog_relative_path,
            "file_count": self.file_count,
            "files": [asdict(item) for item in self.files],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "BackupManifest":
        return cls(
            format_version=int(payload["format_version"]),
            backup_id=str(payload["backup_id"]),
            created_at=str(payload["created_at"]),
            source_root=str(payload["source_root"]),
            logical_seq_watermark=int(payload["logical_seq_watermark"]),
            catalog_relative_path=str(payload["catalog_relative_path"]),
            file_count=int(payload["file_count"]),
            files=[BackupFileRecord(**item) for item in payload["files"]],
        )


@dataclass(frozen=True, slots=True)
class BackupVerificationReport:
    ok: bool
    manifest_path: str
    checked_files: int
    errors: list[str]


@dataclass(frozen=True, slots=True)
class RestoreReceipt:
    backup_id: str
    target_root: str
    restored_files: int
    logical_seq_watermark: int


class SQLiteBackupManager:
    """零运维 SQLite 分片备份/恢复/校验管理器。

    约束：

    - 备份使用 SQLite 官方 backup API，而不是裸复制主 `.db`
    - 恢复使用临时文件 + 原子替换，避免写入半成品
    - manifest 记录 checksum / logical sequence / 文件清单，供 verify 使用
    """

    MANIFEST_FILE = "manifest.json"

    def __init__(self, catalog: ShardCatalog) -> None:
        self.catalog = catalog

    def create_backup(
        self,
        destination_root: str | Path,
        *,
        backup_id: str | None = None,
        include_archived: bool = False,
        verify_after_backup: bool = True,
    ) -> BackupManifest:
        backup_name = backup_id or f"backup-{_utcnow_compact()}"
        destination_root_path = Path(destination_root)
        backup_dir = destination_root_path / backup_name
        if backup_dir.exists():
            raise FileExistsError(f"备份目录已存在: {backup_dir}")
        backup_dir.mkdir(parents=True, exist_ok=False)

        files: list[BackupFileRecord] = []

        catalog_relative = Path("catalog") / self.catalog.catalog_path.name
        catalog_target = backup_dir / catalog_relative
        _sqlite_backup(self.catalog.catalog_path, catalog_target)
        files.append(
            BackupFileRecord(
                role="catalog",
                relative_path=str(catalog_relative).replace("\\", "/"),
                family_name=None,
                shard_id=None,
                size_bytes=catalog_target.stat().st_size,
                sha256=_sha256_file(catalog_target),
                logical_seq_end=self.catalog.get_logical_seq_watermark(),
                row_count=None,
            )
        )

        for shard in self.catalog.list_shards(include_archived=include_archived):
            source_path = Path(shard.db_path)
            if not source_path.exists():
                continue
            relative = Path("shards") / shard.family_name / source_path.name
            destination = backup_dir / relative
            _sqlite_backup(source_path, destination)
            files.append(
                BackupFileRecord(
                    role="shard",
                    relative_path=str(relative).replace("\\", "/"),
                    family_name=shard.family_name,
                    shard_id=shard.shard_id,
                    size_bytes=destination.stat().st_size,
                    sha256=_sha256_file(destination),
                    logical_seq_end=shard.logical_seq_end,
                    row_count=shard.row_count,
                )
            )

        manifest = BackupManifest(
            format_version=1,
            backup_id=backup_name,
            created_at=_utcnow_iso(),
            source_root=str(self.catalog.root_dir),
            logical_seq_watermark=self.catalog.get_logical_seq_watermark(),
            catalog_relative_path=str(catalog_relative).replace("\\", "/"),
            file_count=len(files),
            files=files,
        )
        manifest_path = backup_dir / self.MANIFEST_FILE
        manifest_path.write_text(
            json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _fsync_file(manifest_path)

        if verify_after_backup:
            report = self.verify_backup(backup_dir)
            if not report.ok:
                raise RuntimeError(
                    f"备份校验失败: {report.errors}"
                )
        return manifest

    def verify_backup(self, backup_dir_or_manifest: str | Path) -> BackupVerificationReport:
        manifest_path = self._resolve_manifest_path(backup_dir_or_manifest)
        manifest = self._load_manifest(manifest_path)
        backup_dir = manifest_path.parent
        errors: list[str] = []

        for file_record in manifest.files:
            file_path = backup_dir / Path(file_record.relative_path)
            if not file_path.exists():
                errors.append(f"文件缺失: {file_record.relative_path}")
                continue
            actual_size = file_path.stat().st_size
            if actual_size != file_record.size_bytes:
                errors.append(
                    f"文件大小不匹配: {file_record.relative_path} expected={file_record.size_bytes} actual={actual_size}"
                )
            actual_hash = _sha256_file(file_path)
            if actual_hash != file_record.sha256:
                errors.append(
                    f"checksum 不匹配: {file_record.relative_path} expected={file_record.sha256} actual={actual_hash}"
                )
            try:
                if not _sqlite_integrity_ok(file_path):
                    errors.append(f"integrity_check 失败: {file_record.relative_path}")
            except sqlite3.DatabaseError as exc:
                errors.append(f"无法打开 SQLite 文件 {file_record.relative_path}: {exc}")

        return BackupVerificationReport(
            ok=not errors,
            manifest_path=str(manifest_path),
            checked_files=len(manifest.files),
            errors=errors,
        )

    def restore_backup(
        self,
        backup_dir_or_manifest: str | Path,
        target_root: str | Path,
        *,
        verify_before_restore: bool = True,
        overwrite: bool = True,
    ) -> RestoreReceipt:
        manifest_path = self._resolve_manifest_path(backup_dir_or_manifest)
        manifest = self._load_manifest(manifest_path)
        if verify_before_restore:
            report = self.verify_backup(manifest_path)
            if not report.ok:
                raise RuntimeError(f"恢复前校验失败: {report.errors}")

        backup_dir = manifest_path.parent
        target_root_path = Path(target_root)
        target_root_path.mkdir(parents=True, exist_ok=True)

        for file_record in manifest.files:
            source_path = backup_dir / Path(file_record.relative_path)
            destination_path = target_root_path / Path(file_record.relative_path)
            if destination_path.exists() and not overwrite:
                raise FileExistsError(f"目标文件已存在: {destination_path}")
            temp_name = f"{destination_path.name}.restore-{uuid.uuid4().hex}.tmp"
            temp_path = destination_path.with_name(temp_name)
            if temp_path.exists():
                temp_path.unlink()
            destination_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, temp_path)
            _fsync_file(temp_path)
            try:
                os.replace(temp_path, destination_path)
            except PermissionError as exc:
                raise PermissionError(
                    f"恢复目标文件被占用，无法原子替换: {destination_path}; "
                    "请确保 restore 目标根目录处于离线状态，没有被提前打开的 SQLite catalog / shard 连接。"
                ) from exc
            self._cleanup_wal_sidecars(destination_path)

        return RestoreReceipt(
            backup_id=manifest.backup_id,
            target_root=str(target_root_path),
            restored_files=len(manifest.files),
            logical_seq_watermark=manifest.logical_seq_watermark,
        )

    def _resolve_manifest_path(self, backup_dir_or_manifest: str | Path) -> Path:
        path = Path(backup_dir_or_manifest)
        if path.is_dir():
            return path / self.MANIFEST_FILE
        return path

    def _load_manifest(self, manifest_path: Path) -> BackupManifest:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        return BackupManifest.from_dict(payload)

    @staticmethod
    def _cleanup_wal_sidecars(db_path: Path) -> None:
        for suffix in ("-wal", "-shm"):
            sidecar = db_path.with_name(f"{db_path.name}{suffix}")
            if sidecar.exists():
                try:
                    sidecar.unlink()
                except OSError:
                    pass
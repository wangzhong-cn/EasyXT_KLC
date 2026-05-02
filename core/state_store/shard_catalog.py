from __future__ import annotations

import re
import sqlite3
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

ShardKind = Literal["single", "time_monthly", "symbol_prefix"]

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SYMBOL_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]+")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _coerce_datetime(value: datetime | date | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError(f"不支持的时间类型: {type(value)!r}")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _month_bounds(dt: datetime) -> tuple[datetime, datetime]:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


@dataclass(frozen=True, slots=True)
class ShardFamilyConfig:
    """定义一个表族如何分片。

    - ``time_monthly``：按月份切分，适合事件/时序类 operational 数据
    - ``symbol_prefix``：按标的前缀切分，适合高频按标的聚簇的数据
    - ``single``：保持单文件，适合 manifest / 用户偏好 / 元数据
    """

    name: str
    table_name: str
    shard_kind: ShardKind = "time_monthly"
    time_column: str = "event_ts"
    symbol_column: str | None = None
    symbol_prefix_len: int = 2
    schema_version: int = 1
    file_extension: str = ".sqlite3"

    def __post_init__(self) -> None:
        for field_name in ("name", "table_name", "time_column"):
            value = getattr(self, field_name)
            if not _IDENTIFIER_RE.fullmatch(value):
                raise ValueError(f"非法标识符 {field_name}={value!r}")
        if self.symbol_column is not None and not _IDENTIFIER_RE.fullmatch(self.symbol_column):
            raise ValueError(f"非法标识符 symbol_column={self.symbol_column!r}")
        if self.shard_kind not in ("single", "time_monthly", "symbol_prefix"):
            raise ValueError(f"不支持的分片策略: {self.shard_kind!r}")
        if self.symbol_prefix_len < 1:
            raise ValueError("symbol_prefix_len 必须 >= 1")
        if self.schema_version < 1:
            raise ValueError("schema_version 必须 >= 1")
        if not self.file_extension.startswith("."):
            raise ValueError("file_extension 必须以 '.' 开头")


@dataclass(frozen=True, slots=True)
class ShardRef:
    shard_id: str
    family_name: str
    table_name: str
    shard_key: str
    shard_kind: ShardKind
    db_path: str
    range_start: str | None
    range_end: str | None
    symbol_scope: str | None
    schema_version: int
    logical_seq_start: int | None
    logical_seq_end: int | None
    row_count: int
    status: str
    checksum: str | None
    created_at: str
    updated_at: str
    archived_at: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "ShardRef":
        return cls(
            shard_id=str(row["shard_id"]),
            family_name=str(row["family_name"]),
            table_name=str(row["table_name"]),
            shard_key=str(row["shard_key"]),
            shard_kind=row["shard_kind"],
            db_path=str(row["db_path"]),
            range_start=row["range_start"],
            range_end=row["range_end"],
            symbol_scope=row["symbol_scope"],
            schema_version=int(row["schema_version"]),
            logical_seq_start=(int(row["logical_seq_start"]) if row["logical_seq_start"] is not None else None),
            logical_seq_end=(int(row["logical_seq_end"]) if row["logical_seq_end"] is not None else None),
            row_count=int(row["row_count"] or 0),
            status=str(row["status"]),
            checksum=row["checksum"],
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            archived_at=row["archived_at"],
        )


class ShardCatalog:
    """Operational SQLite 分片目录。

    这是状态主线的秩序来源：

    - 表族定义
    - 分片文件位置
    - 逻辑序号分配
    - 分片状态、行数、校验值、版本边界
    """

    def __init__(self, root_dir: str | Path, *, catalog_path: str | Path | None = None) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_path = Path(catalog_path) if catalog_path else self.root_dir / "catalog" / "shard_catalog.db"
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.catalog_path, timeout=30.0, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def _ensure_schema(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS catalog_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS table_families (
                    name TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    shard_kind TEXT NOT NULL,
                    time_column TEXT NOT NULL,
                    symbol_column TEXT,
                    symbol_prefix_len INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    file_extension TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS shards (
                    shard_id TEXT PRIMARY KEY,
                    family_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    shard_key TEXT NOT NULL,
                    shard_kind TEXT NOT NULL,
                    db_path TEXT NOT NULL UNIQUE,
                    range_start TEXT,
                    range_end TEXT,
                    symbol_scope TEXT,
                    schema_version INTEGER NOT NULL,
                    logical_seq_start INTEGER,
                    logical_seq_end INTEGER,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    checksum TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    archived_at TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_shards_family_status
                    ON shards(family_name, status, range_start, range_end, symbol_scope);
                """
            )
            now = _utcnow_iso()
            con.execute(
                """
                INSERT OR IGNORE INTO catalog_meta(key, value, updated_at)
                VALUES('next_logical_seq', '1', ?)
                """,
                (now,),
            )
            con.commit()

    def register_family(self, config: ShardFamilyConfig) -> None:
        now = _utcnow_iso()
        with self._lock, self._connect() as con:
            con.execute(
                """
                INSERT INTO table_families(
                    name, table_name, shard_kind, time_column, symbol_column,
                    symbol_prefix_len, schema_version, file_extension, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    table_name=excluded.table_name,
                    shard_kind=excluded.shard_kind,
                    time_column=excluded.time_column,
                    symbol_column=excluded.symbol_column,
                    symbol_prefix_len=excluded.symbol_prefix_len,
                    schema_version=excluded.schema_version,
                    file_extension=excluded.file_extension,
                    updated_at=excluded.updated_at
                """,
                (
                    config.name,
                    config.table_name,
                    config.shard_kind,
                    config.time_column,
                    config.symbol_column,
                    config.symbol_prefix_len,
                    config.schema_version,
                    config.file_extension,
                    now,
                    now,
                ),
            )
            con.commit()

    def get_meta(self, key: str) -> str:
        with self._connect() as con:
            row = con.execute("SELECT value FROM catalog_meta WHERE key = ?", (key,)).fetchone()
        if row is None:
            raise KeyError(f"未找到 catalog_meta[{key!r}]")
        return str(row["value"])

    def get_logical_seq_watermark(self) -> int:
        next_seq = int(self.get_meta("next_logical_seq"))
        return max(0, next_seq - 1)

    def get_family(self, family_name: str) -> ShardFamilyConfig:
        with self._connect() as con:
            row = con.execute("SELECT * FROM table_families WHERE name = ?", (family_name,)).fetchone()
        if row is None:
            raise KeyError(f"未注册的表族: {family_name}")
        return ShardFamilyConfig(
            name=str(row["name"]),
            table_name=str(row["table_name"]),
            shard_kind=row["shard_kind"],
            time_column=str(row["time_column"]),
            symbol_column=row["symbol_column"],
            symbol_prefix_len=int(row["symbol_prefix_len"]),
            schema_version=int(row["schema_version"]),
            file_extension=str(row["file_extension"]),
        )

    def allocate_logical_seq(self) -> int:
        with self._lock, self._connect() as con:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT value FROM catalog_meta WHERE key = 'next_logical_seq'"
            ).fetchone()
            if row is None:
                raise RuntimeError("catalog_meta.next_logical_seq 缺失")
            current = int(row["value"])
            con.execute(
                "UPDATE catalog_meta SET value = ?, updated_at = ? WHERE key = 'next_logical_seq'",
                (str(current + 1), _utcnow_iso()),
            )
            con.commit()
            return current

    def resolve_write_shard(
        self,
        family_name: str,
        *,
        event_time: datetime | date | str | None = None,
        symbol: str | None = None,
    ) -> ShardRef:
        family = self.get_family(family_name)
        shard_key, range_start, range_end, symbol_scope = self._derive_shard_key(
            family,
            event_time=event_time,
            symbol=symbol,
        )
        shard_id = f"{family.name}:{shard_key}"
        db_path = self._build_shard_path(family, shard_key)
        now = _utcnow_iso()
        with self._lock, self._connect() as con:
            con.execute(
                """
                INSERT INTO shards(
                    shard_id, family_name, table_name, shard_key, shard_kind, db_path,
                    range_start, range_end, symbol_scope, schema_version,
                    logical_seq_start, logical_seq_end, row_count,
                    status, checksum, created_at, updated_at, archived_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 'active', NULL, ?, ?, NULL)
                ON CONFLICT(shard_id) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    schema_version=excluded.schema_version,
                    status=CASE WHEN shards.status = 'archived' THEN shards.status ELSE 'active' END
                """,
                (
                    shard_id,
                    family.name,
                    family.table_name,
                    shard_key,
                    family.shard_kind,
                    str(db_path),
                    range_start,
                    range_end,
                    symbol_scope,
                    family.schema_version,
                    now,
                    now,
                ),
            )
            row = con.execute("SELECT * FROM shards WHERE shard_id = ?", (shard_id,)).fetchone()
            con.commit()
        if row is None:
            raise RuntimeError(f"无法解析分片: {shard_id}")
        return ShardRef.from_row(row)

    def list_shards(self, family_name: str | None = None, *, include_archived: bool = False) -> list[ShardRef]:
        sql = "SELECT * FROM shards"
        params: list[str] = []
        clauses: list[str] = []
        if family_name is not None:
            clauses.append("family_name = ?")
            params.append(family_name)
        if not include_archived:
            clauses.append("status != 'archived'")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY family_name, range_start, shard_key"
        with self._connect() as con:
            rows = con.execute(sql, tuple(params)).fetchall()
        return [ShardRef.from_row(row) for row in rows]

    def select_shards_for_range(
        self,
        family_name: str,
        *,
        start_time: datetime | date | str | None = None,
        end_time: datetime | date | str | None = None,
        symbol: str | None = None,
    ) -> list[ShardRef]:
        family = self.get_family(family_name)
        clauses = ["family_name = ?", "status = 'active'"]
        params: list[str] = [family_name]
        if family.shard_kind == "time_monthly":
            start_dt = _coerce_datetime(start_time)
            end_dt = _coerce_datetime(end_time)
            if start_dt is not None:
                clauses.append("(range_end IS NULL OR range_end > ?)")
                params.append(start_dt.isoformat(timespec="seconds"))
            if end_dt is not None:
                clauses.append("(range_start IS NULL OR range_start < ?)")
                params.append(end_dt.isoformat(timespec="seconds"))
        elif family.shard_kind == "symbol_prefix" and symbol:
            prefix = self._symbol_prefix(symbol, family.symbol_prefix_len)
            clauses.append("symbol_scope = ?")
            params.append(prefix)
        sql = "SELECT * FROM shards WHERE " + " AND ".join(clauses) + " ORDER BY range_start, shard_key"
        with self._connect() as con:
            rows = con.execute(sql, tuple(params)).fetchall()
        return [ShardRef.from_row(row) for row in rows]

    def mark_shard_write(self, shard_id: str, *, logical_seq: int, row_delta: int = 1) -> None:
        now = _utcnow_iso()
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE shards
                   SET logical_seq_start = COALESCE(logical_seq_start, ?),
                       logical_seq_end = ?,
                       row_count = row_count + ?,
                       updated_at = ?
                 WHERE shard_id = ?
                """,
                (logical_seq, logical_seq, row_delta, now, shard_id),
            )
            con.commit()

    def update_checksum(self, shard_id: str, checksum: str) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                "UPDATE shards SET checksum = ?, updated_at = ? WHERE shard_id = ?",
                (checksum, _utcnow_iso(), shard_id),
            )
            con.commit()

    def mark_archived(self, shard_id: str, *, checksum: str | None = None) -> None:
        with self._lock, self._connect() as con:
            con.execute(
                """
                UPDATE shards
                   SET status = 'archived',
                       archived_at = ?,
                       checksum = COALESCE(?, checksum),
                       updated_at = ?
                 WHERE shard_id = ?
                """,
                (_utcnow_iso(), checksum, _utcnow_iso(), shard_id),
            )
            con.commit()

    def _derive_shard_key(
        self,
        family: ShardFamilyConfig,
        *,
        event_time: datetime | date | str | None,
        symbol: str | None,
    ) -> tuple[str, str | None, str | None, str | None]:
        if family.shard_kind == "single":
            return "default", None, None, None
        if family.shard_kind == "time_monthly":
            dt = _coerce_datetime(event_time)
            if dt is None:
                raise ValueError(f"表族 {family.name} 使用 time_monthly 分片时必须提供 event_time")
            start, end = _month_bounds(dt)
            return (
                start.strftime("%Y-%m"),
                start.isoformat(timespec="seconds"),
                end.isoformat(timespec="seconds"),
                None,
            )
        if family.shard_kind == "symbol_prefix":
            if not symbol:
                raise ValueError(f"表族 {family.name} 使用 symbol_prefix 分片时必须提供 symbol")
            prefix = self._symbol_prefix(symbol, family.symbol_prefix_len)
            return prefix, None, None, prefix
        raise ValueError(f"不支持的分片策略: {family.shard_kind!r}")

    def _build_shard_path(self, family: ShardFamilyConfig, shard_key: str) -> Path:
        shard_dir = self.root_dir / "shards" / family.name
        shard_dir.mkdir(parents=True, exist_ok=True)
        return shard_dir / f"{shard_key}{family.file_extension}"

    @staticmethod
    def _symbol_prefix(symbol: str, length: int) -> str:
        sanitized = _SYMBOL_SANITIZE_RE.sub("_", symbol.upper()).strip("_")
        if not sanitized:
            raise ValueError("symbol 不能为空")
        return sanitized[:length]
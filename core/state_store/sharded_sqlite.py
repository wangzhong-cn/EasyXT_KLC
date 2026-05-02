from __future__ import annotations

import re
import sqlite3
import threading
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from .shard_catalog import ShardCatalog, ShardFamilyConfig, ShardRef

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class WriteReceipt:
    family_name: str
    shard_id: str
    db_path: str
    logical_seq: int
    rowcount: int


class ShardedSQLite:
    """按表族路由的 SQLite (WAL) operational store。

    当前版本聚焦三件事：

    1. 使用 ``ShardCatalog`` 统一注册与解析分片
    2. 对指定表族执行参数化写入
    3. 提供跨分片最小只读查询能力

    backup / restore / verify 与 DuckDB federation planner 将在后续阶段继续叠加。
    """

    def __init__(
        self,
        root_dir: str | Path,
        *,
        catalog: ShardCatalog | None = None,
        timeout_s: float = 30.0,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.catalog = catalog or ShardCatalog(self.root_dir)
        self.timeout_s = timeout_s
        self._connections: dict[str, sqlite3.Connection] = {}
        self._db_locks: dict[str, threading.RLock] = {}
        self._family_ddls: dict[str, str] = {}
        self._schema_initialized: set[tuple[str, str]] = set()
        self._lock = threading.RLock()

    def register_family(self, config: ShardFamilyConfig, *, create_sql: str | None = None) -> None:
        self.catalog.register_family(config)
        if create_sql:
            self._family_ddls[config.name] = create_sql

    def write_row(
        self,
        family_name: str,
        row: Mapping[str, Any],
        *,
        event_time: datetime | date | str | None = None,
        symbol: str | None = None,
        create_sql: str | None = None,
    ) -> WriteReceipt:
        family = self.catalog.get_family(family_name)
        if not row:
            raise ValueError("row 不能为空")
        resolved_event_time = event_time if event_time is not None else row.get(family.time_column)
        resolved_symbol = symbol if symbol is not None else (
            str(row.get(family.symbol_column)) if family.symbol_column and row.get(family.symbol_column) is not None else None
        )
        shard = self.catalog.resolve_write_shard(
            family_name,
            event_time=resolved_event_time,
            symbol=resolved_symbol,
        )
        db_lock = self._lock_for(shard.db_path)
        logical_seq = self.catalog.allocate_logical_seq()
        ddl = create_sql or self._family_ddls.get(family_name)
        columns = list(row.keys())
        for column in columns:
            self._quote_identifier(column)
        placeholders = ", ".join("?" for _ in columns)
        quoted_columns = ", ".join(self._quote_identifier(column) for column in columns)
        insert_sql = (
            f"INSERT INTO {self._quote_identifier(family.table_name)} "
            f"({quoted_columns}) VALUES ({placeholders})"
        )

        with db_lock:
            con = self._get_connection(shard.db_path)
            self._ensure_family_schema(con, family_name, ddl)
            cur = None
            try:
                con.execute("BEGIN IMMEDIATE")
                cur = con.execute(insert_sql, tuple(row[column] for column in columns))
                con.commit()
            except Exception:
                con.rollback()
                raise
        rowcount = cur.rowcount if cur is not None and cur.rowcount not in (None, -1) else 1
        self.catalog.mark_shard_write(shard.shard_id, logical_seq=logical_seq, row_delta=rowcount)
        return WriteReceipt(
            family_name=family_name,
            shard_id=shard.shard_id,
            db_path=shard.db_path,
            logical_seq=logical_seq,
            rowcount=rowcount,
        )

    def query_range(
        self,
        family_name: str,
        query: str,
        *,
        start_time: datetime | date | str | None = None,
        end_time: datetime | date | str | None = None,
        symbol: str | None = None,
        params_factory: Callable[[ShardRef], tuple[Any, ...]] | None = None,
        sort_key: str | None = None,
        reverse: bool = False,
        skip_missing_table: bool = True,
    ) -> list[dict[str, Any]]:
        shards = self.catalog.select_shards_for_range(
            family_name,
            start_time=start_time,
            end_time=end_time,
            symbol=symbol,
        )
        results: list[dict[str, Any]] = []
        for shard in shards:
            db_lock = self._lock_for(shard.db_path)
            with db_lock:
                con = self._get_connection(shard.db_path)
                try:
                    params = params_factory(shard) if params_factory else tuple()
                    rows = con.execute(query, params).fetchall()
                except sqlite3.OperationalError as exc:
                    if skip_missing_table and "no such table" in str(exc).lower():
                        continue
                    raise
            results.extend(dict(row) for row in rows)
        if sort_key:
            results.sort(key=lambda item: item.get(sort_key), reverse=reverse)
        return results

    def list_shards(self, family_name: str | None = None) -> list[ShardRef]:
        return self.catalog.list_shards(family_name)

    def close(self) -> None:
        with self._lock:
            for con in self._connections.values():
                try:
                    con.close()
                except Exception:
                    pass
            self._connections.clear()

    def __enter__(self) -> "ShardedSQLite":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _get_connection(self, db_path: str) -> sqlite3.Connection:
        with self._lock:
            con = self._connections.get(db_path)
            if con is not None:
                return con
            path_obj = Path(db_path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            con = sqlite3.connect(path_obj, timeout=self.timeout_s, check_same_thread=False)
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA foreign_keys=ON")
            con.execute(f"PRAGMA busy_timeout={int(self.timeout_s * 1000)}")
            self._connections[db_path] = con
            return con

    def _ensure_family_schema(self, con: sqlite3.Connection, family_name: str, create_sql: str | None) -> None:
        if not create_sql:
            return
        key = (family_name, str(id(con)))
        if key in self._schema_initialized:
            return
        con.executescript(create_sql)
        con.commit()
        self._schema_initialized.add(key)

    def _lock_for(self, db_path: str) -> threading.RLock:
        with self._lock:
            lock = self._db_locks.get(db_path)
            if lock is None:
                lock = threading.RLock()
                self._db_locks[db_path] = lock
            return lock

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(identifier):
            raise ValueError(f"非法 SQL 标识符: {identifier!r}")
        return f'"{identifier}"'
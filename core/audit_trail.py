"""
审计最小链路（Phase 1）

追踪链路：signal_id → order_id → fill_id → pnl_snapshot

表结构（DuckDB）：
  audit_signals  : 策略信号原始记录
  audit_orders   : 委托提交记录（关联 signal_id）
  audit_fills    : 成交回报 + PnL 快照（关联 order_id）

所有写操作均通过 :class:`~data_manager.duckdb_connection_pool.DuckDBConnectionManager`
的写连接执行，保证线程安全。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


def _utc_ms() -> int:
    """UTC 毫秒时间戳（整数），统一审计链时间基准，防止多机浮点时钟偏差。"""
    return int(time.time() * 1000)


# 当前 Schema 版本（每次修改表结构时递增，用于迁移可观测）
_SCHEMA_VERSION = 3

# DDL —— 首次建表（IF NOT EXISTS 幂等）
_DDL = """
CREATE TABLE IF NOT EXISTS audit_signals (
    signal_id   VARCHAR PRIMARY KEY,
    strategy_id VARCHAR NOT NULL,
    code        VARCHAR NOT NULL,
    direction   VARCHAR NOT NULL,    -- 'buy' | 'sell'
    price_hint  DOUBLE,
    volume_hint DOUBLE,
    created_at  BIGINT NOT NULL,      -- UTC 毫秒时间戳 (int)
    entry_hash  VARCHAR NOT NULL DEFAULT '',
    prev_hash   VARCHAR NOT NULL DEFAULT '',   -- 前一条记录的 entry_hash（链式不可篡改）
    batch_hash  VARCHAR NOT NULL DEFAULT '',   -- 同批次关联写入的组合哈希
    sig_version INTEGER NOT NULL DEFAULT 0,    -- 0=迁移旧记录，1=v1 审计链规范
    account_id  VARCHAR NOT NULL DEFAULT ''    -- 账户ID（Phase 2 多账户治理）
);

CREATE TABLE IF NOT EXISTS audit_orders (
    order_id    VARCHAR PRIMARY KEY,
    signal_id   VARCHAR NOT NULL,
    code        VARCHAR,
    direction   VARCHAR,
    volume      DOUBLE,
    price       DOUBLE,
    submitted_at BIGINT NOT NULL,
    status      VARCHAR DEFAULT 'submitted',
    entry_hash  VARCHAR NOT NULL DEFAULT '',
    prev_hash   VARCHAR NOT NULL DEFAULT '',
    batch_hash  VARCHAR NOT NULL DEFAULT '',
    sig_version INTEGER NOT NULL DEFAULT 0,
    account_id  VARCHAR NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS audit_fills (
    fill_id      VARCHAR PRIMARY KEY,
    order_id     VARCHAR NOT NULL,
    filled_at    BIGINT NOT NULL,
    filled_price DOUBLE,
    filled_volume DOUBLE,
    pnl_snapshot DOUBLE,            -- 成交后账户 PnL 快照
    entry_hash   VARCHAR NOT NULL DEFAULT '',
    prev_hash    VARCHAR NOT NULL DEFAULT '',
    batch_hash   VARCHAR NOT NULL DEFAULT '',   -- SHA-256(signal_id|order_id|fill_id|entry_hash)
    sig_version  INTEGER NOT NULL DEFAULT 0,
    account_id   VARCHAR NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS audit_schema_migrations (
    schema_version  INTEGER PRIMARY KEY,   -- 迁移后的目标版本号（_SCHEMA_VERSION）
    migrated_columns VARCHAR NOT NULL,     -- JSON 数组：本次实际新增的列，eg '["prev_hash"]'
    elapsed_ms      DOUBLE NOT NULL,       -- 本次迁移耗时（毫秒）
    migrated_at     BIGINT NOT NULL,       -- UTC 毫秒时间戳
    host            VARCHAR DEFAULT ''     -- 执行机器标识（os.uname or COMPUTERNAME）
);
"""

# 升级迁移 SQL —— 为旧 schema 追加新字段（幂等，try/except 处理字段已存在）
_MIGRATION_ADDS = [
    ("audit_signals", "prev_hash",   "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_signals", "batch_hash",  "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_signals", "sig_version", "INTEGER NOT NULL DEFAULT 0"),
    ("audit_signals", "account_id",  "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_orders",  "prev_hash",   "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_orders",  "batch_hash",  "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_orders",  "sig_version", "INTEGER NOT NULL DEFAULT 0"),
    ("audit_orders",  "account_id",  "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_fills",   "prev_hash",   "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_fills",   "batch_hash",  "VARCHAR NOT NULL DEFAULT ''"),
    ("audit_fills",   "sig_version", "INTEGER NOT NULL DEFAULT 0"),
    ("audit_fills",   "account_id",  "VARCHAR NOT NULL DEFAULT ''"),
]


@dataclass
class AuditSignal:
    signal_id: str
    strategy_id: str
    code: str
    direction: str
    price_hint: Optional[float]
    volume_hint: Optional[float]
    created_at: int                # UTC 毫秒时间戳
    entry_hash: str = ""
    prev_hash: str = ""            # 前一条信号的 entry_hash（链式不可抵赖）
    batch_hash: str = ""           # 批次组合哈希
    sig_version: int = 0           # 0=迁移旧记录，1=v1 审计链规范
    account_id: str = ""           # 账户ID（Phase 2 多账户治理）


@dataclass
class AuditOrder:
    order_id: str
    signal_id: str
    code: str
    direction: str
    volume: float
    price: float
    submitted_at: int              # UTC 毫秒时间戳
    status: str = "submitted"
    entry_hash: str = ""
    prev_hash: str = ""
    batch_hash: str = ""
    sig_version: int = 0
    account_id: str = ""


@dataclass
class AuditFill:
    fill_id: str
    order_id: str
    filled_at: int                 # UTC 毫秒时间戳
    filled_price: float
    filled_volume: float
    pnl_snapshot: Optional[float]
    entry_hash: str = ""
    prev_hash: str = ""
    batch_hash: str = ""           # SHA-256(signal_id|order_id|fill_id|entry_hash)
    sig_version: int = 0
    account_id: str = ""


@dataclass
class AuditChain:
    """完整的审计链路（signal → orders → fills）。"""
    signal: Optional[AuditSignal]
    orders: List[AuditOrder]
    fills: List[AuditFill]


class AuditTrail:
    """
    审计最小链路。

    初始化时传入 DuckDBConnectionManager；若不传则每次写操作通过
    ``data_manager.duckdb_connection_pool.get_db_manager()`` 获取默认管理器。
    """

    def __init__(self, db_manager: Optional[Any] = None) -> None:
        if db_manager is None:
            from data_manager.duckdb_connection_pool import get_db_manager
            db_manager = get_db_manager()
        self._db = db_manager
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_entry_hash(fields: list) -> str:
        """SHA-256（hex）基于记录各字段，用于写入时的完整性标记。"""
        canon = "|".join("" if v is None else str(v) for v in fields)
        return hashlib.sha256(canon.encode("utf-8")).hexdigest()

    def _ensure_tables(self) -> None:
        t0 = time.monotonic()
        migrated_cols: list[str] = []
        try:
            with self._db.get_write_connection() as con:
                con.execute(_DDL)
                # 迁移旧 schema：为已有表追加新字段（幂等）
                for tbl, col, defn in _MIGRATION_ADDS:
                    try:
                        con.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {defn}")
                        migrated_cols.append(f"{tbl}.{col}")
                    except Exception:
                        pass  # 字段已存在时 DuckDB 会抛异常，忽略即可
                elapsed_ms = (time.monotonic() - t0) * 1000
                host = os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or ""
                con.execute(
                    """
                    INSERT INTO audit_schema_migrations
                        (schema_version, migrated_columns, elapsed_ms, migrated_at, host)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    [_SCHEMA_VERSION, json.dumps(migrated_cols, ensure_ascii=False),
                     round(elapsed_ms, 3), _utc_ms(), host],
                )
                log.info(
                    "审计 schema 迁移完成 version=%d new_cols=%d elapsed_ms=%.1f",
                    _SCHEMA_VERSION, len(migrated_cols), elapsed_ms,
                )
        except Exception:
            log.exception("审计表初始化失败")

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def record_signal(
        self,
        strategy_id: str,
        code: str,
        direction: str,
        price_hint: Optional[float] = None,
        volume_hint: Optional[float] = None,
        signal_id: Optional[str] = None,
        account_id: str = "",
    ) -> str:
        """记录策略信号，返回 signal_id（UUID）。"""
        sid = signal_id or str(uuid.uuid4())
        ts = _utc_ms()
        entry_hash = AuditTrail._compute_entry_hash(
            [sid, strategy_id, code, direction, price_hint, volume_hint, ts]
        )
        try:
            with self._db.get_write_connection() as con:
                prev_row = con.execute(
                    "SELECT entry_hash FROM audit_signals ORDER BY created_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
                prev_hash = prev_row[0] if prev_row else ""
                batch_hash = AuditTrail._compute_entry_hash([sid, entry_hash, prev_hash])
                con.execute(
                    """
                    INSERT INTO audit_signals
                        (signal_id, strategy_id, code, direction, price_hint, volume_hint,
                         created_at, entry_hash, prev_hash, batch_hash, sig_version, account_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [sid, strategy_id, code, direction, price_hint, volume_hint,
                     ts, entry_hash, prev_hash, batch_hash, 1, account_id],
                )
        except Exception:
            log.exception("record_signal 失败 signal_id=%s", sid)
        return sid

    def record_order(
        self,
        order_id: str,
        signal_id: str,
        code: str,
        direction: str,
        volume: float,
        price: float,
        status: str = "submitted",
        account_id: str = "",
    ) -> None:
        """记录委托提交。"""
        ts = _utc_ms()
        entry_hash = AuditTrail._compute_entry_hash(
            [order_id, signal_id, code, direction, volume, price, ts]
        )
        try:
            with self._db.get_write_connection() as con:
                prev_row = con.execute(
                    "SELECT entry_hash FROM audit_orders ORDER BY submitted_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
                prev_hash = prev_row[0] if prev_row else ""
                batch_hash = AuditTrail._compute_entry_hash([signal_id, order_id, entry_hash, prev_hash])
                con.execute(
                    """
                    INSERT OR IGNORE INTO audit_orders
                        (order_id, signal_id, code, direction, volume, price,
                         submitted_at, status, entry_hash, prev_hash, batch_hash, sig_version, account_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [order_id, signal_id, code, direction, volume, price,
                     ts, status, entry_hash, prev_hash, batch_hash, 1, account_id],
                )
        except Exception:
            log.exception("record_order 失败 order_id=%s", order_id)

    def record_fill(
        self,
        order_id: str,
        filled_price: float,
        filled_volume: float,
        pnl_snapshot: Optional[float] = None,
        fill_id: Optional[str] = None,
        account_id: str = "",
    ) -> str:
        """记录成交回报，返回 fill_id。"""
        fid = fill_id or str(uuid.uuid4())
        ts = _utc_ms()
        entry_hash = AuditTrail._compute_entry_hash(
            [fid, order_id, ts, filled_price, filled_volume, pnl_snapshot]
        )
        try:
            with self._db.get_write_connection() as con:
                prev_row = con.execute(
                    "SELECT entry_hash FROM audit_fills ORDER BY filled_at DESC, rowid DESC LIMIT 1"
                ).fetchone()
                prev_hash = prev_row[0] if prev_row else ""
                # batch_hash 跨表关联：将 signal_id 纳入哈希，形成 signal→order→fill 完整链证明
                sig_row = con.execute(
                    "SELECT signal_id FROM audit_orders WHERE order_id=?", [order_id]
                ).fetchone()
                sig_id = sig_row[0] if sig_row else ""
                batch_hash = AuditTrail._compute_entry_hash(
                    [sig_id, order_id, fid, entry_hash, prev_hash]
                )
                con.execute(
                    """
                    INSERT INTO audit_fills
                        (fill_id, order_id, filled_at, filled_price, filled_volume,
                         pnl_snapshot, entry_hash, prev_hash, batch_hash, sig_version, account_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [fid, order_id, ts, filled_price, filled_volume,
                     pnl_snapshot, entry_hash, prev_hash, batch_hash, 1, account_id],
                )
                # 同步更新委托状态
                con.execute(
                    "UPDATE audit_orders SET status='filled' WHERE order_id=?",
                    [order_id],
                )
        except Exception:
            log.exception("record_fill 失败 order_id=%s", order_id)
        return fid

    def update_order_status(self, order_id: str, status: str) -> None:
        """更新委托状态（cancelled / rejected 等）。"""
        try:
            with self._db.get_write_connection() as con:
                con.execute(
                    "UPDATE audit_orders SET status=? WHERE order_id=?",
                    [status, order_id],
                )
        except Exception:
            log.exception("update_order_status 失败 order_id=%s", order_id)

    # ------------------------------------------------------------------
    # Read API
    # ------------------------------------------------------------------

    def get_chain(self, signal_id: str) -> AuditChain:
        """查询完整的 signal → orders → fills 链路。"""
        signal: Optional[AuditSignal] = None
        orders: List[AuditOrder] = []
        fills: List[AuditFill] = []
        try:
            with self._db.get_read_connection() as con:
                sig_row = con.execute(
                    "SELECT * FROM audit_signals WHERE signal_id=?", [signal_id]
                ).fetchone()
                if sig_row:
                    signal = AuditSignal(*sig_row)

                ord_rows = con.execute(
                    "SELECT * FROM audit_orders WHERE signal_id=?", [signal_id]
                ).fetchall()
                orders = [AuditOrder(*r) for r in ord_rows]

                if orders:
                    ids = [o.order_id for o in orders]
                    placeholders = ",".join(["?"] * len(ids))
                    fill_rows = con.execute(
                        "SELECT * FROM audit_fills WHERE order_id IN (" + placeholders + ")",
                        ids,
                    ).fetchall()
                    fills = [AuditFill(*r) for r in fill_rows]
        except Exception:
            log.exception("get_chain 失败 signal_id=%s", signal_id)
        return AuditChain(signal=signal, orders=orders, fills=fills)

    def get_signals_by_strategy(
        self, strategy_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """查询策略最近 N 条信号（概览，含成交状态）。"""
        try:
            with self._db.get_read_connection() as con:
                df = con.execute(
                    """
                    SELECT s.signal_id, s.code, s.direction, s.created_at,
                           COUNT(o.order_id) AS order_count,
                           SUM(CASE WHEN o.status='filled' THEN 1 ELSE 0 END) AS filled_count
                    FROM audit_signals s
                    LEFT JOIN audit_orders o ON s.signal_id = o.signal_id
                    WHERE s.strategy_id = ?
                    GROUP BY s.signal_id, s.code, s.direction, s.created_at
                    ORDER BY s.created_at DESC
                    LIMIT ?
                    """,
                    [strategy_id, limit],
                ).df()
            return df.to_dict("records")
        except Exception:
            log.exception("get_signals_by_strategy 失败 strategy_id=%s", strategy_id)
            return []

    def get_signals_by_account(
        self, account_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """查询账户最近 N 条信号（概览，含成交状态）。"""
        try:
            with self._db.get_read_connection() as con:
                df = con.execute(
                    """
                    SELECT s.signal_id, s.strategy_id, s.code, s.direction, s.created_at,
                           COUNT(o.order_id) AS order_count,
                           SUM(CASE WHEN o.status='filled' THEN 1 ELSE 0 END) AS filled_count
                    FROM audit_signals s
                    LEFT JOIN audit_orders o ON s.signal_id = o.signal_id
                    WHERE s.account_id = ?
                    GROUP BY s.signal_id, s.strategy_id, s.code, s.direction, s.created_at
                    ORDER BY s.created_at DESC
                    LIMIT ?
                    """,
                    [account_id, limit],
                ).df()
            return df.to_dict("records")
        except Exception:
            log.exception("get_signals_by_account 失败 account_id=%s", account_id)
            return []

    # ------------------------------------------------------------------
    # Hash chain integrity verification
    # ------------------------------------------------------------------

    def verify_chain_integrity(self) -> Dict[str, Any]:
        """
        重新计算每条记录的哈希并与存储值对比，检测字段级篡改。

        Returns::

            {
              "signals":  {"total": N, "tampered": M, "tampered_ids": [...]},
              "orders":   {...},
              "fills":    {...},
              "ok": bool,   # 全部通过时为 True
            }

        注意：此方法检测**字段值修改**（UPDATE 篡改）。
        行删除或插入需结合业务层序列号另行检测。
        """
        results: Dict[str, Any] = {"ok": True}

        # 每个 tuple: (table_name, sql, fields_fn, id_fn, hash_fn, prev_fn, ver_fn)
        # fields_fn  → 用于重算 entry_hash 的字段列表（与写入时 _compute_entry_hash 参数一致）
        # prev_fn    → 当前行存储的 prev_hash
        # ver_fn     → 当前行的 sig_version
        checks = [
            (
                "signals",
                "SELECT signal_id, strategy_id, code, direction, price_hint, volume_hint, "
                "created_at, entry_hash, prev_hash, batch_hash, sig_version "
                "FROM audit_signals ORDER BY created_at ASC, rowid ASC",
                lambda row: row[:7],    # fields for entry_hash
                lambda row: row[0],     # id
                lambda row: row[7],     # entry_hash
                lambda row: row[8],     # prev_hash
                lambda row: row[10],    # sig_version
            ),
            (
                "orders",
                "SELECT order_id, signal_id, code, direction, volume, price, "
                "submitted_at, status, entry_hash, prev_hash, batch_hash, sig_version "
                "FROM audit_orders ORDER BY submitted_at ASC, rowid ASC",
                lambda row: row[:7],
                lambda row: row[0],
                lambda row: row[8],     # entry_hash (index 8, after status at 7)
                lambda row: row[9],     # prev_hash
                lambda row: row[11],    # sig_version
            ),
            (
                "fills",
                "SELECT fill_id, order_id, filled_at, filled_price, filled_volume, "
                "pnl_snapshot, entry_hash, prev_hash, batch_hash, sig_version "
                "FROM audit_fills ORDER BY filled_at ASC, rowid ASC",
                lambda row: row[:6],
                lambda row: row[0],
                lambda row: row[6],     # entry_hash
                lambda row: row[7],     # prev_hash
                lambda row: row[9],     # sig_version
            ),
        ]

        try:
            with self._db.get_read_connection() as con:
                for table, sql, fields_fn, id_fn, hash_fn, prev_fn, ver_fn in checks:
                    rows = con.execute(sql).fetchall()
                    tampered_ids = []
                    chain_break_ids = []
                    prev_entry_hash = ""
                    for row in rows:
                        current_hash = hash_fn(row) or ""
                        # ── 字段篡改检测（所有版本）────────────────────────
                        expected = AuditTrail._compute_entry_hash(list(fields_fn(row)))
                        if current_hash and current_hash != expected:
                            tampered_ids.append(id_fn(row))
                        # ── prev_hash 链式断链检测（仅 sig_version >= 1）───
                        if ver_fn(row) >= 1:
                            stored_prev = prev_fn(row) or ""
                            if stored_prev and stored_prev != prev_entry_hash:
                                chain_break_ids.append(id_fn(row))
                        if current_hash:
                            prev_entry_hash = current_hash
                    results[table] = {
                        "total": len(rows),
                        "tampered": len(tampered_ids),
                        "tampered_ids": tampered_ids,
                        "chain_breaks": len(chain_break_ids),
                        "chain_break_ids": chain_break_ids,
                    }
                    if tampered_ids:
                        results["ok"] = False
                        log.error(
                            "审计链路完整性校验失败 table=%s tampered=%d ids=%s",
                            table, len(tampered_ids), tampered_ids,
                        )
                    if chain_break_ids:
                        results["ok"] = False
                        log.error(
                            "审计链路 prev_hash 断链 table=%s breaks=%d ids=%s",
                            table, len(chain_break_ids), chain_break_ids,
                        )
        except Exception:
            log.exception("verify_chain_integrity 执行失败")
            results["ok"] = False

        return results

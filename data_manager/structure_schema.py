"""
四点三线 N 字原子结构 —— DuckDB 四张核心表 Schema

表职责：
  bar_mapped         原始行情 + 局部除权因子 + 映射后价格（数据入库唯一标准形式）
  structure_analyze  N 字结构识别结果（P0/P1/P2/P3 + 吸引子 + 贝叶斯边界）
  structure_audit    结构生命周期审计日志（100% 可复现，不可篡改）
  signal_structured  结构化信号（LONG/SHORT/EXIT/HOLD + 止损锚点 + 回撤快照）

硬约束（永不妥协）：
  1. bar_mapped.close_raw     = 原始不复权价格
  2. bar_mapped.rights_factor = 周期内局部除权因子（禁止全局前/后复权）
  3. bar_mapped.close_mapped  = close_raw × rights_factor（唯一合法映射价）
  4. structure_analyze 中所有价格字段均指向 close_mapped，禁止使用复权价

用法：
    from data_manager.structure_schema import ensure_structure_tables
    from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

    db = get_db_manager(resolve_duckdb_path())
    ensure_structure_tables(db)
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_manager.duckdb_connection_pool import DuckDBConnectionManager

log = logging.getLogger(__name__)

# 每次修改表结构时递增，写入 structure_schema_migrations 以供可观测
_SCHEMA_VERSION = 1

# ── DDL：四张核心表（IF NOT EXISTS 幂等，可在任意时机调用）─────────────────────

_DDL = """
-- ── Table 1: bar_mapped ──────────────────────────────────────────────────────
-- 原始行情 + 局部除权因子 + 映射后价格
-- trade_date: 毫秒级 UTC 时间戳，严格递增，禁止未来数据
-- rights_factor: 仅纳入本行情区间内已发生的除权事件之累积因子
-- *_mapped = *_raw × rights_factor（拓扑不变性保证，禁止全局复权）
CREATE TABLE IF NOT EXISTS bar_mapped (
    code            VARCHAR  NOT NULL,
    interval        VARCHAR  NOT NULL,   -- '1m'|'5m'|'1d' 等
    trade_date      BIGINT   NOT NULL,   -- UTC 毫秒时间戳
    open_raw        DOUBLE,
    high_raw        DOUBLE,
    low_raw         DOUBLE,
    close_raw       DOUBLE   NOT NULL,
    volume          BIGINT,
    amount          DOUBLE,
    rights_factor   DOUBLE   NOT NULL DEFAULT 1.0,
    open_mapped     DOUBLE,
    high_mapped     DOUBLE,
    low_mapped      DOUBLE,
    close_mapped    DOUBLE   NOT NULL,   -- = close_raw × rights_factor
    PRIMARY KEY (code, interval, trade_date)
);

-- ── Table 2: structure_analyze ───────────────────────────────────────────────
-- N 字结构识别结果，每一行代表一个满足公理 1 的有效结构
-- P0/P1/P2/P3 严格对应公理 1 中的四点定义
-- retrace_ratio = (P2-P1)/(P0-P1)，折返深度，结构强度量化指标
-- status: active（持续中）→ closed（P3 已成立但未反转）→ reversed（公理3触发）
CREATE TABLE IF NOT EXISTS structure_analyze (
    id              VARCHAR  NOT NULL,   -- UUID v4
    code            VARCHAR  NOT NULL,
    interval        VARCHAR  NOT NULL,
    created_at      BIGINT   NOT NULL,   -- 结构首次识别的 UTC 毫秒时间戳
    direction       VARCHAR  NOT NULL,   -- 'up' | 'down'
    -- 四点锚位（价格均为 close_mapped）
    p0_ts           BIGINT   NOT NULL,
    p0_price        DOUBLE   NOT NULL,
    p1_ts           BIGINT   NOT NULL,
    p1_price        DOUBLE   NOT NULL,
    p2_ts           BIGINT   NOT NULL,
    p2_price        DOUBLE   NOT NULL,
    p3_ts           BIGINT   NOT NULL,
    p3_price        DOUBLE   NOT NULL,
    -- 吸引子（DPMM 贝叶斯非参数估计，首期可为 NULL）
    attractor_mean  DOUBLE,
    attractor_std   DOUBLE,
    -- 贝叶斯边界（顺序统计量 P0/P1 分位，首期可为 NULL）
    bayes_lower     DOUBLE,
    bayes_upper     DOUBLE,
    -- 结构强度指标
    retrace_ratio   DOUBLE,              -- (P2-P1)/(P0-P1)
    -- 生命周期
    status          VARCHAR  NOT NULL DEFAULT 'active',  -- active|closed|reversed
    closed_at       BIGINT,              -- 状态变为 closed/reversed 的 UTC 毫秒时间戳
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_sa_code_interval ON structure_analyze (code, interval);
CREATE INDEX IF NOT EXISTS idx_sa_status        ON structure_analyze (status);

-- ── Table 3: structure_audit ─────────────────────────────────────────────────
-- 结构生命周期审计日志（不可篡改，每次状态变更追加一行）
-- event_type: create → 公理1确认; extend → 公理2延续; reverse → 公理3反转; close → 手动闭合
-- snapshot_json: 触发事件时完整的 structure_analyze 行的 JSON 快照（100% 可复现）
CREATE TABLE IF NOT EXISTS structure_audit (
    id              VARCHAR  NOT NULL,   -- UUID v4
    structure_id    VARCHAR  NOT NULL,   -- FK → structure_analyze.id
    code            VARCHAR  NOT NULL,
    interval        VARCHAR  NOT NULL,
    event_type      VARCHAR  NOT NULL,   -- 'create'|'extend'|'reverse'|'close'
    event_ts        BIGINT   NOT NULL,   -- UTC 毫秒时间戳
    snapshot_json   VARCHAR  NOT NULL,   -- 完整结构快照（JSON 字符串）
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_sau_structure_id ON structure_audit (structure_id);
CREATE INDEX IF NOT EXISTS idx_sau_code         ON structure_audit (code, interval);

-- ── Table 4: signal_structured ───────────────────────────────────────────────
-- 结构化交易信号
-- stop_loss_price = P2.price（公理定义的唯一止损锚点，禁止主观设置）
-- stop_loss_distance = |trigger_price - stop_loss_price|
-- drawdown_pct: 信号时刻相对权益峰值的回撤百分比（DrawdownTracker 写入）
-- calmar_snapshot: 信号时刻的 Calmar 比率快照（CAGR / 最大回撤）
CREATE TABLE IF NOT EXISTS signal_structured (
    id                  VARCHAR  NOT NULL,   -- UUID v4
    structure_id        VARCHAR  NOT NULL,   -- FK → structure_analyze.id
    code                VARCHAR  NOT NULL,
    interval            VARCHAR  NOT NULL,
    signal_ts           BIGINT   NOT NULL,   -- UTC 毫秒时间戳
    signal_type         VARCHAR  NOT NULL,   -- 'LONG'|'SHORT'|'EXIT'|'HOLD'
    trigger_price       DOUBLE   NOT NULL,   -- 信号触发价（close_mapped）
    stop_loss_price     DOUBLE   NOT NULL,   -- = P2.price（公理锚定，不可改）
    stop_loss_distance  DOUBLE   NOT NULL,   -- |trigger_price - stop_loss_price|
    drawdown_pct        DOUBLE,              -- 当前回撤 %（DrawdownTracker）
    calmar_snapshot     DOUBLE,              -- Calmar 快照（DrawdownTracker）
    remarks             VARCHAR,
    PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS idx_ss_code      ON signal_structured (code, interval);
CREATE INDEX IF NOT EXISTS idx_ss_signal_ts ON signal_structured (signal_ts);

-- ── Schema 迁移日志 ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS structure_schema_migrations (
    schema_version   INTEGER  PRIMARY KEY,
    migrated_at      BIGINT   NOT NULL,
    elapsed_ms       DOUBLE   NOT NULL
);
"""

# 未来版本追加列时在此处扩展（执行后用 try/except 忽略"列已存在"错误）
_MIGRATION_ADDS: list[tuple[str, str, str]] = [
    # (table_name, column_name, column_definition)
    # 示例：("bar_mapped", "vwap", "DOUBLE")
]


def ensure_structure_tables(db_manager: "DuckDBConnectionManager") -> None:
    """在目标 DuckDB 中确保四张核心表存在（幂等，可在应用启动时调用）。

    参数：
        db_manager: 已初始化的 :class:`DuckDBConnectionManager` 实例。

    副作用：
        - 首次调用时建表并写入 ``structure_schema_migrations`` 版本记录。
        - 后续调用检测版本号一致则跳过，已存在则不重复写入。
        - 对 ``_MIGRATION_ADDS`` 中的追加列执行 ALTER TABLE，字段已存在则静默跳过。
    """
    t0 = time.monotonic()
    try:
        # 建表（幂等 CREATE TABLE IF NOT EXISTS）——必须先于迁移表查询
        db_manager.execute_write_query(_DDL)

        # 检查迁移表是否已记录当前版本
        existing = db_manager.execute_read_query(
            "SELECT schema_version FROM structure_schema_migrations "
            "WHERE schema_version = ?",
            (_SCHEMA_VERSION,),
        )
        if not existing.empty:
            log.debug(
                "structure_schema: v%d 已就位，跳过建表", _SCHEMA_VERSION
            )
            return

        # 追加列迁移（向后兼容）
        for table, col, col_def in _MIGRATION_ADDS:
            try:
                db_manager.execute_write_query(
                    f"ALTER TABLE {table} ADD COLUMN {col} {col_def}"
                )
                log.info("structure_schema: ALTER TABLE %s ADD %s", table, col)
            except Exception:
                pass  # 列已存在，忽略

        elapsed = (time.monotonic() - t0) * 1000
        db_manager.execute_write_query(
            "INSERT OR REPLACE INTO structure_schema_migrations "
            "(schema_version, migrated_at, elapsed_ms) VALUES (?, ?, ?)",
            (_SCHEMA_VERSION, int(time.time() * 1000), round(elapsed, 2)),
        )
        log.info(
            "structure_schema: v%d 建表完成 (%.1f ms)", _SCHEMA_VERSION, elapsed
        )
    except Exception:
        log.exception("structure_schema: ensure_structure_tables 异常")
        raise

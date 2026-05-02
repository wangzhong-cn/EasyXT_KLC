"""七层结构语料导出 / Layer 4 实验底座。

目标：
  - 基于 ``structure_analyze`` / ``structure_audit`` / ``signal_structured``
    构建“一行一结构”的离线实验数据集
  - 为 Layer 4 的 Bayesian baseline / attractor / TDA 预研提供稳定输入
  - 保持零 GUI 依赖、零 Qt 依赖，可在 API / CLI / Notebook 中复用

当前版本输出：
  - 结构主表字段（四点、方向、状态、Layer 4 预留列）
  - 审计摘要（事件计数、最后事件时间）
  - 最新信号摘要（类型、触发价、风控快照）

注意：
  - 这是离线实验底座，不参与实盘实时决策
  - drawdown / calmar 当前仍来自结构层代理，不是账户净值级指标
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import pandas as pd

from data_manager.structure_schema import ensure_structure_tables

if TYPE_CHECKING:
    from data_manager.duckdb_connection_pool import DuckDBConnectionManager

log = logging.getLogger(__name__)

_ALLOWED_DIRECTIONS = {"up", "down"}
_ALLOWED_STATUSES = {"active", "closed", "reversed"}
_ALLOWED_SIGNAL_TYPES = {"LONG", "SHORT", "EXIT", "HOLD"}


class StructureDatasetBuilder:
    """从 DuckDB 七层主线表构建离线结构语料。

    默认一行代表一个 ``structure_analyze`` 结构，左连接：
      - ``structure_audit`` 的事件计数摘要
      - ``signal_structured`` 的最新信号摘要
    """

    def __init__(
        self,
        db_manager: "DuckDBConnectionManager | None" = None,
        duckdb_path: str | None = None,
    ) -> None:
        if db_manager is None:
            from data_manager.duckdb_connection_pool import get_db_manager, resolve_duckdb_path

            db_manager = get_db_manager(duckdb_path or resolve_duckdb_path())
        self._db_manager = db_manager
        ensure_structure_tables(self._db_manager)

    def build_dataset(
        self,
        code: str = "",
        interval: str = "",
        direction: str = "",
        statuses: Sequence[str] | None = None,
        signal_types: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int = 0,
        order_desc: bool = False,
    ) -> pd.DataFrame:
        """构建结构级实验数据集。"""
        if direction and direction not in _ALLOWED_DIRECTIONS:
            raise ValueError(f"direction 必须为 {sorted(_ALLOWED_DIRECTIONS)}，得到 {direction!r}")
        self._validate_allowed("statuses", statuses, _ALLOWED_STATUSES)
        self._validate_allowed("signal_types", signal_types, _ALLOWED_SIGNAL_TYPES)
        if limit is not None and limit <= 0:
            raise ValueError("limit 必须为正整数或 None")
        if offset < 0:
            raise ValueError("offset 不能为负数")

        sql = """
            WITH audit_summary AS (
                SELECT
                    structure_id,
                    COUNT(*) AS audit_event_count,
                    SUM(CASE WHEN event_type = 'create' THEN 1 ELSE 0 END) AS create_event_count,
                    SUM(CASE WHEN event_type = 'extend' THEN 1 ELSE 0 END) AS extend_event_count,
                    SUM(CASE WHEN event_type = 'reverse' THEN 1 ELSE 0 END) AS reverse_event_count,
                    MAX(event_ts) AS last_audit_ts
                FROM structure_audit
                GROUP BY structure_id
            ),
            latest_signal AS (
                SELECT * EXCLUDE (rn)
                FROM (
                    SELECT
                        id AS latest_signal_id,
                        structure_id,
                        signal_ts AS latest_signal_ts,
                        signal_type AS latest_signal_type,
                        trigger_price AS latest_trigger_price,
                        stop_loss_price AS latest_stop_loss_price,
                        stop_loss_distance AS latest_stop_loss_distance,
                        drawdown_pct AS latest_drawdown_pct,
                        calmar_snapshot AS latest_calmar_snapshot,
                        remarks AS latest_signal_remarks,
                        ROW_NUMBER() OVER (
                            PARTITION BY structure_id
                            ORDER BY signal_ts DESC,
                                     CASE WHEN signal_type = 'EXIT' THEN 1 ELSE 0 END DESC,
                                     id DESC
                        ) AS rn
                    FROM signal_structured
                ) ranked
                WHERE rn = 1
            )
            SELECT
                sa.id AS structure_id,
                sa.code,
                sa.interval,
                sa.created_at,
                sa.direction,
                sa.status,
                sa.closed_at,
                sa.p0_ts,
                sa.p0_price,
                sa.p1_ts,
                sa.p1_price,
                sa.p2_ts,
                sa.p2_price,
                sa.p3_ts,
                sa.p3_price,
                sa.retrace_ratio,
                sa.attractor_mean,
                sa.attractor_std,
                sa.bayes_lower,
                sa.bayes_upper,
                COALESCE(a.audit_event_count, 0) AS audit_event_count,
                COALESCE(a.create_event_count, 0) AS create_event_count,
                COALESCE(a.extend_event_count, 0) AS extend_event_count,
                COALESCE(a.reverse_event_count, 0) AS reverse_event_count,
                a.last_audit_ts,
                CASE WHEN ls.latest_signal_id IS NULL THEN 0 ELSE 1 END AS has_signal,
                ls.latest_signal_id,
                ls.latest_signal_ts,
                ls.latest_signal_type,
                ls.latest_trigger_price,
                ls.latest_stop_loss_price,
                ls.latest_stop_loss_distance,
                ls.latest_drawdown_pct,
                ls.latest_calmar_snapshot,
                ls.latest_signal_remarks,
                COALESCE(sa.closed_at, sa.created_at) - sa.p0_ts AS lifecycle_duration_ms,
                CASE
                    WHEN sa.closed_at IS NULL THEN NULL
                    ELSE sa.closed_at - sa.created_at
                END AS post_confirmation_window_ms
            FROM structure_analyze sa
            LEFT JOIN audit_summary a ON a.structure_id = sa.id
            LEFT JOIN latest_signal ls ON ls.structure_id = sa.id
        """

        clauses: list[str] = []
        params: list[Any] = []
        if code:
            clauses.append("sa.code = ?")
            params.append(code)
        if interval:
            clauses.append("sa.interval = ?")
            params.append(interval)
        if direction:
            clauses.append("sa.direction = ?")
            params.append(direction)
        if statuses:
            clauses.append(self._in_clause("sa.status", statuses, params))
        if signal_types:
            clauses.append(self._in_clause("ls.latest_signal_type", signal_types, params))
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)

        order_by = "DESC" if order_desc else "ASC"
        sql += f" ORDER BY sa.created_at {order_by}, sa.id {order_by}"
        if limit is not None:
            sql += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            sql += " LIMIT 9223372036854775807 OFFSET ?"
            params.append(offset)

        df = self._db_manager.execute_read_query(sql, tuple(params))
        if df.empty:
            return df
        return df.where(df.notna(), other=None)

    def export_dataset(
        self,
        output_path: str | Path,
        dataset: pd.DataFrame | None = None,
        **build_kwargs: Any,
    ) -> Path:
        """导出结构语料到 CSV / Parquet。"""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        dataset = dataset if dataset is not None else self.build_dataset(**build_kwargs)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            dataset.to_csv(path, index=False, encoding="utf-8-sig")
        elif suffix == ".parquet":
            dataset.to_parquet(path, index=False)
        else:
            raise ValueError("output_path 后缀必须是 .csv 或 .parquet")
        log.info("StructureDatasetBuilder 导出完成: %s rows=%d", path, len(dataset))
        return path

    @staticmethod
    def _validate_allowed(
        field_name: str,
        values: Sequence[str] | None,
        allowed: set[str],
    ) -> None:
        if not values:
            return
        bad = sorted(set(values) - allowed)
        if bad:
            raise ValueError(f"{field_name} 含非法值 {bad}，可选: {sorted(allowed)}")

    @staticmethod
    def _in_clause(column: str, values: Sequence[str], params: list[Any]) -> str:
        placeholders = ", ".join("?" for _ in values)
        params.extend(values)
        return f"{column} IN ({placeholders})"
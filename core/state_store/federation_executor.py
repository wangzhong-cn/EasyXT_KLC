from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence

from .federation_planner import DuckDBFederationPlanner, FederationPlan


@dataclass(frozen=True, slots=True)
class FederationQueryResult:
    plan: FederationPlan
    attach_sql: list[str]
    query_sql: str
    rows: list[dict[str, Any]]
    row_count: int
    latest_logical_seq: int | None


class DuckDBFederationExecutor:
    """执行 DuckDB 联邦读计划。

    当前阶段的职责：

    - 调用 planner 生成 shard 裁剪结果
    - 执行 `ATTACH ... (TYPE SQLITE)` 语句
    - 执行 `UNION ALL` 查询 SQL
    - 返回行数据 + logical sequence metadata

    真正的 API 读模型接线可以在此基础上继续追加缓存、模板化 SQL 与只读连接池。
    """

    def __init__(
        self,
        planner: DuckDBFederationPlanner,
        *,
        connection_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.planner = planner
        self.connection_factory = connection_factory or self._default_connection_factory

    def execute_family_query(
        self,
        family_name: str,
        *,
        start_time: Any = None,
        end_time: Any = None,
        symbol: str | None = None,
        attach_budget: int | None = None,
        selected_columns: list[str] | None = None,
        where_sql: str | None = None,
        order_by: str | None = None,
        query_params: Sequence[Any] | None = None,
        limit: int | None = None,
    ) -> FederationQueryResult:
        plan = self.planner.plan_family_range(
            family_name,
            start_time=start_time,
            end_time=end_time,
            symbol=symbol,
            attach_budget=attach_budget,
        )
        attach_sql = self.planner.build_attach_sql(plan)
        query_sql = self.planner.build_union_sql(
            plan,
            selected_columns=selected_columns,
            where_sql=where_sql,
            order_by=order_by,
            limit=limit,
        )
        connection = self.connection_factory()
        try:
            for statement in attach_sql:
                connection.execute(statement)
            prepared_params = self._prepare_query_params(query_sql, query_params)
            cursor = (
                connection.execute(query_sql, prepared_params)
                if prepared_params is not None
                else connection.execute(query_sql)
            )
            rows = self._cursor_to_records(cursor)
        finally:
            for binding in reversed(plan.bindings):
                try:
                    connection.execute(f"DETACH {binding.alias};")
                except Exception:
                    pass
            try:
                connection.close()
            except Exception:
                pass

        logical_seq_candidates = [item.logical_seq_end for item in plan.bindings if item.logical_seq_end is not None]
        latest_logical_seq = max(logical_seq_candidates) if logical_seq_candidates else None
        return FederationQueryResult(
            plan=plan,
            attach_sql=attach_sql,
            query_sql=query_sql,
            rows=rows,
            row_count=len(rows),
            latest_logical_seq=latest_logical_seq,
        )

    @staticmethod
    def _cursor_to_records(cursor: Any) -> list[dict[str, Any]]:
        description = getattr(cursor, "description", None) or []
        columns = [str(item[0]) for item in description]
        raw_rows = cursor.fetchall()
        return [dict(zip(columns, row, strict=False)) for row in raw_rows]

    @staticmethod
    def _prepare_query_params(
        query_sql: str,
        query_params: Sequence[Any] | None,
    ) -> tuple[Any, ...] | None:
        if not query_params:
            return None
        base_params = tuple(query_params)
        placeholder_count = query_sql.count("?")
        if placeholder_count == 0:
            raise ValueError("query_params 已提供，但 SQL 中不存在占位符 '?'")
        if placeholder_count == len(base_params):
            return base_params
        if placeholder_count % len(base_params) != 0:
            raise ValueError(
                "query_params 数量与 SQL 占位符数量不匹配："
                f"placeholders={placeholder_count}, params={len(base_params)}"
            )
        return base_params * (placeholder_count // len(base_params))

    @staticmethod
    def _default_connection_factory() -> Any:
        import duckdb  # noqa: PLC0415

        connection = duckdb.connect(":memory:")
        try:
            connection.execute("LOAD sqlite;")
        except Exception:
            pass
        return connection
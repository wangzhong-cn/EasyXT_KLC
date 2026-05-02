from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from .shard_catalog import ShardCatalog, ShardRef


class AttachBudgetExceededError(RuntimeError):
    """当计划附加的 SQLite 分片数量超过预算时抛出。"""


@dataclass(frozen=True, slots=True)
class FederationBinding:
    alias: str
    shard_id: str
    family_name: str
    table_name: str
    db_path: str
    logical_seq_end: int | None


@dataclass(frozen=True, slots=True)
class FederationPlan:
    family_name: str
    attach_budget: int
    bindings: list[FederationBinding]
    pruned_shard_count: int

    @property
    def attached_shard_count(self) -> int:
        return len(self.bindings)


class DuckDBFederationPlanner:
    """根据 shard catalog 生成 DuckDB 联邦读计划。

    当前阶段先实现 planner，而不直接在此处执行 DuckDB 查询。这样做的目的：

    - 先把 shard pruning / attach budget 变成显式规则
    - 后续真正执行时，可由 API / 读模型层消费同一份计划
    """

    def __init__(self, catalog: ShardCatalog, *, attach_budget: int = 8) -> None:
        if attach_budget < 1:
            raise ValueError("attach_budget 必须 >= 1")
        self.catalog = catalog
        self.attach_budget = attach_budget

    def plan_family_range(
        self,
        family_name: str,
        *,
        start_time: datetime | date | str | None = None,
        end_time: datetime | date | str | None = None,
        symbol: str | None = None,
        attach_budget: int | None = None,
    ) -> FederationPlan:
        effective_budget = attach_budget or self.attach_budget
        shards = self.catalog.select_shards_for_range(
            family_name,
            start_time=start_time,
            end_time=end_time,
            symbol=symbol,
        )
        if len(shards) > effective_budget:
            raise AttachBudgetExceededError(
                f"family={family_name} 需要附加 {len(shards)} 个分片，超过预算 {effective_budget}；"
                "请进一步裁剪时间范围、symbol scope，或提升 attach budget。"
            )
        bindings = [self._binding_from_shard(index, shard) for index, shard in enumerate(shards)]
        return FederationPlan(
            family_name=family_name,
            attach_budget=effective_budget,
            bindings=bindings,
            pruned_shard_count=len(shards),
        )

    def build_attach_sql(self, plan: FederationPlan) -> list[str]:
        statements: list[str] = []
        for binding in plan.bindings:
            escaped_path = binding.db_path.replace("'", "''")
            statements.append(
                f"ATTACH '{escaped_path}' AS {binding.alias} (TYPE SQLITE);"
            )
        return statements

    def build_union_sql(
        self,
        plan: FederationPlan,
        *,
        selected_columns: list[str] | None = None,
        where_sql: str | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> str:
        if not plan.bindings:
            raise ValueError("空计划无法生成 SQL")
        columns = ", ".join(selected_columns or ["*"])
        union_parts: list[str] = []
        for binding in plan.bindings:
            sql = f"SELECT {columns} FROM {binding.alias}.{binding.table_name}"
            if where_sql:
                sql += f" WHERE {where_sql}"
            union_parts.append(sql)
        union_sql = " UNION ALL ".join(union_parts)
        if order_by or limit is not None:
            wrapped_sql = f"SELECT * FROM ({union_sql}) AS federated_rows"
            if order_by:
                wrapped_sql += f" ORDER BY {order_by}"
            if limit is not None:
                wrapped_sql += f" LIMIT {int(limit)}"
            return wrapped_sql
        return union_sql

    @staticmethod
    def _binding_from_shard(index: int, shard: ShardRef) -> FederationBinding:
        return FederationBinding(
            alias=f"s{index}",
            shard_id=shard.shard_id,
            family_name=shard.family_name,
            table_name=shard.table_name,
            db_path=shard.db_path,
            logical_seq_end=shard.logical_seq_end,
        )
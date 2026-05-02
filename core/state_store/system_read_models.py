from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from .federation_executor import DuckDBFederationExecutor
from .federation_planner import DuckDBFederationPlanner
from .shard_catalog import ShardCatalog
from .system_status import resolve_federation_attach_budget, resolve_state_root


@dataclass(frozen=True, slots=True)
class FrontendEventsReadModel:
    configured: bool
    family_registered: bool
    state_root: str
    source: str
    items: list[dict[str, Any]]
    returned: int
    latest_logical_seq: int | None
    attached_shards: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def read_frontend_events_read_model(
    *,
    limit: int = 20,
    event_type: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    state_root: str | Path | None = None,
    attach_budget: int | None = None,
    executor_factory: Callable[[DuckDBFederationPlanner], DuckDBFederationExecutor] | None = None,
) -> FrontendEventsReadModel:
    resolved_state_root = resolve_state_root(state_root)
    catalog_path = resolved_state_root / "catalog" / "shard_catalog.db"
    if not catalog_path.exists():
        return FrontendEventsReadModel(
            configured=False,
            family_registered=False,
            state_root=str(resolved_state_root),
            source="federation_executor",
            items=[],
            returned=0,
            latest_logical_seq=None,
            attached_shards=0,
        )

    catalog = ShardCatalog(resolved_state_root)
    try:
        catalog.get_family("frontend_events")
    except KeyError:
        return FrontendEventsReadModel(
            configured=True,
            family_registered=False,
            state_root=str(resolved_state_root),
            source="federation_executor",
            items=[],
            returned=0,
            latest_logical_seq=None,
            attached_shards=0,
        )

    effective_budget = attach_budget or resolve_federation_attach_budget()
    planner = DuckDBFederationPlanner(catalog, attach_budget=effective_budget)
    executor = executor_factory(planner) if executor_factory else DuckDBFederationExecutor(planner)

    where_clauses: list[str] = []
    query_params: list[Any] = []
    if event_type:
        where_clauses.append("event_type = ?")
        query_params.append(event_type)
    if start_time:
        where_clauses.append("event_ts >= ?")
        query_params.append(start_time)
    if end_time:
        where_clauses.append("event_ts < ?")
        query_params.append(end_time)

    result = executor.execute_family_query(
        "frontend_events",
        start_time=start_time,
        end_time=end_time,
        selected_columns=["event_id", "event_ts", "event_type", "payload_json"],
        where_sql=" AND ".join(where_clauses) if where_clauses else None,
        order_by="event_ts DESC, event_id DESC",
        query_params=query_params or None,
        limit=limit,
    )

    items = [_serialize_frontend_event(row) for row in result.rows]
    return FrontendEventsReadModel(
        configured=True,
        family_registered=True,
        state_root=str(resolved_state_root),
        source="federation_executor",
        items=items,
        returned=len(items),
        latest_logical_seq=result.latest_logical_seq,
        attached_shards=result.plan.attached_shard_count,
    )


def _serialize_frontend_event(row: dict[str, Any]) -> dict[str, Any]:
    raw_payload = row.get("payload_json")
    parsed_payload: Any = None
    if isinstance(raw_payload, str) and raw_payload:
        try:
            parsed_payload = json.loads(raw_payload)
        except Exception:
            parsed_payload = None
    return {
        "event_id": row.get("event_id"),
        "event_ts": row.get("event_ts"),
        "event_type": row.get("event_type"),
        "payload": parsed_payload,
        "raw_payload_json": raw_payload,
    }
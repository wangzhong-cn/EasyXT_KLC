from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from data_manager.period_registry import PeriodRegistry
from data_manager.session_profile_registry import SessionProfileRegistry
from data_manager.threshold_registry import ThresholdRegistry


def _normalize_trade_date(value: date | datetime | str | None) -> str:
    if value is None:
        return date.today().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return date.today().isoformat()
    return text[:10]


def build_governance_snapshot(
    *,
    symbol: str | None = None,
    trade_date: date | datetime | str | None = None,
    periods: list[str] | tuple[str, ...] | None = None,
    base_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    period_registry = PeriodRegistry(base_path=base_path)
    threshold_registry = ThresholdRegistry(
        period_registry=period_registry,
        base_path=base_path,
    )
    session_registry = SessionProfileRegistry(base_path=base_path)
    session_profile = session_registry.resolve(
        symbol=symbol,
        trade_date=normalized_trade_date,
        explicit_profile=os.environ.get("EASYXT_SESSION_PROFILE"),
    )
    payload: dict[str, Any] = {
        "symbol": str(symbol or "").strip(),
        "trade_date": normalized_trade_date,
        "session_profile_id": session_profile.profile_id,
        "session_profile_version": session_profile.profile_version,
        "auction_policy": session_profile.auction_policy,
        "period_registry_version": period_registry.registry_version,
        "threshold_registry_version": threshold_registry.registry_version,
    }
    normalized_periods = [str(item).strip() for item in periods or [] if str(item).strip()]
    if normalized_periods:
        period_metadata: dict[str, dict[str, str]] = {}
        for period in normalized_periods:
            resolved_period = period_registry.resolve(period)
            resolved_threshold = threshold_registry.resolve(
                period,
                as_of_date=normalized_trade_date,
            )
            period_metadata[period] = {
                "period_code": resolved_period.period_code,
                "period_family": resolved_period.period_family,
                "threshold_version": resolved_threshold.threshold_version,
            }
        payload["period_metadata"] = period_metadata
    return payload

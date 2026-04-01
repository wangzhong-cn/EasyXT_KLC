from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

from data_manager.period_registry import PeriodRegistry

DEFAULT_THRESHOLD_REGISTRY_VERSION = "legacy"
_SOURCE_GRADE_ORDER = {
    "unknown": 0,
    "degraded": 1,
    "partial_trust": 2,
    "golden": 3,
}


@dataclass(frozen=True)
class ThresholdRule:
    rule_id: str
    threshold_version: str
    period_codes: tuple[str, ...]
    period_families: tuple[str, ...]
    market_scope: tuple[str, ...]
    source_grade_floor: str
    ohlc_tolerance: float
    volume_tolerance: float
    bar_count_tolerance: float
    cross_source_overlap_min: float
    effective_from: str | None
    effective_to: str | None
    priority: int
    description: str = ""


@dataclass(frozen=True)
class ResolvedThresholds:
    requested_period: str
    canonical_period: str
    runtime_period: str
    period_family: str
    registry_version: str
    rule_id: str
    threshold_version: str
    market: str | None
    source_grade: str
    source_grade_floor: str
    meets_source_grade_floor: bool
    ohlc_tolerance: float
    volume_tolerance: float
    bar_count_tolerance: float
    cross_source_overlap_min: float
    description: str = ""


class ThresholdRegistry:
    """周期阈值注册表。"""

    def __init__(
        self,
        file_path: str | None = None,
        *,
        period_registry: PeriodRegistry | None = None,
        period_registry_file: str | None = None,
        base_path: str | Path | None = None,
    ) -> None:
        self._base_path = Path(base_path) if base_path is not None else Path.cwd()
        self._file_path = _resolve_path(file_path or "config/period_thresholds.json", self._base_path)
        self._period_registry = period_registry or PeriodRegistry(
            file_path=period_registry_file,
            base_path=self._base_path,
        )
        self._registry_version = DEFAULT_THRESHOLD_REGISTRY_VERSION
        self._default_rule_id = ""
        self._rules: list[ThresholdRule] = []
        self._load()

    @property
    def registry_version(self) -> str:
        return self._registry_version

    def resolve(
        self,
        period: str | None,
        *,
        market: str | None = None,
        source_grade: str | None = None,
        as_of_date: date | datetime | str | None = None,
    ) -> ResolvedThresholds:
        resolved_period = self._period_registry.resolve(period)
        normalized_market = _normalize_market(market)
        normalized_grade = _normalize_source_grade(source_grade)
        day = _coerce_date(as_of_date) or date.today()
        matched_rule = self._match_rule(
            period_code=resolved_period.period_code,
            period_family=resolved_period.period_family,
            market=normalized_market,
            as_of_date=day,
        )
        if matched_rule is None:
            raise ValueError(f"未找到阈值规则: {resolved_period.period_code}")
        return ResolvedThresholds(
            requested_period=str(period or "").strip(),
            canonical_period=resolved_period.period_code,
            runtime_period=resolved_period.runtime_code or resolved_period.period_code,
            period_family=resolved_period.period_family,
            registry_version=self._registry_version,
            rule_id=matched_rule.rule_id,
            threshold_version=matched_rule.threshold_version,
            market=normalized_market,
            source_grade=normalized_grade,
            source_grade_floor=matched_rule.source_grade_floor,
            meets_source_grade_floor=_grade_rank(normalized_grade)
            >= _grade_rank(matched_rule.source_grade_floor),
            ohlc_tolerance=matched_rule.ohlc_tolerance,
            volume_tolerance=matched_rule.volume_tolerance,
            bar_count_tolerance=matched_rule.bar_count_tolerance,
            cross_source_overlap_min=matched_rule.cross_source_overlap_min,
            description=matched_rule.description,
        )

    def _load(self) -> None:
        if not self._file_path.exists():
            return
        try:
            payload_any = json.loads(self._file_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload_any, dict):
            return
        payload = cast(dict[str, Any], payload_any)
        self._registry_version = str(
            payload.get("registry_version") or DEFAULT_THRESHOLD_REGISTRY_VERSION
        ).strip() or DEFAULT_THRESHOLD_REGISTRY_VERSION
        self._default_rule_id = str(payload.get("default_rule_id") or "").strip()
        rules_any = payload.get("rules")
        if not isinstance(rules_any, list):
            return
        parsed: list[ThresholdRule] = []
        for order, item_any in enumerate(cast(list[Any], rules_any)):
            if not isinstance(item_any, dict):
                continue
            item = cast(dict[str, Any], item_any)
            rule = self._parse_rule(item, order)
            if rule is not None:
                parsed.append(rule)
        self._rules = sorted(parsed, key=lambda item: (-item.priority, item.rule_id))

    def _parse_rule(self, payload: dict[str, Any], order: int) -> ThresholdRule | None:
        rule_id = str(payload.get("rule_id") or f"rule_{order + 1}").strip()
        return ThresholdRule(
            rule_id=rule_id,
            threshold_version=str(
                payload.get("threshold_version") or DEFAULT_THRESHOLD_REGISTRY_VERSION
            ).strip(),
            period_codes=_normalize_list(payload.get("period_codes")),
            period_families=_normalize_list(payload.get("period_families")),
            market_scope=_normalize_list(payload.get("market_scope"), upper=True),
            source_grade_floor=_normalize_source_grade(payload.get("source_grade_floor")),
            ohlc_tolerance=_as_float(payload.get("ohlc_tolerance"), 0.0),
            volume_tolerance=_as_float(payload.get("volume_tolerance"), 0.0),
            bar_count_tolerance=_as_float(payload.get("bar_count_tolerance"), 0.0),
            cross_source_overlap_min=_as_float(payload.get("cross_source_overlap_min"), 0.0),
            effective_from=_normalize_optional_date(payload.get("effective_from")),
            effective_to=_normalize_optional_date(payload.get("effective_to")),
            priority=_as_int(payload.get("priority"), 0),
            description=str(payload.get("description") or "").strip(),
        )

    def _match_rule(
        self,
        *,
        period_code: str,
        period_family: str,
        market: str | None,
        as_of_date: date,
    ) -> ThresholdRule | None:
        fallback: ThresholdRule | None = None
        for rule in self._rules:
            if rule.rule_id == self._default_rule_id:
                fallback = rule
            if rule.period_codes and period_code not in rule.period_codes:
                continue
            if rule.period_families and period_family not in rule.period_families:
                continue
            if rule.market_scope and (market or "") not in rule.market_scope:
                continue
            if not _date_in_range(as_of_date, rule.effective_from, rule.effective_to):
                continue
            return rule
        return fallback


def _resolve_path(path_value: str, base_path: Path) -> Path:
    path = Path(str(path_value).strip())
    if not path.is_absolute():
        path = (base_path / path).resolve()
    return path


def _normalize_list(value: Any, *, upper: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items: list[Any] = [value]
    elif isinstance(value, (list, tuple, set)):
        items = list(cast(list[Any] | tuple[Any, ...] | set[Any], value))
    else:
        return ()
    out: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        out.append(text.upper() if upper else text)
    return tuple(out)


def _normalize_market(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _normalize_source_grade(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    return text if text in _SOURCE_GRADE_ORDER else "unknown"


def _grade_rank(value: str) -> int:
    return _SOURCE_GRADE_ORDER.get(_normalize_source_grade(value), 0)


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _normalize_optional_date(value: Any) -> str | None:
    parsed = _coerce_date(value)
    return parsed.isoformat() if parsed is not None else None


def _coerce_date(value: date | datetime | str | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and "-" in text[:10]:
        try:
            return date.fromisoformat(text[:10])
        except Exception:
            return None
    if len(text) >= 8 and text[:8].isdigit():
        try:
            return datetime.strptime(text[:8], "%Y%m%d").date()
        except Exception:
            return None
    return None


def _date_in_range(day: date, start: str | None, end: str | None) -> bool:
    start_date = _coerce_date(start)
    end_date = _coerce_date(end)
    if start_date is not None and day < start_date:
        return False
    if end_date is not None and day > end_date:
        return False
    return True


__all__ = [
    "DEFAULT_THRESHOLD_REGISTRY_VERSION",
    "ResolvedThresholds",
    "ThresholdRegistry",
    "ThresholdRule",
]

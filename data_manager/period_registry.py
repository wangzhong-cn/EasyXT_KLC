from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

DEFAULT_PERIOD_REGISTRY_VERSION = "legacy"
_CALENDAR_FREQ_BY_PERIOD_CODE: dict[str, str] = {
    "1W": "W-FRI",
    "1M": "ME",
    "1Q": "QE-DEC",
    "6M": "6ME",
    "1Y": "YE",
    "2Y": "2YE",
    "3Y": "3YE",
    "5Y": "5YE",
    "10Y": "10YE",
}


@dataclass(frozen=True)
class PeriodDefinition:
    period_code: str
    runtime_code: str | None
    aliases: tuple[str, ...]
    layer: str
    period_family: str
    base_source: str
    alignment: str
    anchor: str
    precompute_default: bool
    ui_visible_default: bool
    validation_level: str
    coverage_mode: str
    supports_partial: bool
    source_grade_floor: str
    tick_verifiable: bool
    quality_required: str
    enabled: bool
    description: str = ""


@dataclass(frozen=True)
class ResolvedPeriod:
    requested_code: str
    matched_alias: str
    registry_version: str
    period_code: str
    runtime_code: str | None
    aliases: tuple[str, ...]
    layer: str
    period_family: str
    base_source: str
    alignment: str
    anchor: str
    precompute_default: bool
    ui_visible_default: bool
    validation_level: str
    coverage_mode: str
    supports_partial: bool
    source_grade_floor: str
    tick_verifiable: bool
    quality_required: str
    enabled: bool
    description: str = ""


class PeriodRegistry:
    """周期注册表：治理码与运行时代码分离。"""

    def __init__(
        self,
        file_path: str | None = None,
        *,
        base_path: str | Path | None = None,
    ) -> None:
        self._base_path = Path(base_path) if base_path is not None else Path.cwd()
        self._file_path = _resolve_path(file_path or "config/period_registry.json", self._base_path)
        self._registry_version = DEFAULT_PERIOD_REGISTRY_VERSION
        self._definitions_by_code: dict[str, PeriodDefinition] = {}
        self._lookup: dict[str, tuple[str, str]] = {}
        self._load()

    @property
    def registry_version(self) -> str:
        return self._registry_version

    def list_period_codes(self, *, enabled_only: bool = True) -> list[str]:
        return sorted(
            code
            for code, definition in self._definitions_by_code.items()
            if (definition.enabled or not enabled_only)
        )

    def resolve(self, period: str | None, *, allow_disabled: bool = False) -> ResolvedPeriod:
        requested = str(period or "").strip()
        if not requested:
            raise ValueError("period 不能为空")
        match = self._lookup.get(requested)
        if match is None:
            raise ValueError(f"未知周期: {requested}")
        period_code, matched_alias = match
        definition = self._definitions_by_code[period_code]
        if not allow_disabled and not definition.enabled:
            raise ValueError(f"周期 {requested} 已注册但尚未启用: {period_code}")
        return ResolvedPeriod(
            requested_code=requested,
            matched_alias=matched_alias,
            registry_version=self._registry_version,
            period_code=definition.period_code,
            runtime_code=definition.runtime_code,
            aliases=definition.aliases,
            layer=definition.layer,
            period_family=definition.period_family,
            base_source=definition.base_source,
            alignment=definition.alignment,
            anchor=definition.anchor,
            precompute_default=definition.precompute_default,
            ui_visible_default=definition.ui_visible_default,
            validation_level=definition.validation_level,
            coverage_mode=definition.coverage_mode,
            supports_partial=definition.supports_partial,
            source_grade_floor=definition.source_grade_floor,
            tick_verifiable=definition.tick_verifiable,
            quality_required=definition.quality_required,
            enabled=definition.enabled,
            description=definition.description,
        )

    def canonical_code(self, period: str | None, *, allow_disabled: bool = False) -> str:
        return self.resolve(period, allow_disabled=allow_disabled).period_code

    def runtime_code(self, period: str | None, *, allow_disabled: bool = False) -> str:
        resolved = self.resolve(period, allow_disabled=allow_disabled)
        return resolved.runtime_code or resolved.period_code

    def list_definitions(self, *, enabled_only: bool = True) -> list[PeriodDefinition]:
        definitions = list(self._definitions_by_code.values())
        if enabled_only:
            definitions = [item for item in definitions if item.enabled]
        return sorted(definitions, key=lambda item: item.period_code)

    def get_intraday_runtime_minutes(self, *, enabled_only: bool = True) -> dict[str, int]:
        out: dict[str, int] = {}
        for definition in self.list_definitions(enabled_only=enabled_only):
            if definition.period_family != "intraday":
                continue
            runtime = definition.runtime_code or definition.period_code
            minutes = _parse_intraday_minutes(runtime) or _parse_intraday_minutes(definition.period_code)
            if minutes is None:
                continue
            out[runtime] = minutes
        return out

    def get_multiday_runtime_days(self, *, enabled_only: bool = True) -> dict[str, int]:
        out: dict[str, int] = {}
        for definition in self.list_definitions(enabled_only=enabled_only):
            if definition.period_family != "multiday_trading":
                continue
            runtime = definition.runtime_code or definition.period_code
            trading_days = (
                _parse_multiday_trading_days(definition.period_code)
                or _parse_multiday_trading_days(runtime)
            )
            if trading_days is None:
                continue
            out[runtime] = trading_days
        return out

    def get_calendar_runtime_aggregation(
        self, *, enabled_only: bool = True
    ) -> dict[str, tuple[str, str]]:
        out: dict[str, tuple[str, str]] = {}
        for definition in self.list_definitions(enabled_only=enabled_only):
            if definition.period_family != "natural_calendar":
                continue
            runtime = definition.runtime_code or definition.period_code
            freq = _CALENDAR_FREQ_BY_PERIOD_CODE.get(definition.period_code)
            if not freq:
                continue
            out[runtime] = (definition.base_source or "1d", freq)
        return out

    def get_precompute_runtime_periods(self, *, enabled_only: bool = True) -> list[str]:
        out: list[str] = []
        for definition in self.list_definitions(enabled_only=enabled_only):
            if not definition.precompute_default:
                continue
            runtime = definition.runtime_code or definition.period_code
            if runtime not in out:
                out.append(runtime)
        return out

    def get_precompute_base_period_map(self, *, enabled_only: bool = True) -> dict[str, str]:
        out: dict[str, str] = {}
        for definition in self.list_definitions(enabled_only=enabled_only):
            if not definition.precompute_default:
                continue
            runtime = definition.runtime_code or definition.period_code
            out[runtime] = definition.base_source or runtime
        return out

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
            payload.get("registry_version") or DEFAULT_PERIOD_REGISTRY_VERSION
        ).strip() or DEFAULT_PERIOD_REGISTRY_VERSION
        periods_any = payload.get("periods")
        if not isinstance(periods_any, list):
            return
        for item_any in cast(list[Any], periods_any):
            if not isinstance(item_any, dict):
                continue
            item = cast(dict[str, Any], item_any)
            definition = self._parse_definition(item)
            if definition is None:
                continue
            self._definitions_by_code[definition.period_code] = definition
            self._register_lookup(definition.period_code, definition.period_code)
            for alias in definition.aliases:
                self._register_lookup(alias, definition.period_code)

    def _parse_definition(self, payload: dict[str, Any]) -> PeriodDefinition | None:
        period_code = str(payload.get("period_code") or "").strip()
        if not period_code:
            return None
        runtime_raw = payload.get("runtime_code")
        runtime_code = None if runtime_raw is None else str(runtime_raw).strip() or None
        aliases = _normalize_aliases(payload.get("aliases"), period_code)
        return PeriodDefinition(
            period_code=period_code,
            runtime_code=runtime_code,
            aliases=aliases,
            layer=str(payload.get("layer") or "derived").strip(),
            period_family=str(payload.get("period_family") or "unknown").strip(),
            base_source=str(payload.get("base_source") or "").strip(),
            alignment=str(payload.get("alignment") or "").strip(),
            anchor=str(payload.get("anchor") or "").strip(),
            precompute_default=_as_bool(payload.get("precompute_default"), False),
            ui_visible_default=_as_bool(payload.get("ui_visible_default"), False),
            validation_level=str(payload.get("validation_level") or "default").strip(),
            coverage_mode=str(payload.get("coverage_mode") or "default").strip(),
            supports_partial=_as_bool(payload.get("supports_partial"), False),
            source_grade_floor=str(payload.get("source_grade_floor") or "unknown").strip(),
            tick_verifiable=_as_bool(payload.get("tick_verifiable"), False),
            quality_required=str(payload.get("quality_required") or "unknown").strip(),
            enabled=_as_bool(payload.get("enabled"), True),
            description=str(payload.get("description") or "").strip(),
        )

    def _register_lookup(self, token: str, period_code: str) -> None:
        key = str(token).strip()
        if not key:
            return
        existing = self._lookup.get(key)
        if existing is not None and existing[0] != period_code:
            raise ValueError(f"period alias 冲突: {key} -> {existing[0]} / {period_code}")
        self._lookup[key] = (period_code, key)


def _resolve_path(path_value: str, base_path: Path) -> Path:
    path = Path(str(path_value).strip())
    if not path.is_absolute():
        path = (base_path / path).resolve()
    return path


def _normalize_aliases(value: Any, period_code: str) -> tuple[str, ...]:
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
        if not text or text == period_code:
            continue
        out.append(text)
    return tuple(out)


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_intraday_minutes(code: str | None) -> int | None:
    text = str(code or "").strip().lower()
    if not text.endswith("m"):
        return None
    digits = text[:-1]
    if not digits.isdigit():
        return None
    return int(digits)


def _parse_multiday_trading_days(code: str | None) -> int | None:
    text = str(code or "").strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered.endswith("d") and lowered[:-1].isdigit():
        return int(lowered[:-1])
    legacy_trade_months = {
        "2M_TRD": 42,
        "3M_TRD": 63,
        "5M_TRD": 105,
        "2M": 42,
        "3M": 63,
        "5M": 105,
    }
    return legacy_trade_months.get(text.upper())


def build_period_runtime_contracts(
    file_path: str | None = None,
    *,
    base_path: str | Path | None = None,
    enabled_only: bool = True,
) -> dict[str, object]:
    registry = PeriodRegistry(file_path=file_path, base_path=base_path)
    return {
        "registry_version": registry.registry_version,
        "intraday_periods": registry.get_intraday_runtime_minutes(enabled_only=enabled_only),
        "multiday_periods": registry.get_multiday_runtime_days(enabled_only=enabled_only),
        "calendar_aggregation": registry.get_calendar_runtime_aggregation(enabled_only=enabled_only),
        "precompute_periods": registry.get_precompute_runtime_periods(enabled_only=enabled_only),
        "precompute_base_period": registry.get_precompute_base_period_map(enabled_only=enabled_only),
    }


__all__ = [
    "build_period_runtime_contracts",
    "DEFAULT_PERIOD_REGISTRY_VERSION",
    "PeriodDefinition",
    "PeriodRegistry",
    "ResolvedPeriod",
]

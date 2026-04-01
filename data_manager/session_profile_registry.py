from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, cast

DEFAULT_SESSION_PROFILE_ID = "CN_A"
DEFAULT_SESSION_PROFILE_VERSION = "legacy"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_AUCTION_POLICY = "unknown"
_FUTURES_EXCHANGES = {"DCE", "SHF", "CZC", "CFFEX", "INE", "GFEX"}


@dataclass(frozen=True)
class SessionProfileVersion:
    profile_id: str
    profile_version: str
    effective_from: str | None
    effective_to: str | None
    timezone: str = DEFAULT_TIMEZONE
    auction_policy: str = DEFAULT_AUCTION_POLICY
    market_scope: tuple[str, ...] = ()
    instrument_types: tuple[str, ...] = ()
    sessions: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class ResolvedSessionProfile:
    profile_id: str
    profile_version: str
    matched_rule_id: str
    timezone: str
    auction_policy: str
    sessions: tuple[tuple[str, str], ...]
    effective_from: str | None
    effective_to: str | None
    market_scope: tuple[str, ...]
    instrument_types: tuple[str, ...]
    resolved_exchange: str | None
    resolved_instrument_type: str | None
    route_source: str


@dataclass(frozen=True)
class _SessionProfileRule:
    rule_id: str
    profile_id: str
    patterns: tuple[str, ...]
    exchanges: tuple[str, ...]
    instrument_types: tuple[str, ...]
    effective_from: str | None
    effective_to: str | None
    priority: int
    order: int

    def matches(
        self,
        *,
        symbol: str,
        trade_date: date,
        exchange: str | None,
        instrument_type: str | None,
    ) -> bool:
        if self.patterns and not any(fnmatchcase(symbol, pattern) for pattern in self.patterns):
            return False
        if self.exchanges and (exchange or "").upper() not in self.exchanges:
            return False
        if self.instrument_types and (instrument_type or "").lower() not in self.instrument_types:
            return False
        if not _date_in_range(trade_date, self.effective_from, self.effective_to):
            return False
        return True


class SessionProfileRegistry:
    """版本化 session profile 注册表与解析器。"""

    def __init__(
        self,
        versions_file: str | None = None,
        rules_file: str | None = None,
        *,
        base_path: str | Path | None = None,
    ) -> None:
        self._base_path = Path(base_path) if base_path is not None else Path.cwd()
        self._versions_file = _resolve_path(
            versions_file or "config/session_profile_versions.json", self._base_path
        )
        self._rules_file = _resolve_path(
            rules_file or "config/session_profile_rules.json", self._base_path
        )
        self._profiles_by_id: dict[str, list[SessionProfileVersion]] = {}
        self._default_profile = DEFAULT_SESSION_PROFILE_ID
        self._rules_default_profile = DEFAULT_SESSION_PROFILE_ID
        self._rules: list[_SessionProfileRule] = []
        self._load_versions()
        self._load_rules()

    @property
    def default_profile_id(self) -> str:
        return self._default_profile

    def resolve(
        self,
        *,
        symbol: str | None,
        trade_date: date | datetime | str | None = None,
        exchange: str | None = None,
        instrument_type: str | None = None,
        explicit_profile: str | None = None,
    ) -> ResolvedSessionProfile:
        normalized_symbol = str(symbol or "").strip().upper()
        resolved_exchange = _normalize_exchange(exchange) or _infer_exchange(normalized_symbol)
        resolved_instrument_type = (
            _normalize_instrument_type(instrument_type)
            or _infer_instrument_type(normalized_symbol, resolved_exchange)
        )
        as_of_date = _coerce_date(trade_date) or date.today()

        explicit = str(explicit_profile or "").strip()
        if explicit and explicit.upper() != "AUTO":
            return self._build_resolution(
                profile_id=explicit,
                matched_rule_id="explicit_env_profile",
                route_source="explicit",
                as_of_date=as_of_date,
                resolved_exchange=resolved_exchange,
                resolved_instrument_type=resolved_instrument_type,
            )

        if not normalized_symbol:
            return self._build_resolution(
                profile_id=self._default_profile,
                matched_rule_id="baseline_default",
                route_source="default",
                as_of_date=as_of_date,
                resolved_exchange=resolved_exchange,
                resolved_instrument_type=resolved_instrument_type,
            )

        for rule in self._rules:
            if rule.matches(
                symbol=normalized_symbol,
                trade_date=as_of_date,
                exchange=resolved_exchange,
                instrument_type=resolved_instrument_type,
            ):
                return self._build_resolution(
                    profile_id=rule.profile_id,
                    matched_rule_id=rule.rule_id,
                    route_source="rules_file",
                    as_of_date=as_of_date,
                    resolved_exchange=resolved_exchange,
                    resolved_instrument_type=resolved_instrument_type,
                )

        return self._build_resolution(
            profile_id=self._rules_default_profile,
            matched_rule_id="rules_default",
            route_source="default",
            as_of_date=as_of_date,
            resolved_exchange=resolved_exchange,
            resolved_instrument_type=resolved_instrument_type,
        )

    def _load_versions(self) -> None:
        if not self._versions_file.exists():
            return
        try:
            payload_any = json.loads(self._versions_file.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload_any, dict):
            return
        payload = cast(dict[str, Any], payload_any)
        default_profile = str(payload.get("default_profile") or DEFAULT_SESSION_PROFILE_ID).strip()
        if default_profile:
            self._default_profile = default_profile

        profiles_payload_any = payload.get("profiles")
        if isinstance(profiles_payload_any, list):
            for item_any in cast(list[Any], profiles_payload_any):
                if not isinstance(item_any, dict):
                    continue
                item = cast(dict[str, Any], item_any)
                profile = self._parse_profile_entry(item)
                if profile is None:
                    continue
                self._profiles_by_id.setdefault(profile.profile_id, []).append(profile)
        else:
            for profile_id_raw, sessions in payload.items():
                profile_id = str(profile_id_raw)
                if profile_id in {"schema_version", "default_profile"}:
                    continue
                profile = self._parse_legacy_profile_entry(profile_id, sessions)
                if profile is None:
                    continue
                self._profiles_by_id.setdefault(profile.profile_id, []).append(profile)

        for versions in self._profiles_by_id.values():
            versions.sort(key=lambda item: (_sort_key_for_date(item.effective_from), item.profile_version))

    def _load_rules(self) -> None:
        if not self._rules_file.exists():
            self._rules_default_profile = self._default_profile
            return
        try:
            payload_any = json.loads(self._rules_file.read_text(encoding="utf-8"))
        except Exception:
            self._rules_default_profile = self._default_profile
            return
        if not isinstance(payload_any, dict):
            self._rules_default_profile = self._default_profile
            return
        payload = cast(dict[str, Any], payload_any)
        default_profile = str(payload.get("default_profile") or self._default_profile).strip()
        self._rules_default_profile = default_profile or self._default_profile
        rules_payload_any = payload.get("rules")
        if not isinstance(rules_payload_any, list):
            return
        parsed: list[_SessionProfileRule] = []
        for order, item_any in enumerate(cast(list[Any], rules_payload_any)):
            if not isinstance(item_any, dict):
                continue
            item = cast(dict[str, Any], item_any)
            profile_id = str(item.get("profile") or "").strip()
            patterns = _normalize_patterns(item)
            has_explicit_exchange = bool(item.get("exchange") or item.get("exchanges"))
            if not profile_id or (not patterns and not has_explicit_exchange):
                # 至少需要 profile，并至少提供 pattern 或 exchange 之一，避免误匹配所有 symbol
                continue
            parsed.append(
                _SessionProfileRule(
                    rule_id=str(item.get("rule_id") or f"rule_{order + 1}"),
                    profile_id=profile_id,
                    patterns=patterns,
                    exchanges=_normalize_list(item.get("exchanges") or item.get("exchange"), upper=True),
                    instrument_types=_normalize_list(
                        item.get("instrument_types") or item.get("instrument_type"), lower=True
                    ),
                    effective_from=_normalize_optional_date(item.get("effective_from")),
                    effective_to=_normalize_optional_date(item.get("effective_to")),
                    priority=_safe_int(item.get("priority"), 0),
                    order=order,
                )
            )
        self._rules = sorted(parsed, key=lambda item: (-item.priority, item.order))

    def _parse_profile_entry(self, payload: dict[str, Any]) -> SessionProfileVersion | None:
        profile_id = str(payload.get("profile_id") or "").strip()
        if not profile_id:
            return None
        return SessionProfileVersion(
            profile_id=profile_id,
            profile_version=str(payload.get("profile_version") or DEFAULT_SESSION_PROFILE_VERSION),
            effective_from=_normalize_optional_date(payload.get("effective_from")),
            effective_to=_normalize_optional_date(payload.get("effective_to")),
            timezone=str(payload.get("timezone") or DEFAULT_TIMEZONE),
            auction_policy=str(payload.get("auction_policy") or DEFAULT_AUCTION_POLICY),
            market_scope=_normalize_list(payload.get("market_scope"), upper=True),
            instrument_types=_normalize_list(payload.get("instrument_types"), lower=True),
            sessions=_normalize_sessions(payload.get("sessions")),
        )

    def _parse_legacy_profile_entry(self, profile_id: str, sessions: Any) -> SessionProfileVersion | None:
        normalized_sessions = _normalize_sessions(sessions)
        if not normalized_sessions:
            return None
        return SessionProfileVersion(
            profile_id=str(profile_id).strip(),
            profile_version=DEFAULT_SESSION_PROFILE_VERSION,
            effective_from=None,
            effective_to=None,
            timezone=DEFAULT_TIMEZONE,
            auction_policy=DEFAULT_AUCTION_POLICY,
            market_scope=(),
            instrument_types=(),
            sessions=normalized_sessions,
        )

    def _build_resolution(
        self,
        *,
        profile_id: str,
        matched_rule_id: str,
        route_source: str,
        as_of_date: date,
        resolved_exchange: str | None,
        resolved_instrument_type: str | None,
    ) -> ResolvedSessionProfile:
        version = self._select_profile_version(profile_id, as_of_date)
        if version is None:
            version = SessionProfileVersion(
                profile_id=profile_id,
                profile_version=f"{DEFAULT_SESSION_PROFILE_VERSION}::{profile_id}",
                effective_from=None,
                effective_to=None,
                timezone=DEFAULT_TIMEZONE,
                auction_policy=DEFAULT_AUCTION_POLICY,
                market_scope=tuple(filter(None, [resolved_exchange])),
                instrument_types=tuple(filter(None, [resolved_instrument_type])),
                sessions=(),
            )
        return ResolvedSessionProfile(
            profile_id=version.profile_id,
            profile_version=version.profile_version,
            matched_rule_id=matched_rule_id,
            timezone=version.timezone,
            auction_policy=version.auction_policy,
            sessions=version.sessions,
            effective_from=version.effective_from,
            effective_to=version.effective_to,
            market_scope=version.market_scope,
            instrument_types=version.instrument_types,
            resolved_exchange=resolved_exchange,
            resolved_instrument_type=resolved_instrument_type,
            route_source=route_source,
        )

    def _select_profile_version(
        self, profile_id: str, as_of_date: date
    ) -> SessionProfileVersion | None:
        versions = self._profiles_by_id.get(profile_id, [])
        if not versions:
            return None

        active = [
            version
            for version in versions
            if _date_in_range(as_of_date, version.effective_from, version.effective_to)
        ]
        if active:
            return active[-1]

        past: list[SessionProfileVersion] = []
        for version in versions:
            effective_from = _coerce_date(version.effective_from)
            if effective_from is not None and effective_from <= as_of_date:
                past.append(version)
        if past:
            return past[-1]

        future: list[SessionProfileVersion] = []
        for version in versions:
            effective_from = _coerce_date(version.effective_from)
            if effective_from is not None and effective_from > as_of_date:
                future.append(version)
        if future:
            return future[0]

        return versions[-1]


def _resolve_path(path_value: str, base_path: Path) -> Path:
    path = Path(str(path_value).strip())
    if not path.is_absolute():
        path = (base_path / path).resolve()
    return path


def _normalize_patterns(payload: dict[str, Any]) -> tuple[str, ...]:
    if "patterns" in payload:
        return _normalize_list(payload.get("patterns"), upper=True)
    return _normalize_list(payload.get("pattern"), upper=True)


def _normalize_sessions(value: Any) -> tuple[tuple[str, str], ...]:
    rows: list[tuple[str, str]] = []
    if not isinstance(value, list):
        return ()
    for item in cast(list[Any], value):
        if not isinstance(item, (list, tuple)):
            continue
        pair = list(cast(list[Any] | tuple[Any, ...], item))
        if len(pair) != 2:
            continue
        start = str(pair[0]).strip()
        end = str(pair[1]).strip()
        if start and end:
            rows.append((start, end))
    return tuple(rows)


def _normalize_list(value: Any, *, upper: bool = False, lower: bool = False) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        values: list[Any] = [value]
    elif isinstance(value, (list, tuple, set)):
        values = list(cast(list[Any] | tuple[Any, ...] | set[Any], value))
    else:
        return ()
    out: list[str] = []
    for item in values:
        text = str(item).strip()
        if not text:
            continue
        if upper:
            text = text.upper()
        if lower:
            text = text.lower()
        out.append(text)
    return tuple(out)


def _safe_int(value: Any, default: int) -> int:
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


def _sort_key_for_date(value: str | None) -> date:
    return _coerce_date(value) or date.min


def _normalize_exchange(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _normalize_instrument_type(value: str | None) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _infer_exchange(symbol: str) -> str | None:
    if "." not in symbol:
        return None
    return symbol.rsplit(".", 1)[1].upper() or None


def _infer_instrument_type(symbol: str, exchange: str | None) -> str | None:
    exchange = (exchange or "").upper()
    if exchange in _FUTURES_EXCHANGES:
        return "future"
    if exchange == "HK":
        return "equity"
    if exchange in {"SH", "SZ"}:
        code = symbol.split(".", 1)[0]
        if code.startswith(("51", "56", "58", "15", "16", "18")):
            return "etf"
        if exchange == "SZ" and code.startswith("399"):
            return "index"
        if exchange == "SH" and code.startswith(("000", "880", "930", "931", "932", "985", "986")):
            return "index"
        return "equity"
    return None


__all__ = [
    "DEFAULT_SESSION_PROFILE_ID",
    "ResolvedSessionProfile",
    "SessionProfileRegistry",
    "SessionProfileVersion",
]

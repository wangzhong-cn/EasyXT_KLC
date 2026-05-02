from __future__ import annotations

"""QMT 本地编排读模型投影器。"""

import hashlib
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .compat import resolve_legacy_qmt_paths
from .contracts import (
    AccountBindingRecord,
    AssetStatus,
    BindingScope,
    BindingStatus,
    ChannelProfile,
    ConflictCode,
    ConflictRecord,
    ConflictSeverity,
    FreshnessState,
    GatewaySessionRecord,
    IntentSource,
    LayoutStatus,
    ProbeStatus,
    QmtAccountProbeRecord,
    QmtLayoutRecord,
    QmtLocalAssetRecord,
    RouteDecisionSnapshot,
    RoutePurpose,
    SessionStatus,
    utc_now_iso,
)


def _stable_id(prefix: str, *parts: Any) -> str:
    joined = "|".join(str(part or "").strip() for part in parts if str(part or "").strip())
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _confidence_from_score(raw_score: Any) -> float:
    try:
        score = float(raw_score or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    return round(min(max(score / 100.0, 0.0), 1.0), 3)


def _scan_root_from_discovered_by(discovered_by: Any) -> str:
    text = str(discovered_by or "").strip()
    if text.startswith("scan:"):
        return text.split(":", 1)[1].strip()
    return ""


def _normalize_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _normalize_path(value: Any) -> str:
    return str(value or "").strip().replace("\\", "/").rstrip("/").lower()


def _paths_mostly_match(left: Any, right: Any) -> bool:
    a = _normalize_path(left)
    b = _normalize_path(right)
    if not a or not b:
        return False
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _serialize_related_item(item: Any) -> dict[str, Any] | None:
    if item is None:
        return None
    if isinstance(item, dict):
        return dict(item)
    if is_dataclass(item) and not isinstance(item, type):
        return asdict(item)
    if hasattr(item, "__dict__"):
        return dict(vars(item))
    return {"value": item}


def _period_present(periods: list[str], *aliases: str) -> bool:
    normalized = {item.lower() for item in periods}
    return any(alias.lower() in normalized for alias in aliases)


def _infer_instrument_families(markets: list[str]) -> list[str]:
    normalized = {item.upper() for item in markets}
    families: list[str] = []
    if normalized & {"SH", "SZ", "BJ", "ASHARE", "沪深A股"}:
        families.append("stock")
    if normalized & {"SHF", "DCE", "CZC", "CFFEX", "INE", "GFEX"}:
        families.append("future")
    return families


def _build_integrity_flags(candidate: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    if candidate.get("has_userdata"):
        flags.append("has_userdata")
    if candidate.get("has_datadir"):
        flags.append("has_datadir")
    if candidate.get("has_downloaded_history"):
        flags.append("has_downloaded_history")
    if not candidate.get("has_downloaded_history"):
        flags.append("history_not_confirmed")
    return flags


def project_qmt_discovery_candidate(candidate: dict[str, Any]) -> tuple[QmtLayoutRecord, QmtLocalAssetRecord]:
    """把 datasource_discovery 的单个候选投影成 layout + asset。"""

    resolved = resolve_legacy_qmt_paths(
        {
            "qmt_install_root": candidate.get("install_path"),
            "qmt_userdata_path": candidate.get("userdata_path"),
            "qmt_datadir_path": candidate.get("datadir_path"),
            "qmt_data_path": candidate.get("data_path"),
            "qmt_datas_path": candidate.get("datas_path"),
            "qmt_cfg_path": candidate.get("cfg_path"),
        }
    )
    discovered_by = str(candidate.get("discovered_by", "candidate_projection") or "candidate_projection")
    market_dirs = _normalize_string_list(candidate.get("market_dirs"))
    period_dirs = _normalize_string_list(candidate.get("period_dirs"))
    sample_files = _normalize_string_list(candidate.get("history_sample_files"))
    raw_score = candidate.get("score", 0)
    confidence = _confidence_from_score(raw_score)
    scan_root = _scan_root_from_discovered_by(discovered_by)
    observed_at = utc_now_iso()

    install_root = resolved.install_root or str(candidate.get("install_path") or "")
    layout_id = _stable_id("layout", install_root, resolved.userdata_path, discovered_by)
    asset_id = _stable_id("asset", layout_id, resolved.userdata_path, resolved.datadir_path)

    layout = QmtLayoutRecord(
        layout_id=layout_id,
        fingerprint=_stable_id("layoutfp", install_root, resolved.exe_path, resolved.userdata_path),
        install_root=install_root,
        exe_path=resolved.exe_path,
        bin_path=str(Path(install_root) / "bin.x64").replace("\\", "/") if install_root else "",
        xtquant_roots=[resolved.xtquant_path] if resolved.xtquant_path else [],
        userdata_roots=[resolved.userdata_path] if resolved.userdata_path else [],
        datadir_paths=[resolved.datadir_path] if resolved.datadir_path else [],
        data_paths=[resolved.data_path] if resolved.data_path else [],
        datas_paths=[resolved.datas_path] if resolved.datas_path else [],
        cfg_paths=[resolved.cfg_path] if resolved.cfg_path else [],
        launchable=bool(install_root),
        discovered_from=[discovered_by],
        scan_root=scan_root,
        raw_hints={
            "score": raw_score,
            "market_dirs": market_dirs,
            "period_dirs": period_dirs,
            "sample_files": sample_files,
            "compat_raw_inputs": resolved.raw_inputs,
        },
        confidence_score=confidence,
        source_refs=[item for item in (install_root, resolved.userdata_path, resolved.datadir_path) if item],
        observed_at=observed_at,
        status=LayoutStatus.NORMALIZED.value,
    )

    normalized_score = round(confidence, 3)
    asset = QmtLocalAssetRecord(
        asset_id=asset_id,
        layout_id=layout_id,
        userdata_path=resolved.userdata_path,
        datadir_path=resolved.datadir_path,
        data_path=resolved.data_path,
        datas_path=resolved.datas_path,
        cfg_path=resolved.cfg_path,
        market_coverage=market_dirs,
        period_coverage=period_dirs,
        instrument_family_coverage=_infer_instrument_families(market_dirs),
        latest_modified_at=str(candidate.get("latest_modified_at") or ""),
        readable_sample_rate=1.0 if candidate.get("has_downloaded_history") else 0.0,
        continuity_gap_rate=0.0,
        parse_failure_rate=0.0,
        integrity_flags=_build_integrity_flags(candidate),
        tick_score=normalized_score if _period_present(period_dirs, "tick") else 0.0,
        m1_score=normalized_score if _period_present(period_dirs, "1m") else 0.0,
        m5_score=normalized_score if _period_present(period_dirs, "5m") else 0.0,
        d1_score=normalized_score if _period_present(period_dirs, "1d", "day") else 0.0,
        stability_score=normalized_score,
        discovered_from=[discovered_by],
        scan_root=scan_root,
        sample_files=sample_files,
        profile_method="candidate_projection",
        confidence_score=confidence,
        source_refs=[item for item in (resolved.userdata_path, resolved.datadir_path) if item],
        observed_at=observed_at,
        stale_after_seconds=86400,
        freshness_state=FreshnessState.FRESH if candidate.get("latest_modified_at") else FreshnessState.UNKNOWN,
        status=AssetStatus.SCORED.value,
    )
    return layout, asset


def project_qmt_discovery_payload(payload: dict[str, Any]) -> dict[str, Any]:
    layouts: list[QmtLayoutRecord] = []
    assets: list[QmtLocalAssetRecord] = []
    for candidate in payload.get("candidates") or []:
        layout, asset = project_qmt_discovery_candidate(candidate)
        layouts.append(layout)
        assets.append(asset)
    return {
        "layouts": layouts,
        "assets": assets,
        "scan_policy": dict(payload.get("scan_policy") or {}),
        "scan_metrics": dict(payload.get("scan_metrics") or {}),
        "root_results": list(payload.get("root_results") or []),
        "candidate_count": int(payload.get("candidate_count", len(assets)) or 0),
    }


def project_trading_account_probe(entry: dict[str, Any]) -> QmtAccountProbeRecord:
    userdata_path = str(entry.get("userdata_path") or "")
    broker_id = str(entry.get("broker_id") or "")
    account_id = str(entry.get("account_id") or "")
    account_type = str(entry.get("account_type") or "")
    login_status = str(entry.get("login_status") or "")
    layout_id = _stable_id("layout", str(Path(userdata_path).parent) if userdata_path else "", userdata_path)
    channel_id = _stable_id("channel", broker_id, userdata_path, account_type)
    probe_status = ProbeStatus.SUCCEEDED.value if account_id else ProbeStatus.PARTIAL.value
    return QmtAccountProbeRecord(
        probe_id=_stable_id("probe", userdata_path, broker_id, account_id, account_type),
        layout_id=layout_id,
        channel_id=channel_id,
        userdata_path=userdata_path,
        broker_id=broker_id,
        account_id=account_id,
        account_type=account_type,
        login_status=login_status,
        reachable=True,
        probe_method="xtquant_query_account_infos",
        raw_account_infos=dict(entry),
        probe_success=bool(account_id),
        confidence_score=1.0 if account_id else 0.5,
        source_refs=[userdata_path] if userdata_path else [],
        observed_at=utc_now_iso(),
        stale_after_seconds=300,
        freshness_state=FreshnessState.FRESH,
        discovered_from=["api:discover_trading_accounts"],
        source_process="XtQuantTrader",
        status=probe_status,
    )


def project_trading_account_probe_payload(payload: dict[str, Any]) -> dict[str, Any]:
    probes = [project_trading_account_probe(item) for item in payload.get("discovered") or []]
    return {
        "probes": probes,
        "errors": list(payload.get("errors") or []),
    }


def project_gateway_session_payload(entries: list[dict[str, Any]]) -> dict[str, Any]:
    sessions: list[GatewaySessionRecord] = []
    observed_at = utc_now_iso()
    for entry in entries:
        userdata_path = str(entry.get("userdata_path") or "").strip()
        if not userdata_path:
            continue
        broker_id = str(entry.get("broker_id") or "").strip()
        broker_guess = str(entry.get("broker_guess") or broker_id).strip()
        connected_accounts = _normalize_string_list(entry.get("connected_accounts"))
        supported_account_types = _normalize_string_list(entry.get("supported_account_types"))
        current_route_claims = _normalize_string_list(entry.get("current_route_claims"))
        last_error = str(entry.get("last_error") or "").strip()
        connected = bool(entry.get("connected")) or bool(connected_accounts)
        authenticated = bool(entry.get("authenticated")) or bool(connected_accounts)
        process_status = str(entry.get("process_status") or ("process_alive" if connected else "disconnected"))
        login_status = str(
            entry.get("login_status")
            or ("connected" if authenticated else ("login_pending" if process_status == "process_alive" else "disconnected"))
        )
        if connected and authenticated:
            session_status = SessionStatus.HEALTHY.value
            session_health = "healthy"
            freshness_state = FreshnessState.FRESH
        elif process_status == "process_alive":
            session_status = SessionStatus.LOGIN_PENDING.value
            session_health = "login_pending"
            freshness_state = FreshnessState.FRESH
        elif last_error:
            session_status = SessionStatus.DEGRADED.value
            session_health = "degraded"
            freshness_state = FreshnessState.STALE
        else:
            session_status = SessionStatus.DISCONNECTED.value
            session_health = "disconnected"
            freshness_state = FreshnessState.UNKNOWN

        layout_id = _stable_id("layout", str(Path(userdata_path).parent), userdata_path)
        channel_id = _stable_id(
            "channel",
            broker_id or broker_guess,
            userdata_path,
            "|".join(supported_account_types),
        )
        sessions.append(
            GatewaySessionRecord(
                session_id=_stable_id("session", userdata_path, broker_id or broker_guess),
                session_anchor_key=userdata_path,
                layout_id=layout_id,
                userdata_path=userdata_path,
                connected_accounts=connected_accounts,
                current_route_claims=current_route_claims,
                channel_profile=ChannelProfile(
                    channel_id=channel_id,
                    broker_id=broker_id,
                    broker_guess=broker_guess,
                    channel_kind="qmt",
                    supported_account_types=supported_account_types,
                    login_entry_hint=userdata_path,
                ),
                process_status=process_status,
                login_status=login_status,
                session_health=session_health,
                connected=connected,
                authenticated=authenticated,
                last_error=last_error,
                source_refs=[userdata_path],
                observed_at=str(entry.get("observed_at") or observed_at),
                stale_after_seconds=int(entry.get("stale_after_seconds") or 60),
                freshness_state=freshness_state,
                status=session_status,
            )
        )
    return {"sessions": sessions}


def build_qmt_conflict_projection(
    *,
    layouts: list[Any],
    assets: list[Any],
    probes: list[Any],
    sessions: list[Any],
) -> dict[str, Any]:
    conflicts: list[ConflictRecord] = []

    account_to_userdatas: dict[str, set[str]] = {}
    userdata_to_brokers: dict[str, set[str]] = {}
    for probe in probes:
        account_id = str(_field(probe, "account_id") or "").strip()
        userdata_path = str(_field(probe, "userdata_path") or "").strip()
        broker_id = str(_field(probe, "broker_id") or "").strip()
        if account_id and userdata_path:
            account_to_userdatas.setdefault(account_id, set()).add(userdata_path)
        if userdata_path and broker_id:
            userdata_to_brokers.setdefault(userdata_path, set()).add(broker_id)

    for account_id, userdatas in sorted(account_to_userdatas.items()):
        if len(userdatas) <= 1:
            continue
        conflicts.append(
            ConflictRecord(
                code=ConflictCode.ACCOUNT_MULTI_USERDATA,
                severity=ConflictSeverity.BLOCKING,
                message=f"资金账号 {account_id} 同时出现在多个 userdata 路径",
                target_kind="probe_account",
                target_id=account_id,
                details={"userdata_paths": sorted(userdatas)},
            )
        )

    for userdata_path, brokers in sorted(userdata_to_brokers.items()):
        if len(brokers) <= 1:
            continue
        conflicts.append(
            ConflictRecord(
                code=ConflictCode.USERDATA_MULTI_BROKER,
                severity=ConflictSeverity.BLOCKING,
                message=f"同一 userdata 路径映射到多个券商: {userdata_path}",
                target_kind="userdata_path",
                target_id=userdata_path,
                details={"broker_ids": sorted(brokers)},
            )
        )

    for layout in layouts:
        layout_id = str(_field(layout, "layout_id") or "").strip()
        install_root = str(_field(layout, "install_root") or "").strip()
        exe_path = str(_field(layout, "exe_path") or "").strip()
        userdata_roots = _normalize_string_list(_field(layout, "userdata_roots"))
        if not install_root:
            continue
        if exe_path and userdata_roots:
            continue
        conflicts.append(
            ConflictRecord(
                code=ConflictCode.LAYOUT_PATH_INCONSISTENT,
                severity=ConflictSeverity.BLOCKING,
                message=f"布局路径不完整，无法确认 exe/userdata 一致性: {layout_id or install_root}",
                target_kind="layout",
                target_id=layout_id or install_root,
                details={
                    "install_root": install_root,
                    "exe_path": exe_path,
                    "userdata_roots": userdata_roots,
                },
            )
        )

    for session in sessions:
        session_id = str(_field(session, "session_id") or "").strip()
        status = str(_field(session, "status") or "").strip()
        claims = _normalize_string_list(_field(session, "current_route_claims"))
        if status not in {SessionStatus.DEGRADED.value, SessionStatus.QUARANTINED.value, SessionStatus.STALE.value}:
            continue
        if not claims:
            continue
        conflicts.append(
            ConflictRecord(
                code=ConflictCode.PRIMARY_ROUTE_DEGRADED,
                severity=ConflictSeverity.WARNING,
                message=f"会话 {session_id} 已降级，但仍持有路由声明",
                target_kind="session",
                target_id=session_id,
                details={"current_route_claims": claims, "status": status},
            )
        )

    low_confidence_assets = [
        str(_field(asset, "asset_id") or "").strip()
        for asset in assets
        if float(_field(asset, "confidence_score") or 0.0) < 0.35
    ]
    for asset_id in low_confidence_assets:
        if not asset_id:
            continue
        conflicts.append(
            ConflictRecord(
                code=ConflictCode.ASSET_SCORE_UNTRUSTED,
                severity=ConflictSeverity.WARNING,
                message=f"资产 {asset_id} 评分置信度偏低，当前只适合候选展示",
                target_kind="asset",
                target_id=asset_id,
            )
        )

    return {"items": conflicts, "total": len(conflicts)}


def _asset_route_score(asset: Any) -> float:
    return float(_field(asset, "tick_score") or 0.0) + float(_field(asset, "m1_score") or 0.0) + float(
        _field(asset, "d1_score") or 0.0
    ) + float(_field(asset, "stability_score") or 0.0)


def _session_route_score(session: Any) -> float:
    score = 0.0
    if bool(_field(session, "connected")):
        score += 2.0
    if bool(_field(session, "authenticated")):
        score += 2.0
    if str(_field(session, "status") or "") == SessionStatus.HEALTHY.value:
        score += 1.0
    score += 0.1 * len(_normalize_string_list(_field(session, "connected_accounts")))
    return score


def _pick_binding_session(account: Any, sessions: list[Any]) -> Any | None:
    trade_account = str(_field(account, "trade_account") or "").strip()
    userdata_path = str(_field(account, "qmt_userdata_path") or "").strip()
    for item in sessions:
        connected_accounts = _normalize_string_list(_field(item, "connected_accounts"))
        if trade_account and trade_account in connected_accounts:
            return item
    for item in sessions:
        if _paths_mostly_match(_field(item, "userdata_path"), userdata_path):
            return item
    return None


def _pick_binding_probe(account: Any, probes: list[Any]) -> Any | None:
    trade_account = str(_field(account, "trade_account") or "").strip()
    userdata_path = str(_field(account, "qmt_userdata_path") or "").strip()
    for item in probes:
        account_id = str(_field(item, "account_id") or "").strip()
        if trade_account and trade_account == account_id:
            return item
    for item in probes:
        if _paths_mostly_match(_field(item, "userdata_path"), userdata_path):
            return item
    return None


def _pick_binding_route(account: Any, session: Any | None, routes: list[Any]) -> Any | None:
    trade_account = str(_field(account, "trade_account") or "").strip()
    if trade_account:
        for item in routes:
            if str(_field(item, "account_id") or "").strip() == trade_account:
                return item
    session_id = str(_field(session, "session_id") or "").strip()
    if not session_id:
        return None
    for item in routes:
        candidate_ids = _normalize_string_list(_field(item, "candidate_ids"))
        if str(_field(item, "winner") or "").strip() == session_id or session_id in candidate_ids:
            return item
    return None


def _pick_binding_conflicts(account: Any, session: Any | None, probe: Any | None, conflicts: list[Any]) -> list[Any]:
    trade_account = str(_field(account, "trade_account") or "").strip()
    broker_account_id = str(_field(account, "id") or "").strip()
    comparable_userdata = _normalize_path(_field(account, "qmt_userdata_path"))
    session_id = str(_field(session, "session_id") or "").strip()
    probe_id = str(_field(probe, "probe_id") or "").strip()
    related: list[Any] = []
    for item in conflicts:
        target_id = str(_field(item, "target_id") or "").strip()
        if target_id and target_id in {trade_account, broker_account_id, session_id, probe_id}:
            related.append(item)
            continue
        if comparable_userdata and _paths_mostly_match(target_id, comparable_userdata):
            related.append(item)
            continue
        detail_userdatas = [
            value for value in (_field(item, "details", {}) or {}).get("userdata_paths", [])
            if isinstance(value, str)
        ]
        if comparable_userdata and any(_paths_mostly_match(value, comparable_userdata) for value in detail_userdatas):
            related.append(item)
    return related


def _pick_binding_asset(account: Any, assets: list[Any], layouts_by_id: dict[str, Any]) -> Any | None:
    normalized_userdata = _normalize_path(_field(account, "qmt_userdata_path"))
    normalized_exe = _normalize_path(_field(account, "qmt_exe_path"))
    matched_by_exe: Any | None = None
    for asset in assets:
        asset_userdata = _normalize_path(_field(asset, "userdata_path"))
        if normalized_userdata and asset_userdata and normalized_userdata == asset_userdata:
            return asset
        layout = layouts_by_id.get(str(_field(asset, "layout_id") or "").strip())
        install_root = _normalize_path(_field(layout, "install_root"))
        if normalized_exe and install_root and normalized_exe.startswith(f"{install_root}/"):
            matched_by_exe = matched_by_exe or asset
    return matched_by_exe


def build_qmt_account_binding_projection(
    *,
    accounts: list[Any],
    layouts: list[Any],
    assets: list[Any],
    probes: list[Any],
    sessions: list[Any],
    conflicts: list[Any],
    routes: list[Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    layouts_by_id = {
        str(_field(layout, "layout_id") or "").strip(): layout
        for layout in layouts
        if str(_field(layout, "layout_id") or "").strip()
    }

    for account in accounts:
        broker_account_id = str(_field(account, "id") or "").strip()
        if not broker_account_id:
            continue
        trade_account = str(_field(account, "trade_account") or "").strip()
        qmt_userdata_path = str(_field(account, "qmt_userdata_path") or "").strip()
        account_label = str(_field(account, "label") or trade_account or broker_account_id).strip()
        session = _pick_binding_session(account, sessions)
        probe = _pick_binding_probe(account, probes)
        route = _pick_binding_route(account, session, routes)
        asset = _pick_binding_asset(account, assets, layouts_by_id)
        related_conflicts = _pick_binding_conflicts(account, session, probe, conflicts)
        apply_path = (
            str(_field(probe, "userdata_path") or "").strip()
            or str(_field(session, "userdata_path") or "").strip()
            or str(_field(asset, "userdata_path") or "").strip()
            or qmt_userdata_path
        )
        reasons: list[str] = []
        score = 0
        recommendation_status = "missing"
        recommendation_message = "当前没有稳定的本地路径候选，需要人工补全。"

        if asset and qmt_userdata_path and _paths_mostly_match(qmt_userdata_path, _field(asset, "userdata_path")):
            recommendation_status = "matched"
            recommendation_message = "账户路径已命中本地 QMT 候选。"
            reasons.append("账户路径已命中本地 QMT 候选。")
            score += 45
        elif asset and _field(account, "qmt_exe_path"):
            recommendation_status = "suggested"
            recommendation_message = "账户 exe 路径命中本地布局，可建议绑定 userdata。"
            reasons.append("账户 exe 路径命中本地布局，可建议绑定 userdata。")
            score += 25
        elif asset:
            recommendation_status = "suggested"
            recommendation_message = "存在可建议的本地候选路径，可作为 draft 绑定目标。"
            reasons.append("存在可建议的本地候选路径，可作为 draft 绑定目标。")
            score += 20
        else:
            reasons.append("当前没有稳定的本地路径候选，需要人工补全。")

        probe_account_id = str(_field(probe, "account_id") or "").strip()
        if probe_account_id and probe_account_id == trade_account:
            reasons.append("按需 probe 命中同一资金账号。")
            score += 30
        elif probe and _field(probe, "userdata_path"):
            reasons.append("probe 命中同一路径，但未直接命中资金账号。")
            score += 15

        connected_accounts = _normalize_string_list(_field(session, "connected_accounts"))
        if trade_account and trade_account in connected_accounts:
            reasons.append("运行态 session 当前持有该资金账号。")
            score += 20
        elif session and _field(session, "userdata_path"):
            reasons.append("运行态 session 与当前 userdata 路径相符。")
            score += 10

        if route:
            reasons.append(f"已存在 {str(_field(route, 'purpose') or '').strip() or '默认'} 路由快照，可作为后续 policy explain 锚点。")
            score += 5

        blocking_conflicts = [
            item for item in related_conflicts
            if str(_field(item, "severity") or "").strip()
            in {ConflictSeverity.BLOCKING.value, ConflictSeverity.MANUAL_REVIEW_REQUIRED.value}
        ]
        if related_conflicts:
            reasons.append(f"关联冲突 {len(related_conflicts)} 条，需要人工复核。")
            score -= 30 if blocking_conflicts else 10

        confidence_score = round(min(max(score / 100.0, 0.0), 1.0), 3)
        status_value = BindingStatus.DRAFT.value
        approval_required = False
        approval_state = "draft"
        if blocking_conflicts:
            status_value = BindingStatus.CONFLICTED.value
            approval_required = True
            approval_state = "review_required"
        elif qmt_userdata_path and apply_path and _paths_mostly_match(qmt_userdata_path, apply_path) and (
            probe_account_id == trade_account or session is not None
        ):
            status_value = BindingStatus.CONFIRMED.value
            approval_state = "confirmed"
        elif apply_path:
            status_value = BindingStatus.PROPOSED.value
            approval_required = True
            approval_state = "pending_manual_confirmation"

        session_anchor_key = str(_field(session, "session_anchor_key") or _field(session, "userdata_path") or apply_path).strip()
        record = AccountBindingRecord(
            binding_id=_stable_id("binding", broker_account_id, trade_account, apply_path or qmt_userdata_path or recommendation_status),
            broker_account_id=broker_account_id,
            probe_id=str(_field(probe, "probe_id") or "").strip(),
            channel_id=str(_field(probe, "channel_id") or _field(_field(session, "channel_profile", {}), "channel_id") or "").strip(),
            asset_id=str(_field(asset, "asset_id") or "").strip(),
            session_anchor_key=session_anchor_key,
            binding_scope=BindingScope.TRADE_DEFAULT if bool(_field(account, "is_default")) else BindingScope.MANUAL_ONLY,
            priority=int(round(confidence_score * 100)),
            manual_override=bool(qmt_userdata_path and apply_path and _paths_mostly_match(qmt_userdata_path, apply_path)),
            intent_source=IntentSource.MANUAL if qmt_userdata_path else IntentSource.HEURISTIC_SUGGESTION,
            change_reason=recommendation_message,
            conflict_flags=[
                str(_field(item, "code") or "").strip()
                for item in related_conflicts
                if str(_field(item, "code") or "").strip()
            ],
            approval_required=approval_required,
            approval_state=approval_state,
            confidence_score=confidence_score,
            source_refs=[value for value in [qmt_userdata_path, apply_path, str(_field(asset, "asset_id") or "").strip()] if value],
            notes=" | ".join(reasons[:3]),
            status=status_value,
        )
        items.append(
            {
                **asdict(record),
                "account_label": account_label,
                "broker": str(_field(account, "broker") or "").strip(),
                "trade_account": trade_account,
                "configured_userdata_path": qmt_userdata_path,
                "configured_exe_path": str(_field(account, "qmt_exe_path") or "").strip(),
                "recommendation_status": recommendation_status,
                "recommendation_message": recommendation_message,
                "candidate_path": str(_field(asset, "userdata_path") or "").strip() or None,
                "apply_path": apply_path or None,
                "reasons": reasons,
                "session": _serialize_related_item(session),
                "probe": _serialize_related_item(probe),
                "route": _serialize_related_item(route),
                "conflicts": [_serialize_related_item(item) for item in related_conflicts if _serialize_related_item(item) is not None],
            }
        )

    return {"items": items, "total": len(items)}


def build_qmt_route_decision_projection(*, assets: list[Any], sessions: list[Any]) -> dict[str, Any]:
    snapshots: list[RouteDecisionSnapshot] = []
    now = utc_now_iso()

    ranked_assets = sorted(
        assets,
        key=lambda item: (
            _asset_route_score(item),
            str(_field(item, "freshness_state") or ""),
            str(_field(item, "latest_modified_at") or ""),
        ),
        reverse=True,
    )
    if ranked_assets:
        winner = ranked_assets[0]
        runner_up = ranked_assets[1] if len(ranked_assets) > 1 else None
        winner_id = str(_field(winner, "asset_id") or "").strip()
        snapshots.append(
            RouteDecisionSnapshot(
                snapshot_id=_stable_id("route", "history", winner_id, now),
                policy_id="policy_history_default_v01",
                purpose=RoutePurpose.HISTORY.value,
                candidate_ids=[str(_field(item, "asset_id") or "") for item in ranked_assets if str(_field(item, "asset_id") or "")],
                winner=winner_id,
                runner_up=str(_field(runner_up, "asset_id") or "") if runner_up is not None else "",
                score_breakdown={
                    str(_field(item, "asset_id") or ""): round(_asset_route_score(item), 4)
                    for item in ranked_assets[:5]
                    if str(_field(item, "asset_id") or "")
                },
                decision_reason="按历史资产评分与稳定性选择默认历史路由",
                rejection_reasons=[],
                effective_from=now,
            )
        )
        snapshots.append(
            RouteDecisionSnapshot(
                snapshot_id=_stable_id("route", "chart", winner_id, now),
                policy_id="policy_chart_default_v01",
                purpose=RoutePurpose.CHART.value,
                candidate_ids=[str(_field(item, "asset_id") or "") for item in ranked_assets if str(_field(item, "asset_id") or "")],
                winner=winner_id,
                runner_up=str(_field(runner_up, "asset_id") or "") if runner_up is not None else "",
                score_breakdown={
                    str(_field(item, "asset_id") or ""): round(_asset_route_score(item), 4)
                    for item in ranked_assets[:5]
                    if str(_field(item, "asset_id") or "")
                },
                decision_reason="图表场景暂复用默认历史路由快照",
                rejection_reasons=[],
                effective_from=now,
            )
        )

    ranked_sessions = sorted(sessions, key=_session_route_score, reverse=True)
    if ranked_sessions:
        winner = ranked_sessions[0]
        runner_up = ranked_sessions[1] if len(ranked_sessions) > 1 else None
        connected_accounts = _normalize_string_list(_field(winner, "connected_accounts"))
        account_targets = connected_accounts or [""]
        for account_id in account_targets:
            snapshots.append(
                RouteDecisionSnapshot(
                    snapshot_id=_stable_id("route", "trade", account_id or "default", now),
                    policy_id="policy_trade_default_v01",
                    purpose=RoutePurpose.TRADE.value,
                    account_id=account_id,
                    candidate_ids=[
                        str(_field(item, "session_id") or "")
                        for item in ranked_sessions
                        if str(_field(item, "session_id") or "")
                    ],
                    winner=str(_field(winner, "session_id") or ""),
                    runner_up=str(_field(runner_up, "session_id") or "") if runner_up is not None else "",
                    score_breakdown={
                        str(_field(item, "session_id") or ""): round(_session_route_score(item), 4)
                        for item in ranked_sessions[:5]
                        if str(_field(item, "session_id") or "")
                    },
                    runtime_snapshot_refs=[
                        str(_field(item, "session_id") or "")
                        for item in ranked_sessions[:5]
                        if str(_field(item, "session_id") or "")
                    ],
                    decision_reason="按连接态与认证态选择默认交易会话",
                    rejection_reasons=[],
                    effective_from=now,
                )
            )

    return {"items": snapshots, "total": len(snapshots)}

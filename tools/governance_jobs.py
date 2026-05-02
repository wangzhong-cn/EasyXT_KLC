#!/usr/bin/env python3
import argparse
import datetime
import hashlib
import hmac
import json
import os
import statistics
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_manager.unified_data_interface import UnifiedDataInterface
from tools.strategy_impact_gate import evaluate_strategy_impact


_WATERMARK_PROFILES: dict[str, dict[str, float]] = {
    "balanced": {"weight_late": 0.45, "weight_ooo": 0.35, "weight_lateness": 0.20, "qscore_floor": 0.97, "lookback_days": 7.0},
    "conservative": {"weight_late": 0.50, "weight_ooo": 0.35, "weight_lateness": 0.15, "qscore_floor": 0.985, "lookback_days": 14.0},
    "aggressive": {"weight_late": 0.40, "weight_ooo": 0.30, "weight_lateness": 0.30, "qscore_floor": 0.95, "lookback_days": 7.0},
}


def _resolve_watermark_profile(profile: str) -> dict[str, float]:
    key = str(profile or "balanced").strip().lower()
    if key not in _WATERMARK_PROFILES:
        key = "balanced"
    return {"profile": key, **_WATERMARK_PROFILES[key]}


def _summarize_watermark_events(
    *,
    event_file: Path,
    lookback_days: int,
    qscore_floor: float,
    weight_late: float,
    weight_ooo: float,
    weight_lateness: float,
) -> dict[str, object]:
    w_sum = max(float(weight_late) + float(weight_ooo) + float(weight_lateness), 1e-9)
    w_late = float(weight_late) / w_sum
    w_ooo = float(weight_ooo) / w_sum
    w_lateness = float(weight_lateness) / w_sum
    if not event_file.exists():
        return {
            "status": "missing",
            "file": str(event_file),
            "today": {"q_score": 1.0, "total": 0, "late": 0, "ooo": 0, "drop_rate": 0.0, "max_lateness_ms": 0},
            "trend": [],
            "trend_days": int(max(1, lookback_days)),
            "q_score_floor": float(qscore_floor),
            "q_score_pass": True,
            "weights": {"late": round(w_late, 6), "ooo": round(w_ooo, 6), "lateness": round(w_lateness, 6)},
        }
    today = datetime.date.today()
    cutoff = today - datetime.timedelta(days=max(1, int(lookback_days)) - 1)
    daily: dict[str, dict[str, float]] = {}
    try:
        with event_file.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                created_at = str(row.get("created_at") or "")
                day = created_at[:10]
                try:
                    day_d = datetime.date.fromisoformat(day)
                except Exception:
                    continue
                if day_d < cutoff:
                    continue
                item = daily.setdefault(day, {"total": 0.0, "late": 0.0, "ooo": 0.0, "max_lateness_ms": 0.0})
                item["total"] += 1.0
                reason = str(row.get("reason") or "")
                if reason == "late_watermark_exceeded":
                    item["late"] += 1.0
                if reason == "out_of_order_sequence":
                    item["ooo"] += 1.0
                try:
                    lat_ms = float(row.get("lateness_ms") or 0.0)
                except Exception:
                    lat_ms = 0.0
                item["max_lateness_ms"] = max(float(item["max_lateness_ms"]), lat_ms)
    except Exception:
        pass
    trend: list[dict[str, object]] = []
    for day in sorted(daily.keys()):
        d = daily[day]
        total = int(d["total"])
        late = int(d["late"])
        ooo = int(d["ooo"])
        max_lateness_ms = int(d["max_lateness_ms"])
        drop_rate = (late + ooo) / total if total > 0 else 0.0
        late_rate = late / total if total > 0 else 0.0
        ooo_rate = ooo / total if total > 0 else 0.0
        lateness_norm = min(max_lateness_ms / 120000.0, 1.0)
        late_score = max(0.0, min(1.0, 1.0 - late_rate))
        ooo_score = max(0.0, min(1.0, 1.0 - ooo_rate))
        lateness_score = max(0.0, min(1.0, 1.0 - lateness_norm))
        q_score = max(0.0, min(1.0, w_late * late_score + w_ooo * ooo_score + w_lateness * lateness_score))
        trend.append(
            {
                "date": day,
                "q_score": round(q_score, 6),
                "late_score": round(late_score, 6),
                "ooo_score": round(ooo_score, 6),
                "lateness_score": round(lateness_score, 6),
                "total": total,
                "late": late,
                "ooo": ooo,
                "drop_rate": round(drop_rate, 6),
                "max_lateness_ms": max_lateness_ms,
            }
        )
    today_key = today.isoformat()
    today_item = next((x for x in trend if str(x.get("date")) == today_key), None)
    if today_item is None:
        today_item = {
            "date": today_key,
            "q_score": 1.0,
            "late_score": 1.0,
            "ooo_score": 1.0,
            "lateness_score": 1.0,
            "total": 0,
            "late": 0,
            "ooo": 0,
            "drop_rate": 0.0,
            "max_lateness_ms": 0,
        }
        trend.append(today_item)
    trend = sorted(trend, key=lambda x: str(x.get("date")))[-max(1, int(lookback_days)) :]
    q_vals = [float(x.get("q_score", 1.0) or 1.0) for x in trend]
    late_vals = [float(x.get("late_score", 1.0) or 1.0) for x in trend]
    ooo_vals = [float(x.get("ooo_score", 1.0) or 1.0) for x in trend]
    lateness_vals = [float(x.get("lateness_score", 1.0) or 1.0) for x in trend]
    q_mean = float(statistics.fmean(q_vals)) if q_vals else 1.0
    q_vol = float(statistics.pstdev(q_vals)) if len(q_vals) > 1 else 0.0
    late_mean = float(statistics.fmean(late_vals)) if late_vals else 1.0
    late_vol = float(statistics.pstdev(late_vals)) if len(late_vals) > 1 else 0.0
    ooo_mean = float(statistics.fmean(ooo_vals)) if ooo_vals else 1.0
    ooo_vol = float(statistics.pstdev(ooo_vals)) if len(ooo_vals) > 1 else 0.0
    lateness_mean = float(statistics.fmean(lateness_vals)) if lateness_vals else 1.0
    lateness_vol = float(statistics.pstdev(lateness_vals)) if len(lateness_vals) > 1 else 0.0
    return {
        "status": "ok",
        "file": str(event_file),
        "today": today_item,
        "trend": trend,
        "trend_days": int(max(1, lookback_days)),
        "q_score_floor": float(qscore_floor),
        "q_score_pass": float(today_item.get("q_score", 0.0)) >= float(qscore_floor),
        "weights": {"late": round(w_late, 6), "ooo": round(w_ooo, 6), "lateness": round(w_lateness, 6)},
        "q_score_mean_7d": round(q_mean, 6),
        "q_score_vol_7d": round(q_vol, 6),
        "late_score_mean_7d": round(late_mean, 6),
        "late_score_vol_7d": round(late_vol, 6),
        "ooo_score_mean_7d": round(ooo_mean, 6),
        "ooo_score_vol_7d": round(ooo_vol, 6),
        "lateness_score_mean_7d": round(lateness_mean, 6),
        "lateness_score_vol_7d": round(lateness_vol, 6),
    }


def _summarize_watermark_profile_audit(*, audit_file: Path, max_entries: int = 5) -> dict[str, object]:
    if not audit_file.exists():
        return {"status": "missing", "file": str(audit_file), "count": 0, "recent": []}
    rows: list[dict[str, object]] = []
    try:
        with audit_file.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if not s or not s.startswith("{"):
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    except Exception:
        rows = []
    recent = rows[-max(1, int(max_entries)) :]
    compact = []
    for it in recent:
        compact.append(
            {
                "ts": str(it.get("ts") or ""),
                "action": str(it.get("action") or ""),
                "profile": str(it.get("profile") or ""),
                "success": bool(it.get("success", False)),
                "message": str(it.get("message") or ""),
            }
        )
    return {"status": "ok", "file": str(audit_file), "count": len(rows), "recent": compact}


def _validate_watermark_profile_approval(
    *,
    profile: str,
    release_env: str,
    required_profiles: str,
    approval_id: str,
    approver: str,
    approval_registry_file: str,
    approval_max_age_days: int,
    approval_signing_key: str = "",
    approval_require_signature: bool = True,
    approval_multisig_threshold: int = 1,
    approval_signers: str = "",
    approval_usage_log_file: str = "artifacts/watermark_approval_usage.jsonl",
    approval_expiry_warn_days: int = 2,
    approval_usage_warn_ratio: float = 0.8,
    consume_usage: bool = True,
) -> dict[str, object]:
    current_profile = str(profile or "").strip().lower() or "balanced"
    env_name = str(release_env or "").strip().lower() or "preprod"
    required_set = {
        p.strip().lower()
        for p in str(required_profiles or "").split(",")
        if p.strip()
    }
    requires_approval = env_name == "prod" and current_profile in required_set
    if not requires_approval:
        return {
            "required": False,
            "valid": True,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "not_required",
        }
    missing = []
    if not str(approval_id or "").strip():
        missing.append("approval_id")
    if not str(approver or "").strip():
        missing.append("approver")
    if missing:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "missing_required_fields",
            "missing_fields": missing,
        }
    registry_path = Path(str(approval_registry_file or "").strip() or "artifacts/watermark_approval_registry.json")
    if not registry_path.is_absolute():
        registry_path = PROJECT_ROOT / registry_path
    if not registry_path.exists():
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "registry_missing",
            "registry_path": str(registry_path),
        }
    try:
        payload = json.loads(registry_path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "registry_parse_error",
            "registry_path": str(registry_path),
        }
    approvals = payload.get("approvals") if isinstance(payload, dict) else []
    approvals = approvals if isinstance(approvals, list) else []
    match = None
    for item in approvals:
        if not isinstance(item, dict):
            continue
        if str(item.get("approval_id") or "").strip() != str(approval_id or "").strip():
            continue
        match = item
        break
    if not isinstance(match, dict):
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "approval_id_not_found",
            "registry_path": str(registry_path),
        }
    if bool(match.get("revoked", False)):
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "approval_revoked",
            "registry_path": str(registry_path),
        }
    approved_by = str(match.get("approver") or "").strip()
    if approved_by and approved_by != str(approver or "").strip():
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "approver_mismatch",
            "registry_path": str(registry_path),
            "registry_approver": approved_by,
        }
    profile_in_registry = str(match.get("profile") or "").strip().lower()
    if profile_in_registry and profile_in_registry != current_profile:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "profile_mismatch",
            "registry_path": str(registry_path),
            "registry_profile": profile_in_registry,
        }
    signing_key = str(approval_signing_key or "").strip()
    require_sig = bool(approval_require_signature)
    signature_raw = str(match.get("signature") or "").strip()
    threshold_entry = int(match.get("required_signatures") or 0) if str(match.get("required_signatures") or "").strip() else 0
    threshold = max(threshold_entry or int(approval_multisig_threshold or 1), 1)
    allowed_signers = {s.strip() for s in str(approval_signers or "").split(",") if s.strip()}
    valid_sign_count = 0
    if require_sig:
        if not signing_key:
            return {
                "required": True,
                "valid": False,
                "release_env": env_name,
                "profile": current_profile,
                "approval_id": str(approval_id or ""),
                "approver": str(approver or ""),
                "reason": "signing_key_missing",
                "registry_path": str(registry_path),
            }
        signatures = match.get("signatures") if isinstance(match.get("signatures"), list) else []
        if not signature_raw and not signatures:
            return {
                "required": True,
                "valid": False,
                "release_env": env_name,
                "profile": current_profile,
                "approval_id": str(approval_id or ""),
                "approver": str(approver or ""),
                "reason": "signature_missing",
                "registry_path": str(registry_path),
            }
        sign_parts = [
            str(match.get("approval_id") or "").strip(),
            str(match.get("approver") or "").strip(),
            str(match.get("approved_at") or "").strip(),
            str(match.get("expires_at") or "").strip(),
            str(match.get("profile") or "").strip(),
            str(match.get("reason") or "").strip(),
            str(match.get("max_uses") or "").strip(),
        ]
        sign_payload = "|".join(sign_parts)
        expected_single = hmac.new(signing_key.encode("utf-8"), sign_payload.encode("utf-8"), hashlib.sha256).hexdigest()
        legacy_valid = bool(signature_raw) and hmac.compare_digest(signature_raw.lower(), expected_single.lower())
        valid_signers: set[str] = set()
        for item in signatures:
            if not isinstance(item, dict):
                continue
            signer = str(item.get("signer") or "").strip()
            sign = str(item.get("signature") or "").strip()
            if not signer or not sign:
                continue
            if allowed_signers and signer not in allowed_signers:
                continue
            signer_payload = sign_payload + "|" + signer
            expected = hmac.new(signing_key.encode("utf-8"), signer_payload.encode("utf-8"), hashlib.sha256).hexdigest()
            if hmac.compare_digest(sign.lower(), expected.lower()):
                valid_signers.add(signer)
        valid_sign_count = len(valid_signers)
        if legacy_valid:
            valid_sign_count = max(valid_sign_count, 1)
        if valid_sign_count < threshold:
            return {
                "required": True,
                "valid": False,
                "release_env": env_name,
                "profile": current_profile,
                "approval_id": str(approval_id or ""),
                "approver": str(approver or ""),
                "reason": "signature_invalid",
                "registry_path": str(registry_path),
                "signatures_required": threshold,
                "signatures_valid_count": valid_sign_count,
            }
    approved_at_raw = str(match.get("approved_at") or "")
    try:
        approved_at = datetime.datetime.fromisoformat(approved_at_raw.replace("Z", "+00:00"))
        approved_at = approved_at.replace(tzinfo=None)
    except Exception:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "approved_at_invalid",
            "registry_path": str(registry_path),
        }
    max_age = max(int(approval_max_age_days), 1)
    expire_by_age = approved_at + datetime.timedelta(days=max_age)
    expires_at_raw = str(match.get("expires_at") or "").strip()
    explicit_expire = None
    if expires_at_raw:
        try:
            explicit_expire = datetime.datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return {
                "required": True,
                "valid": False,
                "release_env": env_name,
                "profile": current_profile,
                "approval_id": str(approval_id or ""),
                "approver": str(approver or ""),
                "reason": "expires_at_invalid",
                "registry_path": str(registry_path),
            }
    final_expire = min(expire_by_age, explicit_expire) if explicit_expire is not None else expire_by_age
    now = datetime.datetime.now()
    usage_log_path = Path(str(approval_usage_log_file or "").strip() or "artifacts/watermark_approval_usage.jsonl")
    if not usage_log_path.is_absolute():
        usage_log_path = PROJECT_ROOT / usage_log_path
    max_uses_raw = match.get("max_uses")
    max_uses = int(max_uses_raw) if str(max_uses_raw or "").strip() else 0
    used_count = 0
    if usage_log_path.exists():
        try:
            with usage_log_path.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    s = line.strip()
                    if not s or not s.startswith("{"):
                        continue
                    try:
                        row = json.loads(s)
                    except Exception:
                        continue
                    if str(row.get("approval_id") or "") == str(approval_id or ""):
                        used_count += 1
        except Exception:
            pass
    if max_uses > 0 and used_count >= max_uses:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "usage_limit_exceeded",
            "registry_path": str(registry_path),
            "max_uses": max_uses,
            "used_count": used_count,
            "usage_log_file": str(usage_log_path),
            "signatures_required": threshold,
            "signatures_valid_count": valid_sign_count,
        }
    if now > final_expire:
        return {
            "required": True,
            "valid": False,
            "release_env": env_name,
            "profile": current_profile,
            "approval_id": str(approval_id or ""),
            "approver": str(approver or ""),
            "reason": "approval_expired",
            "registry_path": str(registry_path),
            "approved_at": approved_at.isoformat(timespec="seconds"),
            "expires_at": final_expire.isoformat(timespec="seconds"),
            "approval_max_age_days": max_age,
            "days_to_expire": -1,
            "max_uses": max_uses,
            "used_count": used_count,
            "usage_log_file": str(usage_log_path),
            "signatures_required": threshold,
            "signatures_valid_count": valid_sign_count,
        }
    days_to_expire = max(int((final_expire - now).total_seconds() // 86400), 0)
    if consume_usage:
        try:
            usage_log_path.parent.mkdir(parents=True, exist_ok=True)
            with usage_log_path.open("a", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {
                            "ts": now.isoformat(timespec="seconds"),
                            "approval_id": str(approval_id or ""),
                            "approver": str(approver or ""),
                            "profile": current_profile,
                            "release_env": env_name,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            used_count += 1
        except Exception:
            pass
    remaining_uses = (max_uses - used_count) if max_uses > 0 else -1
    warnings: list[str] = []
    if days_to_expire <= max(int(approval_expiry_warn_days), 0):
        warnings.append("expiry_soon")
    if max_uses > 0:
        ratio = used_count / max(max_uses, 1)
        if ratio >= float(approval_usage_warn_ratio):
            warnings.append("usage_near_limit")
    risk_level = "warn" if warnings else "ok"
    return {
        "required": True,
        "valid": True,
        "release_env": env_name,
        "profile": current_profile,
        "approval_id": str(approval_id or ""),
        "approver": str(approver or ""),
        "reason": "approved",
        "registry_path": str(registry_path),
        "approved_at": approved_at.isoformat(timespec="seconds"),
        "expires_at": final_expire.isoformat(timespec="seconds"),
        "approval_max_age_days": max_age,
        "days_to_expire": days_to_expire,
        "signature_required": require_sig,
        "signature_valid": True if require_sig else False,
        "signatures_required": threshold,
        "signatures_valid_count": valid_sign_count,
        "max_uses": max_uses,
        "used_count": used_count,
        "remaining_uses": remaining_uses,
        "usage_log_file": str(usage_log_path),
        "warnings": warnings,
        "risk_level": risk_level,
    }


def _validate_strategy_impact_baseline_metadata(path: Path | None) -> dict[str, object]:
    if path is None:
        return {"checked": False, "valid": True, "reason": "baseline_not_provided"}
    if not path.exists():
        return {"checked": True, "valid": False, "reason": "baseline_missing", "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"checked": True, "valid": False, "reason": "baseline_parse_error", "path": str(path)}
    if not isinstance(payload, dict):
        return {"checked": True, "valid": False, "reason": "baseline_not_object", "path": str(path)}
    meta = payload.get("_meta", {})
    if not isinstance(meta, dict):
        return {"checked": True, "valid": False, "reason": "meta_missing", "path": str(path)}
    required = ("approval_id", "approver", "updated_at", "reason_excerpt")
    missing = [k for k in required if not str(meta.get(k, "")).strip()]
    if missing:
        return {
            "checked": True,
            "valid": False,
            "reason": "meta_required_fields_missing",
            "missing_fields": missing,
            "path": str(path),
        }
    return {"checked": True, "valid": True, "reason": "ok", "path": str(path)}


def _run_batch_multiperiod_rebuild(
    ui: UnifiedDataInterface,
    *,
    start_date: str,
    end_date: str,
    periods: list[str],
    symbol_limit: int,
) -> dict[str, object]:
    details: list[dict[str, object]] = []
    if ui.con is None:
        return {"ok": False, "error": "duckdb_connection_missing", "processed": 0, "succeeded": 0, "failed": 0, "details": details}
    try:
        rows = ui.con.execute(
            """
            SELECT stock_code, COUNT(*) AS c
            FROM stock_1m
            WHERE period = '1m' AND datetime >= ?
            GROUP BY stock_code
            ORDER BY c DESC
            LIMIT ?
            """,
            [f"{start_date} 00:00:00", max(int(symbol_limit), 1)],
        ).fetchall()
    except Exception as e:
        return {"ok": False, "error": f"symbol_query_failed:{e}", "processed": 0, "succeeded": 0, "failed": 0, "details": details}
    symbols = [str(r[0]).strip() for r in rows if str(r[0]).strip()]
    if not symbols:
        return {"ok": False, "error": "no_symbols_for_rebuild", "processed": 0, "succeeded": 0, "failed": 0, "details": details}
    succeeded = 0
    failed = 0
    for symbol in symbols:
        result = ui.run_multiperiod_rebuild(
            stock_code=symbol,
            start_date=start_date,
            end_date=end_date,
            periods=periods,
        )
        ok = bool(result.get("ok", False))
        if ok:
            succeeded += 1
        else:
            failed += 1
        details.append(result)
    return {
        "ok": failed == 0,
        "mode": "batch_auto_symbols",
        "processed": len(symbols),
        "succeeded": succeeded,
        "failed": failed,
        "symbols": symbols,
        "details": details,
    }


def _validate_rebuild_receipt(payload: dict[str, object]) -> dict[str, object]:
    receipt = payload.get("audit_receipt") if isinstance(payload, dict) else {}
    receipt = receipt if isinstance(receipt, dict) else {}
    governance = receipt.get("governance") if isinstance(receipt.get("governance"), dict) else {}
    governance = governance if isinstance(governance, dict) else {}
    period_metadata = (
        governance.get("period_metadata") if isinstance(governance.get("period_metadata"), dict) else {}
    )
    period_metadata = period_metadata if isinstance(period_metadata, dict) else {}
    rebuild_id = str(payload.get("rebuild_id") or "")
    ok = bool(payload.get("ok", False))
    status = str(receipt.get("status") or "")
    receipt_hash = str(receipt.get("receipt_hash") or "")
    required_governance_fields = (
        "session_profile_id",
        "session_profile_version",
        "auction_policy",
        "period_registry_version",
        "threshold_registry_version",
        "period_metadata",
    )
    missing_governance_fields = [
        field
        for field in required_governance_fields
        if (
            field == "period_metadata" and not period_metadata
        )
        or (
            field != "period_metadata" and not str(governance.get(field) or "").strip()
        )
    ]
    target_periods = receipt.get("target_periods") if isinstance(receipt.get("target_periods"), list) else []
    target_periods = [str(item).strip() for item in target_periods if str(item).strip()]
    missing_period_metadata_periods = [
        period for period in target_periods if period not in period_metadata
    ]
    governance_valid = not missing_governance_fields and not missing_period_metadata_periods
    valid = bool(rebuild_id) and ok and status == "success" and bool(receipt_hash) and governance_valid
    return {
        "valid": valid,
        "rebuild_id": rebuild_id,
        "status": status,
        "receipt_hash": receipt_hash,
        "governance_valid": governance_valid,
        "missing_governance_fields": missing_governance_fields,
        "missing_period_metadata_periods": missing_period_metadata_periods,
        "session_profile_id": str(governance.get("session_profile_id") or ""),
        "session_profile_version": str(governance.get("session_profile_version") or ""),
        "period_registry_version": str(governance.get("period_registry_version") or ""),
        "threshold_registry_version": str(governance.get("threshold_registry_version") or ""),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run governance replay and SLA jobs.")
    parser.add_argument("--job", choices=["replay", "late_replay", "rebuild", "sla", "all"], default="all")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--late-limit", type=int, default=120)
    parser.add_argument("--late-max-retries", type=int, default=5)
    parser.add_argument("--late-reason-regex", default=r"(late|out_of_order|watermark|stale|reorder)")
    parser.add_argument("--report-date", default=None)
    parser.add_argument("--duckdb-path", default=None)
    parser.add_argument("--stock-code", default="")
    parser.add_argument("--start-date", default="")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--periods", default="1m,5m,15m,30m,60m,1d,1w,1M")
    parser.add_argument("--rebuild-auto-symbol-limit", type=int, default=3)
    parser.add_argument("--rebuild-lookback-days", type=int, default=7)
    parser.add_argument("--strict-sla", action="store_true")
    parser.add_argument("--strict-dead-letter", action="store_true")
    parser.add_argument("--strict-late-replay", action="store_true")
    parser.add_argument("--strict-rebuild", action="store_true")
    parser.add_argument("--strict-watermark-qscore", action="store_true")
    parser.add_argument("--strict-watermark-profile-approval", action="store_true")
    parser.add_argument("--strict-watermark-profile-risk", action="store_true")
    parser.add_argument("--watermark-event-file", default="artifacts/realtime_watermark_events.jsonl")
    parser.add_argument("--watermark-profile", default=os.environ.get("EASYXT_WM_PROFILE", "balanced"))
    parser.add_argument("--release-env", default=os.environ.get("EASYXT_RELEASE_ENV", "preprod"))
    parser.add_argument("--watermark-approval-required-profiles", default=os.environ.get("EASYXT_WM_APPROVAL_REQUIRED_PROFILES", "aggressive"))
    parser.add_argument("--watermark-approval-id", default=os.environ.get("EASYXT_WM_APPROVAL_ID", ""))
    parser.add_argument("--watermark-approver", default=os.environ.get("EASYXT_WM_APPROVER", ""))
    parser.add_argument("--watermark-approval-registry-file", default=os.environ.get("EASYXT_WM_APPROVAL_REGISTRY_PATH", "artifacts/watermark_approval_registry.json"))
    parser.add_argument("--watermark-approval-max-age-days", type=int, default=int(os.environ.get("EASYXT_WM_APPROVAL_MAX_AGE_DAYS", "7")))
    parser.add_argument("--watermark-approval-signing-key", default=os.environ.get("EASYXT_WM_APPROVAL_SIGNING_KEY", ""))
    parser.add_argument("--watermark-approval-multisig-threshold", type=int, default=int(os.environ.get("EASYXT_WM_APPROVAL_MULTISIG_THRESHOLD", "1")))
    parser.add_argument("--watermark-approval-signers", default=os.environ.get("EASYXT_WM_APPROVAL_SIGNERS", ""))
    parser.add_argument("--watermark-approval-expiry-warn-days", type=int, default=int(os.environ.get("EASYXT_WM_APPROVAL_EXPIRY_WARN_DAYS", "2")))
    parser.add_argument("--watermark-approval-usage-warn-ratio", type=float, default=float(os.environ.get("EASYXT_WM_APPROVAL_USAGE_WARN_RATIO", "0.8")))
    parser.add_argument(
        "--watermark-approval-require-signature",
        action="store_true",
        default=os.environ.get("EASYXT_WM_APPROVAL_REQUIRE_SIGNATURE", "1") in ("1", "true", "True"),
    )
    parser.add_argument("--watermark-approval-usage-log-file", default=os.environ.get("EASYXT_WM_APPROVAL_USAGE_LOG_PATH", "artifacts/watermark_approval_usage.jsonl"))
    parser.add_argument("--watermark-profile-audit-file", default="artifacts/watermark_profile_audit.jsonl")
    parser.add_argument("--watermark-profile-audit-max-entries", type=int, default=5)
    parser.add_argument("--watermark-lookback-days", type=int, default=int(os.environ.get("EASYXT_WM_LOOKBACK_DAYS", "7")))
    parser.add_argument("--watermark-qscore-floor", type=float, default=float(os.environ.get("EASYXT_WM_QSCORE_FLOOR", "0.97")))
    parser.add_argument("--watermark-weight-late", type=float, default=float(os.environ.get("EASYXT_WM_WEIGHT_LATE", "0.45")))
    parser.add_argument("--watermark-weight-ooo", type=float, default=float(os.environ.get("EASYXT_WM_WEIGHT_OOO", "0.35")))
    parser.add_argument("--watermark-weight-lateness", type=float, default=float(os.environ.get("EASYXT_WM_WEIGHT_LATENESS", "0.20")))
    parser.add_argument("--strict-strategy-impact", action="store_true")
    parser.add_argument("--strategy-impact-baseline", default=None)
    parser.add_argument("--strategy-impact-results-dir", default=None)
    parser.add_argument("--strategy-impact-delta-return", type=float, default=3.0)
    parser.add_argument("--strategy-impact-delta-mdd", type=float, default=1.5)
    parser.add_argument("--strategy-impact-enforce-sharpe-sign", action="store_true")
    parser.add_argument("--strict-strategy-impact-baseline-meta", action="store_true")
    args = parser.parse_args()
    profile_cfg = _resolve_watermark_profile(getattr(args, "watermark_profile", "balanced"))
    if "--watermark-lookback-days" not in sys.argv:
        args.watermark_lookback_days = int(profile_cfg["lookback_days"])
    if "--watermark-qscore-floor" not in sys.argv:
        args.watermark_qscore_floor = float(profile_cfg["qscore_floor"])
    if "--watermark-weight-late" not in sys.argv:
        args.watermark_weight_late = float(profile_cfg["weight_late"])
    if "--watermark-weight-ooo" not in sys.argv:
        args.watermark_weight_ooo = float(profile_cfg["weight_ooo"])
    if "--watermark-weight-lateness" not in sys.argv:
        args.watermark_weight_lateness = float(profile_cfg["weight_lateness"])

    ui = UnifiedDataInterface(duckdb_path=args.duckdb_path, silent_init=True)
    if not ui.connect(read_only=False):
        print(json.dumps({"ok": False, "error": "duckdb_connect_failed"}, ensure_ascii=False))
        return 1
    result: dict[str, object] = {"ok": True}
    try:
        result["watermark_quality"] = _summarize_watermark_events(
            event_file=Path(args.watermark_event_file),
            lookback_days=max(int(args.watermark_lookback_days), 1),
            qscore_floor=float(args.watermark_qscore_floor),
            weight_late=max(float(args.watermark_weight_late), 0.0),
            weight_ooo=max(float(args.watermark_weight_ooo), 0.0),
            weight_lateness=max(float(args.watermark_weight_lateness), 0.0),
        )
        result["watermark_quality"]["profile"] = str(profile_cfg.get("profile") or "balanced")
        result["watermark_profile_audit"] = _summarize_watermark_profile_audit(
            audit_file=Path(args.watermark_profile_audit_file),
            max_entries=max(int(args.watermark_profile_audit_max_entries), 1),
        )
        result["watermark_profile_approval"] = _validate_watermark_profile_approval(
            profile=str(result["watermark_quality"].get("profile") or "balanced"),
            release_env=str(args.release_env or "preprod"),
            required_profiles=str(args.watermark_approval_required_profiles or "aggressive"),
            approval_id=str(args.watermark_approval_id or ""),
            approver=str(args.watermark_approver or ""),
            approval_registry_file=str(args.watermark_approval_registry_file or ""),
            approval_max_age_days=max(int(args.watermark_approval_max_age_days), 1),
            approval_signing_key=str(args.watermark_approval_signing_key or ""),
            approval_require_signature=bool(args.watermark_approval_require_signature),
            approval_multisig_threshold=max(int(args.watermark_approval_multisig_threshold), 1),
            approval_signers=str(args.watermark_approval_signers or ""),
            approval_usage_log_file=str(args.watermark_approval_usage_log_file or ""),
            approval_expiry_warn_days=max(int(args.watermark_approval_expiry_warn_days), 0),
            approval_usage_warn_ratio=max(float(args.watermark_approval_usage_warn_ratio), 0.0),
            consume_usage=True,
        )
        if args.job in ("replay", "all"):
            result["replay"] = ui.run_quarantine_replay(
                limit=max(args.limit, 1), max_retries=max(args.max_retries, 1)
            )
        if args.job in ("late_replay", "all"):
            result["late_event_replay"] = ui.run_late_event_replay(
                limit=max(args.late_limit, 1),
                max_retries=max(args.late_max_retries, 1),
                reason_regex=str(args.late_reason_regex or "").strip() or r"(late|out_of_order|watermark|stale|reorder)",
            )
        if args.job in ("rebuild", "all"):
            periods = [p.strip() for p in str(args.periods or "").split(",") if p.strip()]
            start_date = str(args.start_date or "").strip()
            end_date = str(args.end_date or "").strip()
            if not start_date or not end_date:
                end_dt = datetime.date.today()
                start_dt = end_dt - datetime.timedelta(days=max(int(args.rebuild_lookback_days), 1))
                start_date = start_dt.strftime("%Y-%m-%d")
                end_date = end_dt.strftime("%Y-%m-%d")
            if not args.stock_code:
                result["multiperiod_rebuild"] = _run_batch_multiperiod_rebuild(
                    ui,
                    start_date=start_date,
                    end_date=end_date,
                    periods=periods,
                    symbol_limit=max(int(args.rebuild_auto_symbol_limit), 1),
                )
            else:
                result["multiperiod_rebuild"] = ui.run_multiperiod_rebuild(
                    stock_code=str(args.stock_code).strip(),
                    start_date=start_date,
                    end_date=end_date,
                    periods=periods,
                )
            rebuild_payload = result.get("multiperiod_rebuild") if isinstance(result.get("multiperiod_rebuild"), dict) else {}
            if isinstance(rebuild_payload, dict):
                if rebuild_payload.get("mode") == "batch_auto_symbols":
                    items = rebuild_payload.get("details") if isinstance(rebuild_payload.get("details"), list) else []
                    result["multiperiod_rebuild_receipt_check"] = {
                        "valid": all(_validate_rebuild_receipt(it).get("valid", False) for it in items) if items else False,
                        "items": [_validate_rebuild_receipt(it) for it in items],
                    }
                else:
                    result["multiperiod_rebuild_receipt_check"] = _validate_rebuild_receipt(rebuild_payload)
        if args.job in ("sla", "all"):
            result["sla"] = ui.generate_daily_sla_report(report_date=args.report_date)
            result["step6_validation"] = ui.get_step6_validation_metrics()
            baseline_path = Path(args.strategy_impact_baseline) if args.strategy_impact_baseline else None
            results_dir = Path(args.strategy_impact_results_dir) if args.strategy_impact_results_dir else None
            kwargs: dict[str, object] = {
                "delta_return_threshold": float(args.strategy_impact_delta_return),
                "delta_mdd_threshold": float(args.strategy_impact_delta_mdd),
                "enforce_sharpe_sign": bool(args.strategy_impact_enforce_sharpe_sign),
            }
            if baseline_path is not None:
                kwargs["baseline_path"] = baseline_path
            if results_dir is not None:
                kwargs["results_dir"] = results_dir
            result["strategy_impact"] = evaluate_strategy_impact(**kwargs)
            result["strategy_impact_baseline_meta"] = _validate_strategy_impact_baseline_metadata(baseline_path)
    finally:
        ui.close()
    print(json.dumps(result, ensure_ascii=False))
    if args.strict_sla and isinstance(result.get("sla"), dict):
        gate_pass = bool((result.get("sla") or {}).get("gate_pass", False))
        if not gate_pass:
            return 2
    if args.strict_dead_letter and isinstance(result.get("replay"), dict):
        dead_letter = int((result.get("replay") or {}).get("dead_letter", 0) or 0)
        if dead_letter > 0:
            return 3
    if args.strict_late_replay and isinstance(result.get("late_event_replay"), dict):
        late_failed = int((result.get("late_event_replay") or {}).get("failed", 0) or 0)
        late_dead = int((result.get("late_event_replay") or {}).get("dead_letter", 0) or 0)
        if late_failed > 0 or late_dead > 0:
            return 6
    if args.strict_rebuild and isinstance(result.get("multiperiod_rebuild"), dict):
        rebuild_payload = result.get("multiperiod_rebuild") or {}
        rebuild_failed = int(rebuild_payload.get("failed", 0) or 0)
        rebuild_ok = bool(rebuild_payload.get("ok", False))
        receipt_check = result.get("multiperiod_rebuild_receipt_check") if isinstance(result.get("multiperiod_rebuild_receipt_check"), dict) else {}
        receipt_valid = bool(receipt_check.get("valid", False))
        if rebuild_failed > 0 or not rebuild_ok or not receipt_valid:
            return 7
    if args.strict_watermark_qscore and isinstance(result.get("watermark_quality"), dict):
        wm = result.get("watermark_quality") or {}
        if not bool(wm.get("q_score_pass", False)):
            return 8
    if args.strict_watermark_profile_approval and isinstance(result.get("watermark_profile_approval"), dict):
        appr = result.get("watermark_profile_approval") or {}
        if not bool(appr.get("valid", False)):
            return 9
    if args.strict_watermark_profile_risk and isinstance(result.get("watermark_profile_approval"), dict):
        appr = result.get("watermark_profile_approval") or {}
        if str(appr.get("risk_level") or "").lower() == "warn":
            return 10
    if args.strict_strategy_impact and isinstance(result.get("strategy_impact"), dict):
        impact = result.get("strategy_impact") or {}
        if bool(impact.get("available", False)) and not bool(impact.get("gate_pass", True)):
            return 4
    if args.strict_strategy_impact_baseline_meta and isinstance(result.get("strategy_impact_baseline_meta"), dict):
        meta_check = result.get("strategy_impact_baseline_meta") or {}
        if not bool(meta_check.get("valid", False)):
            return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

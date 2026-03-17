from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, request

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_NOTIFICATION = PROJECT_ROOT / "artifacts" / "peak_release_notification_latest.json"
DEFAULT_OUT_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notify_delivery_latest.json"
DEFAULT_LOG_JSONL = PROJECT_ROOT / "logs" / "peak_release_notify_delivery.jsonl"
DEFAULT_DEDUPE_INDEX_JSON = PROJECT_ROOT / "artifacts" / "peak_release_notify_dedupe_index.json"


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_notification(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _send_webhook(webhook_url: str, payload: dict[str, Any], timeout_sec: int) -> dict[str, Any]:
    req = request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=max(1, timeout_sec)) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return {"ok": 200 <= resp.status < 300, "status_code": int(resp.status), "response_body": body[:500]}
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        return {"ok": False, "status_code": int(exc.code), "response_body": body[:500]}
    except Exception as exc:
        return {"ok": False, "status_code": 0, "error": str(exc)}


def _build_text_message(notification: dict[str, Any]) -> dict[str, Any]:
    return {
        "msg_type": "text",
        "content": {"text": str(notification.get("selected_message", "") or notification.get("message", ""))},
    }


def _build_post_message(notification: dict[str, Any]) -> dict[str, Any] | None:
    title = str(notification.get("title", "") or "").strip()
    message = str(notification.get("selected_message", "") or notification.get("message", "")).strip()
    if not title or not message:
        return None
    level = str(notification.get("level", "unknown") or "unknown")
    level_badge = {"pass": "🟢 PASS", "warn": "🟡 WARN", "fail": "🔴 FAIL"}.get(level, "⚪ UNKNOWN")
    consec = int(notification.get("consecutive_compliant_days", 0) or 0)
    fail_days = int(notification.get("fail_consecutive_days", 0) or 0)
    gap_days = int(notification.get("gap_to_fail_days", 0) or 0)
    ratio = float(notification.get("compliance_ratio_pct", 0.0) or 0.0)
    content = [
        [{"tag": "text", "text": f"status: {level_badge}"}],
        [{"tag": "text", "text": f"release_env: {notification.get('release_env', 'N/A') or 'N/A'}"}],
        [{"tag": "text", "text": f"consecutive: {consec}/{fail_days} | gap: {gap_days}d | ratio: {ratio:.2f}%"}],
        [{"tag": "text", "text": message}],
    ]
    return {
        "msg_type": "post",
        "content": {"post": {"zh_cn": {"title": title, "content": content}}},
    }


def _build_message(notification: dict[str, Any], message_format: str) -> tuple[dict[str, Any], str]:
    fmt = (message_format or "auto").strip().lower()
    if fmt == "text":
        return _build_text_message(notification), "text"
    if fmt == "post":
        post = _build_post_message(notification)
        if post is not None:
            return post, "post"
        return _build_text_message(notification), "text_fallback"
    post = _build_post_message(notification)
    if post is not None:
        return post, "post"
    return _build_text_message(notification), "text_fallback"


def _should_retry(sent: dict[str, Any]) -> bool:
    if bool(sent.get("ok", False)):
        return False
    code = int(sent.get("status_code", 0) or 0)
    return code in (0, 408, 429) or code >= 500


def _send_webhook_with_retry(
    webhook_url: str,
    payload: dict[str, Any],
    *,
    timeout_sec: int,
    max_attempts: int,
    retry_backoff_sec: float,
    sender: Any = None,
    sleep_fn: Any = None,
) -> dict[str, Any]:
    send_once = sender or _send_webhook
    sleep = sleep_fn or time.sleep
    attempts = max(1, int(max_attempts))
    backoff = max(0.0, float(retry_backoff_sec))
    last: dict[str, Any] = {"ok": False, "status_code": 0, "error": "unknown"}
    for i in range(attempts):
        last = send_once(webhook_url, payload, timeout_sec)
        if not _should_retry(last):
            break
        if i < attempts - 1 and backoff > 0:
            sleep(backoff * (2**i))
    out = dict(last)
    out["attempt_count"] = i + 1
    out["retried"] = bool(i > 0)
    return out


def _append_log(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _load_dedupe_index(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"entries": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"entries": {}}
    if not isinstance(payload, dict):
        return {"entries": {}}
    entries = payload.get("entries", {})
    if not isinstance(entries, dict):
        return {"entries": {}}
    return {"entries": entries}


def _save_dedupe_index(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _dedupe_index_hit(path: Path, dedupe_key: str) -> bool:
    if not dedupe_key:
        return False
    idx = _load_dedupe_index(path)
    row = idx.get("entries", {}).get(dedupe_key)
    return isinstance(row, dict) and bool(row.get("delivered", False))


def _dedupe_index_mark_delivered(path: Path, dedupe_key: str, *, max_entries: int) -> None:
    if not dedupe_key:
        return
    idx = _load_dedupe_index(path)
    entries = idx.setdefault("entries", {})
    if not isinstance(entries, dict):
        entries = {}
        idx["entries"] = entries
    entries[dedupe_key] = {"delivered": True, "updated_at": _utc_now()}
    if max_entries > 0 and len(entries) > max_entries:
        keys = list(entries.keys())
        drop_n = len(entries) - max_entries
        for k in keys[:drop_n]:
            entries.pop(k, None)
    _save_dedupe_index(path, idx)


def _classify_failure(*, status: str, ok: bool, reason: str, status_code: int, error_text: str) -> str:
    if status == "sent" and ok:
        return "success"
    if status == "skipped":
        if reason == "duplicate_delivery_deduped":
            return "deduped"
        if reason == "webhook_not_configured":
            return "not_configured"
        return "skipped"
    if reason == "webhook_required_but_missing":
        return "config_missing"
    if reason == "notification_missing_or_invalid":
        return "payload_invalid"
    if status_code in (408, 429):
        return "throttle_timeout"
    if status_code >= 500:
        return "server_error"
    if status_code >= 400:
        return "client_error"
    err = (error_text or "").lower()
    if "timeout" in err or "timed out" in err:
        return "network_timeout"
    if status_code == 0:
        return "network_error"
    return "unknown_failure"


def _was_delivered(path: Path, run_id: str, template_key: str, *, max_scan_lines: int) -> bool:
    if not path.exists() or not run_id or not template_key:
        return False
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return False
    if max_scan_lines > 0:
        lines = lines[-max_scan_lines:]
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("run_id", "")) != run_id:
            continue
        if str(row.get("selected_template_key", "")) != template_key:
            continue
        if bool(row.get("ok", False)) and str(row.get("status", "")) == "sent":
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="发送峰值门禁通知到Webhook并记录投递日志")
    parser.add_argument("--notification", type=Path, default=DEFAULT_NOTIFICATION)
    parser.add_argument("--webhook-url", default="")
    parser.add_argument("--timeout-sec", type=int, default=10)
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--retry-backoff-sec", type=float, default=1.0)
    parser.add_argument("--message-format", choices=["auto", "text", "post"], default="auto")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--require-webhook", action="store_true")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--enable-dedupe", action="store_true")
    parser.add_argument("--dedupe-max-scan-lines", type=int, default=2000)
    parser.add_argument("--dedupe-index", type=Path, default=DEFAULT_DEDUPE_INDEX_JSON)
    parser.add_argument("--dedupe-index-max-entries", type=int, default=5000)
    parser.add_argument("--out-json", type=Path, default=DEFAULT_OUT_JSON)
    parser.add_argument("--log-jsonl", type=Path, default=DEFAULT_LOG_JSONL)
    args = parser.parse_args(argv)

    notification = _load_notification(args.notification)
    run_id = str(args.run_id or "")
    template_key = str(notification.get("selected_template_key", ""))
    dedupe_key = f"{run_id}:{template_key}" if run_id and template_key else ""
    result: dict[str, Any] = {
        "ts": _utc_now(),
        "notification_path": str(args.notification),
        "level": str(notification.get("level", "unknown")),
        "release_env": str(notification.get("release_env", "")),
        "selected_template_key": template_key,
        "run_id": run_id,
        "dedupe_key": dedupe_key,
        "dedupe_enabled": bool(args.enable_dedupe),
        "webhook_enabled": bool(args.webhook_url),
        "dedupe_source": "none",
    }
    if not args.webhook_url:
        if args.require_webhook:
            result.update({"status": "failed", "ok": False, "reason": "webhook_required_but_missing"})
        else:
            result.update({"status": "skipped", "ok": True, "reason": "webhook_not_configured"})
    elif not notification:
        result.update({"status": "failed", "ok": False, "reason": "notification_missing_or_invalid"})
    elif bool(args.enable_dedupe) and _dedupe_index_hit(args.dedupe_index, dedupe_key):
        result.update(
            {
                "status": "skipped",
                "ok": True,
                "reason": "duplicate_delivery_deduped",
                "dedupe_hit": True,
                "dedupe_source": "index",
            }
        )
    elif bool(args.enable_dedupe) and _was_delivered(
        args.log_jsonl,
        run_id=run_id,
        template_key=template_key,
        max_scan_lines=max(1, int(args.dedupe_max_scan_lines)),
    ):
        result.update(
            {
                "status": "skipped",
                "ok": True,
                "reason": "duplicate_delivery_deduped",
                "dedupe_hit": True,
                "dedupe_source": "log",
            }
        )
        _dedupe_index_mark_delivered(
            args.dedupe_index, dedupe_key, max_entries=max(1, int(args.dedupe_index_max_entries))
        )
    else:
        payload, used_format = _build_message(notification, args.message_format)
        sent = _send_webhook_with_retry(
            args.webhook_url,
            payload,
            timeout_sec=args.timeout_sec,
            max_attempts=args.max_attempts,
            retry_backoff_sec=args.retry_backoff_sec,
        )
        result.update(
            {
                "status": "sent" if bool(sent.get("ok", False)) else "failed",
                "ok": bool(sent.get("ok", False)),
                "message_format_requested": str(args.message_format),
                "message_format_used": used_format,
                "attempt_count": int(sent.get("attempt_count", 1) or 1),
                "retried": bool(sent.get("retried", False)),
                "status_code": int(sent.get("status_code", 0) or 0),
                "response_body": str(sent.get("response_body", "")),
                "error": str(sent.get("error", "")) if sent.get("error") else "",
                "dedupe_hit": False,
                "dedupe_source": "none",
            }
        )
        if bool(result.get("ok", False)) and str(result.get("status", "")) == "sent":
            _dedupe_index_mark_delivered(
                args.dedupe_index, dedupe_key, max_entries=max(1, int(args.dedupe_index_max_entries))
            )

    result["failure_class"] = _classify_failure(
        status=str(result.get("status", "")),
        ok=bool(result.get("ok", False)),
        reason=str(result.get("reason", "")),
        status_code=int(result.get("status_code", 0) or 0),
        error_text=str(result.get("error", "")),
    )

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_log(args.log_jsonl, result)
    print(json.dumps(result, ensure_ascii=False))
    if args.strict and not bool(result.get("ok", False)):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import json
from pathlib import Path

from tools import peak_release_notify_webhook as notify


def test_main_skips_when_webhook_not_configured(tmp_path):
    notification = tmp_path / "notification.json"
    notification.write_text(
        json.dumps(
            {
                "level": "warn",
                "release_env": "preprod",
                "selected_template_key": "preprod_warn",
                "selected_message": "【预发预警】峰值发布门禁预警",
            }
        ),
        encoding="utf-8",
    )
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    rc = notify.main(
        argv=[
            "--notification",
            str(notification),
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 0
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "skipped"
    assert payload["ok"] is True
    assert "webhook_not_configured" in payload["reason"]
    assert payload["failure_class"] == "not_configured"
    assert Path(log_jsonl).exists()


def test_main_strict_fails_when_notification_invalid_with_webhook(tmp_path):
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    rc = notify.main(
        argv=[
            "--notification",
            str(tmp_path / "missing.json"),
            "--webhook-url",
            "https://example.com/webhook",
            "--strict",
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 1
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["ok"] is False
    assert payload["failure_class"] == "payload_invalid"


def test_main_require_webhook_fails_when_missing(tmp_path):
    notification = tmp_path / "notification.json"
    notification.write_text(json.dumps({"selected_message": "msg"}), encoding="utf-8")
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    rc = notify.main(
        argv=[
            "--notification",
            str(notification),
            "--require-webhook",
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 0
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["ok"] is False
    assert payload["reason"] == "webhook_required_but_missing"
    assert payload["failure_class"] == "config_missing"


def test_build_message_prefers_post_in_auto_mode():
    payload, used = notify._build_message(
        {
            "title": "峰值发布门禁阻断（prod）",
            "level": "fail",
            "release_env": "prod",
            "consecutive_compliant_days": 6,
            "fail_consecutive_days": 14,
            "gap_to_fail_days": 8,
            "compliance_ratio_pct": 82.5,
            "selected_message": "【生产阻断】峰值发布门禁阻断",
        },
        "auto",
    )
    assert used == "post"
    assert payload["msg_type"] == "post"
    assert "🔴 FAIL" in payload["content"]["post"]["zh_cn"]["content"][0][0]["text"]
    assert "consecutive: 6/14 | gap: 8d | ratio: 82.50%" in payload["content"]["post"]["zh_cn"]["content"][2][0]["text"]


def test_build_message_fallbacks_to_text_when_post_missing_title():
    payload, used = notify._build_message(
        {
            "title": "",
            "selected_message": "fallback text",
        },
        "post",
    )
    assert used == "text_fallback"
    assert payload["msg_type"] == "text"


def test_send_webhook_with_retry_recovers_on_second_attempt():
    calls = {"n": 0}

    def fake_sender(_url, _payload, _timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"ok": False, "status_code": 500, "response_body": "temporary"}
        return {"ok": True, "status_code": 200, "response_body": "ok"}

    out = notify._send_webhook_with_retry(
        "https://example.com/webhook",
        {"msg_type": "text", "content": {"text": "x"}},
        timeout_sec=3,
        max_attempts=3,
        retry_backoff_sec=0.0,
        sender=fake_sender,
        sleep_fn=lambda _s: None,
    )
    assert out["ok"] is True
    assert out["attempt_count"] == 2
    assert out["retried"] is True


def test_send_webhook_with_retry_not_retry_on_400():
    calls = {"n": 0}

    def fake_sender(_url, _payload, _timeout):
        calls["n"] += 1
        return {"ok": False, "status_code": 400, "response_body": "bad request"}

    out = notify._send_webhook_with_retry(
        "https://example.com/webhook",
        {"msg_type": "text", "content": {"text": "x"}},
        timeout_sec=3,
        max_attempts=3,
        retry_backoff_sec=0.0,
        sender=fake_sender,
        sleep_fn=lambda _s: None,
    )
    assert out["ok"] is False
    assert out["attempt_count"] == 1
    assert out["retried"] is False


def test_main_dedupe_skips_duplicate_delivery(tmp_path):
    notification = tmp_path / "notification.json"
    notification.write_text(
        json.dumps(
            {
                "level": "fail",
                "release_env": "prod",
                "selected_template_key": "prod_block",
                "selected_message": "【生产阻断】峰值发布门禁阻断",
            }
        ),
        encoding="utf-8",
    )
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    dedupe_index = tmp_path / "dedupe_index.json"
    log_jsonl.write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "selected_template_key": "prod_block",
                "status": "sent",
                "ok": True,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    rc = notify.main(
        argv=[
            "--notification",
            str(notification),
            "--webhook-url",
            "https://example.com/webhook",
            "--run-id",
            "run_001",
            "--enable-dedupe",
            "--dedupe-index",
            str(dedupe_index),
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 0
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "skipped"
    assert payload["ok"] is True
    assert payload["reason"] == "duplicate_delivery_deduped"
    assert payload["dedupe_hit"] is True
    assert payload["dedupe_key"] == "run_001:prod_block"
    assert payload["dedupe_source"] == "log"
    assert payload["failure_class"] == "deduped"


def test_main_dedupe_not_hit_when_template_diff(tmp_path, monkeypatch):
    notification = tmp_path / "notification.json"
    notification.write_text(
        json.dumps(
            {
                "level": "warn",
                "release_env": "preprod",
                "selected_template_key": "preprod_warn",
                "selected_message": "【预发预警】峰值发布门禁预警",
            }
        ),
        encoding="utf-8",
    )
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    dedupe_index = tmp_path / "dedupe_index.json"
    log_jsonl.write_text(
        json.dumps(
            {
                "run_id": "run_001",
                "selected_template_key": "prod_block",
                "status": "sent",
                "ok": True,
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    def fake_send_with_retry(*_args, **_kwargs):
        return {"ok": True, "status_code": 200, "response_body": "ok", "attempt_count": 1, "retried": False}

    monkeypatch.setattr(notify, "_send_webhook_with_retry", fake_send_with_retry)
    rc = notify.main(
        argv=[
            "--notification",
            str(notification),
            "--webhook-url",
            "https://example.com/webhook",
            "--run-id",
            "run_001",
            "--enable-dedupe",
            "--dedupe-index",
            str(dedupe_index),
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 0
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "sent"
    assert payload["ok"] is True
    assert payload["dedupe_hit"] is False
    assert payload["dedupe_source"] == "none"
    assert payload["failure_class"] == "success"


def test_was_delivered_respects_max_scan_lines(tmp_path):
    log_jsonl = tmp_path / "delivery.jsonl"
    rows = [
        {"run_id": "run_001", "selected_template_key": "prod_block", "status": "sent", "ok": True},
        {"run_id": "run_x", "selected_template_key": "x", "status": "sent", "ok": True},
        {"run_id": "run_y", "selected_template_key": "y", "status": "sent", "ok": True},
    ]
    log_jsonl.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n", encoding="utf-8")
    assert (
        notify._was_delivered(log_jsonl, run_id="run_001", template_key="prod_block", max_scan_lines=1) is False
    )
    assert notify._was_delivered(log_jsonl, run_id="run_001", template_key="prod_block", max_scan_lines=10) is True


def test_main_dedupe_hits_index_first(tmp_path):
    notification = tmp_path / "notification.json"
    notification.write_text(
        json.dumps(
            {
                "level": "fail",
                "release_env": "prod",
                "selected_template_key": "prod_block",
                "selected_message": "【生产阻断】峰值发布门禁阻断",
            }
        ),
        encoding="utf-8",
    )
    out_json = tmp_path / "delivery.json"
    log_jsonl = tmp_path / "delivery.jsonl"
    dedupe_index = tmp_path / "dedupe_index.json"
    dedupe_index.write_text(
        json.dumps({"entries": {"run_002:prod_block": {"delivered": True, "updated_at": "2026-01-01T00:00:00Z"}}}),
        encoding="utf-8",
    )
    rc = notify.main(
        argv=[
            "--notification",
            str(notification),
            "--webhook-url",
            "https://example.com/webhook",
            "--run-id",
            "run_002",
            "--enable-dedupe",
            "--dedupe-index",
            str(dedupe_index),
            "--out-json",
            str(out_json),
            "--log-jsonl",
            str(log_jsonl),
        ]
    )
    assert rc == 0
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["status"] == "skipped"
    assert payload["dedupe_source"] == "index"
    assert payload["failure_class"] == "deduped"


def test_classify_failure_matrix():
    assert (
        notify._classify_failure(status="failed", ok=False, reason="", status_code=500, error_text="")
        == "server_error"
    )
    assert (
        notify._classify_failure(status="failed", ok=False, reason="", status_code=429, error_text="")
        == "throttle_timeout"
    )
    assert (
        notify._classify_failure(status="failed", ok=False, reason="", status_code=0, error_text="timed out")
        == "network_timeout"
    )
    assert (
        notify._classify_failure(status="failed", ok=False, reason="", status_code=401, error_text="")
        == "client_error"
    )

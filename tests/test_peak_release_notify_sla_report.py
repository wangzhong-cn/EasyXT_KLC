from __future__ import annotations

import json

from tools import peak_release_notify_sla_report as sla


def test_build_json_core_metrics():
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [
            {"status": "sent", "ok": True, "attempt_count": 1, "retried": False, "reason": "", "failure_class": "success"},
            {"status": "failed", "ok": False, "attempt_count": 2, "retried": True, "reason": "http_500", "failure_class": "server_error"},
            {"status": "skipped", "ok": True, "reason": "duplicate_delivery_deduped", "failure_class": "deduped"},
        ],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    assert payload["record_count"] == 3
    assert payload["schema_version"] == "v1"
    assert payload["attempted_count"] == 2
    assert payload["sent_ok_count"] == 1
    assert payload["failed_count"] == 1
    assert payload["dedupe_skipped_count"] == 1
    assert payload["retry_count"] == 1
    assert payload["avg_attempt_count"] == 1.5
    assert payload["success_rate_pct"] == 50.0
    assert payload["meets_sla_99"] is False
    assert payload["failure_class_breakdown"]["server_error"] == 1
    assert payload["failure_owner_breakdown"]["三方服务"] == 1
    assert any(x["failure_class"] == "server_error" for x in payload["escalation_paths"])
    assert payload["oncall_handoff"]["primary_failure_class"] == "server_error"
    assert payload["oncall_handoff"]["primary_owner"] == "三方服务"
    assert payload["oncall_handoff"]["response_sla_minutes"] == 30
    assert payload["oncall_event"]["incident_key"].startswith("peak-notify-")
    assert payload["oncall_event"]["schema_version"] == "v1"
    assert payload["oncall_event"]["severity"] == "P2"
    assert payload["oncall_event"]["primary_owner"] == "三方服务"
    assert payload["oncall_event"]["escalation_policy_version"] == "v1"
    assert payload["oncall_event"]["response_deadline_utc"].endswith("Z")
    assert payload["oncall_event"]["event_status"] == "open"
    assert payload["oncall_event"]["sla_breach_level"] == "none"
    assert payload["oncall_event"]["breach_score"] == 10
    assert payload["oncall_event"]["escalation_required"] is False
    assert payload["oncall_event"]["page_required"] is False
    assert payload["oncall_event"]["dispatch_priority"] == "low"
    assert payload["oncall_event"]["escalation_wait_minutes"] == 60
    assert payload["oncall_event"]["escalation_deadline_utc"].endswith("Z")
    assert payload["oncall_event"]["response_overdue"] is False
    assert payload["oncall_event"]["escalation_overdue"] is False
    assert payload["oncall_event"]["overdue_stage"] == "none"
    assert "按既定SLA节奏持续跟进" in payload["oncall_event"]["immediate_action"]
    assert payload["oncall_event"]["next_checkpoint_utc"].endswith("Z")
    assert "owner=三方服务" in payload["oncall_event"]["handoff_summary"]
    assert payload["oncall_event"]["timeline"][0]["stage"] == "detected"
    assert len(payload["oncall_event"]["closure_criteria"]) >= 1
    assert payload["oncall_event"]["execution_checklist"][0]["id"] == "ack"
    assert payload["oncall_event"]["ticket_payload"]["labels"][0] == "peak_release_notify"
    assert payload["oncall_event"]["ticket_payload"]["priority"] == "low"
    assert payload["oncall_event"]["ack_state"]["ack_status"] == "pending"
    assert payload["oncall_event"]["ack_state"]["ack_revision"] == 0
    assert payload["oncall_event"]["ack_state"]["ack_at_utc"] == ""
    assert payload["oncall_event"]["ack_state"]["history"] == []
    assert payload["alert_level"] == "medium"
    assert len(payload["runbook_actions"]) >= 1


def test_main_writes_md_and_json(tmp_path):
    log_path = tmp_path / "notify.jsonl"
    log_path.write_text(
        "\n".join(
            [
                json.dumps({"ts": "2026-03-14T00:00:00Z", "status": "sent", "ok": True, "attempt_count": 1}),
                json.dumps({"ts": "2026-03-14T01:00:00Z", "status": "skipped", "ok": True, "reason": "duplicate_delivery_deduped", "failure_class": "deduped"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out_md = tmp_path / "sla.md"
    out_json = tmp_path / "sla.json"
    out_oncall_json = tmp_path / "oncall.json"
    rc = sla.main(
        argv=[
            "--log-jsonl",
            str(log_path),
            "--window-days",
            "30",
            "--out-md",
            str(out_md),
            "--out-json",
            str(out_json),
            "--out-oncall-json",
            str(out_oncall_json),
        ]
    )
    assert rc == 0
    j = json.loads(out_json.read_text(encoding="utf-8"))
    assert j["record_count"] == 2
    assert j["dedupe_skipped_count"] == 1
    oncall = json.loads(out_oncall_json.read_text(encoding="utf-8"))
    assert oncall["schema_version"] == "v1"
    assert "severity" in oncall
    assert "primary_owner" in oncall
    assert "incident_key" in oncall
    assert "escalation_deadline_utc" in oncall
    assert "handoff_summary" in oncall
    assert "next_checkpoint_utc" in oncall
    assert "timeline" in oncall
    assert "closure_criteria" in oncall
    assert "execution_checklist" in oncall
    assert "breach_score" in oncall
    assert "execution_command_template" in oncall
    assert "page_required" in oncall
    assert "dispatch_priority" in oncall
    assert "ack_state" in oncall
    md = out_md.read_text(encoding="utf-8")
    assert "峰值通知投递SLA报告" in md
    assert "success_rate_pct" in md
    assert "alert_level" in md
    assert "责任归属分布" in md
    assert "升级路径" in md
    assert "值班闭环" in md
    assert "incident_key" in md
    assert "escalation_deadline_utc" in md
    assert "sla_breach_level" in md
    assert "dispatch_priority" in md
    assert "ack_status" in md
    assert "ack_revision" in md
    assert "ack_history_count" in md
    assert "execution_command_template" in md
    assert "升级时间线" in md
    assert "关闭判据" in md
    assert "执行清单" in md
    assert "处置建议" in md


def test_build_json_overdue_flags():
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
        reference_utc="2026-03-15T02:30:00Z",
    )
    assert payload["oncall_event"]["response_overdue"] is True
    assert payload["oncall_event"]["escalation_overdue"] is True
    assert payload["oncall_event"]["overdue_stage"] == "escalation"
    assert payload["oncall_event"]["event_status"] == "breached"
    assert payload["oncall_event"]["sla_breach_level"] == "critical"
    assert payload["oncall_event"]["breach_score"] == 110
    assert payload["oncall_event"]["escalation_required"] is True
    assert payload["oncall_event"]["page_required"] is True
    assert payload["oncall_event"]["dispatch_priority"] == "urgent"


def test_load_policy_override_dispatch_and_score(tmp_path):
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "dispatch_priority": {"critical": "p0"},
                "breach_level_score": {"critical": 120},
            }
        ),
        encoding="utf-8",
    )
    sla.ACTIVE_POLICY = sla._load_policy(policy_path)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
        reference_utc="2026-03-15T02:30:00Z",
    )
    assert payload["oncall_event"]["dispatch_priority"] == "p0"
    assert payload["oncall_event"]["breach_score"] == 130


def test_main_update_ack_state(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_json = tmp_path / "sla.json"
    out_md = tmp_path / "sla.md"
    out_oncall = tmp_path / "oncall.json"
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(sla.render_md(payload), encoding="utf-8")
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--owner-note",
            "值班已确认",
            "--event-time-utc",
            "2026-03-15T00:05:00Z",
            "--out-json",
            str(out_json),
            "--out-md",
            str(out_md),
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 0
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "acked"
    assert oncall["ack_state"]["ack_revision"] == 1
    assert oncall["ack_state"]["ack_at_utc"] == "2026-03-15T00:05:00Z"
    assert oncall["ack_state"]["owner_note"] == "值班已确认"
    assert oncall["ack_state"]["history"][-1]["from_status"] == "pending"
    assert oncall["ack_state"]["history"][-1]["to_status"] == "acked"
    assert oncall["event_status"] == "open"
    report = json.loads(out_json.read_text(encoding="utf-8"))
    assert report["oncall_event"]["ack_state"]["ack_status"] == "acked"


def test_main_update_ack_state_invalid_transition(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "closed",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 1


def test_main_update_ack_state_invalid_event_time(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--event-time-utc",
            "bad-time",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 1


def test_main_update_ack_state_reject_older_timestamp(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--event-time-utc",
            "2026-03-14T23:59:59Z",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 1


def test_main_update_ack_state_close_allowed_with_existing_note(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--owner-note",
            "已确认",
            "--event-time-utc",
            "2026-03-15T00:05:00Z",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 0
    rc2 = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "closed",
            "--event-time-utc",
            "2026-03-15T00:10:00Z",
            "--owner-note",
            "",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc2 == 0


def test_main_update_ack_state_optimistic_lock_conflict(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--event-time-utc",
            "2026-03-15T00:05:00Z",
            "--expected-last-updated-utc",
            "2026-03-14T00:00:00Z",
            "--expected-ack-revision",
            "1",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 1


def test_main_update_ack_state_optimistic_lock_success(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--event-time-utc",
            "2026-03-15T00:05:00Z",
            "--expected-last-updated-utc",
            "2026-03-15T00:00:00Z",
            "--expected-ack-revision",
            "0",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 0


def test_main_batch_update_ack_state_success(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    batch_file = tmp_path / "batch.json"
    batch_report = tmp_path / "batch_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "r1", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "r2", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:10:00Z"},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 0
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "mitigated"
    assert oncall["ack_state"]["ack_revision"] == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["success_count"] == 2
    assert report["failed_count"] == 0
    assert report["skipped_count"] == 0
    assert report["dominant_error_type"] == ""
    assert report["dominant_error_count"] == 0
    assert report["error_total_count"] == 0
    assert report["dominant_error_ratio"] == 0.0
    assert report["error_type_count"] == 0
    assert report["error_signature"] == ""
    assert report["error_signature_basis"] == ""
    assert report["error_concentration_hhi"] == 0.0
    assert report["error_diversity_index"] == 0.0
    assert report["error_entropy"] == 0.0
    assert report["error_entropy_normalized"] == 0.0
    assert report["error_effective_type_count"] == 0.0
    assert report["error_top2_ratio"] == 0.0
    assert report["error_long_tail_ratio"] == 0.0
    assert report["dominant_to_second_ratio"] == 0.0
    assert report["error_tail_type_count"] == 0
    assert report["error_tail_avg_ratio_per_type"] == 0.0
    assert report["dominant_gap_to_second_ratio"] == 0.0
    assert report["error_pareto_80_type_count"] == 0
    assert report["error_pareto_80_ratio_covered"] == 0.0
    assert report["error_pareto_90_type_count"] == 0
    assert report["error_pareto_90_ratio_covered"] == 0.0
    assert report["error_pareto_95_type_count"] == 0
    assert report["error_pareto_95_ratio_covered"] == 0.0
    assert report["error_single_point_failure"] is False
    assert report["error_long_tail_present"] is False
    assert report["error_structure_tag"] == "none"
    assert report["error_concentration_tag"] == "none"
    assert report["error_focus_index"] == 0.0
    assert report["error_tail_pressure"] == 0.0
    assert report["error_governance_mode"] == "idle"
    assert report["error_governance_urgency_score"] == 0.0
    assert report["error_governance_priority"] == "none"
    assert report["error_governance_reason_tags"] == []
    assert report["error_governance_eta_minutes"] == 0
    assert report["error_governance_owner"] == "none"
    assert report["error_governance_route"] == "observe"
    assert report["error_governance_playbook_id"] == "PB-IDLE-000"
    assert report["error_governance_policy_version"] == "v1"
    assert report["error_governance_rule_hits"] == []
    assert report["error_alert_merge_key"] == ""
    assert report["error_alert_merge_key_basis"] == ""
    assert report["trend_recent_count"] >= 1
    assert report["trend_priority_distribution"]["none"] >= 1
    assert report["trend_structure_distribution"]["none"] >= 1
    assert report["error_types_sorted"] == []
    assert report["first_error_index"] == -1
    assert report["first_error_reason"] == ""
    assert report["first_error_request_id"] == ""
    assert report["aborted"] is False
    assert report["applied_count"] == 2
    assert report["success_request_ids"] == ["r1", "r2"]
    assert report["failed_request_ids"] == []
    assert report["skipped_request_ids"] == []
    assert report["request_status_by_id"]["r1"][0]["ok"] is True
    assert report["request_status_by_id"]["r2"][0]["ok"] is True
    assert report["request_final_outcome_by_id"]["r1"] == "success"
    assert report["request_final_outcome_by_id"]["r2"] == "success"
    assert report["request_outcome_counts"]["success"] == 2
    assert report["request_final_detail_by_id"]["r1"]["outcome"] == "success"
    assert report["request_final_detail_by_id"]["r1"]["final_index"] == 0
    assert report["request_final_detail_by_id"]["r2"]["final_index"] == 1
    assert report["request_unique_count"] == 2
    assert report["request_multi_event_ids"] == []
    assert report["request_failed_with_error_ids"] == []
    assert report["has_failures"] is False
    assert report["has_skipped"] is False
    assert report["has_conflicts"] is False
    assert report["has_validation_errors"] is False
    assert report["request_health"] == "healthy"
    assert report["risk_score"] == 0
    assert report["risk_level"] == "info"
    assert report["risk_components"]["failure"] == 0
    assert report["risk_policy_version"] == "v1"
    assert report["sla_alert_level"] == "p4"
    assert report["recommended_action"] == "monitor"
    assert report["final_ack_status"] == "mitigated"
    assert report["final_ack_revision"] == 2
    assert report["results"][0]["request_id"] == "r1"
    assert report["results"][1]["request_id"] == "r2"


def test_main_batch_update_ack_state_partial_conflict(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall.json"
    batch_file = tmp_path / "batch_conflict.json"
    batch_report = tmp_path / "batch_conflict_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "ok-1", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "bad-1", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 2
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "acked"
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["success_count"] == 1
    assert report["failed_count"] == 1
    assert report["skipped_count"] == 0
    assert report["first_error_index"] == 1
    assert "owner-note" in report["first_error_reason"]
    assert report["first_error_request_id"] == "bad-1"
    assert report["aborted"] is False
    assert report["applied_count"] == 1
    assert report["success_request_ids"] == ["ok-1"]
    assert report["failed_request_ids"] == ["bad-1"]
    assert report["skipped_request_ids"] == []
    assert report["request_status_by_id"]["bad-1"][0]["ok"] is False
    assert "owner-note" in report["request_status_by_id"]["bad-1"][0]["error"]
    assert report["request_final_outcome_by_id"]["ok-1"] == "success"
    assert report["request_final_outcome_by_id"]["bad-1"] == "failed"
    assert report["request_outcome_counts"]["success"] == 1
    assert report["request_outcome_counts"]["failed"] == 1
    assert report["request_final_detail_by_id"]["bad-1"]["outcome"] == "failed"
    assert "owner-note" in report["request_final_detail_by_id"]["bad-1"]["final_error"]
    assert report["request_failed_with_error_ids"] == ["bad-1"]
    assert report["has_failures"] is True
    assert report["has_skipped"] is False
    assert report["has_conflicts"] is False
    assert report["has_validation_errors"] is False
    assert report["request_health"] == "partial"
    assert report["dominant_error_type"] == "owner_note"
    assert report["dominant_error_count"] == 1
    assert report["error_total_count"] == 1
    assert report["dominant_error_ratio"] == 1.0
    assert report["error_type_count"] == 1
    assert report["error_signature"] != ""
    assert report["error_signature_basis"] == "owner_note:1"
    assert report["error_concentration_hhi"] == 1.0
    assert report["error_diversity_index"] == 0.0
    assert report["error_entropy"] == 0.0
    assert report["error_entropy_normalized"] == 0.0
    assert report["error_effective_type_count"] == 1.0
    assert report["error_top2_ratio"] == 1.0
    assert report["error_long_tail_ratio"] == 0.0
    assert report["dominant_to_second_ratio"] == 0.0
    assert report["error_tail_type_count"] == 0
    assert report["error_tail_avg_ratio_per_type"] == 0.0
    assert report["dominant_gap_to_second_ratio"] == 1.0
    assert report["error_pareto_80_type_count"] == 1
    assert report["error_pareto_80_ratio_covered"] == 1.0
    assert report["error_pareto_90_type_count"] == 1
    assert report["error_pareto_90_ratio_covered"] == 1.0
    assert report["error_pareto_95_type_count"] == 1
    assert report["error_pareto_95_ratio_covered"] == 1.0
    assert report["error_single_point_failure"] is True
    assert report["error_long_tail_present"] is False
    assert report["error_structure_tag"] == "single_point"
    assert report["error_concentration_tag"] == "very_high"
    assert report["error_focus_index"] == 1.0
    assert report["error_tail_pressure"] == 0.0
    assert report["error_governance_mode"] == "stabilize_single_point"
    assert report["error_governance_urgency_score"] == 1.0
    assert report["error_governance_priority"] == "p1"
    assert "single_point_failure" in report["error_governance_reason_tags"]
    assert "high_concentration" in report["error_governance_reason_tags"]
    assert report["error_governance_eta_minutes"] == 15
    assert report["error_governance_owner"] == "qa_oncall"
    assert report["error_governance_route"] == "immediate_page"
    assert report["error_governance_playbook_id"] == "PB-SP-001"
    assert report["error_governance_policy_version"] == "v1"
    assert any(x.startswith("priority=") for x in report["error_governance_rule_hits"])
    assert report["error_alert_merge_key"] != ""
    assert report["error_alert_merge_key_basis"] != ""
    assert report["error_types_sorted"][0]["type"] == "owner_note"
    assert report["risk_score"] == 40
    assert report["risk_level"] == "medium"
    assert report["risk_components"]["failure"] == 40
    assert report["risk_policy_version"] == "v1"
    assert report["sla_alert_level"] == "p2"
    assert report["recommended_action"] == "retry_failed_requests"
    assert report["error_breakdown"]["owner_note"] == 1
    assert report["results"][1]["request_id"] == "bad-1"


def test_main_batch_update_ack_state_atomic_conflict(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_atomic.json"
    batch_file = tmp_path / "batch_atomic_conflict.json"
    batch_report = tmp_path / "batch_atomic_conflict_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "a1", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "a2", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""},
                {"request_id": "a3", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:07:00Z"},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--batch-atomic",
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 3
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "pending"
    assert oncall["ack_state"]["ack_revision"] == 0
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["aborted"] is True
    assert report["applied_count"] == 0
    assert report["skipped_count"] == 1
    assert report["first_error_index"] == 1
    assert "owner-note" in report["first_error_reason"]
    assert report["first_error_request_id"] == "a2"
    assert report["success_request_ids"] == ["a1"]
    assert report["failed_request_ids"] == ["a2"]
    assert report["skipped_request_ids"] == ["idx-2"]
    assert report["request_status_by_id"]["idx-2"][0]["skipped"] is True
    assert report["request_final_outcome_by_id"]["a1"] == "success"
    assert report["request_final_outcome_by_id"]["a2"] == "failed"
    assert report["request_final_outcome_by_id"]["idx-2"] == "skipped"
    assert report["request_outcome_counts"]["skipped"] == 1
    assert report["request_final_detail_by_id"]["idx-2"]["outcome"] == "skipped"
    assert report["request_final_detail_by_id"]["idx-2"]["final_skipped"] is True
    assert report["request_failed_with_error_ids"] == ["a2"]
    assert report["has_failures"] is True
    assert report["has_skipped"] is True
    assert report["has_conflicts"] is False
    assert report["has_validation_errors"] is False
    assert report["request_health"] == "aborted"
    assert report["dominant_error_type"] == "aborted"
    assert report["dominant_error_count"] == 1
    assert report["error_total_count"] == 2
    assert report["dominant_error_ratio"] == 0.5
    assert report["error_type_count"] == 2
    assert report["error_signature"] != ""
    assert report["error_signature_basis"] == "aborted:1|owner_note:1"
    assert report["error_concentration_hhi"] == 0.5
    assert report["error_diversity_index"] == 0.5
    assert report["error_entropy"] == 1.0
    assert report["error_entropy_normalized"] == 1.0
    assert report["error_effective_type_count"] == 2.0
    assert report["error_top2_ratio"] == 1.0
    assert report["error_long_tail_ratio"] == 0.0
    assert report["dominant_to_second_ratio"] == 1.0
    assert report["error_tail_type_count"] == 0
    assert report["error_tail_avg_ratio_per_type"] == 0.0
    assert report["dominant_gap_to_second_ratio"] == 0.0
    assert report["error_pareto_80_type_count"] == 2
    assert report["error_pareto_80_ratio_covered"] == 1.0
    assert report["error_pareto_90_type_count"] == 2
    assert report["error_pareto_90_ratio_covered"] == 1.0
    assert report["error_pareto_95_type_count"] == 2
    assert report["error_pareto_95_ratio_covered"] == 1.0
    assert report["error_single_point_failure"] is False
    assert report["error_long_tail_present"] is False
    assert report["error_structure_tag"] == "balanced"
    assert report["error_concentration_tag"] == "high"
    assert report["error_focus_index"] == 0.25
    assert report["error_tail_pressure"] == 0.0
    assert report["error_governance_mode"] == "balance_monitoring"
    assert report["error_governance_urgency_score"] == 0.8
    assert report["error_governance_priority"] == "p1"
    assert "high_concentration" in report["error_governance_reason_tags"]
    assert report["error_governance_eta_minutes"] == 15
    assert report["error_governance_owner"] == "platform_oncall"
    assert report["error_governance_route"] == "immediate_page"
    assert report["error_governance_playbook_id"] == "PB-BM-001"
    assert report["error_types_sorted"][0]["type"] == "aborted"
    assert report["risk_score"] == 75
    assert report["risk_level"] == "high"
    assert report["risk_components"]["aborted"] == 15
    assert report["risk_policy_version"] == "v1"
    assert report["sla_alert_level"] == "p1"
    assert report["recommended_action"] == "rollback_and_manual_review"
    assert report["error_breakdown"]["owner_note"] == 1
    assert report["error_breakdown"]["aborted"] == 1
    assert report["results"][2]["skipped"] is True
    assert report["results"][2]["request_id"] == "idx-2"


def test_main_batch_update_ack_state_conflict_breakdown(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_conflict_breakdown.json"
    batch_file = tmp_path / "batch_conflict_breakdown.json"
    batch_report = tmp_path / "batch_conflict_breakdown_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "c1", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "c2", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:10:00Z", "expected_ack_revision": 0},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["has_conflicts"] is True
    assert report["error_breakdown"]["conflict"] == 1
    assert report["dominant_error_type"] == "conflict"
    assert report["dominant_error_count"] == 1
    assert report["error_signature_basis"] == "conflict:1"
    assert report["error_concentration_hhi"] == 1.0
    assert report["error_diversity_index"] == 0.0
    assert report["error_entropy"] == 0.0
    assert report["error_entropy_normalized"] == 0.0
    assert report["error_effective_type_count"] == 1.0
    assert report["error_top2_ratio"] == 1.0
    assert report["error_long_tail_ratio"] == 0.0
    assert report["dominant_to_second_ratio"] == 0.0
    assert report["error_tail_type_count"] == 0
    assert report["error_tail_avg_ratio_per_type"] == 0.0
    assert report["dominant_gap_to_second_ratio"] == 1.0
    assert report["error_pareto_80_type_count"] == 1
    assert report["error_pareto_80_ratio_covered"] == 1.0
    assert report["error_pareto_90_type_count"] == 1
    assert report["error_pareto_90_ratio_covered"] == 1.0
    assert report["error_pareto_95_type_count"] == 1
    assert report["error_pareto_95_ratio_covered"] == 1.0
    assert report["error_single_point_failure"] is True
    assert report["error_long_tail_present"] is False
    assert report["error_structure_tag"] == "single_point"
    assert report["error_concentration_tag"] == "very_high"
    assert report["error_focus_index"] == 1.0
    assert report["error_tail_pressure"] == 0.0
    assert report["error_governance_mode"] == "stabilize_single_point"
    assert report["error_governance_urgency_score"] == 1.0
    assert report["error_governance_priority"] == "p1"
    assert "single_point_failure" in report["error_governance_reason_tags"]
    assert "has_conflicts" in report["error_governance_reason_tags"]
    assert "high_concentration" in report["error_governance_reason_tags"]
    assert report["error_governance_eta_minutes"] == 15
    assert report["error_governance_owner"] == "release_oncall"
    assert report["error_governance_route"] == "immediate_page"
    assert report["error_governance_playbook_id"] == "PB-SP-001"
    assert report["error_alert_merge_key"] != ""
    assert report["risk_components"]["conflict"] == 25


def test_main_batch_update_ack_state_long_tail_structure(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_long_tail.json"
    batch_file = tmp_path / "batch_long_tail.json"
    batch_report = tmp_path / "batch_long_tail_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "lt-a", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "lt-b", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""},
                {"request_id": "lt-c", "ack_status": "closed", "event_time_utc": "2026-03-15T00:07:00Z", "owner_note": ""},
                {"request_id": "lt-d", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:08:00Z", "expected_ack_revision": 0},
                {"request_id": "lt-e", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:09:00Z", "expected_ack_revision": 0},
                {"request_id": "lt-a", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:10:00Z"},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["error_total_count"] == 5
    assert report["error_breakdown"]["owner_note"] == 2
    assert report["error_breakdown"]["conflict"] == 2
    assert report["error_breakdown"]["duplicate_request_id"] == 1
    assert report["error_top2_ratio"] == 0.8
    assert report["error_long_tail_ratio"] == 0.2
    assert report["error_tail_type_count"] == 1
    assert report["error_tail_avg_ratio_per_type"] == 0.2
    assert report["error_pareto_80_type_count"] == 2
    assert report["error_pareto_80_ratio_covered"] == 0.8
    assert report["error_pareto_90_type_count"] == 3
    assert report["error_pareto_90_ratio_covered"] == 1.0
    assert report["error_pareto_95_type_count"] == 3
    assert report["error_pareto_95_ratio_covered"] == 1.0
    assert report["error_single_point_failure"] is False
    assert report["error_long_tail_present"] is True
    assert report["error_structure_tag"] == "long_tail"
    assert report["error_concentration_tag"] == "medium"
    assert report["error_focus_index"] == 0.144
    assert report["error_tail_pressure"] == 0.6
    assert report["error_governance_mode"] == "reduce_long_tail"
    assert report["error_governance_urgency_score"] == 1.0
    assert report["error_governance_priority"] == "p1"
    assert "long_tail_present" in report["error_governance_reason_tags"]
    assert "has_conflicts" in report["error_governance_reason_tags"]
    assert "has_validation_errors" in report["error_governance_reason_tags"]
    assert report["error_governance_eta_minutes"] == 15
    assert report["error_governance_owner"] == "release_oncall"
    assert report["error_governance_route"] == "immediate_page"
    assert report["error_governance_playbook_id"] == "PB-LT-001"
    assert report["error_alert_merge_key"] != ""


def test_main_update_ack_state_dry_run_no_write(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_dry_run.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--ack-status",
            "acked",
            "--event-time-utc",
            "2026-03-15T00:05:00Z",
            "--dry-run",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 0
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "pending"
    assert oncall["ack_state"]["ack_revision"] == 0


def test_main_batch_update_ack_state_dry_run_no_write(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_batch_dry_run.json"
    batch_file = tmp_path / "batch_dry_run.json"
    batch_report = tmp_path / "batch_dry_run_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"ack_status": "mitigated", "event_time_utc": "2026-03-15T00:10:00Z"},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--dry-run",
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 0
    oncall = json.loads(out_oncall.read_text(encoding="utf-8"))
    assert oncall["ack_state"]["ack_status"] == "pending"
    assert oncall["ack_state"]["ack_revision"] == 0
    assert batch_report.exists() is False


def test_main_reject_mutually_exclusive_update_modes(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_conflict_mode.json"
    batch_file = tmp_path / "batch_conflict_mode.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(json.dumps([], ensure_ascii=False), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--update-ack-state",
            "--batch-update-ack-file",
            str(batch_file),
            "--ack-status",
            "acked",
            "--out-oncall-json",
            str(out_oncall),
        ]
    )
    assert rc == 1


def test_main_batch_update_invalid_expected_ack_revision(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_invalid_rev.json"
    batch_file = tmp_path / "batch_invalid_rev.json"
    batch_report = tmp_path / "batch_invalid_rev_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [{"ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z", "expected_ack_revision": "x"}],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["error_breakdown"]["invalid_request"] == 1


def test_main_batch_update_duplicate_request_id(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_dup_req.json"
    batch_file = tmp_path / "batch_dup_req.json"
    batch_report = tmp_path / "batch_dup_req_report.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [
                {"request_id": "dup-1", "ack_status": "acked", "event_time_utc": "2026-03-15T00:05:00Z"},
                {"request_id": "dup-1", "ack_status": "mitigated", "event_time_utc": "2026-03-15T00:10:00Z"},
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["error_breakdown"]["duplicate_request_id"] == 1
    assert report["first_error_index"] == 1
    assert "duplicate request_id" in report["first_error_reason"]
    assert report["first_error_request_id"] == "dup-1"
    assert report["request_status_by_id"]["dup-1"][1]["ok"] is False
    assert report["request_final_outcome_by_id"]["dup-1"] == "failed"
    assert report["request_final_detail_by_id"]["dup-1"]["final_index"] == 1
    assert report["request_multi_event_ids"] == ["dup-1"]
    assert report["request_failed_with_error_ids"] == ["dup-1"]
    assert report["has_conflicts"] is False
    assert report["has_validation_errors"] is True
    assert report["request_health"] == "partial"
    assert report["dominant_error_type"] == "duplicate_request_id"
    assert report["dominant_error_count"] == 1
    assert report["error_total_count"] == 1
    assert report["dominant_error_ratio"] == 1.0
    assert report["error_type_count"] == 1
    assert report["error_signature"] != ""
    assert report["error_signature_basis"] == "duplicate_request_id:1"
    assert report["error_concentration_hhi"] == 1.0
    assert report["error_diversity_index"] == 0.0
    assert report["error_entropy"] == 0.0
    assert report["error_entropy_normalized"] == 0.0
    assert report["error_effective_type_count"] == 1.0
    assert report["error_top2_ratio"] == 1.0
    assert report["error_long_tail_ratio"] == 0.0
    assert report["dominant_to_second_ratio"] == 0.0
    assert report["error_tail_type_count"] == 0
    assert report["error_tail_avg_ratio_per_type"] == 0.0
    assert report["dominant_gap_to_second_ratio"] == 1.0
    assert report["error_pareto_80_type_count"] == 1
    assert report["error_pareto_80_ratio_covered"] == 1.0
    assert report["error_pareto_90_type_count"] == 1
    assert report["error_pareto_90_ratio_covered"] == 1.0
    assert report["error_pareto_95_type_count"] == 1
    assert report["error_pareto_95_ratio_covered"] == 1.0
    assert report["error_single_point_failure"] is True
    assert report["error_long_tail_present"] is False
    assert report["error_structure_tag"] == "single_point"
    assert report["error_concentration_tag"] == "very_high"
    assert report["error_focus_index"] == 1.0
    assert report["error_tail_pressure"] == 0.0
    assert report["error_governance_mode"] == "stabilize_single_point"
    assert report["error_governance_urgency_score"] == 1.0
    assert report["error_governance_priority"] == "p1"
    assert "single_point_failure" in report["error_governance_reason_tags"]
    assert "has_validation_errors" in report["error_governance_reason_tags"]
    assert "high_concentration" in report["error_governance_reason_tags"]
    assert report["error_governance_eta_minutes"] == 15
    assert report["error_governance_owner"] == "release_oncall"
    assert report["error_governance_route"] == "immediate_page"
    assert report["error_governance_playbook_id"] == "PB-SP-001"
    assert report["error_alert_merge_key"] != ""
    assert report["error_types_sorted"][0]["type"] == "duplicate_request_id"
    assert report["risk_score"] == 55
    assert report["risk_level"] == "high"
    assert report["risk_components"]["validation"] == 15
    assert report["risk_policy_version"] == "v1"
    assert report["sla_alert_level"] == "p1"
    assert report["recommended_action"] == "retry_failed_requests"


def test_main_batch_update_custom_risk_policy(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_custom_risk.json"
    batch_file = tmp_path / "batch_custom_risk.json"
    batch_report = tmp_path / "batch_custom_risk_report.json"
    policy_file = tmp_path / "custom_policy.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [{"request_id": "x1", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""}],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    custom_policy = dict(sla.DEFAULT_POLICY)
    custom_policy["risk_policy"] = {
        "version": "v2-test",
        "weights": {"failure": 10, "skipped": 0, "conflict": 0, "validation": 0, "aborted": 0},
        "level_thresholds": {
            "low_max_exclusive": 15,
            "medium_max_exclusive": 30,
            "high_max_exclusive": 60,
            "max_score": 100,
        },
        "alert_level_map": {"critical": "p1", "high": "p2", "medium": "p3", "low": "p4", "info": "p4"},
        "recommended_action_map": {
            "healthy": "monitor",
            "partial": "retry_failed_requests",
            "aborted": "rollback_and_manual_review",
            "failed": "manual_intervention_required",
        },
    }
    policy_file.write_text(json.dumps(custom_policy, ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
            "--policy-json",
            str(policy_file),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["risk_policy_version"] == "v2-test"
    assert report["risk_score"] == 10
    assert report["risk_level"] == "low"
    assert report["sla_alert_level"] == "p4"
    assert report["effective_risk_policy"]["weights"]["failure"] == 10
    assert report["effective_risk_policy"]["level_thresholds"]["low_max_exclusive"] == 15


def test_main_batch_update_custom_governance_policy(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_custom_governance.json"
    batch_file = tmp_path / "batch_custom_governance.json"
    batch_report = tmp_path / "batch_custom_governance_report.json"
    policy_file = tmp_path / "custom_governance_policy.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [{"request_id": "g1", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""}],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    custom_policy = dict(sla.DEFAULT_POLICY)
    custom_policy["governance_policy"] = {
        "version": "g2-test",
        "single_point_threshold": 0.8,
        "concentration_thresholds": {"very_high_min": 0.9, "high_min": 0.6, "medium_min": 0.4},
        "urgency_thresholds": {"p1_min": 0.8, "p2_min": 0.6, "p3_min": 0.3},
        "risk_level_weight": {"critical": 1.0, "high": 0.8, "medium": 0.6, "low": 0.3, "info": 0.1},
        "priority_to_eta_minutes": {"p1": 10, "p2": 45, "p3": 180, "p4": 720, "none": 0},
        "priority_to_route": {"p1": "war_room", "p2": "expedite_queue", "p3": "planned_queue", "p4": "backlog_review", "none": "observe"},
        "owner_map": {"default": "platform_oncall", "idle": "none", "single_point": "sre_oncall", "release": "release_oncall"},
        "playbook_map": {"idle": "PB-IDLE-000", "stabilize_single_point": "PB-SP-999", "reduce_long_tail": "PB-LT-001", "balance_monitoring": "PB-BM-001"},
        "trend_window": 3,
        "priority_order": ["none", "p4", "p3", "p2", "p1"],
        "merge_key_fields": ["error_structure_tag", "error_governance_owner"],
    }
    policy_file.write_text(json.dumps(custom_policy, ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
            "--policy-json",
            str(policy_file),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["error_governance_policy_version"] == "g2-test"
    assert report["error_governance_owner"] == "sre_oncall"
    assert report["error_governance_route"] == "war_room"
    assert report["error_governance_playbook_id"] == "PB-SP-999"
    assert report["error_governance_eta_minutes"] == 10
    assert report["error_alert_merge_key_basis"] == "single_point|sre_oncall"


def test_main_batch_update_invalid_risk_policy_fallback(tmp_path):
    sla.ACTIVE_POLICY = dict(sla.DEFAULT_POLICY)
    payload = sla.build_json(
        [{"status": "failed", "ok": False, "failure_class": "server_error"}],
        window_days=7,
        generated_at="2026-03-15T00:00:00Z",
    )
    out_oncall = tmp_path / "oncall_invalid_risk.json"
    batch_file = tmp_path / "batch_invalid_risk.json"
    batch_report = tmp_path / "batch_invalid_risk_report.json"
    policy_file = tmp_path / "invalid_risk_policy.json"
    out_oncall.write_text(json.dumps(payload["oncall_event"], ensure_ascii=False, indent=2), encoding="utf-8")
    batch_file.write_text(
        json.dumps(
            [{"request_id": "x1", "ack_status": "closed", "event_time_utc": "2026-03-15T00:06:00Z", "owner_note": ""}],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    broken_policy = dict(sla.DEFAULT_POLICY)
    broken_policy["risk_policy"] = {
        "version": "v-broken",
        "weights": {"failure": -10, "skipped": "x", "conflict": None, "validation": 2, "aborted": 1},
        "level_thresholds": {
            "low_max_exclusive": 0,
            "medium_max_exclusive": 0,
            "high_max_exclusive": 0,
            "max_score": -1,
        },
        "alert_level_map": {"critical": "p1", "high": "p1", "medium": "p2", "low": "p3", "info": "p4"},
        "recommended_action_map": {
            "healthy": "monitor",
            "partial": "retry_failed_requests",
            "aborted": "rollback_and_manual_review",
            "failed": "manual_intervention_required",
        },
    }
    policy_file.write_text(json.dumps(broken_policy, ensure_ascii=False, indent=2), encoding="utf-8")
    rc = sla.main(
        argv=[
            "--batch-update-ack-file",
            str(batch_file),
            "--out-oncall-json",
            str(out_oncall),
            "--batch-report-json",
            str(batch_report),
            "--policy-json",
            str(policy_file),
        ]
    )
    assert rc == 2
    report = json.loads(batch_report.read_text(encoding="utf-8"))
    assert report["risk_policy_version"] == "v-broken"
    assert report["effective_risk_policy"]["weights"]["failure"] == 0
    assert report["effective_risk_policy"]["weights"]["skipped"] == 20
    assert report["effective_risk_policy"]["level_thresholds"]["low_max_exclusive"] == 25
    assert report["effective_risk_policy"]["level_thresholds"]["medium_max_exclusive"] == 50
    assert report["effective_risk_policy"]["level_thresholds"]["high_max_exclusive"] == 80
    assert report["effective_risk_policy"]["level_thresholds"]["max_score"] == 100

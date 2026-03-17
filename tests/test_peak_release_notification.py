from __future__ import annotations

from tools.peak_release_notification import build_notification, render_md


def test_build_notification_fail_message():
    payload = build_notification(
        {
            "level": "fail",
            "release_env": "prod",
            "gap_to_fail_days": 8,
            "consecutive_compliant_days": 6,
            "fail_consecutive_days": 14,
            "compliance_ratio_pct": 82.5,
        },
        {
            "action": "阻断生产放量，优先修复门禁项并在下一周期复核。",
            "recent_summary": {"records": 7, "gate_pass_days": 5},
        },
    )
    assert payload["level"] == "fail"
    assert "阻断" in payload["title"]
    assert "env=prod" in payload["message"]
    assert "consecutive=6/14" in payload["message"]
    assert "阻断生产放量" in payload["email_body"]
    assert payload["selected_template_key"] == "prod_block"
    assert "生产阻断" in payload["selected_message"]
    assert payload["consecutive_compliant_days"] == 6
    assert payload["fail_consecutive_days"] == 14
    assert payload["gap_to_fail_days"] == 8
    assert payload["compliance_ratio_pct"] == 82.5


def test_render_md_contains_templates():
    md = render_md(
        {
            "generated_at": "2026-03-15T00:00:00Z",
            "title": "峰值发布门禁预警（preprod）",
            "level": "warn",
            "release_env": "preprod",
            "consecutive_compliant_days": 10,
            "fail_consecutive_days": 14,
            "gap_to_fail_days": 4,
            "compliance_ratio_pct": 93.2,
            "message": "峰值发布门禁预警 | env=preprod",
            "selected_template_key": "preprod_warn",
            "selected_message": "【预发预警】峰值发布门禁预警",
            "fixed_templates": {
                "preprod_warn": "【预发预警】峰值发布门禁预警",
                "prod_block": "【生产阻断】峰值发布门禁阻断",
            },
            "email_subject": "[preprod] 峰值发布门禁预警",
            "email_body": "line1\nline2",
        }
    )
    assert "峰值发布门禁通知模板" in md
    assert "飞书/IM 消息" in md
    assert "固定文案（直接可发）" in md
    assert "selected_template_key" in md
    assert "[preprod] 峰值发布门禁预警" in md
    assert "consecutive_compliant_days" in md

from __future__ import annotations

import json
import pathlib

from tools.stability_30d_report import main as stability_main


def test_stability_step6_thresholds_from_env(tmp_path, monkeypatch):
    recs = [
        {"ts": "2026-03-01T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.01},
        {"ts": "2026-03-02T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.02},
        {"ts": "2026-03-03T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.03},
        {"ts": "2026-03-04T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.04},
        {"ts": "2026-03-05T00:00:00Z", "strict_gate_pass": True, "step6_sampled": 10, "step6_hard_fail_rate": 0.05},
    ]
    hist_path = tmp_path / "p0_trend_history.json"
    hist_path.write_text(json.dumps(recs), encoding="utf-8")
    md_out = tmp_path / "out.md"
    json_out = tmp_path / "out.json"
    monkeypatch.setenv("EASYXT_STEP6_WARN_DAYS", "2")
    monkeypatch.setenv("EASYXT_STEP6_FAIL_DAYS", "4")
    from tools import stability_30d_report as s30

    old_history = s30.HISTORY_PATH
    s30.HISTORY_PATH = pathlib.Path(hist_path)
    try:
        rc = stability_main(
            [
                "--window-days", "365",
                "--out", str(md_out),
                "--json-out", str(json_out),
            ]
        )
    finally:
        s30.HISTORY_PATH = old_history
    assert rc == 1
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["step6_warn_days"] == 2
    assert payload["step6_fail_days"] == 4

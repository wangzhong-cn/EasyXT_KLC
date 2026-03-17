import asyncio
import json
from datetime import datetime
from pathlib import Path

from easy_xt.realtime_data.monitor.alert_manager import AlertManager
from easy_xt.realtime_data.monitor.monitor_dashboard import MonitorDashboard


class DummyRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


def _rollup_alerts_log_once(log_dir: Path, offset: int = 0):
    alerts_path = log_dir / "alerts.log"
    if not alerts_path.exists():
        return None, offset

    current_size = alerts_path.stat().st_size
    if offset > current_size:
        offset = 0

    with alerts_path.open("r", encoding="utf-8") as f:
        f.seek(offset)
        data = f.read()
        next_offset = f.tell()

    lines = [line for line in data.splitlines() if line.strip()]
    if not lines:
        return None, next_offset

    type_counts: dict[str, int] = {}
    mode_counts: dict[str, int] = {}
    for line in lines:
        parts = line.split("\t")
        if len(parts) >= 2:
            type_counts[parts[1]] = type_counts.get(parts[1], 0) + 1
        if len(parts) >= 3:
            mode_counts[parts[2]] = mode_counts.get(parts[2], 0) + 1

    summary = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(lines),
        "by_type": type_counts,
        "by_mode": mode_counts,
    }

    summary_path = log_dir / "alerts_summary.log"
    with summary_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")

    return summary, next_offset


def test_alerts_log_rollup_ingest_resolve_chain(tmp_path):
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    alerts_path = log_dir / "alerts.log"

    alerts_path.write_text(
        "2026-03-04 10:00:00\tREALTIME_DEGRADE\tdegraded\tinterval=400ms\tsymbol=600000.SH\n",
        encoding="utf-8",
    )

    summary, offset = _rollup_alerts_log_once(log_dir, 0)
    assert summary is not None
    assert offset > 0
    assert summary["by_type"].get("REALTIME_DEGRADE") == 1
    assert (log_dir / "alerts_summary.log").exists()

    dashboard = MonitorDashboard()
    dashboard.register_alert_manager(AlertManager(config={}))

    asyncio.run(dashboard._handle_alerts_ingest(DummyRequest(summary)))
    active_alerts = dashboard.alert_manager.get_active_alerts()
    assert len(active_alerts) == 1
    assert active_alerts[0].rule_name == "realtime_degrade"
    assert active_alerts[0].source == "alerts_ingest"
    assert active_alerts[0].tags.get("mode_degraded") == "1"

    resolve_payload = {
        "rule_name": "realtime_degrade",
        "status": "resolved",
        "message": "实时链路恢复",
        "source": "alerts_ingest",
        "tags": {"mode_degraded": "1"},
    }
    asyncio.run(dashboard._handle_alerts_ingest(DummyRequest(resolve_payload)))

    assert len(dashboard.alert_manager.get_active_alerts()) == 0
    assert any(
        r.get("rule_name") == "realtime_degrade" and r.get("status") == "resolved"
        for r in dashboard._alerts_rollups
    )

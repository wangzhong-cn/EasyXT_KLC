import asyncio

import pytest

from easy_xt.realtime_data.monitor.alert_manager import AlertManager
from easy_xt.realtime_data.monitor.monitor_dashboard import MonitorDashboard


class DummyRequest:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


@pytest.mark.asyncio
async def test_ingest_trigger_and_resolve():
    dashboard = MonitorDashboard()
    dashboard.register_alert_manager(AlertManager(config={}))

    trigger_payload = {
        "rule_name": "realtime_degrade",
        "status": "triggered",
        "level": "warning",
        "title": "降级触发",
        "message": "进入降级",
        "value": 1.0,
        "threshold": 0.0,
        "source": "kline_chart_workspace",
        "tags": {"type": "degraded", "component": "realtime_pipeline"},
    }
    await dashboard._handle_alerts_ingest(DummyRequest(trigger_payload))
    assert len(dashboard.alert_manager.get_active_alerts()) == 1

    resolve_payload = {
        "rule_name": "realtime_degrade",
        "status": "resolved",
        "message": "恢复正常",
        "source": "kline_chart_workspace",
        "tags": {"type": "degraded", "component": "realtime_pipeline"},
    }
    await dashboard._handle_alerts_ingest(DummyRequest(resolve_payload))
    assert len(dashboard.alert_manager.get_active_alerts()) == 0

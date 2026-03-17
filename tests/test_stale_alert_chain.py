from datetime import datetime, timedelta

from easy_xt.realtime_data.monitor.alert_manager import AlertManager
from easy_xt.realtime_data.monitor.integration import MonitoringService
from easy_xt.realtime_data.monitor.metrics_collector import MetricPoint, MetricsCollector
from easy_xt.realtime_data.monitor.monitor_dashboard import MonitorDashboard


class DummyRequest:
    def __init__(self, query):
        self.query = query


def test_stale_flag_alert_history_chain():
    alert_manager = AlertManager()
    integration = MonitoringService(config={})
    integration.alert_manager = alert_manager
    integration._setup_alert_rules()

    metrics = MetricsCollector(collection_interval=999)
    integration.metrics_collector = metrics

    metrics._add_metric_point(
        MetricPoint(
            timestamp=datetime.now(),
            metric_name="datasource.stale_flag",
            value=1.0,
            tags={"source": "tdx"},
            source="test",
        )
    )

    import asyncio

    asyncio.run(integration.check_and_trigger_alerts())

    assert any(a.rule_name == "data_source_stale" for a in alert_manager.alert_history)

    dashboard = MonitorDashboard()
    dashboard.register_alert_manager(alert_manager)
    request = DummyRequest(query={"duration": "1h", "limit": "10", "rule": "data_source_stale", "source": "tdx"})
    response = asyncio.run(dashboard._handle_alerts_history(request))
    assert response.status == 200
    assert "data_source_stale" in response.text

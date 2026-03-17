"""
tests/test_metrics_endpoint.py
集成测试：core.api_server 的 /metrics 端点

使用 FastAPI TestClient（httpx 同步后端），无需启动真实服务器。
"""

import pytest

pytest.importorskip("fastapi", reason="fastapi not installed")
pytest.importorskip("httpx", reason="httpx not installed")

from fastapi.testclient import TestClient

from core.api_server import app


# ---------------------------------------------------------------------------
# 辅助：一次性创建 client（重用同一个 app 实例）
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


# ---------------------------------------------------------------------------
# 基础可达性
# ---------------------------------------------------------------------------


class TestMetricsEndpoint:
    def test_metrics_status_200(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200

    def test_metrics_content_type_prometheus(self, client):
        resp = client.get("/metrics")
        ct = resp.headers.get("content-type", "")
        # Prometheus text format: text/plain; ... OR application/openmetrics-text
        assert "text" in ct or "openmetrics" in ct

    def test_metrics_non_empty(self, client):
        resp = client.get("/metrics")
        assert len(resp.text) > 0

    def test_metrics_contains_easyxt_prefix(self, client):
        """所有自定义指标应带 easyxt_ 前缀。"""
        resp = client.get("/metrics")
        body = resp.text
        assert "easyxt_" in body

    def test_metrics_known_gauge_names(self, client):
        """检查几个核心指标名称存在于响应体中。"""
        resp = client.get("/metrics")
        body = resp.text
        # 至少存在以下指标之一
        expected = [
            "easyxt_strategies_running",
            "easyxt_ws_queue_total_len",
            "easyxt_uptime_seconds",
        ]
        found = [name for name in expected if name in body]
        assert len(found) > 0, (
            f"None of {expected} found in /metrics body:\n{body[:500]}"
        )

    def test_metrics_idempotent(self, client):
        """两次请求均应返回 200。"""
        r1 = client.get("/metrics")
        r2 = client.get("/metrics")
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_metrics_not_in_openapi_schema(self, client):
        """include_in_schema=False：/metrics 不应出现在 OpenAPI JSON。"""
        resp = client.get("/openapi.json")
        if resp.status_code != 200:
            pytest.skip("OpenAPI schema endpoint not available")
        schema = resp.json()
        paths = schema.get("paths", {})
        assert "/metrics" not in paths

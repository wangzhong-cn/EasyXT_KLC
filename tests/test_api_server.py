"""
FastAPI 中台服务单元测试。

覆盖范围：
  - /health
  - /api/v1/strategies/         — list
  - /api/v1/strategies/{id}     — get（存在 & 不存在）
  - /api/v1/strategies/{id}/status — patch（合法 & 非法值）
  - /api/v1/strategies/snapshot — post
  - /api/v1/market/snapshot/{symbol} — HTTP 行情快照
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# 导入被测模块
# ---------------------------------------------------------------------------

from core.api_server import app

client = TestClient(app, raise_server_exceptions=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def _mock_registry():
    """注入一个带有预置策略的假注册中心。"""
    from strategies.registry import StrategyInfo

    info_running = StrategyInfo(
        strategy_id="ma_cross_v1",
        strategy_obj=None,
        account_id="88001234",
        params={"fast": 5, "slow": 20},
        tags=["trend"],
        status="running",
        registered_at=time.time(),
    )
    info_stopped = StrategyInfo(
        strategy_id="grid_v2",
        strategy_obj=None,
        account_id="88001234",
        params={"levels": 10},
        tags=["grid"],
        status="stopped",
        registered_at=time.time(),
    )

    mock_reg = MagicMock()
    mock_reg.list_all.return_value = [
        {"strategy_id": "ma_cross_v1", "account_id": "88001234",
         "status": "running", "tags": ["trend"], "params_keys": ["fast", "slow"],
         "registered_at": info_running.registered_at, "has_instance": False},
        {"strategy_id": "grid_v2", "account_id": "88001234",
         "status": "stopped", "tags": ["grid"], "params_keys": ["levels"],
         "registered_at": info_stopped.registered_at, "has_instance": False},
    ]
    mock_reg.list_running.return_value = [info_running]
    mock_reg.get.side_effect = lambda sid: (
        info_running if sid == "ma_cross_v1" else
        info_stopped if sid == "grid_v2" else None
    )
    mock_reg.update_status.return_value = (True, "")
    mock_reg.snapshot_to_db.return_value = 2

    with patch("core.api_server.app") as _:
        pass  # 只需注入 registry

    # 通过 module-level patch 替换 strategy_registry
    with patch("strategies.registry.strategy_registry", mock_reg):
        yield mock_reg


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class TestHealth:
    def test_returns_ok(self, _mock_registry):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "server_time" in body
        assert isinstance(body["strategies_running"], int)
        assert "uptime_s" in body
        assert "build_version" in body
        assert "commit_sha" in body
        # 语义分层 checks 子对象
        assert "checks" in body
        assert "registry" in body["checks"]
        assert "ws" in body["checks"]
        assert "db" in body["checks"]
        assert body["checks"]["registry"]["status"] == "ok"
        assert isinstance(body["checks"]["registry"]["strategies_running"], int)
        assert body["checks"]["ws"]["status"] == "ok"
        assert "symbols" in body["checks"]["ws"]
        assert "cleanup" in body["checks"]["ws"]
        assert "error_count" in body["checks"]["ws"]["cleanup"]
        assert "drop_counts" in body["checks"]["ws"]
        assert "drop_rate" in body["checks"]["ws"]
        assert isinstance(body["checks"]["ws"]["drop_rate"], float)
        assert "drop_rate_1m" in body["checks"]["ws"]
        assert isinstance(body["checks"]["ws"]["drop_rate_1m"], float)
        assert "drop_alert" in body["checks"]["ws"]
        assert body["checks"]["ws"]["drop_alert"] in ("ok", "ok_low_sample", "warning", "critical")
        assert "drop_alert_thresholds" in body["checks"]["ws"]
        thresholds = body["checks"]["ws"]["drop_alert_thresholds"]
        assert "warn" in thresholds and "crit" in thresholds and "min_samples" in thresholds
        assert thresholds["warn"] < thresholds["crit"]
        assert isinstance(thresholds["min_samples"], int)
        assert "queue_len" in body["checks"]["ws"]
        assert isinstance(body["checks"]["ws"]["queue_len"], int)
        assert "publish_latency_ms" in body["checks"]["ws"]
        assert "status" in body["checks"]["db"]

    def test_server_time_is_recent(self, _mock_registry):
        resp = client.get("/health")
        ts = resp.json()["server_time"]
        assert abs(ts - int(time.time() * 1000)) < 5000


class TestDatasourceHealth:
    def test_returns_datasource_health_payload(self):
        mock_iface = MagicMock()
        mock_iface.data_registry.get_health_summary.return_value = {
            "duckdb": {"name": "duckdb", "available": True},
            "tushare": {"name": "tushare", "available": False},
        }
        mock_iface.get_quarantine_status_counts.return_value = {
            "pending": 10,
            "failed": 2,
            "resolved": 100,
            "dead_letter": 1,
            "total": 113,
        }
        mock_iface.get_data_quality_incident_counts.return_value = {
            "total": 1,
            "critical": 1,
            "high": 0,
            "medium": 0,
            "low": 0,
        }
        mock_iface.get_step6_validation_metrics.return_value = {
            "total": 10, "sampled": 10, "skipped": 0, "hard_failed": 1, "quarantined": 1, "sample_rate": 1.0, "hard_fail_rate": 0.1
        }
        mock_iface._cb_state = {"open": False, "fail_count": 0}
        with patch("core.api_server._get_datasource_health_interface", return_value=mock_iface):
            resp = client.get("/health/datasource")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "checks" in body and "sources" in body["checks"]
        assert "quarantine" in body["checks"]
        assert "dead_letter_ratio" in body["checks"]["quarantine"]
        assert "data_quality_incident" in body["checks"]
        assert "step6_validation" in body["checks"]
        assert "thresholds" in body["checks"]
        assert "server_time" in body

    def test_degraded_when_datasource_health_fails(self):
        with patch("core.api_server._get_datasource_health_interface", side_effect=RuntimeError("boom")):
            resp = client.get("/health/datasource")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert "error" in body["checks"]

    def test_degraded_when_dead_letter_exceeds_threshold(self, monkeypatch):
        mock_iface = MagicMock()
        mock_iface.data_registry.get_health_summary.return_value = {"duckdb": {"available": True}}
        mock_iface.get_quarantine_status_counts.return_value = {
            "pending": 0,
            "failed": 0,
            "resolved": 0,
            "dead_letter": 5,
            "total": 10,
        }
        mock_iface.get_data_quality_incident_counts.return_value = {"total": 0, "critical": 0, "high": 0, "medium": 0, "low": 0}
        mock_iface.get_step6_validation_metrics.return_value = {"total": 1, "sampled": 1, "skipped": 0, "hard_failed": 0, "quarantined": 0, "sample_rate": 1.0, "hard_fail_rate": 0.0}
        mock_iface._cb_state = {"open": False, "fail_count": 0}
        monkeypatch.setenv("EASYXT_QUARANTINE_DEADLETTER_WARN", "3")
        monkeypatch.setenv("EASYXT_QUARANTINE_DEADLETTER_RATIO_WARN", "0.4")
        with patch("core.api_server._get_datasource_health_interface", return_value=mock_iface):
            resp = client.get("/health/datasource")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"


class TestSlaHealth:
    """GET /health/sla — 数据质量 SLA 报告端点。"""

    _GOOD_SLA = {
        "report_date": "2026-03-15",
        "completeness": 0.999,
        "consistency": 1.0,
        "lag_p95_ms": 120.0,
        "trust_score": 0.999,
        "gate_pass": True,
        "write_total_rows": 1000,
        "write_expected_rows": 1001,
        "conflict_count": 0,
        "step6_total_checks": 100,
        "step6_sampled_checks": 100,
        "step6_skipped_checks": 0,
        "step6_hard_failed_checks": 0,
        "step6_hard_fail_rate": 0.0,
        "step6_sample_rate": 1.0,
        "canary_shadow_write_enabled": False,
        "canary_shadow_only": True,
        "reject_count": 0,
    }

    def test_returns_sla_payload_with_gate_pass(self):
        mock_iface = MagicMock()
        mock_iface.generate_daily_sla_report.return_value = self._GOOD_SLA
        with patch("core.api_server._get_datasource_health_interface", return_value=mock_iface):
            resp = client.get("/health/sla")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "sla" in body
        assert body["sla"]["gate_pass"] is True
        assert "server_time" in body
        assert "build_version" in body
        mock_iface.generate_daily_sla_report.assert_called_once_with(None)

    def test_degraded_when_gate_fails(self):
        bad_sla = dict(self._GOOD_SLA, gate_pass=False, trust_score=0.85)
        mock_iface = MagicMock()
        mock_iface.generate_daily_sla_report.return_value = bad_sla
        with patch("core.api_server._get_datasource_health_interface", return_value=mock_iface):
            resp = client.get("/health/sla")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"

    def test_accepts_report_date_param(self):
        mock_iface = MagicMock()
        mock_iface.generate_daily_sla_report.return_value = dict(self._GOOD_SLA, report_date="2026-01-01")
        with patch("core.api_server._get_datasource_health_interface", return_value=mock_iface):
            resp = client.get("/health/sla?report_date=2026-01-01")
        assert resp.status_code == 200
        mock_iface.generate_daily_sla_report.assert_called_once_with("2026-01-01")

    def test_degraded_when_exception_raised(self):
        with patch("core.api_server._get_datasource_health_interface", side_effect=RuntimeError("db gone")):
            resp = client.get("/health/sla")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "degraded"
        assert "error" in body


# ---------------------------------------------------------------------------
# Strategy List
# ---------------------------------------------------------------------------

class TestStrategyList:
    def test_list_returns_all(self, _mock_registry):
        resp = client.get("/api/v1/strategies/")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_list_filter_running(self, _mock_registry):
        _mock_registry.list_all.return_value = [
            {"strategy_id": "ma_cross_v1", "status": "running",
             "account_id": "", "tags": [], "params_keys": [],
             "registered_at": 0, "has_instance": False},
            {"strategy_id": "grid_v2", "status": "stopped",
             "account_id": "", "tags": [], "params_keys": [],
             "registered_at": 0, "has_instance": False},
        ]
        resp = client.get("/api/v1/strategies/?status_filter=running")
        assert resp.status_code == 200
        result = resp.json()
        assert all(r["status"] == "running" for r in result)

    def test_list_empty_registry(self, _mock_registry):
        _mock_registry.list_all.return_value = []
        resp = client.get("/api/v1/strategies/")
        assert resp.status_code == 200
        assert resp.json() == []


# ---------------------------------------------------------------------------
# Strategy Get
# ---------------------------------------------------------------------------

class TestStrategyGet:
    def test_get_existing(self, _mock_registry):
        resp = client.get("/api/v1/strategies/ma_cross_v1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["strategy_id"] == "ma_cross_v1"
        assert body["status"] == "running"
        assert "params" in body

    def test_get_nonexistent_returns_404(self, _mock_registry):
        resp = client.get("/api/v1/strategies/nonexistent_id")
        assert resp.status_code == 404
        assert "未找到" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Strategy Status Patch
# ---------------------------------------------------------------------------

class TestStrategyStatusPatch:
    def test_patch_valid_status(self, _mock_registry):
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status",
            json={"status": "stopped"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "stopped"
        assert body["updated"] is True

    def test_patch_invalid_status_422(self, _mock_registry):
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status",
            json={"status": "unknown_value"},
        )
        assert resp.status_code == 422

    def test_patch_nonexistent_404(self, _mock_registry):
        _mock_registry.update_status.return_value = None
        resp = client.patch(
            "/api/v1/strategies/not_exist/status",
            json={"status": "stopped"},
        )
        assert resp.status_code == 404

    @pytest.mark.parametrize("allowed", ["running", "paused", "stopped", "error"])
    def test_all_allowed_statuses(self, allowed, _mock_registry):
        _mock_registry.update_status.return_value = (True, "")
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status",
            json={"status": allowed},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

class TestSnapshot:
    def test_snapshot_returns_count(self, _mock_registry):
        resp = client.post("/api/v1/strategies/snapshot")
        assert resp.status_code == 200
        body = resp.json()
        assert body["snapshot_written"] == 2


# ---------------------------------------------------------------------------
# Market Snapshot (HTTP)
# ---------------------------------------------------------------------------

class TestMarketSnapshot:
    def test_unavailable_returns_graceful(self):
        """当 data_manager 不可用时，应返回 200 + source=unavailable（不抛 500）。"""
        resp = client.get("/api/v1/market/snapshot/000001.SZ")
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "000001.SZ"
        assert body["source"] in ("duckdb", "unavailable")

    def test_returns_symbol_in_response(self):
        resp = client.get("/api/v1/market/snapshot/600519.SH")
        assert resp.json()["symbol"] == "600519.SH"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

class TestAuth:
    def test_protected_endpoint_open_when_auth_disabled(self, _mock_registry):
        """_API_TOKEN 为空时无需 Token 即可访问受保护端点。"""
        resp = client.get("/api/v1/strategies/")
        assert resp.status_code == 200

    def test_returns_401_when_token_required_but_missing(self, _mock_registry):
        with patch("core.api_server._API_TOKEN", "secret-token"):
            resp = client.get("/api/v1/strategies/")
        assert resp.status_code == 401

    def test_authorized_with_correct_token(self, _mock_registry):
        with patch("core.api_server._API_TOKEN", "secret-token"):
            resp = client.get(
                "/api/v1/strategies/",
                headers={"X-API-Token": "secret-token"},
            )
        assert resp.status_code == 200

    def test_health_always_accessible(self, _mock_registry):
        """/health 无论是否开启鉴权都应可访问，且包含 auth_enabled 字段。"""
        with patch("core.api_server._API_TOKEN", "secret-token"):
            resp = client.get("/health")
        assert resp.status_code == 200
        assert "auth_enabled" in resp.json()


# ---------------------------------------------------------------------------
# State Machine
# ---------------------------------------------------------------------------

class TestStateMachine:
    def test_illegal_transition_returns_409(self, _mock_registry):
        _mock_registry.update_status.return_value = (False, "stopped \u2192 running \u4e0d\u5141\u8bb8")
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status",
            json={"status": "running"},
        )
        assert resp.status_code == 409
        assert "\u975e\u6cd5\u72b6\u6001\u8f6c\u6362" in resp.json()["detail"]

    def test_not_found_returns_404(self, _mock_registry):
        _mock_registry.update_status.return_value = None
        resp = client.patch(
            "/api/v1/strategies/no_such_id/status",
            json={"status": "stopped"},
        )
        assert resp.status_code == 404

    def test_paused_is_valid_status_value(self, _mock_registry):
        _mock_registry.update_status.return_value = (True, "")
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status",
            json={"status": "paused"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "paused"


# ---------------------------------------------------------------------------
# Rate Limit
# ---------------------------------------------------------------------------

class TestRateLimit:
    def test_rate_limit_blocks_excess_requests(self, _mock_registry):
        from core.api_server import _rate_buckets
        _rate_buckets.clear()
        try:
            with patch("core.api_server._RATE_LIMIT", 2):
                r1 = client.get("/api/v1/strategies/")
                r2 = client.get("/api/v1/strategies/")
                r3 = client.get("/api/v1/strategies/")
            assert r1.status_code == 200
            assert r2.status_code == 200
            assert r3.status_code == 429
        finally:
            _rate_buckets.clear()

    def test_rate_limit_zero_means_unlimited(self, _mock_registry):
        from core.api_server import _rate_buckets
        _rate_buckets.clear()
        try:
            with patch("core.api_server._RATE_LIMIT", 0):
                for _ in range(5):
                    resp = client.get("/api/v1/strategies/")
                    assert resp.status_code == 200
        finally:
            _rate_buckets.clear()

    def test_rate_limit_hits_incremented(self, _mock_registry):
        """超限请求应递增 _rate_limit_hits 计数器。"""
        import core.api_server as api_mod
        from core.api_server import _rate_buckets
        _rate_buckets.clear()
        original_hits = api_mod._rate_limit_hits
        original_limit = api_mod._RATE_LIMIT
        api_mod._RATE_LIMIT = 1
        try:
            client.get("/api/v1/strategies/")   # passes
            client.get("/api/v1/strategies/")   # hits limit → _rate_limit_hits += 1
            assert api_mod._rate_limit_hits > original_hits
        finally:
            api_mod._RATE_LIMIT = original_limit
            _rate_buckets.clear()

    def test_health_exposes_rate_limit_hits(self, _mock_registry):
        """/health 应暴露 rate_limit_hits 字段。"""
        resp = client.get("/health")
        assert resp.status_code == 200
        assert "rate_limit_hits" in resp.json()
        assert isinstance(resp.json()["rate_limit_hits"], int)


# ---------------------------------------------------------------------------
# Error Format
# ---------------------------------------------------------------------------

class TestErrorFormat:
    """验证所有 HTTP 错误响应遵循统一格式 {code, message, detail, trace_id}。"""

    def test_404_has_standard_error_shape(self, _mock_registry):
        resp = client.get("/api/v1/strategies/no_such_id")
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == 404
        assert body["message"] == "Not Found"
        assert "detail" in body
        assert "trace_id" in body
        # trace_id 应为 UUID 格式
        assert len(body["trace_id"]) == 36

    def test_409_has_standard_error_shape(self, _mock_registry):
        _mock_registry.update_status.return_value = (False, "stopped → running 不允许")
        resp = client.patch(
            "/api/v1/strategies/ma_cross_v1/status", json={"status": "running"}
        )
        assert resp.status_code == 409
        body = resp.json()
        assert body["code"] == 409
        assert body["message"] == "Conflict"
        assert "trace_id" in body

    def test_401_has_standard_error_shape(self, _mock_registry):
        with patch("core.api_server._API_TOKEN", "test-token"):
            resp = client.get("/api/v1/strategies/")
        assert resp.status_code == 401
        body = resp.json()
        assert body["code"] == 401
        assert body["message"] == "Unauthorized"
        assert "trace_id" in body

    def test_429_has_standard_error_shape(self, _mock_registry):
        from core.api_server import _rate_buckets
        _rate_buckets.clear()
        try:
            with patch("core.api_server._RATE_LIMIT", 1):
                client.get("/api/v1/strategies/")    # passes
                resp = client.get("/api/v1/strategies/")  # 429
            assert resp.status_code == 429
            body = resp.json()
            assert body["code"] == 429
            assert body["message"] == "Too Many Requests"
            assert "trace_id" in body
        finally:
            _rate_buckets.clear()

    def test_each_error_gets_unique_trace_id(self, _mock_registry):
        """每次错误应生成不同的 trace_id（UUID 随机性）。"""
        r1 = client.get("/api/v1/strategies/ghost_1")
        r2 = client.get("/api/v1/strategies/ghost_2")
        assert r1.json()["trace_id"] != r2.json()["trace_id"]


# ---------------------------------------------------------------------------
# 行情订阅管理
# ---------------------------------------------------------------------------

class TestSubscription:
    """POST/DELETE/GET /api/v1/market/subscribe* 端点测试。"""

    def _mock_qmt_feed(self, **overrides):
        """Returns a MagicMock qmt_feed with sensible defaults."""
        feed = MagicMock()
        feed.subscribe.return_value = {"subscribed": True, "source": "mock", "message": "订阅成功"}
        feed.unsubscribe.return_value = {"unsubscribed": True, "message": "取消成功"}
        feed.all_subscriptions.return_value = []
        feed.stats.return_value = {"total_subscriptions": 0, "total_ingested": 0,
                                   "total_errors": 0, "qmt_available": False}
        for k, v in overrides.items():
            setattr(feed, k, v)
        return feed

    def test_subscribe_returns_symbol(self, _mock_registry):
        feed = self._mock_qmt_feed()
        with patch("core.api_server.subscribe_symbol.__module__", create=True), \
             patch("core.qmt_feed.qmt_feed", feed):
            resp = client.post("/api/v1/market/subscribe",
                               json={"symbol": "000001.SZ", "period": "tick"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "000001.SZ"
        assert body["period"] == "tick"
        assert "subscribed" in body

    def test_subscribe_default_period_is_tick(self, _mock_registry):
        feed = self._mock_qmt_feed()
        with patch("core.qmt_feed.qmt_feed", feed):
            resp = client.post("/api/v1/market/subscribe", json={"symbol": "600519.SH"})
        assert resp.status_code == 200
        assert resp.json()["period"] == "tick"

    def test_subscribe_calls_feed_subscribe(self, _mock_registry):
        feed = self._mock_qmt_feed()
        with patch("core.qmt_feed.qmt_feed", feed):
            client.post("/api/v1/market/subscribe",
                        json={"symbol": "000001.SZ", "period": "1m"})
        feed.subscribe.assert_called_once_with("000001.SZ", "1m")

    def test_unsubscribe_not_subscribed_returns_200(self, _mock_registry):
        feed = self._mock_qmt_feed()
        feed.unsubscribe.return_value = {"unsubscribed": False, "message": "未订阅"}
        with patch("core.qmt_feed.qmt_feed", feed):
            resp = client.delete("/api/v1/market/subscribe/000001.SZ")
        assert resp.status_code == 200
        assert resp.json()["symbol"] == "000001.SZ"
        assert resp.json()["unsubscribed"] is False

    def test_unsubscribe_success(self, _mock_registry):
        feed = self._mock_qmt_feed()
        with patch("core.qmt_feed.qmt_feed", feed):
            resp = client.delete("/api/v1/market/subscribe/600519.SH")
        assert resp.status_code == 200
        assert resp.json()["unsubscribed"] is True

    def test_list_subscriptions_returns_dict(self, _mock_registry):
        feed = self._mock_qmt_feed()
        feed.all_subscriptions.return_value = [
            {"symbol": "000001.SZ", "period": "tick", "subscribed_at": 0,
             "ingested_count": 10, "error_count": 0, "last_tick_ts": None}
        ]
        feed.stats.return_value = {"total_subscriptions": 1, "total_ingested": 10,
                                   "total_errors": 0, "qmt_available": False}
        with patch("core.qmt_feed.qmt_feed", feed):
            resp = client.get("/api/v1/market/subscriptions")
        assert resp.status_code == 200
        body = resp.json()
        assert "subscriptions" in body
        assert "stats" in body
        assert len(body["subscriptions"]) == 1
        assert body["stats"]["total_subscriptions"] == 1

    def test_list_subscriptions_empty_when_feed_raises(self, _mock_registry):
        """qmt_feed 异常时，端点应优雅降级返回空结果。"""
        with patch("core.qmt_feed.qmt_feed", side_effect=ImportError("no module")):
            resp = client.get("/api/v1/market/subscriptions")
        assert resp.status_code == 200
        assert resp.json()["subscriptions"] == []

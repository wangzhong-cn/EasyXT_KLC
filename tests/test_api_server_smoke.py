from __future__ import annotations

from fastapi.testclient import TestClient

from core import api_server
from core.account_registry import account_registry
from strategies.registry import strategy_registry


def _drain_in_memory_state() -> None:
    account_registry._accounts.clear()
    strategy_registry._registry.clear()
    api_server._rate_buckets.clear()
    api_server._rate_limit_hits = 0


def test_health_endpoint_smoke() -> None:
    _drain_in_memory_state()
    with TestClient(api_server.app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] in {"ok", "degraded"}
        assert "checks" in payload
        assert "db" in payload["checks"]


def test_auth_guard_smoke() -> None:
    _drain_in_memory_state()
    token_backup = api_server._API_TOKEN
    try:
        api_server._API_TOKEN = "smoke-token"
        with TestClient(api_server.app) as client:
            unauth = client.get("/api/v1/accounts/")
            assert unauth.status_code == 401
            auth = client.get("/api/v1/accounts/", headers={"X-API-Token": "smoke-token"})
            assert auth.status_code == 200
            assert isinstance(auth.json(), list)
    finally:
        api_server._API_TOKEN = token_backup


def test_account_crud_smoke() -> None:
    _drain_in_memory_state()
    with TestClient(api_server.app) as client:
        create = client.post(
            "/api/v1/accounts/",
            json={"account_id": "SIM-001", "broker": "sim", "enabled": True},
        )
        assert create.status_code == 200
        payload = create.json()
        assert payload["account_id"] == "SIM-001"

        fetch = client.get("/api/v1/accounts/SIM-001")
        assert fetch.status_code == 200
        assert fetch.json()["broker"] == "sim"

        list_resp = client.get("/api/v1/accounts/")
        assert list_resp.status_code == 200
        assert any(item["account_id"] == "SIM-001" for item in list_resp.json())

        delete = client.delete("/api/v1/accounts/SIM-001")
        assert delete.status_code == 200
        assert delete.json()["deleted"] is True


def test_market_websocket_ping_pong_smoke() -> None:
    _drain_in_memory_state()
    with TestClient(api_server.app) as client:
        with client.websocket_connect("/ws/market/000001.SZ") as ws:
            ws.send_text("ping")
            for _ in range(8):
                msg = ws.receive_text()
                if "pong" in msg:
                    assert msg == '{"type":"pong"}'
                    return
            raise AssertionError("websocket 未返回 pong")

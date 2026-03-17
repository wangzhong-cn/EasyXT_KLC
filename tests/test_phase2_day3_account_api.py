"""
Phase 2 Day3 — 账户 REST 接口测试

覆盖：
  POST   /api/v1/accounts/          — 注册账户
  GET    /api/v1/accounts/          — 列出账户
  GET    /api/v1/accounts/{id}      — 获取单账户
  DELETE /api/v1/accounts/{id}      — 注销账户
"""

import pytest
from fastapi.testclient import TestClient

pytest.importorskip("fastapi")
pytest.importorskip("core.api_server")

from core.api_server import app  # noqa: E402
from core.account_registry import AccountRegistry  # noqa: E402


@pytest.fixture()
def client(monkeypatch):
    """每个测试用独立 AccountRegistry，避免单例状态污染。"""
    import core.account_registry as _mod
    fresh = AccountRegistry()
    monkeypatch.setattr(_mod, "account_registry", fresh)

    # api_server 内部的延迟导入需要看到同一 fresh 实例
    import core.api_server as _srv_mod

    def _patched_get_registry():
        return fresh

    # api_server 端点每次调用时 `from core.account_registry import account_registry`
    # monkeypatch 已覆盖 _mod，PyImport cache 保证同一对象
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c, fresh


# ---------------------------------------------------------------------------


def test_register_account_returns_payload(client):
    c, _ = client
    body = {"account_id": "ACCT001", "broker": "qmt", "enabled": True}
    resp = c.post("/api/v1/accounts/", json=body)
    assert resp.status_code == 200
    data = resp.json()
    assert data["account_id"] == "ACCT001"
    assert data["broker"] == "qmt"
    assert "created_at_ms" in data


def test_list_accounts_contains_registered(client):
    c, _ = client
    c.post("/api/v1/accounts/", json={"account_id": "A1", "broker": "sim"})
    c.post("/api/v1/accounts/", json={"account_id": "A2", "broker": "qmt"})
    resp = c.get("/api/v1/accounts/")
    assert resp.status_code == 200
    ids = {item["account_id"] for item in resp.json()}
    assert {"A1", "A2"}.issubset(ids)


def test_get_account_found_and_not_found(client):
    c, _ = client
    c.post("/api/v1/accounts/", json={"account_id": "EXISTS"})
    resp_ok = c.get("/api/v1/accounts/EXISTS")
    assert resp_ok.status_code == 200
    assert resp_ok.json()["account_id"] == "EXISTS"

    resp_miss = c.get("/api/v1/accounts/NO_SUCH")
    assert resp_miss.status_code == 404


def test_delete_account_removes_and_returns_404_on_repeat(client):
    c, _ = client
    c.post("/api/v1/accounts/", json={"account_id": "DEL_ME"})

    resp_del = c.delete("/api/v1/accounts/DEL_ME")
    assert resp_del.status_code == 200
    assert resp_del.json()["deleted"] is True

    # 再次删除 → 404
    resp_again = c.delete("/api/v1/accounts/DEL_ME")
    assert resp_again.status_code == 404

    # list 中消失
    ids = {item["account_id"] for item in c.get("/api/v1/accounts/").json()}
    assert "DEL_ME" not in ids


def test_register_upsert_updates_broker(client):
    c, _ = client
    c.post("/api/v1/accounts/", json={"account_id": "UPS", "broker": "old"})
    c.post("/api/v1/accounts/", json={"account_id": "UPS", "broker": "new"})
    resp = c.get("/api/v1/accounts/UPS")
    assert resp.json()["broker"] == "new"

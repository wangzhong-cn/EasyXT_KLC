from __future__ import annotations

from types import SimpleNamespace

import core.qmt_registry as qmt_registry
from fastapi.testclient import TestClient

from core import api_server
from core.qmt_registry import build_qmt_account_binding_projection


def _account(**overrides: object) -> SimpleNamespace:
    payload = {
        "id": "acct-1",
        "label": "测试账户",
        "broker": "申万宏源",
        "trade_account": "A001",
        "qmt_exe_path": "",
        "qmt_userdata_path": "",
        "is_default": False,
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _layout(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "layout_id": "layout-1",
        "install_root": "D:/QMT/Client",
    }
    payload.update(overrides)
    return payload


def _asset(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "asset_id": "asset-1",
        "layout_id": "layout-1",
        "userdata_path": "D:/QMT/Client/userdata_mini",
    }
    payload.update(overrides)
    return payload


def _probe(**overrides: object) -> SimpleNamespace:
    payload = {
        "probe_id": "probe-1",
        "channel_id": "broker-stock",
        "userdata_path": "D:/QMT/Client/userdata_mini",
        "account_id": "A001",
        "status": "succeeded",
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _session(**overrides: object) -> SimpleNamespace:
    payload = {
        "session_id": "sess-1",
        "session_anchor_key": "D:/QMT/Client/userdata_mini",
        "userdata_path": "D:/QMT/Client/userdata_mini",
        "connected_accounts": ["A001"],
        "channel_profile": {"channel_id": "broker-stock"},
    }
    payload.update(overrides)
    return SimpleNamespace(**payload)


def _route(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "snapshot_id": "route-1",
        "purpose": "trade",
        "account_id": "A001",
        "candidate_ids": ["sess-1"],
        "winner": "sess-1",
        "score_breakdown": {"freshness": 0.6, "affinity": 0.4},
        "decision_reason": "session 命中资金账号",
        "rejection_reasons": [],
    }
    payload.update(overrides)
    return payload


def _conflict(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "code": "ACCOUNT_MULTI_USERDATA",
        "severity": "blocking",
        "message": "资金账号同时落在多个 userdata。",
        "target_kind": "probe_account",
        "target_id": "A001",
        "details": {"userdata_paths": ["D:/QMT/Client/userdata_mini", "D:/Other/userdata_mini"]},
    }
    payload.update(overrides)
    return payload


def test_binding_projection_marks_proposed_when_asset_candidate_exists() -> None:
    projection = build_qmt_account_binding_projection(
        accounts=[_account(qmt_exe_path="D:/QMT/Client/bin/XtItClient.exe")],
        layouts=[_layout()],
        assets=[_asset()],
        probes=[],
        sessions=[],
        conflicts=[],
        routes=[],
    )

    assert projection["total"] == 1
    item = projection["items"][0]
    assert item["status"] == "proposed"
    assert item["approval_required"] is True
    assert item["recommendation_status"] == "suggested"
    assert item["apply_path"] == "D:/QMT/Client/userdata_mini"
    assert any("可建议绑定" in reason or "exe 路径命中本地布局" in reason for reason in item["reasons"])


def test_binding_projection_marks_confirmed_when_runtime_matches_configured_path() -> None:
    projection = build_qmt_account_binding_projection(
        accounts=[_account(qmt_userdata_path="D:/QMT/Client/userdata_mini", is_default=True)],
        layouts=[_layout()],
        assets=[_asset()],
        probes=[_probe()],
        sessions=[_session()],
        conflicts=[],
        routes=[_route()],
    )

    item = projection["items"][0]
    assert item["status"] == "confirmed"
    assert item["approval_required"] is False
    assert item["manual_override"] is True
    assert item["binding_scope"] == "trade_default"
    assert item["confidence_score"] > 0.5
    assert item["probe"]["account_id"] == "A001"
    assert item["session"]["connected_accounts"] == ["A001"]


def test_binding_projection_marks_conflicted_when_blocking_conflict_present() -> None:
    projection = build_qmt_account_binding_projection(
        accounts=[_account(qmt_userdata_path="D:/QMT/Client/userdata_mini")],
        layouts=[_layout()],
        assets=[_asset()],
        probes=[_probe()],
        sessions=[_session()],
        conflicts=[_conflict()],
        routes=[_route()],
    )

    item = projection["items"][0]
    assert item["status"] == "conflicted"
    assert item["approval_required"] is True
    assert item["approval_state"] == "review_required"
    assert item["conflict_flags"] == ["ACCOUNT_MULTI_USERDATA"]
    assert len(item["conflicts"]) == 1


def test_account_bindings_endpoint_returns_projection_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        api_server,
        "_load_qmt_account_binding_projection",
        lambda **_: {
            "bindings": {
                "items": [
                    {
                        "binding_id": "binding-1",
                        "broker_account_id": "acct-1",
                        "status": "proposed",
                        "approval_required": True,
                        "approval_state": "pending_manual_confirmation",
                        "reasons": ["存在可建议的本地候选路径，可作为 draft 绑定目标。"],
                    }
                ],
                "total": 1,
            },
            "probe_errors": [],
            "include_probes": False,
        },
    )

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.get("/api/v1/account-bindings")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["total"] == 1
    assert payload["items"][0]["binding_id"] == "binding-1"
    assert payload["items"][0]["status"] == "proposed"
    assert payload["probe_errors"] == []


def test_account_bindings_endpoint_preserves_runtime_explain_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        api_server,
        "_load_qmt_account_binding_projection",
        lambda **_: {
            "bindings": {
                "items": [
                    {
                        "binding_id": "binding-1",
                        "broker_account_id": "acct-1",
                        "status": "confirmed",
                        "approval_required": False,
                        "approval_state": "confirmed",
                        "recommendation_status": "matched",
                        "recommendation_message": "账户路径已命中本地 QMT 候选。",
                        "candidate_path": "D:/QMT/Client/userdata_mini",
                        "apply_path": "D:/QMT/Client/userdata_mini",
                        "reasons": ["按需 probe 命中同一资金账号。"],
                        "session": {
                            "session_id": "sess-1",
                            "session_anchor_key": "D:/QMT/Client/userdata_mini",
                            "userdata_path": "D:/QMT/Client/userdata_mini",
                            "connected_accounts": ["A001"],
                            "current_route_claims": ["trade"],
                            "login_status": "connected",
                            "session_health": "healthy",
                            "freshness_state": "fresh",
                            "status": "healthy",
                        },
                        "probe": {
                            "probe_id": "probe-1",
                            "userdata_path": "D:/QMT/Client/userdata_mini",
                            "account_id": "A001",
                            "login_status": "connected",
                            "probe_success": True,
                            "probe_error_code": "",
                            "probe_error_message": "",
                            "freshness_state": "fresh",
                            "status": "succeeded",
                        },
                        "route": {
                            "snapshot_id": "route-1",
                            "purpose": "trade",
                            "winner": "sess-1",
                            "candidate_ids": ["sess-1"],
                            "decision_reason": "session 命中资金账号",
                            "rejection_reasons": [],
                        },
                        "conflicts": [
                            {
                                "code": "PRIMARY_ROUTE_DEGRADED",
                                "severity": "warning",
                                "message": "默认路由已降级。",
                                "target_kind": "session",
                                "target_id": "sess-1",
                                "details": {"current_route_claims": ["trade"]},
                            }
                        ],
                    }
                ],
                "total": 1,
            },
            "probe_errors": [],
            "include_probes": True,
        },
    )

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.get("/api/v1/account-bindings?include_probes=true")

    assert resp.status_code == 200
    payload = resp.json()
    item = payload["items"][0]
    assert payload["include_probes"] is True
    assert item["session"]["session_health"] == "healthy"
    assert item["session"]["current_route_claims"] == ["trade"]
    assert item["probe"]["probe_error_code"] == ""
    assert item["probe"]["freshness_state"] == "fresh"
    assert item["route"]["decision_reason"] == "session 命中资金账号"
    assert item["conflicts"][0]["details"]["current_route_claims"] == ["trade"]


def test_account_bindings_endpoint_reports_include_probes_flag_and_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        api_server,
        "_load_qmt_account_binding_projection",
        lambda **kwargs: {
            "bindings": {"items": [], "total": 0},
            "probe_errors": ["probe timeout"],
            "include_probes": bool(kwargs.get("include_probes")),
        },
    )

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.get("/api/v1/account-bindings?include_probes=true")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["items"] == []
    assert payload["total"] == 0
    assert payload["include_probes"] is True
    assert payload["probe_errors"] == ["probe timeout"]


def test_account_bindings_discover_endpoint_forwards_flags_and_scope(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_account_binding_projection",
        lambda **kwargs: (
            captured.update(kwargs)
            or {
                "bindings": {
                    "items": [
                        {
                            "binding_id": "binding-1",
                            "broker_account_id": "acct-1",
                            "status": "proposed",
                            "approval_required": True,
                            "approval_state": "pending_manual_confirmation",
                            "reasons": ["存在可建议的本地候选路径，可作为 draft 绑定目标。"],
                        }
                    ],
                    "total": 1,
                },
                "probe_errors": ["probe timeout"],
                "include_probes": True,
            }
        ),
    )

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/api/v1/account-bindings/discover",
            json={"include_probes": True, "force": True},
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert captured == {
        "owner_user_id": "normal",
        "include_probes": True,
        "force": True,
    }
    assert payload["operation"] == "discover"
    assert payload["force"] is True
    assert payload["include_probes"] is True
    assert payload["probe_errors"] == ["probe timeout"]
    assert payload["items"][0]["binding_id"] == "binding-1"


def test_account_bindings_apply_endpoint_uses_formal_binding_operation(monkeypatch) -> None:
    updates: list[tuple[str, str]] = []
    projection_calls: list[dict[str, object]] = []

    class DummyAccount:
        def __init__(self) -> None:
            self.id = "acct-1"
            self.owner_user_id = "normal"
            self.qmt_userdata_path = ""

        def to_safe_dict(self) -> dict[str, object]:
            return {
                "id": self.id,
                "owner_user_id": self.owner_user_id,
                "qmt_userdata_path": self.qmt_userdata_path,
            }

    account = DummyAccount()
    broker_mgr = SimpleNamespace(
        list_accounts=lambda owner_user_id=None: [{"label": "stub-account"}],
        add_account=lambda **kwargs: None,
        get_account=lambda account_id: account if account_id == "acct-1" else None,
        update_account=lambda account_id, **kwargs: (
            updates.append((account_id, str(kwargs.get("qmt_userdata_path") or ""))),
            setattr(account, "qmt_userdata_path", str(kwargs.get("qmt_userdata_path") or "")),
        )[-1],
    )

    def _projection(**kwargs: object) -> dict[str, object]:
        projection_calls.append(dict(kwargs))
        applied = bool(account.qmt_userdata_path)
        return {
            "bindings": {
                "items": [
                    {
                        "binding_id": "binding-1",
                        "broker_account_id": "acct-1",
                        "status": "confirmed" if applied else "proposed",
                        "approval_required": not applied,
                        "approval_state": "confirmed" if applied else "pending_manual_confirmation",
                        "apply_path": "D:/QMT/Client/userdata_mini",
                        "reasons": ["formal binding 建议写回 userdata。"],
                    }
                ],
                "total": 1,
            },
            "probe_errors": [],
            "include_probes": bool(kwargs.get("include_probes")),
        }

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(api_server, "_load_qmt_account_binding_projection", _projection)
    monkeypatch.setattr("core.broker_accounts.get_broker_manager", lambda: broker_mgr)

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/api/v1/account-bindings/binding-1/apply",
            json={"include_probes": True, "force": True},
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert updates == [("acct-1", "D:/QMT/Client/userdata_mini")]
    assert projection_calls == [
        {"owner_user_id": "normal", "include_probes": True, "force": True},
        {"owner_user_id": "normal", "include_probes": True, "force": False},
    ]
    assert payload["operation"] == "apply"
    assert payload["updated"] is True
    assert payload["applied_path"] == "D:/QMT/Client/userdata_mini"
    assert payload["binding"]["status"] == "confirmed"
    assert payload["account"]["qmt_userdata_path"] == "D:/QMT/Client/userdata_mini"


def test_account_bindings_apply_endpoint_rejects_review_required_binding(monkeypatch) -> None:
    update_called = {"value": False}

    broker_mgr = SimpleNamespace(
        list_accounts=lambda owner_user_id=None: [{"label": "stub-account"}],
        add_account=lambda **kwargs: None,
        get_account=lambda _account_id: SimpleNamespace(owner_user_id="normal"),
        update_account=lambda *_args, **_kwargs: update_called.__setitem__("value", True),
    )

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_account_binding_projection",
        lambda **kwargs: {
            "bindings": {
                "items": [
                    {
                        "binding_id": "binding-1",
                        "broker_account_id": "acct-1",
                        "status": "conflicted",
                        "approval_required": True,
                        "approval_state": "review_required",
                        "apply_path": "D:/QMT/Client/userdata_mini",
                        "reasons": ["关联 blocking conflict，需人工复核。"],
                    }
                ],
                "total": 1,
            },
            "probe_errors": [],
            "include_probes": bool(kwargs.get("include_probes")),
        },
    )
    monkeypatch.setattr("core.broker_accounts.get_broker_manager", lambda: broker_mgr)

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.post(
            "/api/v1/account-bindings/binding-1/apply",
            json={"include_probes": True, "force": True},
        )

    assert resp.status_code == 409
    assert "review_required/conflicted" in resp.json()["detail"]
    assert update_called["value"] is False


def test_qmt_runtime_endpoints_scope_session_projection_to_current_user(monkeypatch) -> None:
    calls: list[str | None] = []

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_session_projection",
        lambda *, owner_user_id=None: (
            calls.append(owner_user_id)
            or {"projection": {"sessions": []}, "cache_size": 0}
        ),
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_registry_projection",
        lambda **_: {"registry_projection": {"layouts": [], "assets": []}},
    )
    monkeypatch.setattr(qmt_registry, "build_qmt_conflict_projection", lambda **_: {"items": []})
    monkeypatch.setattr(qmt_registry, "build_qmt_route_decision_projection", lambda **_: {"items": []})

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        assert client.get("/api/v1/qmt/sessions").status_code == 200
        assert client.get("/api/v1/qmt/conflicts").status_code == 200
        assert client.get("/api/v1/qmt/route-decisions").status_code == 200

    assert calls == ["normal", "normal", "normal"]


def test_qmt_runtime_endpoints_leave_admin_unscoped(monkeypatch) -> None:
    calls: list[str | None] = []

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="admin", role="admin"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_session_projection",
        lambda *, owner_user_id=None: (
            calls.append(owner_user_id)
            or {"projection": {"sessions": []}, "cache_size": 0}
        ),
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_registry_projection",
        lambda **_: {"registry_projection": {"layouts": [], "assets": []}},
    )
    monkeypatch.setattr(qmt_registry, "build_qmt_conflict_projection", lambda **_: {"items": []})
    monkeypatch.setattr(qmt_registry, "build_qmt_route_decision_projection", lambda **_: {"items": []})

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        assert client.get("/api/v1/qmt/sessions").status_code == 200
        assert client.get("/api/v1/qmt/conflicts").status_code == 200
        assert client.get("/api/v1/qmt/route-decisions").status_code == 200

    assert calls == [None, None, None]


def test_qmt_probes_endpoint_scopes_probe_loader_to_current_user(monkeypatch) -> None:
    calls: list[tuple[str | None, str | None]] = []

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_probe_projection",
        lambda userdata_path=None, *, owner_user_id=None: (
            calls.append((userdata_path, owner_user_id))
            or {"raw": {"errors": []}, "projection": {"probes": []}}
        ),
    )

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.get("/api/v1/qmt/probes?userdata_path=D:/QMT/Client/userdata_mini")

    assert resp.status_code == 200
    assert calls == [("D:/QMT/Client/userdata_mini", "normal")]


def test_qmt_conflicts_endpoint_scopes_probe_loader_when_enabled(monkeypatch) -> None:
    probe_calls: list[str | None] = []

    monkeypatch.setattr(api_server, "_require_user", lambda _request: SimpleNamespace(user_id="normal", role="user"))
    monkeypatch.setattr(
        api_server,
        "_load_qmt_session_projection",
        lambda *, owner_user_id=None: {"projection": {"sessions": []}, "cache_size": 0},
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_registry_projection",
        lambda **_: {"registry_projection": {"layouts": [], "assets": []}},
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_probe_projection",
        lambda userdata_path=None, *, owner_user_id=None: (
            probe_calls.append(owner_user_id)
            or {"raw": {"errors": []}, "projection": {"probes": []}}
        ),
    )
    monkeypatch.setattr(qmt_registry, "build_qmt_conflict_projection", lambda **_: {"items": []})

    with TestClient(api_server.app, raise_server_exceptions=False) as client:
        resp = client.get("/api/v1/qmt/conflicts?include_probes=true")

    assert resp.status_code == 200
    assert probe_calls == ["normal"]


def test_account_binding_loader_scopes_probe_projection_to_current_user(monkeypatch) -> None:
    probe_calls: list[str | None] = []

    monkeypatch.setattr(
        api_server,
        "_load_qmt_registry_projection",
        lambda **_: {"registry_projection": {"layouts": [], "assets": []}},
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_session_projection",
        lambda *, owner_user_id=None: {"projection": {"sessions": []}, "cache_size": 0},
    )
    monkeypatch.setattr(
        api_server,
        "_load_qmt_probe_projection",
        lambda userdata_path=None, *, owner_user_id=None: (
            probe_calls.append(owner_user_id)
            or {"raw": {"errors": []}, "projection": {"probes": []}}
        ),
    )
    monkeypatch.setattr(
        "core.broker_accounts.get_broker_manager",
        lambda: SimpleNamespace(list_account_objects=lambda owner_user_id=None: []),
    )
    monkeypatch.setattr(qmt_registry, "build_qmt_conflict_projection", lambda **_: {"items": []})
    monkeypatch.setattr(qmt_registry, "build_qmt_route_decision_projection", lambda **_: {"items": []})
    monkeypatch.setattr(qmt_registry, "build_qmt_account_binding_projection", lambda **_: {"items": [], "total": 0})

    payload = api_server._load_qmt_account_binding_projection(owner_user_id="normal", include_probes=True)

    assert payload["bindings"] == {"items": [], "total": 0}
    assert probe_calls == ["normal"]


def test_probe_projection_rejects_userdata_outside_current_user_scope(monkeypatch) -> None:
    discover_calls: list[dict[str, object] | None] = []

    monkeypatch.setattr(
        "core.broker_accounts.get_broker_manager",
        lambda: SimpleNamespace(
            list_account_objects=lambda owner_user_id=None: [
                SimpleNamespace(qmt_userdata_path="D:/QMT/Client/userdata_mini")
            ]
        ),
    )
    monkeypatch.setattr(
        api_server,
        "discover_trading_accounts",
        lambda body=None: (
            discover_calls.append(body)
            or {"discovered": [{"userdata_path": "D:/QMT/Client/userdata_mini"}], "errors": []}
        ),
    )
    monkeypatch.setattr(
        qmt_registry,
        "project_trading_account_probe_payload",
        lambda payload: {"probes": list(payload.get("discovered") or [])},
    )

    payload = api_server._load_qmt_probe_projection(
        "D:/Other/userdata_mini",
        owner_user_id="normal",
    )

    assert discover_calls == []
    assert payload["projection"]["probes"] == []
    assert len(payload["raw"]["errors"]) == 1
    error_message = str(payload["raw"]["errors"][0])
    assert "当前用户不可访问指定 userdata_path" in error_message
    assert error_message.lower().endswith("d:\\other\\userdata_mini")

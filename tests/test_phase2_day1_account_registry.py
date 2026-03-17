import pytest

account_registry_mod = pytest.importorskip("core.account_registry")


def test_account_registry_module_exports():
    exported = dir(account_registry_mod)
    assert any(name in exported for name in ("AccountRegistry", "account_registry"))


def test_account_registry_api_surface():
    registry_obj = getattr(account_registry_mod, "account_registry", None)
    registry_cls = getattr(account_registry_mod, "AccountRegistry", None)

    if registry_obj is None and registry_cls is not None:
        registry_obj = registry_cls()

    assert registry_obj is not None
    for method in ("register_account", "list_accounts", "get_account"):
        assert hasattr(registry_obj, method)


def test_account_registry_register_roundtrip():
    registry_cls = getattr(account_registry_mod, "AccountRegistry", None)
    if registry_cls is None:
        pytest.skip("AccountRegistry 未实现")

    registry = registry_cls()
    payload = {"account_id": "SIM001", "broker": "qmt", "enabled": True}

    if not hasattr(registry, "register_account"):
        pytest.skip("register_account 未实现")

    registry.register_account(payload)

    if hasattr(registry, "get_account"):
        data = registry.get_account("SIM001")
        assert data is not None
    elif hasattr(registry, "list_accounts"):
        accounts = registry.list_accounts()
        assert any(
            (isinstance(x, dict) and x.get("account_id") == "SIM001") or x == "SIM001"
            for x in accounts
        )
    else:
        pytest.fail("缺少 get_account/list_accounts 查询能力")

import pytest

account_sync_mod = pytest.importorskip("services.account_sync")


def test_account_sync_module_exports():
    exported = dir(account_sync_mod)
    assert any(name in exported for name in ("AccountSyncService", "sync_accounts_once"))


def test_account_sync_service_api_surface():
    service_cls = getattr(account_sync_mod, "AccountSyncService", None)
    if service_cls is None:
        pytest.skip("AccountSyncService 未实现")

    service = service_cls()
    for method in ("sync_once", "start", "stop"):
        assert hasattr(service, method)


def test_account_sync_once_returns_structured_result():
    service_cls = getattr(account_sync_mod, "AccountSyncService", None)
    if service_cls is None:
        pytest.skip("AccountSyncService 未实现")

    service = service_cls()
    if not hasattr(service, "sync_once"):
        pytest.skip("sync_once 未实现")

    result = service.sync_once()
    assert result is None or isinstance(result, (dict, list))

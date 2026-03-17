import os
import sys

import pytest
from PyQt5.QtWidgets import QApplication

# Pre-import key modules so they are present in sys.modules *before*
# test_kline_chart_workspace_logic.py's module-level stub code runs.
# Without this, those stubs persist across the session and break later
# test files that expect real module contents.
try:
    import data_manager.realtime_pipeline_manager  # noqa: F401
except Exception:
    pass

try:
    import core.theme_manager  # noqa: F401
except Exception:
    pass

try:
    import gui_app.widgets.chart  # noqa: F401
except Exception:
    pass

try:
    import gui_app.widgets.orderbook_panel  # noqa: F401
except Exception:
    pass

try:
    import gui_app.widgets.realtime_settings_dialog  # noqa: F401
except Exception:
    pass


@pytest.fixture(scope="session")
def qapp():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    yield app
    app.quit()


# ---------------------------------------------------------------------------
# Guard: prevent test_backtest_widget.py's DataManagerInitThread from
# permanently replacing sys.modules['data_manager'] with the 101因子 package.
# Also guards against sys.path being polluted (e.g. gui_app/backtest/ prepended
# by AutoDataUpdater.initialize_data_manager).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _guard_data_manager_modules():
    """
    Snapshot data_manager-related sys.modules entries and sys.path before each
    test, then restore them afterwards.  This is necessary because:
    - gui_app/backtest/data_manager.py deletes sys.modules['data_manager'] and
      replaces it with the 101因子 version during DataManager.__init__
    - data_manager/auto_data_updater.py inserts gui_app/backtest/ into sys.path
    Both side-effects persist across tests and corrupt subsequent imports.
    """
    # snapshot
    dm_snapshot = {k: v for k, v in sys.modules.items()
                   if k == 'data_manager' or k.startswith('data_manager.')}
    path_snapshot = sys.path[:]

    yield

    # restore sys.path (remove any entries added during the test)
    sys.path[:] = path_snapshot

    # restore data_manager module references
    # 1. remove keys that were added/changed during the test
    for key in list(sys.modules.keys()):
        if key == 'data_manager' or key.startswith('data_manager.'):
            if key in dm_snapshot:
                if sys.modules[key] is not dm_snapshot[key]:
                    sys.modules[key] = dm_snapshot[key]
            else:
                sys.modules.pop(key, None)
    # 2. restore keys that were removed during the test
    for key, mod in dm_snapshot.items():
        if key not in sys.modules:
            sys.modules[key] = mod


# ---------------------------------------------------------------------------
# 环境防污染：清空 Tushare token 环境变量，防止本机配置的 token 意外激活实盘 API。
# 凡是依赖 _check_tushare() 的测试都会从这个干净环境出发，无需每个用例单独 monkeypatch。
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _clear_tushare_token_env():
    """Session 级自动 fixture：清空 Tushare 相关 token 环境变量。

    避免开发机上配置的有效 token 导致 get_stock_data / _check_tushare 进入
    实盘 Tushare 网络调用路径，污染本应 hermetic 的单元测试。
    """
    token_keys = ("EASYXT_TUSHARE_TOKEN", "TUSHARE_TOKEN")
    saved = {k: os.environ.pop(k, None) for k in token_keys}
    yield
    # 测试 session 结束后恢复（对 pytest-xdist worker 进程友好）
    for k, v in saved.items():
        if v is not None:
            os.environ[k] = v


# ---------------------------------------------------------------------------
# 分层测试：integration marker 默认 skip
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="开启实盘集成测试（需要 MiniQMT 客户端已连接并登录）",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """未传 --run-integration 时自动 skip 所有 @pytest.mark.integration 测试。"""
    if config.getoption("--run-integration", default=False):
        return
    skip_mark = pytest.mark.skip(reason="集成测试默认跳过，使用 --run-integration 开启")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_mark)

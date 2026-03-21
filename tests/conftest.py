import os
import sys

import pytest
from PyQt5.QtWidgets import QApplication

# ── DuckDB checkpoint daemon 线程防崩溃 ───────────────────────────────────────
# 必须在任何 DuckDBConnectionManager 被实例化之前设置，
# 否则 __init__ 中已经读取了旧值并启动了后台线程。
# pytest_configure 是 conftest.py 中最早执行的钩子。
def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    # 强制覆盖（不用 setdefault），防止开发机环境变量设为 "1" 导致测试中启动 checkpoint 线程
    os.environ["EASYXT_ENABLE_AUTO_CHECKPOINT"] = "0"
    # 强制 read_only=True 真正生效：DataCoverageWidget 用只读连接查询覆盖率，
    # 不需要执行 DDL（_ensure_tables_exist 对只读连接直接返回）。
    # 避免 10+ 并发 DuckDB 写连接同时调用 CREATE TABLE IF NOT EXISTS 触发堆损坏。
    # （写模式的测试明确传 read_only=False，不受此影响）
    os.environ["EASYXT_DUCKDB_PREFER_RW"] = "0"
    # 强制禁用 QMT 在线模式：各测试需要 QMT=1 时，使用 monkeypatch.setenv 单独覆盖
    # 并在测试结束后自动恢复。全局=0 防止 _ChartDataLoadThread 调用 xtquant C++ 层，
    # 避免 close() 强杀线程破坏 DLL 状态导致后续测试 access violation。
    os.environ["EASYXT_ENABLE_QMT_ONLINE"] = "0"
    # 禁用 WebSocket 行情 worker：测试环境没有 WS 服务，_WsMarketQuoteWorker 会在后台
    # 持续重连（open_timeout=5s），导致测试会话结束时线程仍在运行，引发 0xC0000409 崩溃。
    os.environ["EASYXT_USE_WS_QUOTE"] = "0"


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

# Pre-import xtquant so that test_api.py's module-level _stub_module()
# (which checks "if key not in sys.modules") does NOT replace the real
# package with a plain types.ModuleType stub (which has no __path__ and
# breaks "import xtquant.xtconstant" in later tests).
try:
    import xtquant  # noqa: F401
    import xtquant.xtdata  # noqa: F401
    import xtquant.xtconstant  # noqa: F401
    import xtquant.xttype  # noqa: F401
    import xtquant.xttrader  # noqa: F401
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
# DuckDB checkpoint 线程回收（双保险）
# 即使某个测试通过 monkeypatch/env 绕过了 EASYXT_ENABLE_AUTO_CHECKPOINT=0 而
# 启动了后台线程，session 结束时也会被显式 join，避免 Windows ACCESS VIOLATION。
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def _cleanup_duckdb_checkpoint_threads():
    yield
    # ── 步骤 1：停止所有 DuckDB checkpoint 后台线程 ──────────────────────────
    try:
        from data_manager.duckdb_connection_pool import DuckDBConnectionManager
        for instance in list(DuckDBConnectionManager._instances.values()):
            try:
                instance.stop_checkpoint_worker(timeout=2.0)
            except Exception:
                pass
    except Exception:
        pass
    # ── 步骤 2：等待所有存活的非 daemon/非主线程结束（含 QThread 工作线程）───
    # DataGovernancePanel → DataCoverageWidget → _CoverageLoadThread 等 QThread
    # 在测试中被创建但 panel.close() 不 join 线程；若线程仍持有 DuckDB 连接，
    # Python 解释器关闭前必须先停止它们，否则 C 扩展卸载时触发 ACCESS VIOLATION。
    import threading
    deadline_per_thread = 3.0  # 每条线程最多等待 3 秒
    for t in list(threading.enumerate()):
        if t is threading.main_thread():
            continue
        if t.daemon:
            continue
        try:
            t.join(timeout=deadline_per_thread)
        except Exception:
            pass
    # ── 步骤 3：停止所有仍在运行的 Qt QThread ────────────────────────────────
    # monkeypatch 的 wait 覆盖可能导致 stuck 线程未被真正等待，此处兜底清理
    # 避免 Python 解释器退出时 GC QThread 对象触发 "Destroyed while thread is running"
    try:
        from PyQt5.QtCore import QThread
        import gc
        gc.collect()
        for obj in gc.get_objects():
            try:
                if not isinstance(obj, QThread):
                    continue
                if not obj.isRunning():
                    continue
                print(f"\n[conftest cleanup] Found running QThread: {type(obj).__name__}", flush=True)
                # _WsMarketQuoteWorker 使用 threading.Event 停止，不响应 quit()
                if hasattr(obj, "stop") and callable(obj.stop):
                    obj.stop()
                else:
                    obj.quit()
                if not obj.wait(1500):
                    obj.terminate()
                    obj.wait(500)
            except Exception:
                pass
    except Exception:
        pass


# ---------------------------------------------------------------------------
# DataCoverageWidget / DataGovernancePanel 订阅僵尸清理
# ──────────────────────────────────────────────────────────────────────────
# test_data_governance_panel_smoke.py 每个测试创建完整 DataGovernancePanel，
# 每个 Panel 产生 2 条 DATA_INGESTION_COMPLETE 订阅（Panel 自身 + DataCoverageWidget）。
# panel.close() 应触发 closeEvent 并移除订阅；但 Qt offscreen 模式下极端情况
# close() 可能不及时执行。此 function-scope fixture 在每个测试结束后兜底清理：
# 扫描 DATA_INGESTION_COMPLETE 订阅，将 _closed=True 的僵尸处理函数移除。
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _cleanup_coverage_zombie_subscribers():
    yield
    try:
        from core.signal_bus import signal_bus
        from core.events import Events
        event_key = Events.DATA_INGESTION_COMPLETE
        handlers = list(signal_bus._subscribers.get(event_key, []))
        for h in handlers:
            obj = getattr(h, '__self__', None)
            if obj is None:
                continue
            # 无论是否已关闭，直接移除订阅 + 标记 _closed=True
            # 不调用 obj.close()（会触发 QThread.wait() 导致挂起）
            try:
                obj._closed = True
            except Exception:
                pass
            try:
                signal_bus.unsubscribe(event_key, h)
            except Exception:
                pass
    except Exception:
        pass


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

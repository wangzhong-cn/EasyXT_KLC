"""
回归测试：gui_app/backtest/data_manager.py 初始化前后
sys.modules["data_manager"] 及其子模块引用必须保持不变。

背景：
  旧代码在 DataManager.__init__ 中执行 del sys.modules["data_manager"]，
  会永久破坏后续测试中任何依赖真实 data_manager 包的导入。
  2026-03 修复为 save/restore try/finally 模式，本测试作为回归门禁。
"""

from __future__ import annotations

import sys
import importlib
import pytest

import pytest


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _snapshot_dm_modules() -> dict:
    return {k: v for k, v in sys.modules.items()
            if k == "data_manager" or k.startswith("data_manager.")}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDataManagerModuleIntegrity:
    """DataManager 初始化不得污染 sys.modules["data_manager"] 引用。"""

    def test_sys_modules_unchanged_after_init(self):
        """DataManager(use_local_cache=True, defer_checks=True) 不改变 data_manager 模块引用。"""
        # 确保 data_manager 根包已加载（模拟正常运行状态）
        import data_manager  # noqa: F401

        snapshot_before = _snapshot_dm_modules()
        # data_manager 根包必须在 sys.modules 中
        assert "data_manager" in snapshot_before, \
            "前置条件：data_manager 根包应已加载"

        dm_ref_before = snapshot_before["data_manager"]

        # 实例化 DataManager（触发 sys.modules 操作）
        try:
            from gui_app.backtest.data_manager import DataManager
            _dm_instance = DataManager(use_local_cache=True, defer_checks=True)
        except Exception:
            pass  # 允许 DataManager 初始化失败（如路径不存在），关注 sys.modules 副作用

        snapshot_after = _snapshot_dm_modules()

        # 核心断言：data_manager 模块对象不能被替换
        assert "data_manager" in snapshot_after, \
            "DataManager 初始化后 sys.modules['data_manager'] 不应消失"

        assert snapshot_after["data_manager"] is dm_ref_before, (
            "DataManager 初始化污染了 sys.modules['data_manager']！\n"
            f"  before: {dm_ref_before!r}\n"
            f"  after : {snapshot_after['data_manager']!r}"
        )

    def test_sys_path_unchanged_after_init(self):
        """DataManager 初始化不应向 sys.path 插入并遗留 101因子 路径。"""
        factor_keyword = "101因子"
        path_before = [p for p in sys.path if factor_keyword in p]

        try:
            from gui_app.backtest.data_manager import DataManager
            DataManager(use_local_cache=True, defer_checks=True)
        except Exception:
            pass

        path_after = [p for p in sys.path if factor_keyword in p]

        # 初始化前后，101因子 路径数量应相同
        assert len(path_after) == len(path_before), (
            "DataManager 初始化在 sys.path 中遗留了 101因子 条目！\n"
            f"  before entries: {path_before}\n"
            f"  after  entries: {path_after}"
        )

    def test_submodules_not_replaced(self):
        """data_manager 子模块引用（如 duckdb_connection_pool）不应被替换。"""
        # 确保目标子模块已加载
        try:
            import data_manager.duckdb_connection_pool  # noqa: F401
        except Exception:
            pytest.skip("duckdb_connection_pool 不可用，跳过")

        sub_ref_before = sys.modules.get("data_manager.duckdb_connection_pool")

        try:
            from gui_app.backtest.data_manager import DataManager
            DataManager(use_local_cache=True, defer_checks=True)
        except Exception:
            pass

        sub_ref_after = sys.modules.get("data_manager.duckdb_connection_pool")

        assert sub_ref_after is sub_ref_before, (
            "DataManager 初始化替换了 data_manager.duckdb_connection_pool 引用！\n"
            f"  before: {sub_ref_before!r}\n"
            f"  after : {sub_ref_after!r}"
        )

    def test_multiple_inits_idempotent(self):
        """多次实例化 DataManager 后 sys.modules 保持一致性。"""
        import data_manager  # noqa: F401
        dm_ref_before = sys.modules["data_manager"]

        try:
            from gui_app.backtest.data_manager import DataManager
            for _ in range(3):
                DataManager(use_local_cache=True, defer_checks=True)
        except Exception:
            pass

        assert sys.modules.get("data_manager") is dm_ref_before, \
            "多次初始化后 sys.modules['data_manager'] 发生了改变"


# ──────────────────────────────────────────────────────────────────────────────
# TestValidateEnvironment — data_manager.validate_environment() P0 补强
# ──────────────────────────────────────────────────────────────────────────────


class TestValidateEnvironment:
    """validate_environment() 启动时环境完整性校验（P0 补强）。"""

    def test_returns_dict(self):
        """无论环境如何，始终返回 dict。"""
        from data_manager import validate_environment
        result = validate_environment()
        assert isinstance(result, dict)

    def test_all_values_are_strings(self):
        """所有返回值均为字符串（OK / WARN: xxx / ERROR: xxx）。"""
        from data_manager import validate_environment
        result = validate_environment()
        assert all(isinstance(v, str) for v in result.values())

    def test_invalid_int_env_gives_error(self, monkeypatch):
        """非法整数环境变量返回 ERROR 前缀。"""
        from data_manager import validate_environment
        monkeypatch.setenv("EASYXT_REMOTE_CB_THRESHOLD", "not_an_int")
        result = validate_environment()
        assert result["EASYXT_REMOTE_CB_THRESHOLD"].startswith("ERROR")

    def test_valid_int_env_gives_ok(self, monkeypatch):
        """合法整数环境变量返回 OK。"""
        from data_manager import validate_environment
        monkeypatch.setenv("EASYXT_REMOTE_CB_THRESHOLD", "5")
        result = validate_environment()
        assert result["EASYXT_REMOTE_CB_THRESHOLD"] == "OK"

    def test_invalid_float_env_gives_error(self, monkeypatch):
        """非法浮点数环境变量返回 ERROR 前缀。"""
        from data_manager import validate_environment
        monkeypatch.setenv("EASYXT_REMOTE_BACKOFF_BASE_S", "abc")
        result = validate_environment()
        assert result["EASYXT_REMOTE_BACKOFF_BASE_S"].startswith("ERROR")

    def test_nonexistent_qmt_exe_gives_warn(self, monkeypatch):
        """不存在的 QMT_EXE 路径返回 WARN 前缀。"""
        from data_manager import validate_environment
        monkeypatch.setenv("QMT_EXE", "C:/nonexistent_dir/XtItClient.exe")
        result = validate_environment()
        assert result["QMT_EXE"].startswith("WARN")

    def test_raise_on_error_raises(self, monkeypatch):
        """raise_on_error=True 时，ERROR 项触发 RuntimeError。"""
        from data_manager import validate_environment
        monkeypatch.setenv("EASYXT_REMOTE_CB_THRESHOLD", "bad_value")
        with pytest.raises(RuntimeError):
            validate_environment(raise_on_error=True)

    def test_exported_in_all(self):
        """validate_environment 必须在 data_manager.__all__ 中。"""
        import data_manager
        assert "validate_environment" in data_manager.__all__

    def test_tushare_token_ok_when_set(self, monkeypatch):
        """设置 EASYXT_TUSHARE_TOKEN 时，输出应为 OK。"""
        from data_manager import validate_environment
        monkeypatch.setenv("EASYXT_TUSHARE_TOKEN", "test_token_123")
        result = validate_environment()
        assert result["EASYXT_TUSHARE_TOKEN"] == "OK"

    def test_tushare_token_warn_when_missing(self, monkeypatch):
        """两个 token 环境变量均未设置时，输出应为 WARN。"""
        from data_manager import validate_environment
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
        result = validate_environment()
        assert result["EASYXT_TUSHARE_TOKEN"].startswith("WARN")

    def test_tushare_token_fallback_to_TUSHARE_TOKEN(self, monkeypatch):
        """EASYXT_TUSHARE_TOKEN 未设置但 TUSHARE_TOKEN 设置时，应识别为 OK。"""
        from data_manager import validate_environment
        monkeypatch.delenv("EASYXT_TUSHARE_TOKEN", raising=False)
        monkeypatch.setenv("TUSHARE_TOKEN", "fallback_token")
        result = validate_environment()
        assert result["EASYXT_TUSHARE_TOKEN"] == "OK"

    def test_akshare_ok_when_installed(self, monkeypatch):
        """akshare 可导入时， AKSHARE 应为 OK。"""
        import sys
        import types
        from data_manager import validate_environment
        # 在 sys.modules 中动态注册一个假 akshare，避免依赖爬取
        fake_mod = types.ModuleType("akshare")
        monkeypatch.setitem(sys.modules, "akshare", fake_mod)
        result = validate_environment()
        assert result["AKSHARE"] == "OK"

    def test_akshare_warn_when_not_installed(self, monkeypatch):
        """akshare 未安装时，AKSHARE 应为 WARN。"""
        import sys
        from data_manager import validate_environment
        monkeypatch.delitem(sys.modules, "akshare", raising=False)
        # 覆盖 importlib.import_module 让它抛 ImportError
        import importlib as _il
        original = _il.import_module

        def _mock_import(name, *args, **kwargs):
            if name == "akshare":
                raise ImportError("No module named 'akshare'")
            return original(name, *args, **kwargs)

        monkeypatch.setattr(_il, "import_module", _mock_import)
        result = validate_environment()
        assert result["AKSHARE"].startswith("WARN")

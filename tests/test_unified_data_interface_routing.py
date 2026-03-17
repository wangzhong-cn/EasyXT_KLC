"""
AKShare 数据路由单测 (护栏)
=================================
确保 _is_index_code 和 _read_from_akshare 的路由逻辑在代码变更后仍然正确。

覆盖三类用例:
  A. 指数代码判别 (_is_index_code)
  B. 股票 AKShare 路由 (mock: stock_zh_a_hist 应被调用)
  C. 指数 AKShare 路由 (mock: index_zh_a_hist 应被调用)
  D. 指数分钟线路由 (应返回 None，不调用任何 AKShare 接口)
  E. AKShare 重试逻辑 (第一次失败 -> sleep -> 第二次成功)
  F. 外置配置 explicit_index_codes 优先级
"""
import pathlib
import sys
import time
import types

import pandas as pd
import pytest

# ── 让 pytest 能找到 data_manager ──────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ── 重置类级缓存，避免测试间互相干扰 ─────────────────────────────────────────
@pytest.fixture(autouse=True)
def _reset_routing_cache():
    from data_manager.unified_data_interface import UnifiedDataInterface as UDI
    old_cfg  = UDI._AKSHARE_ROUTING_CFG
    old_flag = UDI._AKSHARE_ROUTING_LOADED
    yield
    UDI._AKSHARE_ROUTING_CFG    = old_cfg
    UDI._AKSHARE_ROUTING_LOADED = old_flag


# ═══════════════════════════════════════════════════════════════════════════
# A. _is_index_code 指数代码判别
# ═══════════════════════════════════════════════════════════════════════════
class TestIsIndexCode:
    """验证 _is_index_code 对各类标的的判断正确性。"""

    @pytest.fixture(autouse=True)
    def _preload(self):
        """强制使用内置 fallback 配置，保证测试不依赖磁盘文件。"""
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI
        UDI._AKSHARE_ROUTING_CFG = {
            "index_rules": {
                "suffix_sh_prefixes": ["000", "399", "999", "688"],
                "suffix_sz_prefixes": ["399"],
                "explicit_index_codes": ["899050.BJ"],
            }
        }
        UDI._AKSHARE_ROUTING_LOADED = True

    @pytest.mark.parametrize("code,expected", [
        # 沪市宽基指数
        ("000300.SH", True),   # 沪深300
        ("000016.SH", True),   # 上证50
        ("000905.SH", True),   # 中证500
        ("000688.SH", True),   # 科创50
        ("999999.SH", True),   # 上证综指
        # 深市指数 (399前缀)
        ("399001.SZ", True),   # 深证成指
        ("399006.SZ", True),   # 创业板指
        # 个股（不应为指数）
        ("000001.SZ", False),  # 平安银行 (深沪同前缀000但.SZ)
        ("600000.SH", False),  # 浦发银行
        ("300001.SZ", False),  # 创业板个股
        ("601318.SH", False),  # 中国平安
        # 显式指数代码列表
        ("899050.BJ", True),   # 北交所50
    ])
    def test_is_index(self, code: str, expected: bool):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI
        assert UDI._is_index_code(code) is expected, (
            f"_is_index_code({code!r}) 应为 {expected}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# 辅助：构造标准 AKShare 响应 DataFrame
# ═══════════════════════════════════════════════════════════════════════════
def _make_akshare_df(rows: int = 5) -> pd.DataFrame:
    """返回 AKShare 格式的中文列名 DataFrame。

    数据来源：000001.SZ（平安银行）2023 年 1 月真实历史行情，
    来自 tests/fixtures/real_market_data.py。铁律 0：严禁使用任何硬编码价格。
    """
    from tests.fixtures.real_market_data import RECORDS_000001_SZ_2023Q1
    cap = min(rows, len(RECORDS_000001_SZ_2023Q1))
    records = RECORDS_000001_SZ_2023Q1[:cap]
    return pd.DataFrame({
        "日期":   [r[0] for r in records],
        "开盘":   [r[1] for r in records],
        "收盘":   [r[4] for r in records],
        "最高":   [r[2] for r in records],
        "最低":   [r[3] for r in records],
        "成交量": [r[5] for r in records],
        "成交额": [round(r[4] * r[5], 2) for r in records],
    })


# ═══════════════════════════════════════════════════════════════════════════
# B. 股票路由：应调用 stock_zh_a_hist
# ═══════════════════════════════════════════════════════════════════════════
class TestStockRouting:
    """股票代码应路由到 ak.stock_zh_a_hist。"""

    def test_stock_calls_stock_api(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        fake_ak = types.SimpleNamespace()
        calls: list[str] = []

        def mock_stock_zh_a_hist(**kwargs):
            calls.append("stock_zh_a_hist")
            return _make_akshare_df()

        fake_ak.stock_zh_a_hist = mock_stock_zh_a_hist
        monkeypatch.setitem(sys.modules, "akshare", fake_ak)

        udi = UDI.__new__(UDI)
        # 最小化实例——只需 _log 和路由相关方法可用
        udi._log = lambda msg: None  # noqa: E731

        result = udi._read_from_akshare("000001.SZ", "2024-01-01", "2024-03-31", "1d")

        assert "stock_zh_a_hist" in calls, "股票代码应调用 stock_zh_a_hist"
        assert result is not None and not result.empty

    def test_stock_does_not_call_index_api(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        fake_ak = types.SimpleNamespace()
        index_calls: list[str] = []

        fake_ak.stock_zh_a_hist = lambda **kw: _make_akshare_df()
        fake_ak.index_zh_a_hist = lambda **kw: (index_calls.append("index_zh_a_hist"), _make_akshare_df())[1]

        monkeypatch.setitem(sys.modules, "akshare", fake_ak)

        udi = UDI.__new__(UDI)
        udi._log = lambda msg: None
        udi._read_from_akshare("600000.SH", "2024-01-01", "2024-03-31", "1d")

        assert index_calls == [], "股票代码不应调用 index_zh_a_hist"


# ═══════════════════════════════════════════════════════════════════════════
# C. 指数路由：应调用 index_zh_a_hist
# ═══════════════════════════════════════════════════════════════════════════
class TestIndexRouting:
    """指数代码应路由到 ak.index_zh_a_hist。"""

    def test_index_calls_index_api(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        fake_ak = types.SimpleNamespace()
        calls: list[str] = []

        def mock_index_zh_a_hist(**kwargs):
            calls.append("index_zh_a_hist")
            return _make_akshare_df()

        fake_ak.index_zh_a_hist = mock_index_zh_a_hist
        # stock 接口若被意外调用则抛异常
        fake_ak.stock_zh_a_hist = lambda **kw: (_ for _ in ()).throw(
            AssertionError("指数代码不应调用 stock_zh_a_hist")
        )

        monkeypatch.setitem(sys.modules, "akshare", fake_ak)

        udi = UDI.__new__(UDI)
        udi._log = lambda msg: None

        result = udi._read_from_akshare("000300.SH", "2024-01-01", "2024-03-31", "1d")

        assert "index_zh_a_hist" in calls, "指数代码应调用 index_zh_a_hist"
        assert result is not None and not result.empty

    @pytest.mark.parametrize("code", ["000300.SH", "000905.SH", "399001.SZ", "000016.SH"])
    def test_known_indices_call_index_api(self, monkeypatch, code: str):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        fake_ak = types.SimpleNamespace()
        calls: list[str] = []
        fake_ak.index_zh_a_hist = lambda **kw: (calls.append("index"), _make_akshare_df())[1]
        fake_ak.stock_zh_a_hist = lambda **kw: (_ for _ in ()).throw(
            AssertionError(f"指数 {code} 不应调用 stock_zh_a_hist")
        )
        monkeypatch.setitem(sys.modules, "akshare", fake_ak)

        udi = UDI.__new__(UDI)
        udi._log = lambda msg: None
        udi._read_from_akshare(code, "2024-01-01", "2024-03-31", "1d")

        assert "index" in calls


# ═══════════════════════════════════════════════════════════════════════════
# D. 指数分钟线：应返回 None，不调用任何 AKShare 接口
# ═══════════════════════════════════════════════════════════════════════════
class TestIndexMinuteLine:
    """指数分钟线不被支持，_read_from_akshare 应返回 None 且不调用任何接口。"""

    @pytest.mark.parametrize("period", ["1m", "5m"])
    def test_index_minute_returns_none(self, monkeypatch, period: str):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        fake_ak = types.SimpleNamespace()
        calls: list[str] = []

        def _fail(name):
            def _inner(**kw):
                calls.append(name)
                raise AssertionError(f"指数分钟线不应调用 {name}")
            return _inner

        fake_ak.index_zh_a_hist        = _fail("index_zh_a_hist")
        fake_ak.stock_zh_a_hist        = _fail("stock_zh_a_hist")
        fake_ak.stock_zh_a_hist_min_em = _fail("stock_zh_a_hist_min_em")

        monkeypatch.setitem(sys.modules, "akshare", fake_ak)

        logs: list[str] = []
        udi = UDI.__new__(UDI)
        udi._log = logs.append

        result = udi._read_from_akshare("000300.SH", "2024-01-01", "2024-01-31", period)

        assert result is None, "指数分钟线应返回 None"
        assert calls == [], "不应调用任何 AKShare 接口"
        # 应有 WARNING 日志
        assert any("WARNING" in log or "warning" in log.lower() for log in logs), (
            f"应记录 WARNING，当前日志: {logs}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# E. AKShare 重试逻辑：一次失败后重试成功
# ═══════════════════════════════════════════════════════════════════════════
class TestAKShareRetry:
    """首次请求失败时应按配置重试，重试成功则返回数据。"""

    def test_retry_succeeds_on_second_attempt(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        # 强制重试配置：max_retries=1, backoff_seconds=0 (加速测试)
        UDI._AKSHARE_ROUTING_CFG = {
            "index_rules": {
                "suffix_sh_prefixes": ["000", "399", "999", "688"],
                "suffix_sz_prefixes": ["399"],
                "explicit_index_codes": [],
            },
            "akshare_retry": {"max_retries": 1, "backoff_seconds": 0}
        }
        UDI._AKSHARE_ROUTING_LOADED = True

        fake_ak = types.SimpleNamespace()
        attempt_count = [0]

        def mock_stock_zh_a_hist(**kwargs):
            attempt_count[0] += 1
            if attempt_count[0] == 1:
                raise ConnectionError("模拟网络超时")
            return _make_akshare_df()

        fake_ak.stock_zh_a_hist = mock_stock_zh_a_hist
        monkeypatch.setitem(sys.modules, "akshare", fake_ak)
        # 跳过实际 sleep
        monkeypatch.setattr(time, "sleep", lambda s: None)

        logs: list[str] = []
        udi = UDI.__new__(UDI)
        udi._log = logs.append

        result = udi._read_from_akshare("000001.SZ", "2024-01-01", "2024-03-31", "1d")

        assert result is not None and not result.empty, "重试后应成功返回数据"
        assert attempt_count[0] == 2, f"应重试一次，实际调用 {attempt_count[0]} 次"
        assert any("WARN" in log for log in logs), "重试时应有 WARN 日志"

    def test_all_retries_exhausted_returns_none(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        UDI._AKSHARE_ROUTING_CFG = {
            "index_rules": {
                "suffix_sh_prefixes": ["000", "399", "999", "688"],
                "suffix_sz_prefixes": ["399"],
                "explicit_index_codes": [],
            },
            "akshare_retry": {"max_retries": 1, "backoff_seconds": 0}
        }
        UDI._AKSHARE_ROUTING_LOADED = True

        fake_ak = types.SimpleNamespace()
        fake_ak.stock_zh_a_hist = lambda **kw: (_ for _ in ()).throw(ConnectionError("连接失败"))
        monkeypatch.setitem(sys.modules, "akshare", fake_ak)
        monkeypatch.setattr(time, "sleep", lambda s: None)

        logs: list[str] = []
        udi = UDI.__new__(UDI)
        udi._log = logs.append

        result = udi._read_from_akshare("000001.SZ", "2024-01-01", "2024-03-31", "1d")

        assert result is None, "全部重试耗尽后应返回 None"
        assert any("ERROR" in log for log in logs), "耗尽后应有 ERROR 日志"


# ═══════════════════════════════════════════════════════════════════════════
# F. 外置配置 explicit_index_codes 优先级
# ═══════════════════════════════════════════════════════════════════════════
class TestExplicitIndexCodes:
    """explicit_index_codes 可以覆盖前缀规则，将特殊代码强制标记为指数。"""

    def test_explicit_code_recognized_as_index(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        # 北交所代码 899050.BJ 不符合 .SH/.SZ 前缀规则，但在 explicit_index_codes 中
        UDI._AKSHARE_ROUTING_CFG = {
            "index_rules": {
                "suffix_sh_prefixes": ["000", "399", "999", "688"],
                "suffix_sz_prefixes": ["399"],
                "explicit_index_codes": ["899050.BJ"],
            }
        }
        UDI._AKSHARE_ROUTING_LOADED = True

        assert UDI._is_index_code("899050.BJ") is True

    def test_normal_code_not_in_explicit_list(self, monkeypatch):
        from data_manager.unified_data_interface import UnifiedDataInterface as UDI

        UDI._AKSHARE_ROUTING_CFG = {
            "index_rules": {
                "suffix_sh_prefixes": ["000", "399", "999", "688"],
                "suffix_sz_prefixes": ["399"],
                "explicit_index_codes": [],
            }
        }
        UDI._AKSHARE_ROUTING_LOADED = True

        assert UDI._is_index_code("899050.BJ") is False  # 不在列表里就不算指数

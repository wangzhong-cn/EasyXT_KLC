from __future__ import annotations

import sys
import types
from unittest.mock import patch

import pandas as pd

import easy_xt.triple_source_manager as tsm


def _make_manager() -> tsm.TripleSourceDataManager:
    mgr = tsm.TripleSourceDataManager.__new__(tsm.TripleSourceDataManager)
    mgr.priority = ["qmt", "qstock", "akshare"]
    mgr.cache = {}
    mgr.stats = {"qmt_hits": 0, "qstock_hits": 0, "akshare_hits": 0, "failures": 0}
    mgr.sources = {"qmt": False, "qstock": False, "akshare": False}
    mgr.xt = None
    mgr.qs = None
    mgr.ak = None
    mgr.tdx_client = None
    return mgr


def test_get_market_data_prefers_qmt_when_available():
    mgr = _make_manager()
    mgr.sources["qmt"] = True

    class _TdxCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_market_data(self, **kwargs):
            return pd.DataFrame({"close": [10.5], "stock_code": ["000001.SZ"]})

    mgr.tdx_client = _TdxCtx
    result = mgr.get_market_data("000001.SZ", "20240101", "20240131", period="1d")
    assert not result.empty
    assert mgr.stats["qmt_hits"] == 1


def test_get_market_data_fallback_to_qstock():
    mgr = _make_manager()
    mgr.sources["qmt"] = True
    mgr.sources["qstock"] = True

    class _EmptyTdxCtx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def get_market_data(self, **kwargs):
            return pd.DataFrame()

    class _Qs:
        @staticmethod
        def get_data(code, start, end):
            return pd.DataFrame({"close": [11.2]})

    mgr.tdx_client = _EmptyTdxCtx
    mgr.qs = _Qs()
    result = mgr.get_market_data("000001.SZ", "20240101", "20240131", period="1d")
    assert not result.empty
    assert "stock_code" in result.columns
    assert mgr.stats["qstock_hits"] == 1


def test_get_market_data_all_fail_increments_failures():
    mgr = _make_manager()
    mgr.sources["qmt"] = False
    mgr.sources["qstock"] = False
    mgr.sources["akshare"] = False
    result = mgr.get_market_data("000001.SZ", "20240101", "20240131", period="1d")
    assert result.empty
    assert mgr.stats["failures"] == 1


def test_get_market_data_cache_hits_for_same_query():
    mgr = _make_manager()
    mgr.sources["qstock"] = True
    calls = {"n": 0}

    class _Qs:
        @staticmethod
        def get_data(code, start, end):
            calls["n"] += 1
            return pd.DataFrame({"close": [10.0]})

    mgr.qs = _Qs()
    first = mgr.get_market_data("000001.SZ", "20240101", "20240131", period="1d")
    second = mgr.get_market_data("000001.SZ", "20240101", "20240131", period="1d")
    assert not first.empty and not second.empty
    assert calls["n"] == 1


def test_get_sector_list_from_akshare_when_qmt_unavailable():
    mgr = _make_manager()
    mgr.sources["akshare"] = True

    class _Ak:
        @staticmethod
        def stock_board_industry_name_em():
            return pd.DataFrame({"板块名称": ["半导体", "软件"]})

    mgr.ak = _Ak()
    result = mgr.get_sector_list()
    assert result["industry"] == ["半导体", "软件"]
    assert mgr.stats["akshare_hits"] == 1


def test_get_realtime_data_qmt_dict_merge():
    mgr = _make_manager()
    mgr.sources["qmt"] = True

    class _Xt:
        @staticmethod
        def get_market_data_ex(stock_codes, period="tick"):
            return {
                "000001.SZ": pd.DataFrame({"price": [10.1], "code": ["000001"]}),
                "600000.SH": pd.DataFrame({"price": [9.9], "code": ["600000"]}),
            }

    mgr.xt = _Xt()
    result = mgr.get_realtime_data(["000001.SZ", "600000.SH"])
    assert not result.empty
    assert len(result) == 2
    assert mgr.stats["qmt_hits"] == 1


def test_get_realtime_data_fallback_qstock_and_filter():
    mgr = _make_manager()
    mgr.sources["qmt"] = False
    mgr.sources["qstock"] = True

    class _Qs:
        @staticmethod
        def realtime_data():
            return pd.DataFrame(
                {"code": ["000001", "600000", "300001"], "price": [10.1, 9.9, 5.5]}
            )

    mgr.qs = _Qs()
    result = mgr.get_realtime_data(["000001.SZ", "600000.SH"])
    assert set(result["code"].tolist()) == {"000001", "600000"}
    assert mgr.stats["qstock_hits"] == 1


def test_get_stats_and_clear_cache():
    mgr = _make_manager()
    mgr.stats["qmt_hits"] = 2
    mgr.stats["qstock_hits"] = 1
    mgr.stats["akshare_hits"] = 1
    mgr.stats["failures"] = 1
    mgr.cache["x"] = ("y", 0.0)
    stats = mgr.get_stats()
    assert stats["total_requests"] == 5
    assert stats["cache_size"] == 1
    mgr.clear_cache()
    assert mgr.cache == {}


def test_singleton_getter_returns_same_instance():
    old = tsm._triple_source_manager
    try:
        tsm._triple_source_manager = None

        class _Fake(tsm.TripleSourceDataManager):
            def __init__(self, priority="qmt,qstock,akshare"):
                self.priority = priority.split(",")
                self.cache = {}
                self.stats = {"qmt_hits": 0, "qstock_hits": 0, "akshare_hits": 0, "failures": 0}
                self.sources = {"qmt": False, "qstock": False, "akshare": False}
                self.xt = None
                self.qs = None
                self.ak = None
                self.tdx_client = None

        original_cls = tsm.TripleSourceDataManager
        tsm.TripleSourceDataManager = _Fake
        a = tsm.get_triple_source_manager()
        b = tsm.get_triple_source_manager()
        assert a is b
    finally:
        tsm.TripleSourceDataManager = original_cls
        tsm._triple_source_manager = old


def test_get_money_flow_fallback_to_qstock_when_akshare_unavailable():
    mgr = _make_manager()
    mgr.sources["akshare"] = True
    mgr.sources["qstock"] = True

    class _Qs:
        @staticmethod
        def moneyflow_stock(code, w_list):
            return pd.DataFrame({"net_inflow": [1.2]})

    class _MoneyFlowAnalyzer:
        def get_stock_money_flow(self, stock_code, days):
            raise RuntimeError("akshare money flow failed")

    fake_module = types.SimpleNamespace(MoneyFlowAnalyzer=_MoneyFlowAnalyzer)
    mgr.qs = _Qs()
    with patch.dict(sys.modules, {"money_flow": fake_module}):
        result = mgr.get_money_flow("000001.SZ", days=5)
    assert not result.empty
    assert result["stock_code"].iloc[0] == "000001.SZ"
    assert mgr.stats["qstock_hits"] == 1


def test_get_dragon_tiger_fallback_to_akshare_when_qstock_fails():
    mgr = _make_manager()
    mgr.sources["qstock"] = True
    mgr.sources["akshare"] = True

    class _Qs:
        @staticmethod
        def stock_billboard():
            raise RuntimeError("qstock unavailable")

    class _DragonTigerData:
        @staticmethod
        def get_daily_list(date):
            return pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})

    fake_module = types.SimpleNamespace(DragonTigerData=_DragonTigerData)
    mgr.qs = _Qs()
    with patch.dict(sys.modules, {"dragon_tiger": fake_module}):
        result = mgr.get_dragon_tiger("20240101")
    assert not result.empty
    assert mgr.stats["akshare_hits"] == 1


def test_get_stock_indicator_fallback_to_akshare_factor_library():
    mgr = _make_manager()
    mgr.sources["qstock"] = True
    mgr.sources["akshare"] = True

    class _Qs:
        @staticmethod
        def stock_indicator(code):
            raise RuntimeError("qstock indicator failed")

    class _EasyFactor:
        def __init__(self, duckdb_path):
            self.duckdb_path = duckdb_path

        def get_all_factors(self, stock_code, date):
            return pd.DataFrame({"factor_x": [0.3], "stock_code": [stock_code]})

    fake_module = types.SimpleNamespace(EasyFactor=_EasyFactor)
    mgr.qs = _Qs()
    with patch.dict(sys.modules, {"factor_library": fake_module}):
        result = mgr.get_stock_indicator("000001.SZ")
    assert not result.empty
    assert mgr.stats["akshare_hits"] == 1


def test_get_sector_stocks_prefers_qstock_for_index():
    mgr = _make_manager()
    mgr.sources["qstock"] = True

    class _Qs:
        @staticmethod
        def index_member(sector_name):
            return pd.DataFrame({"code": ["000001"], "name": ["平安银行"]})

    mgr.qs = _Qs()
    result = mgr.get_sector_stocks("000300", sector_type="index")
    assert not result.empty
    assert mgr.stats["qstock_hits"] == 1


def test_get_sector_stocks_fallback_to_akshare_concept_with_rename():
    mgr = _make_manager()
    mgr.sources["akshare"] = True

    class _Ak:
        @staticmethod
        def stock_board_concept_cons_em(symbol):
            return pd.DataFrame({"代码": ["000001"], "名称": ["平安银行"]})

    mgr.ak = _Ak()
    result = mgr.get_sector_stocks("AI概念", sector_type="concept")
    assert not result.empty
    assert "stock_code" in result.columns
    assert "stock_name" in result.columns
    assert mgr.stats["akshare_hits"] == 1


def test_print_stats_outputs_summary(capsys):
    """print_stats 打印包含命中率的统计信息（覆盖 572-582）"""
    mgr = _make_manager()
    mgr.stats["qmt_hits"] = 3
    mgr.stats["qstock_hits"] = 1
    mgr.stats["akshare_hits"] = 1
    mgr.stats["failures"] = 0
    mgr.print_stats()
    captured = capsys.readouterr()
    assert "总请求数" in captured.out
    assert "QMT命中" in captured.out
    assert "qstock命中" in captured.out


def test_get_realtime_data_qmt_series_branch():
    """QMT 返回 pd.Series 时正常转 DataFrame 返回（覆盖 522-527）"""
    mgr = _make_manager()
    mgr.sources["qmt"] = True

    series_result = pd.Series({"close": 10.5, "open": 10.0})

    class _Xt:
        @staticmethod
        def get_market_data_ex(codes, period):
            return series_result

    mgr.xt = _Xt()
    result = mgr.get_realtime_data(["000001.SZ"])
    assert not result.empty
    assert mgr.stats["qmt_hits"] == 1


def test_get_realtime_data_qstock_fallback():
    """QMT 不可用时回退 qstock 实时行情，并按 stock_codes 过滤（覆盖 540-543）"""
    mgr = _make_manager()
    mgr.sources["qstock"] = True

    class _Qs:
        @staticmethod
        def realtime_data():
            return pd.DataFrame({
                "code": ["000001", "000002"],
                "close": [10.5, 20.0],
            })

    mgr.qs = _Qs()
    result = mgr.get_realtime_data(["000001.SZ"])
    assert not result.empty
    assert len(result) == 1
    assert result["code"].iloc[0] == "000001"
    assert mgr.stats["qstock_hits"] == 1


def test_get_market_data_akshare_path_returns_data():
    """akshare 路径返回数据时统计 akshare_hits（覆盖 208-209, 222-223）"""
    from unittest.mock import patch

    mgr = _make_manager()
    mgr.sources["akshare"] = True
    with patch.object(
        mgr,
        "_get_market_from_akshare",
        return_value=pd.DataFrame({"close": [10.5], "stock_code": ["000001.SZ"]}),
    ):
        result = mgr.get_market_data("000001.SZ", "20240101", "20240131")
    assert not result.empty
    assert mgr.stats["akshare_hits"] == 1


def test_get_market_from_akshare_returns_empty_dataframe():
    """_get_market_from_akshare 简化实现始终返回空 DataFrame（覆盖 227-233）"""
    mgr = _make_manager()
    result = mgr._get_market_from_akshare("000001.SZ", "20240101", "20240131", "1d")
    assert isinstance(result, pd.DataFrame)
    assert result.empty

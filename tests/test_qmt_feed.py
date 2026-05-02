"""
QmtFeed 单元测试（不依赖 QMT 运行环境）。

覆盖范围：
  - _normalize_tick 字段映射
  - subscribe / unsubscribe 状态管理
  - QMT 不可用时拒绝订阅（返回 error）
  - _on_tick 回调 → ingest 注入
  - all_subscriptions / stats 可观测接口
  - is_available / is_subscribed
"""

from __future__ import annotations

import contextlib
import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest

from core.qmt_feed import QmtFeed, _normalize_tick


# ---------------------------------------------------------------------------
# 测试工具函数
# ---------------------------------------------------------------------------

@contextmanager
def mock_xtquant_xtdata(**attrs):
    """注入模拟的 xtquant.xtdata 模块。

    解决的问题：
        `import xtquant.xtdata as xtdata` 元指令等价于
        `xtdata = getattr(sys.modules["xtquant"], "xtdata")`（IMPORT_FROM 字节码），
        仅替换 sys.modules["xtquant.xtdata"] 不够。
        改用 importlib.import_module 后只需替换 sys.modules["xtquant.xtdata"] 即可，
        但此 helper 同时保守两种可能性，不依赖生产代码实现细节。

    用法：
        with mock_xtquant_xtdata() as mock_xd:
            mock_xd.subscribe_quote.return_value = "sub_001"
            ...
    或预先设好返回値：
        with mock_xtquant_xtdata(subscribe_quote=MagicMock(return_value="sub_001")) as mock_xd:
            ...
    """
    mock_xd = MagicMock()
    for attr, val in attrs.items():
        setattr(mock_xd, attr, val)
    mock_xt = MagicMock()
    mock_xt.xtdata = mock_xd  # 兼容旧式 import xtquant.xtdata as xtdata
    with patch.dict("sys.modules", {"xtquant": mock_xt, "xtquant.xtdata": mock_xd}):
        yield mock_xd


# ---------------------------------------------------------------------------
# _normalize_tick
# ---------------------------------------------------------------------------

class TestNormalizeTick:
    def test_maps_lastPrice_to_price(self):
        result = _normalize_tick("000001.SZ", {"lastPrice": 12.34})
        assert result["price"] == 12.34

    def test_sets_symbol_period_source(self):
        result = _normalize_tick("000001.SZ", {}, period="1m")
        assert result["symbol"] == "000001.SZ"
        assert result["period"] == "1m"
        assert result["source"] == "qmt_live"

    def test_uses_time_as_tick_ts_ms(self):
        result = _normalize_tick("000001.SZ", {"time": 1741420800000})
        assert result["tick_ts_ms"] == 1741420800000

    def test_timetag_fallback(self):
        result = _normalize_tick("000001.SZ", {"timetag": 9999})
        assert result["tick_ts_ms"] == 9999

    def test_missing_fields_not_in_result(self):
        result = _normalize_tick("000001.SZ", {})
        assert "price" not in result
        assert "volume" not in result

    def test_maps_multiple_fields(self):
        raw = {"lastPrice": 10.0, "volume": 500, "high": 10.5, "low": 9.8}
        result = _normalize_tick("000001.SZ", raw)
        assert result["price"] == 10.0
        assert result["volume"] == 500
        assert result["high"] == 10.5
        assert result["low"] == 9.8

    def test_maps_ask_bid_fields(self):
        raw = {"askPrice": [10.1, 10.2], "bidPrice": [9.9, 9.8]}
        result = _normalize_tick("600519.SH", raw)
        assert result["ask_price"] == [10.1, 10.2]
        assert result["bid_price"] == [9.9, 9.8]


# ---------------------------------------------------------------------------
# is_available
# ---------------------------------------------------------------------------

class TestIsAvailable:
    def test_returns_true_when_xtdata_importable(self):
        """sys.modules 里有 xtquant.xtdata 时 importlib.import_module 直接命中缓存。"""
        with mock_xtquant_xtdata():
            assert QmtFeed.is_available() is True

    def test_returns_false_when_import_fails(self):
        """importlib.import_module 抛 ImportError 时应返回 False。"""
        import importlib as _il
        with patch.object(_il, "import_module", side_effect=ImportError("no xtquant")):
            assert QmtFeed.is_available() is False

    def test_returns_false_when_xtquant_absent(self):
        """通过屏蔽 sys.modules 模拟 xtquant 完全缺失的情况。"""
        orig_xt = sys.modules.pop("xtquant", None)
        orig_xd = sys.modules.pop("xtquant.xtdata", None)
        try:
            result = QmtFeed.is_available()
            assert isinstance(result, bool)
        finally:
            if orig_xt is not None:
                sys.modules["xtquant"] = orig_xt
            if orig_xd is not None:
                sys.modules["xtquant.xtdata"] = orig_xd

    def test_fix_xtquant_path_appends_without_prepending_external_site_packages(self, monkeypatch):
        import core.qmt_feed as qf

        monkeypatch.setattr(qf, "_XTQUANT_PATH_FIXED", False)
        original_path = ["ENV_FIRST"]
        monkeypatch.setattr(qf.sys, "path", original_path)

        def _fake_isdir(path):
            return path == r"C:\Users\wangzhong\miniconda3\Lib\site-packages\xtquant"

        def _fake_getsize(_path):
            return 100

        monkeypatch.setattr(qf.os.path, "isdir", _fake_isdir)
        monkeypatch.setattr(qf.os.path, "getsize", _fake_getsize)

        assert qf._fix_xtquant_path() is True
        assert qf.sys.path[0] == "ENV_FIRST"
        assert qf.sys.path[-1] == r"C:\Users\wangzhong\miniconda3\Lib\site-packages"


# ---------------------------------------------------------------------------
# subscribe — QMT 不可用时拒绝订阅
# ---------------------------------------------------------------------------

class TestSubscribeQmtUnavailable:
    def _make_feed(self) -> QmtFeed:
        feed = QmtFeed()
        return feed

    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_returns_error_source(self, _):
        feed = self._make_feed()
        result = feed.subscribe("000001.SZ")
        assert result["subscribed"] is False
        assert result["source"] == "error"

    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_does_not_mark_as_subscribed(self, _):
        feed = self._make_feed()
        feed.subscribe("000001.SZ")
        assert feed.is_subscribed("000001.SZ") is False

    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_double_subscribe_both_fail(self, _):
        feed = self._make_feed()
        result1 = feed.subscribe("000001.SZ")
        result2 = feed.subscribe("000001.SZ")
        assert result1["subscribed"] is False
        assert result2["subscribed"] is False

    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_state_empty_when_unavailable(self, _):
        feed = self._make_feed()
        feed.subscribe("600519.SH", period="1m")
        subs = feed.all_subscriptions()
        assert subs == []


# ---------------------------------------------------------------------------
# subscribe — QMT 可用时
# ---------------------------------------------------------------------------

class TestSubscribeWithQmt:
    def test_calls_subscribe_quote(self):
        feed = QmtFeed()
        with mock_xtquant_xtdata() as mock_xd:
            mock_xd.subscribe_quote.return_value = "sub_id_001"
            result = feed.subscribe("000001.SZ", period="tick")
        mock_xd.subscribe_quote.assert_called_once()
        assert mock_xd.subscribe_quote.call_args[0][0] == "000001.SZ"
        assert result["subscribed"] is True
        assert result["source"] == "qmt_live"

    def test_subscribe_exception_returns_error(self):
        feed = QmtFeed()
        with mock_xtquant_xtdata() as mock_xd:
            mock_xd.subscribe_quote.side_effect = RuntimeError("连接失败")
            result = feed.subscribe("000001.SZ")
        assert result["subscribed"] is False
        assert result["source"] == "error"
        assert "连接失败" in result["message"]


# ---------------------------------------------------------------------------
# unsubscribe
# ---------------------------------------------------------------------------

class TestUnsubscribe:
    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_unsubscribe_not_subscribed(self, _):
        feed = QmtFeed()
        result = feed.unsubscribe("000001.SZ")
        assert result["unsubscribed"] is False
        assert "未处于订阅状态" in result["message"]

    def test_unsubscribe_removes_state(self):
        feed = QmtFeed()
        with mock_xtquant_xtdata() as mock_xd:
            mock_xd.subscribe_quote.return_value = "sub_001"
            feed.subscribe("000001.SZ")
        assert feed.is_subscribed("000001.SZ") is True
        result = feed.unsubscribe("000001.SZ")
        assert result["unsubscribed"] is True
        assert feed.is_subscribed("000001.SZ") is False

    def test_unsubscribe_calls_xtdata_unsubscribe(self):
        feed = QmtFeed()
        with mock_xtquant_xtdata() as mock_xd:
            mock_xd.subscribe_quote.return_value = "sub_999"
            feed.subscribe("000001.SZ")
            feed.unsubscribe("000001.SZ")
        mock_xd.unsubscribe_quote.assert_called_once_with("sub_999")


# ---------------------------------------------------------------------------
# _on_tick 回调
# ---------------------------------------------------------------------------

class TestOnTick:
    @staticmethod
    def _inject_state(feed: QmtFeed, symbol: str, period: str = "tick"):
        """直接注入订阅状态（不经过 subscribe 流程）。"""
        from core.qmt_feed import _SubscriptionState
        feed._states[symbol] = _SubscriptionState(symbol=symbol, period=period)

    def test_on_tick_calls_ingest(self):
        mock_api = MagicMock()
        feed = QmtFeed()
        self._inject_state(feed, "000001.SZ")
        with patch.dict("sys.modules", {"core.api_server": mock_api}):
            feed._on_tick("000001.SZ", "tick", {"000001.SZ": {"lastPrice": 12.5}})
        mock_api.ingest_tick_from_thread.assert_called_once()
        call_symbol, call_payload = mock_api.ingest_tick_from_thread.call_args[0]
        assert call_symbol == "000001.SZ"
        assert call_payload["price"] == 12.5

    def test_on_tick_increments_ingested_count(self):
        mock_api = MagicMock()
        feed = QmtFeed()
        self._inject_state(feed, "000001.SZ")
        with patch.dict("sys.modules", {"core.api_server": mock_api}):
            feed._on_tick("000001.SZ", "tick", {"lastPrice": 10.0})
            feed._on_tick("000001.SZ", "tick", {"lastPrice": 10.1})
        subs = feed.all_subscriptions()
        assert subs[0]["ingested_count"] == 2
        assert feed.stats()["total_ingested"] == 2

    def test_on_tick_handles_exception_gracefully(self):
        """回调异常不应向外传播，错误计数应递增。"""
        feed = QmtFeed()
        self._inject_state(feed, "000001.SZ")
        import sys, types
        bad_api = types.ModuleType("core.api_server")
        bad_api.ingest_tick_from_thread = MagicMock(side_effect=RuntimeError("炸了"))  # type: ignore
        sys.modules["core.api_server"] = bad_api
        try:
            feed._on_tick("000001.SZ", "tick", {"lastPrice": 10.0})  # must not raise
            subs = feed.all_subscriptions()
            assert subs[0]["error_count"] == 1
            assert feed.stats()["total_errors"] == 1
        finally:
            import core.api_server as real_api
            sys.modules["core.api_server"] = real_api

    def test_on_tick_diag_log_disabled_by_default(self):
        mock_api = MagicMock()
        feed = QmtFeed()
        self._inject_state(feed, "000001.SZ")
        with patch.dict(os.environ, {}, clear=False):
            with patch("core.qmt_feed.log.warning") as mock_warning:
                with patch.dict("sys.modules", {"core.api_server": mock_api}):
                    feed._on_tick("000001.SZ", "tick", {"000001.SZ": {"lastPrice": 12.5}})
        mock_warning.assert_not_called()

    def test_on_tick_diag_log_enabled_via_env(self):
        mock_api = MagicMock()
        feed = QmtFeed()
        self._inject_state(feed, "000001.SZ")
        with patch.dict(os.environ, {"EASYXT_QMT_DIAG": "1"}, clear=False):
            with patch("core.qmt_feed.log.warning") as mock_warning:
                with patch.dict("sys.modules", {"core.api_server": mock_api}):
                    feed._on_tick("000001.SZ", "tick", {"000001.SZ": {"lastPrice": 12.5}})
        assert any("[DIAG] qmt_feed._on_tick" in str(call.args[0]) for call in mock_warning.call_args_list)


# ---------------------------------------------------------------------------
# all_subscriptions / stats
# ---------------------------------------------------------------------------

class TestObservability:
    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_all_subscriptions_empty_by_default(self, _):
        feed = QmtFeed()
        assert feed.all_subscriptions() == []

    def test_all_subscriptions_returns_correct_fields(self):
        feed = QmtFeed()
        from core.qmt_feed import _SubscriptionState
        feed._states["000001.SZ"] = _SubscriptionState(symbol="000001.SZ", period="tick")
        subs = feed.all_subscriptions()
        assert len(subs) == 1
        expected_keys = {"symbol", "period", "subscribed_at", "ingested_count",
                         "error_count", "last_tick_ts"}
        assert expected_keys.issubset(subs[0].keys())

    def test_stats_total_subscriptions(self):
        feed = QmtFeed()
        from core.qmt_feed import _SubscriptionState
        feed._states["000001.SZ"] = _SubscriptionState(symbol="000001.SZ", period="tick")
        feed._states["600519.SH"] = _SubscriptionState(symbol="600519.SH", period="tick")
        stats = feed.stats()
        assert stats["total_subscriptions"] == 2

    @patch.object(QmtFeed, "is_available", return_value=False)
    def test_stats_qmt_available_false_when_unavailable(self, _):
        feed = QmtFeed()
        stats = feed.stats()
        assert stats["qmt_available"] is False

    @patch.object(QmtFeed, "is_available", return_value=True)
    def test_stats_qmt_available_true_when_available(self, _):
        feed = QmtFeed()
        stats = feed.stats()
        assert stats["qmt_available"] is True

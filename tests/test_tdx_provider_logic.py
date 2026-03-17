from unittest.mock import MagicMock, patch

from easy_xt.realtime_data.providers.tdx_provider import TdxDataProvider


def _make_provider_stub() -> TdxDataProvider:
    p = object.__new__(TdxDataProvider)
    p.logger = MagicMock()
    p.error_log_cooldown = 10.0
    p._error_log_last_ts = {}
    p._error_log_suppressed = {}
    return p


def test_log_throttled_suppresses_repeats_then_reports_summary():
    p = _make_provider_stub()
    with patch("easy_xt.realtime_data.providers.tdx_provider.time.monotonic", side_effect=[0.0, 1.0, 12.0]):
        p._log_throttled("error", "connect_exc:a", "连接异常: a")
        p._log_throttled("error", "connect_exc:a", "连接异常: a")
        p._log_throttled("error", "connect_exc:a", "连接异常: a")
    assert p.logger.error.call_count == 2
    last_msg = p.logger.error.call_args_list[-1][0][0]
    assert "抑制重复日志 1 次" in last_msg


def test_resolve_error_log_cooldown_prefers_env():
    with patch.dict("os.environ", {"EASYXT_TDX_ERROR_LOG_COOLDOWN": "7.5"}, clear=False):
        value = TdxDataProvider._resolve_error_log_cooldown({"error_log_cooldown": 20})
    assert value == 7.5


def test_resolve_error_log_cooldown_fallback_to_config():
    with patch.dict("os.environ", {}, clear=True):
        value = TdxDataProvider._resolve_error_log_cooldown({"error_log_cooldown": 9})
    assert value == 9.0


def test_resolve_error_log_cooldown_invalid_returns_default():
    with patch.dict("os.environ", {"EASYXT_TDX_ERROR_LOG_COOLDOWN": "not_number"}, clear=False):
        value = TdxDataProvider._resolve_error_log_cooldown({})
    assert value == 15.0

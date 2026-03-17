import time
from collections import deque

import easy_xt.realtime_data.unified_api as unified_api_module
from easy_xt.realtime_data.unified_api import UnifiedDataAPI


class DummyProvider:
    def get_realtime_quotes(self, codes):
        return []

    def get_provider_info(self):
        return {"name": "dummy"}


def test_source_quality_auto_recovery(monkeypatch):
    def _init_providers(self):
        self.providers = {"dummy": DummyProvider()}

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    api = UnifiedDataAPI()
    api._quality_events["dummy"] = deque()
    api._latency_switch_threshold_ms = 1.0
    api._min_success_rate = 0.9
    api._disable_cooldown_s = 1
    api.source_status["dummy"].available = True

    api.report_source_quality("dummy", latency_ms=10000, success=False)
    assert api.source_status["dummy"].available is False

    api._quality_events["dummy"].clear()
    api._source_disabled_until["dummy"] = time.time() - 1
    api._min_success_rate = 0.0
    api._latency_switch_threshold_ms = 1000.0
    api.report_source_quality("dummy", latency_ms=1, success=True)
    assert api.source_status["dummy"].available is True


def test_source_quality_thresholds_and_cooldown(monkeypatch):
    def _init_providers(self):
        self.providers = {"dummy": DummyProvider()}

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    api = UnifiedDataAPI()
    api._quality_events["dummy"] = deque()
    api._quality_window_seconds = 10
    api._latency_switch_threshold_ms = 50.0
    api._min_success_rate = 0.9
    api._disable_cooldown_s = 1
    api.source_status["dummy"].available = True

    for _ in range(5):
        api.report_source_quality("dummy", latency_ms=100.0, success=True)
    assert api.source_status["dummy"].available is False

    api._quality_events["dummy"].clear()
    api._source_disabled_until["dummy"] = time.time() - 1
    api._latency_switch_threshold_ms = 1000.0
    api.report_source_quality("dummy", latency_ms=1.0, success=True)
    assert api.source_status["dummy"].available is True


def test_source_priority_and_staleness(monkeypatch):
    class ProviderA(DummyProvider):
        def get_provider_info(self):
            return {"supported_data_types": ["实时行情"]}

    class ProviderB(DummyProvider):
        def get_provider_info(self):
            return {"supported_data_types": ["实时行情"]}

    def _init_providers(self):
        self.providers = {"a": ProviderA(), "b": ProviderB()}

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    api = UnifiedDataAPI()
    for name in api.providers:
        api.source_status[name].connected = True
        api.source_status[name].available = True
        api.source_status[name].last_update = time.time()
        api.source_status[name].response_time = 0.1
    api.source_priority = ["b", "a"]
    api.max_staleness_ms = 1000.0
    ordered = api.get_available_providers("realtime_quotes")
    assert ordered[0] == "b"

    api.source_status["b"].last_update = time.time() - 5
    api.max_staleness_ms = 1000.0
    ordered = api.get_available_providers("realtime_quotes")
    assert "b" not in ordered


def test_connect_all_timeout_shutdown_non_blocking(monkeypatch):
    class Provider(DummyProvider):
        def connect(self):
            return True

    def _init_providers(self):
        self.providers = {"dummy": Provider()}

    class FakeFuture:
        def cancel(self):
            return True

    class FakePool:
        instances = []

        def __init__(self, *args, **kwargs):
            self.shutdown_calls = []
            FakePool.instances.append(self)

        def submit(self, fn, *args, **kwargs):
            return FakeFuture()

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))

    def _fake_as_completed(*args, **kwargs):
        raise unified_api_module.TimeoutError()
        yield

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    monkeypatch.setattr(unified_api_module, "ThreadPoolExecutor", FakePool)
    monkeypatch.setattr(unified_api_module, "as_completed", _fake_as_completed)

    api = UnifiedDataAPI()
    api.timeout = 0.01
    result = api.connect_all()

    assert result["dummy"] is False
    assert FakePool.instances[-1].shutdown_calls[-1] == (False, True)


def test_get_multi_source_data_timeout_returns_partial(monkeypatch):
    class FastProvider(DummyProvider):
        def get_realtime_quotes(self, codes):
            return [{"code": codes[0], "price": 1.0}]

        def get_provider_info(self):
            return {"supported_data_types": ["实时行情"]}

    class SlowProvider(DummyProvider):
        def get_realtime_quotes(self, codes):
            return [{"code": codes[0], "price": 2.0}]

        def get_provider_info(self):
            return {"supported_data_types": ["实时行情"]}

    def _init_providers(self):
        self.providers = {"fast": FastProvider(), "slow": SlowProvider()}

    class FakeFuture:
        def __init__(self, value):
            self._value = value
            self._done = False
            self.cancelled = False

        def result(self, timeout=None):
            self._done = True
            return self._value

        def done(self):
            return self._done

        def cancel(self):
            self.cancelled = True
            self._done = True
            return True

    class FakePool:
        instances = []

        def __init__(self, *args, **kwargs):
            self._futures = []
            self.shutdown_calls = []
            FakePool.instances.append(self)

        def submit(self, fn, provider_codes):
            value = fn(provider_codes)
            future = FakeFuture(value)
            self._futures.append(future)
            return future

        def shutdown(self, wait=True, cancel_futures=False):
            self.shutdown_calls.append((wait, cancel_futures))

    def _fake_as_completed(future_to_source, timeout=None):
        futures = list(future_to_source.keys())
        if futures:
            yield futures[0]
        raise unified_api_module.TimeoutError()

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    monkeypatch.setattr(unified_api_module, "ThreadPoolExecutor", FakePool)
    monkeypatch.setattr(unified_api_module, "as_completed", _fake_as_completed)

    api = UnifiedDataAPI()
    api.timeout = 0.01
    api.source_status["fast"].connected = True
    api.source_status["fast"].available = True
    api.source_status["slow"].connected = True
    api.source_status["slow"].available = True

    result = api.get_multi_source_data(["000001.SZ"])

    assert "fast" in result
    assert "slow" not in result
    assert FakePool.instances[-1].shutdown_calls[-1] == (False, True)


def test_connect_all_logs_budget_exceeded(monkeypatch, caplog):
    class Provider(DummyProvider):
        def connect(self):
            return True

    def _init_providers(self):
        self.providers = {"dummy": Provider()}

    ticks = iter([100.0, 106.5, 106.5, 106.5, 106.5])

    def _fake_monotonic():
        try:
            return next(ticks)
        except StopIteration:
            return 106.5

    monkeypatch.setattr(UnifiedDataAPI, "_init_providers", _init_providers)
    monkeypatch.setattr(unified_api_module.time, "monotonic", _fake_monotonic)

    api = UnifiedDataAPI()
    api.timeout = 3
    with caplog.at_level("WARNING"):
        result = api.connect_all()

    assert result["dummy"] is True
    assert "connect_all连接预算超限" in caplog.text

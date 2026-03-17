"""Unit tests for easy_xt.decorators — SimpleCache, retry, cache, validate_params, rate_limit."""
import time
from unittest.mock import MagicMock, patch

import pytest

from easy_xt.data_types import ConnectionError as EasyConnectionError, DataError
from easy_xt.decorators import SimpleCache, retry, cache, validate_params, rate_limit


# ─────────────────────────────────────────────────────────────
# SimpleCache
# ─────────────────────────────────────────────────────────────
class TestSimpleCache:
    def test_get_returns_none_for_missing_key(self):
        c = SimpleCache()
        assert c.get("no_such_key") is None

    def test_set_and_get(self):
        c = SimpleCache()
        c.set("k", 42, ttl=60)
        assert c.get("k") == 42

    def test_get_returns_none_after_expiry(self):
        c = SimpleCache()
        c.set("k", "value", ttl=0)  # TTL=0 → already expired
        # Adding 0 seconds → expires at now, so should be expired immediately
        # Force by setting with a tiny ttl and advancing time via mock
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        _SH = ZoneInfo("Asia/Shanghai")
        future = datetime.now(tz=_SH) + timedelta(seconds=10)
        with patch("easy_xt.decorators.datetime") as mock_dt:
            mock_dt.now.return_value = future
            assert c.get("k") is None

    def test_clear_removes_all_entries(self):
        c = SimpleCache()
        c.set("a", 1)
        c.set("b", 2)
        c.clear()
        assert c.get("a") is None
        assert c.get("b") is None

    def test_evicts_oldest_when_full(self):
        c = SimpleCache(max_size=2)
        c.set("first", 1)
        time.sleep(0.01)  # ensure ordering
        c.set("second", 2)
        # Adding a third entry should evict "first"
        c.set("third", 3)
        assert len(c.cache) == 2
        assert c.get("first") is None
        assert c.get("second") == 2
        assert c.get("third") == 3

    def test_overwrite_existing_key(self):
        c = SimpleCache()
        c.set("k", "old")
        c.set("k", "new")
        assert c.get("k") == "new"


# ─────────────────────────────────────────────────────────────
# retry decorator
# ─────────────────────────────────────────────────────────────
class TestRetryDecorator:
    def test_succeeds_on_first_call(self):
        calls = []

        @retry(max_attempts=3, delay=0.0)
        def fn():
            calls.append(1)
            return "ok"

        assert fn() == "ok"
        assert calls == [1]

    def test_retries_on_retryable_exception(self):
        calls = []

        @retry(max_attempts=3, delay=0.0, exceptions=(EasyConnectionError,))
        def fn():
            calls.append(1)
            if len(calls) < 3:
                raise EasyConnectionError("temp fail")
            return "ok"

        result = fn()
        assert result == "ok"
        assert len(calls) == 3

    def test_raises_after_max_attempts(self):
        @retry(max_attempts=2, delay=0.0, exceptions=(EasyConnectionError,))
        def always_fail():
            raise EasyConnectionError("perm fail")

        with pytest.raises(EasyConnectionError):
            always_fail()

    def test_non_retryable_exception_propagates_immediately(self):
        calls = []

        @retry(max_attempts=3, delay=0.0, exceptions=(EasyConnectionError,))
        def fn():
            calls.append(1)
            raise ValueError("not retryable")

        with pytest.raises(ValueError):
            fn()
        assert len(calls) == 1  # only tried once

    def test_on_retry_callback_called(self):
        retried = []

        @retry(max_attempts=3, delay=0.0, exceptions=(EasyConnectionError,),
               on_retry=lambda attempt, exc: retried.append(attempt))
        def fn():
            if len(retried) < 2:
                raise EasyConnectionError("tmp")
            return "ok"

        fn()
        assert retried == [1, 2]

    def test_preserves_function_name(self):
        @retry(delay=0.0)
        def my_function():
            return 1

        assert my_function.__name__ == "my_function"


# ─────────────────────────────────────────────────────────────
# cache decorator
# ─────────────────────────────────────────────────────────────
class TestCacheDecorator:
    def test_result_is_cached(self):
        calls = []

        @cache(ttl=60)
        def expensive(x):
            calls.append(x)
            return x * 2

        assert expensive(5) == 10
        assert expensive(5) == 10
        assert calls == [5]  # only called once

    def test_different_args_cached_separately(self):
        calls = []

        @cache(ttl=60)
        def fn(x):
            calls.append(x)
            return x

        fn(1)
        fn(2)
        fn(1)
        assert calls == [1, 2]

    def test_custom_cache_instance_used(self):
        my_cache = SimpleCache()

        @cache(ttl=60, cache_instance=my_cache)
        def fn(x):
            return x * 3

        result = fn(4)
        assert result == 12
        # The result should be in our custom cache
        assert len(my_cache.cache) == 1

    def test_custom_key_func(self):
        calls = []

        @cache(ttl=60, key_func=lambda x: "constant_key")
        def fn(x):
            calls.append(x)
            return x

        fn(1)
        fn(2)  # different arg but same cache key
        assert calls == [1]  # only called once

    def test_preserves_function_name(self):
        @cache(ttl=60)
        def my_cached_fn():
            return 0

        assert my_cached_fn.__name__ == "my_cached_fn"


# ─────────────────────────────────────────────────────────────
# validate_params decorator
# ─────────────────────────────────────────────────────────────
class TestValidateParamsDecorator:
    def test_valid_params_pass_through(self):
        @validate_params(x=lambda v: v * 2)
        def fn(x):
            return x

        assert fn(3) == 6  # validator doubles the value

    def test_invalid_param_raises_validation_error(self):
        from easy_xt.data_types import ValidationError

        def strict_positive(v):
            if v <= 0:
                raise ValueError("must be positive")
            return v

        @validate_params(x=strict_positive)
        def fn(x):
            return x

        with pytest.raises(ValidationError):
            fn(-1)

    def test_unvalidated_params_unchanged(self):
        @validate_params(a=lambda v: v + 1)
        def fn(a, b):
            return a, b

        result = fn(1, 99)
        assert result == (2, 99)

    def test_validator_called_with_kwarg(self):
        seen = []

        @validate_params(x=lambda v: seen.append(v) or v)
        def fn(x):
            return x

        fn(x=42)
        assert seen == [42]


# ─────────────────────────────────────────────────────────────
# rate_limit decorator
# ─────────────────────────────────────────────────────────────
class TestRateLimitDecorator:
    def test_function_still_returns_correctly(self):
        @rate_limit(calls_per_second=1000.0)  # high limit → no real delay
        def fn(x):
            return x * 2

        assert fn(3) == 6

    def test_rate_limited_calls_take_minimum_interval(self):
        interval = 0.05  # 50 ms
        rps = 1.0 / interval

        @rate_limit(calls_per_second=rps)
        def fn():
            return time.time()

        t1 = fn()
        t2 = fn()
        assert (t2 - t1) >= interval * 0.9  # allow 10% tolerance

    def test_preserves_function_name(self):
        @rate_limit(calls_per_second=100.0)
        def my_rate_limited_fn():
            return 0

        assert my_rate_limited_fn.__name__ == "my_rate_limited_fn"

"""Unit tests for DragonTigerData pure-logic helpers.

Tests the two private methods that contain no I/O or external dependencies:
  - _date_arg: optional date defaulting
  - _convert_to_full_code: stock code suffix assignment
"""
import types

import pytest

from easy_xt.dragon_tiger import DragonTigerData


def _make_dt() -> DragonTigerData:
    """Instantiate DragonTigerData with akshare import stubbed out."""
    dt = object.__new__(DragonTigerData)
    dt.ak_available = False  # type: ignore[attr-defined]
    dt.ak = None  # type: ignore[attr-defined]
    dt._cache = {}  # type: ignore[attr-defined]
    return dt  # type: ignore[return-value]


DT = _make_dt()


# ─────────────────────────────────────────────────────────────
# _date_arg
# ─────────────────────────────────────────────────────────────
class TestDateArg:
    def test_none_returns_empty_string(self):
        assert DT._date_arg(None) == ""

    def test_provided_date_returned_unchanged(self):
        assert DT._date_arg("20240101") == "20240101"

    def test_empty_string_is_falsy_returns_empty_string(self):
        # An empty string is falsy → returns ""
        assert DT._date_arg("") == ""


# ─────────────────────────────────────────────────────────────
# _convert_to_full_code
# ─────────────────────────────────────────────────────────────
class TestConvertToFullCode:
    def test_sh_code_starts_with_6(self):
        assert DT._convert_to_full_code("600000") == "600000.SH"

    def test_sh_code_starts_with_5(self):
        # ETF codes start with 5
        assert DT._convert_to_full_code("510300") == "510300.SH"

    def test_sz_code_starts_with_0(self):
        assert DT._convert_to_full_code("000001") == "000001.SZ"

    def test_sz_code_starts_with_3(self):
        # ChiNext codes start with 3
        assert DT._convert_to_full_code("300750") == "300750.SZ"

    def test_stk_prefix_stripped(self):
        assert DT._convert_to_full_code("stk600000") == "600000.SH"

    def test_dot_separator_stripped(self):
        # Code already has a dot (e.g. passed in as "600.000") gets cleaned
        assert DT._convert_to_full_code("600.000") == "600000.SH"

    def test_empty_string_returns_empty(self):
        assert DT._convert_to_full_code("") == ""

    def test_code_as_int_like_string_works(self):
        # str() call inside should handle numeric-looking strings
        assert DT._convert_to_full_code("000002") == "000002.SZ"

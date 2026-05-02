"""
gui_app/backtest/engine_status_ui.py 三个纯函数的全分支覆盖测试。
零 Qt / 零外部依赖，直接调用模块函数。
"""

import pytest

from gui_app.backtest.engine_status_ui import (
    build_engine_status_detail,
    format_engine_status_log,
    format_engine_status_ui,
)


# ===========================================================================
# format_engine_status_ui
# ===========================================================================

class TestFormatEngineStatusUi:
    """4 条返回路径 × 边界变体。"""

    # ------ path 1: backtrader 可用 ------

    def test_backtrader_available_text(self):
        result = format_engine_status_ui({"available": True, "mode": "backtrader"})
        assert "Backtrader" in result["text"]
        assert "✅" in result["text"]

    def test_backtrader_available_color_green(self):
        result = format_engine_status_ui({"available": True, "mode": "backtrader"})
        assert result["color"].startswith("#2e7d")

    def test_backtrader_custom_label_prefix(self):
        result = format_engine_status_ui(
            {"available": True, "mode": "backtrader"}, label_prefix="测试引擎"
        )
        assert result["text"].startswith("测试引擎")

    # ------ path 2: mock / unavailable 展示为不可用 ------

    def test_mock_mode_text_contains_unavailable(self):
        result = format_engine_status_ui({"mode": "mock"})
        assert "不可用" in result["text"]
        assert "❌" in result["text"]

    def test_mock_mode_color_red(self):
        result = format_engine_status_ui({"mode": "mock"})
        assert result["color"] == "#d32f2f"

    def test_mock_mode_with_error_type_in_text(self):
        result = format_engine_status_ui({"mode": "mock", "error_type": "ImportError"})
        assert "ImportError" in result["text"]

    def test_mock_mode_without_error_type_no_parenthesis(self):
        result = format_engine_status_ui({"mode": "mock"})
        assert "(" not in result["text"]

    def test_mock_mode_error_message_in_tooltip(self):
        result = format_engine_status_ui(
            {"mode": "mock", "error_message": "找不到backtrader"}
        )
        assert "找不到backtrader" in result["tooltip"]
        assert "已禁止模拟引擎降级" in result["tooltip"]

    def test_mock_mode_hint_in_tooltip(self):
        result = format_engine_status_ui(
            {"mode": "mock", "hint": "请运行 pip install backtrader"}
        )
        assert "pip install backtrader" in result["tooltip"]

    def test_mock_mode_no_hint_no_suggestion_line(self):
        result = format_engine_status_ui({"mode": "mock"})
        assert "建议" not in result["tooltip"]

    # ------ path 3: available is None ------

    def test_available_none_text(self):
        result = format_engine_status_ui({"available": None})
        assert "待检测" in result["text"]

    def test_available_none_color_gray(self):
        result = format_engine_status_ui({"available": None})
        assert result["color"] == "#999999"

    # ------ path 4: 默认 / 状态未知 ------

    def test_unknown_status_text(self):
        result = format_engine_status_ui({"available": False, "mode": "other"})
        assert "状态未知" in result["text"]
        assert "❓" in result["text"]

    def test_unknown_status_color(self):
        result = format_engine_status_ui({"available": False, "mode": "other"})
        assert result["color"] == "#666666"

    # ------ edge: None 输入 ------

    def test_none_input_treated_as_empty(self):
        result = format_engine_status_ui(None)
        assert isinstance(result, dict)
        assert "text" in result and "color" in result and "tooltip" in result

    def test_empty_dict_input(self):
        result = format_engine_status_ui({})
        assert isinstance(result, dict)

    # ------ 返回结构完整性 ------

    def test_all_paths_return_three_keys(self):
        cases = [
            {"available": True, "mode": "backtrader"},
            {"mode": "mock"},
            {"available": None},
            {"available": False, "mode": "other"},
        ]
        for status in cases:
            result = format_engine_status_ui(status)
            assert set(result.keys()) == {"text", "color", "tooltip"}, f"Failed for {status}"


# ===========================================================================
# build_engine_status_detail
# ===========================================================================

class TestBuildEngineStatusDetail:
    """详情字符串构建，覆盖有无 traceback 的分支。"""

    def test_returns_string(self):
        result = build_engine_status_detail({})
        assert isinstance(result, str)

    def test_none_input_returns_string(self):
        result = build_engine_status_detail(None)
        assert isinstance(result, str)

    def test_contains_available_field(self):
        result = build_engine_status_detail({"available": True})
        assert "可用" in result

    def test_contains_mode_field(self):
        result = build_engine_status_detail({"mode": "backtrader"})
        assert "backtrader" in result

    def test_no_error_shows_none_marker(self):
        result = build_engine_status_detail({"available": True, "mode": "backtrader"})
        assert "无" in result

    def test_with_error_type(self):
        result = build_engine_status_detail({"error_type": "ModuleNotFoundError"})
        assert "ModuleNotFoundError" in result

    def test_with_error_message(self):
        result = build_engine_status_detail({"error_message": "No module named backtrader"})
        assert "No module named backtrader" in result

    def test_with_hint(self):
        result = build_engine_status_detail({"hint": "请安装 backtrader"})
        assert "请安装 backtrader" in result

    def test_without_traceback_no_traceback_section(self):
        result = build_engine_status_detail({"available": True})
        assert "Traceback" not in result

    def test_short_traceback_included_verbatim(self):
        trace = "Traceback (most recent call last):\n  File test.py, line 1\nImportError"
        result = build_engine_status_detail({"traceback": trace})
        assert "Traceback" in result
        assert "ImportError" in result
        assert "..." not in result  # short → no truncation

    def test_long_traceback_truncated_with_ellipsis(self):
        trace = "A" * 2000
        result = build_engine_status_detail({"traceback": trace})
        assert "..." in result

    def test_long_traceback_first_1500_chars_present(self):
        trace = "X" * 1600 + "UNIQUE_TAIL"
        result = build_engine_status_detail({"traceback": trace})
        # 截断在1500处，UNIQUE_TAIL在1500+以后，不应出现
        assert "UNIQUE_TAIL" not in result

    def test_exactly_1500_chars_no_truncation(self):
        trace = "B" * 1500
        result = build_engine_status_detail({"traceback": trace})
        # 1500 chars exactly → len(trace) == 1500, not > 1500 → no "..."
        assert "..." not in result


# ===========================================================================
# format_engine_status_log
# ===========================================================================

class TestFormatEngineStatusLog:
    """日志格式串，覆盖所有分支路径。"""

    # ------ path 1: backtrader 可用 ------

    def test_backtrader_log_level_info(self):
        result = format_engine_status_log({"available": True, "mode": "backtrader"})
        assert "INFO" in result

    def test_backtrader_log_mode_in_message(self):
        result = format_engine_status_log({"available": True, "mode": "backtrader"})
        assert "backtrader" in result

    def test_backtrader_custom_prefix(self):
        result = format_engine_status_log(
            {"available": True, "mode": "backtrader"}, prefix="MY_ENGINE"
        )
        assert "MY_ENGINE" in result

    # ------ path 2: mock / unavailable 日志 ------

    def test_mock_log_level_error(self):
        result = format_engine_status_log({"mode": "mock"})
        assert "ERROR" in result

    def test_mock_mode_in_log(self):
        result = format_engine_status_log({"mode": "mock"})
        assert "mock" in result
        assert "已禁止模拟引擎降级" in result

    def test_mock_with_error_type_in_log(self):
        result = format_engine_status_log(
            {"mode": "mock", "error_type": "ImportError"}
        )
        assert "ImportError" in result

    def test_mock_with_error_message_in_log(self):
        result = format_engine_status_log(
            {"mode": "mock", "error_message": "module not found"}
        )
        assert "module not found" in result

    def test_mock_with_hint_in_log(self):
        result = format_engine_status_log(
            {"mode": "mock", "hint": "pip install backtrader"}
        )
        assert "pip install backtrader" in result

    def test_mock_without_extras_no_extra_fields(self):
        result = format_engine_status_log({"mode": "mock"})
        assert "error_type" not in result
        assert "hint" not in result

    # ------ path 3: available is None ------

    def test_available_none_log_info(self):
        result = format_engine_status_log({"available": None})
        assert "INFO" in result

    def test_available_none_available_in_log(self):
        result = format_engine_status_log({"available": None})
        assert "None" in result

    # ------ path 4: 未知状态 ------

    def test_unknown_mode_log_warn(self):
        result = format_engine_status_log({"available": False, "mode": "unknown_xyz"})
        assert "WARN" in result

    def test_unknown_mode_with_extras_in_log(self):
        result = format_engine_status_log(
            {"available": False, "mode": "xyz", "error_type": "SomeError", "hint": "fix it"}
        )
        assert "SomeError" in result
        assert "fix it" in result

    def test_unknown_mode_without_extras_no_extra_keys(self):
        result = format_engine_status_log({"available": False, "mode": "xyz"})
        assert "error_type" not in result
        assert "hint" not in result

    # ------ None 或空输入 ------

    def test_none_input_handled(self):
        result = format_engine_status_log(None)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_empty_dict_handled(self):
        result = format_engine_status_log({})
        assert isinstance(result, str)

    # ------ 默认 prefix ------

    def test_default_prefix_in_log(self):
        result = format_engine_status_log({})
        assert "BACKTEST_ENGINE" in result

    def test_unknown_mode_with_error_message_in_log(self):
        """unknown mode + error_message truthy → line 109 covered."""
        result = format_engine_status_log(
            {"available": False, "mode": "xyz", "error_message": "some error detail"}
        )
        assert "some error detail" in result

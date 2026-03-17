"""
回测引擎状态 UI 统一格式化
- 统一主窗口与回测页的文案、颜色、tooltip 规则
"""

from __future__ import annotations

from typing import Any


def format_engine_status_ui(status: dict[str, Any] | None, label_prefix: str = "引擎") -> dict[str, str]:
    status = status or {}
    available = status.get("available")
    mode = str(status.get("mode", "unknown"))
    error_type = status.get("error_type")
    error_message = status.get("error_message") or ""
    hint = status.get("hint") or ""

    if available is True and mode == "backtrader":
        return {
            "text": f"{label_prefix}: Backtrader ✅",
            "color": "#2e7d32",
            "tooltip": "Backtrader可用，使用真实回测引擎",
        }
    if available is True and mode == "native":
        return {
            "text": f"{label_prefix}: 原生引擎 ✅",
            "color": "#1976d2",
            "tooltip": "easyxt_backtest 原生引擎可用，使用无 backtrader 依赖路径",
        }

    if mode == "mock":
        suffix = f" ({error_type})" if error_type else ""
        tooltip = "Backtrader不可用，已降级模拟模式"
        if error_message:
            tooltip += f"\n原因: {error_message}"
        if hint:
            tooltip += f"\n建议: {hint}"
        return {
            "text": f"{label_prefix}: 模拟模式 ⚠️{suffix}",
            "color": "#ef6c00",
            "tooltip": tooltip,
        }

    if available is None:
        return {
            "text": f"{label_prefix}: 待检测",
            "color": "#999999",
            "tooltip": "等待状态上报",
        }

    return {
        "text": f"{label_prefix}: 状态未知 ❓",
        "color": "#666666",
        "tooltip": "无法确认回测引擎状态，请查看详情",
    }


def build_engine_status_detail(status: dict[str, Any] | None) -> str:
    status = status or {}
    available = status.get("available")
    mode = status.get("mode", "unknown")
    error_type = status.get("error_type") or ""
    error_message = status.get("error_message") or ""
    hint = status.get("hint") or ""
    trace = status.get("traceback") or ""
    trace_preview = trace[:1500] + ("\n..." if len(trace) > 1500 else "")

    detail = (
        f"可用: {available}\n"
        f"模式: {mode}\n"
        f"错误类型: {error_type or '无'}\n"
        f"错误信息: {error_message or '无'}\n"
        f"建议: {hint or '无'}"
    )
    if trace_preview:
        detail += f"\n\nTraceback:\n{trace_preview}"
    return detail


def format_engine_status_log(status: dict[str, Any] | None, prefix: str = "BACKTEST_ENGINE") -> str:
    status = status or {}
    available = status.get("available")
    mode = str(status.get("mode", "unknown"))
    error_type = status.get("error_type") or ""
    error_message = status.get("error_message") or ""
    hint = status.get("hint") or ""

    if available is True and mode == "backtrader":
        return f"[{prefix}] level=INFO mode=backtrader available=True message=Backtrader可用"
    if available is True and mode == "native":
        return f"[{prefix}] level=INFO mode=native available=True message=原生引擎可用"

    if mode == "mock":
        parts = [
            f"[{prefix}] level=WARN mode=mock available={available}",
            "message=Backtrader不可用，已降级模拟模式",
        ]
        if error_type:
            parts.append(f"error_type={error_type}")
        if error_message:
            parts.append(f"error_message={error_message}")
        if hint:
            parts.append(f"hint={hint}")
        return " | ".join(parts)

    if available is None:
        return f"[{prefix}] level=INFO mode={mode} available=None message=等待状态上报"

    parts = [
        f"[{prefix}] level=WARN mode={mode} available={available}",
        "message=回测引擎状态未知",
    ]
    if error_type:
        parts.append(f"error_type={error_type}")
    if error_message:
        parts.append(f"error_message={error_message}")
    if hint:
        parts.append(f"hint={hint}")
    return " | ".join(parts)

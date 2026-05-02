#!/usr/bin/env python3
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import json
import re
import sys
from typing import Any

EDIT_TOOL_NAMES = {
    "apply_patch",
    "create_file",
    "edit_notebook_file",
    "vscode_renameSymbol",
}
PATH_KEYS = {"filePath", "dirPath", "uri", "old_path", "new_path", "oldPath", "newPath"}
PATCH_FILE_RE = re.compile(r"^\*\*\* (?:Add|Update|Delete) File: (?P<path>.+?)\s*$", re.MULTILINE)
HIGH_RISK_HINTS = {
    "gui_app": "GUI/QThread 改动：注意 test_mode 守卫、closeEvent 分层等待和 xtdata 线程安全。",
    "data_manager": "数据层改动：注意 DuckDB/checkpoint、环境变量门控和统一数据链路。",
    "easy_xt": "easy_xt 改动：注意 xtdata 统一执行器、静默导入和兼容性。",
    "easyxt_backtest": "回测改动：优先跑对应回测/策略子集，避免只做表层冒烟。",
    "core": "核心基础设施改动：检查锁、线程、导入副作用与事件总线影响面。",
    "tests": "测试改动：保持 hermetic，避免伪造市场数据与环境泄漏。",
}


def _collect_named_paths(value: Any) -> list[str]:
    paths: list[str] = []
    stack: list[Any] = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for key, nested in current.items():
                if key in PATH_KEYS and isinstance(nested, str):
                    paths.append(nested)
                else:
                    stack.append(nested)
        elif isinstance(current, list):
            stack.extend(current)
    return paths


def _normalize_path(path: str, cwd: str) -> str:
    normalized = path.replace("\\", "/")
    if normalized.startswith("file://"):
        normalized = normalized[7:]
    cwd_normalized = cwd.replace("\\", "/").rstrip("/")
    if cwd_normalized and normalized.lower().startswith(cwd_normalized.lower() + "/"):
        normalized = normalized[len(cwd_normalized) + 1 :]
    return normalized.lstrip("/")


def _extract_paths(tool_name: str, tool_input: dict[str, Any], cwd: str) -> list[str]:
    paths: set[str] = set()
    if tool_name == "apply_patch":
        patch_text = str(tool_input.get("input", ""))
        for match in PATCH_FILE_RE.finditer(patch_text):
            paths.add(_normalize_path(match.group("path").strip(), cwd))
    for path in _collect_named_paths(tool_input):
        paths.add(_normalize_path(path, cwd))
    return sorted(path for path in paths if path)


def _detect_high_risk_areas(paths: list[str]) -> list[str]:
    matched: set[str] = set()
    for path in paths:
        first_segment = path.split("/", 1)[0].lower()
        if first_segment in HIGH_RISK_HINTS:
            matched.add(first_segment)
    return sorted(matched)


def _build_context(matched_areas: list[str], paths: list[str]) -> dict[str, Any]:
    hints = "\n".join(f"- {HIGH_RISK_HINTS[area]}" for area in matched_areas)
    touched = "\n".join(f"- {path}" for path in paths[:6])
    more = "\n- ..." if len(paths) > 6 else ""
    return {
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": (
                "本次成功修改了高风险区域文件。结束前请运行最小相关回归，优先使用 `/EasyXT Regression Subset`、现有 VS Code task，或与改动直接相关的 pytest 子集，而不是跳过验证。\n"
                f"涉及区域：{', '.join(matched_areas)}\n"
                f"本次触及文件：\n{touched}{more}\n"
                f"专项提醒：\n{hints}"
            ),
        }
    }


def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        print(json.dumps({}))
        return 0

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        print(json.dumps({}))
        return 0

    if payload.get("hookEventName") != "PostToolUse":
        print(json.dumps({}))
        return 0

    tool_name = str(payload.get("tool_name", ""))
    if tool_name not in EDIT_TOOL_NAMES:
        print(json.dumps({}))
        return 0

    tool_input = payload.get("tool_input", {})
    if not isinstance(tool_input, dict):
        print(json.dumps({}))
        return 0

    cwd = str(payload.get("cwd", ""))
    paths = _extract_paths(tool_name, tool_input, cwd)
    matched_areas = _detect_high_risk_areas(paths)
    if not matched_areas:
        print(json.dumps({}))
        return 0

    print(json.dumps(_build_context(matched_areas, paths), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

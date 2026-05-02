#!/usr/bin/env python3
# pyright: reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false
from __future__ import annotations

import json
import re
import sys
from typing import Any

PROTECTED_DIRS = {"external", "xtquant_backup", "myenv"}
DIRECT_MUTATION_TOOLS = {
    "apply_patch",
    "create_file",
    "create_directory",
    "edit_notebook_file",
    "vscode_renameSymbol",
}
TERMINAL_TOOLS = {"run_in_terminal"}
PATH_KEYS = {"filePath", "dirPath", "uri", "old_path", "new_path", "oldPath", "newPath"}
PATCH_FILE_RE = re.compile(r"^\*\*\* (?:Add|Update|Delete) File: (?P<path>.+?)\s*$", re.MULTILINE)
DESTRUCTIVE_TERMINAL_HINTS = (
    "remove-item",
    "rename-item",
    "move-item",
    "copy-item",
    "set-content",
    "add-content",
    "out-file",
    "new-item",
    "del ",
    "erase ",
    "rm ",
    "rmdir",
    " rd ",
    " ren ",
    " move ",
    " copy ",
    ">",
)
TERMINAL_PROTECTED_DIR_RE = re.compile(
    r"(?:^|[\\/\s'\"=])(?P<dir>external|xtquant_backup|myenv)(?=(?:[\\/\s'\"=]|$))",
    re.IGNORECASE,
)


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


def _is_direct_mutation_tool(tool_name: str) -> bool:
    lowered = tool_name.lower()
    if tool_name in DIRECT_MUTATION_TOOLS:
        return True
    return any(token in lowered for token in ("patch", "edit", "rename", "delete", "remove"))


def _protected_dirs_in_paths(paths: list[str]) -> list[str]:
    matched: set[str] = set()
    for item in paths:
        normalized = item.replace("\\", "/")
        segments = [segment.strip().lower() for segment in normalized.split("/") if segment.strip()]
        matched.update(segment for segment in segments if segment in PROTECTED_DIRS)
    return sorted(matched)


def _protected_dirs_in_terminal(command_text: str) -> list[str]:
    if not any(hint in command_text for hint in DESTRUCTIVE_TERMINAL_HINTS):
        return []
    return sorted({match.group("dir").lower() for match in TERMINAL_PROTECTED_DIR_RE.finditer(command_text)})


def _ask_output(tool_name: str, matched_dirs: list[str]) -> dict[str, Any]:
    dirs = ", ".join(matched_dirs)
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": (
                f"检测到工具 {tool_name} 可能修改受保护目录：{dirs}。"
                "这些目录通常包含 vendored 代码、备份或本地环境，请确认这是有意操作。"
            ),
            "additionalContext": (
                "仓库约定默认不修改 external/、xtquant_backup/、myenv/。"
                "若用户明确要求修改这些目录，再继续执行。"
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

    if payload.get("hookEventName") != "PreToolUse":
        print(json.dumps({}))
        return 0

    tool_name = str(payload.get("tool_name", ""))
    tool_input = payload.get("tool_input", {})
    if not isinstance(tool_input, dict):
        print(json.dumps({}))
        return 0
    cwd = str(payload.get("cwd", ""))

    if tool_name in TERMINAL_TOOLS:
        command_text = str(tool_input.get("command", "")).lower()
        matched_dirs = _protected_dirs_in_terminal(command_text)
        if matched_dirs:
            print(json.dumps(_ask_output(tool_name, matched_dirs), ensure_ascii=False))
            return 0
        print(json.dumps({}))
        return 0

    if _is_direct_mutation_tool(tool_name):
        matched_dirs = _protected_dirs_in_paths(_extract_paths(tool_name, tool_input, cwd))
        if matched_dirs:
            print(json.dumps(_ask_output(tool_name, matched_dirs), ensure_ascii=False))
            return 0

    print(json.dumps({}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

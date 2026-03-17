#!/usr/bin/env python
"""
OpenAPI 契约基线更新工具

用途：
    在 API 变更（新增端点、调整参数结构等）后，显式更新
    tests/fixtures/openapi_schema.json 基线文件。

    与直接删除文件触发"隐式更新"的区别：
    - 本脚本先展示 diff，要求用户二次确认，避免意外覆盖。
    - 支持 --force 参数跳过确认（CI 强制更新场景）。

用法：
    # 查看当前 schema 与基线的 diff，并确认更新
    python tools/update_openapi_baseline.py

    # CI 中强制更新（跳过确认）
    python tools/update_openapi_baseline.py --force

    # 仅展示 diff，不更新
    python tools/update_openapi_baseline.py --diff-only
"""
from __future__ import annotations

import argparse
import datetime
import getpass
import json
import os
import sys
import tempfile
import time
from pathlib import Path

# 将项目根目录加入 sys.path，使 `from core.api_server import app` 可用
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

_FIXTURE_PATH = _ROOT / "tests" / "fixtures" / "openapi_schema.json"
_CHANGELOG_PATH = _ROOT / "tests" / "fixtures" / "openapi_changelog.json"


def _load_current_schema() -> dict:
    from core.api_server import app  # noqa: import inside function for lazy load

    return app.openapi()


def _diff_paths(saved: dict, current: dict) -> tuple[set, set]:
    """返回 (removed_paths, added_paths)。"""
    saved_paths = set(saved.get("paths", {}).keys())
    curr_paths = set(current.get("paths", {}).keys())
    return saved_paths - curr_paths, curr_paths - saved_paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Update OpenAPI contract baseline")
    parser.add_argument("--force", action="store_true", help="跳过二次确认直接更新")
    parser.add_argument("--diff-only", action="store_true", help="仅展示 diff，不写入")
    args = parser.parse_args()

    print("正在加载当前 OpenAPI schema …")
    current = _load_current_schema()
    current_json = json.dumps(current, indent=2, ensure_ascii=False)

    if not _FIXTURE_PATH.exists():
        print(f"  基线文件不存在，将新建: {_FIXTURE_PATH.relative_to(_ROOT)}")
        removed, added = set(), set(current.get("paths", {}).keys())
    else:
        saved = json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))
        removed, added = _diff_paths(saved, current)
        print(f"  基线文件: {_FIXTURE_PATH.relative_to(_ROOT)}")
        if not removed and not added:
            print("  ✅ 无变更，基线保持最新。")
            if args.diff_only:
                return
        else:
            if removed:
                print(f"  ⚠️  端点已删除（破坏性变更）:")
                for p in sorted(removed):
                    print(f"       - {p}")
            if added:
                print(f"  ℹ️  端点已新增（兼容变更）:")
                for p in sorted(added):
                    print(f"       + {p}")

    if args.diff_only:
        print("  (--diff-only 模式，不写入)")
        return

    if not args.force:
        if removed:
            confirm = input(
                "\n  ⚠️  检测到端点删除，确认更新可能影响已接入方。继续? [yes/N] "
            ).strip().lower()
            if confirm != "yes":
                print("  已取消。")
                sys.exit(0)
        else:
            confirm = input("\n  确认更新基线? [y/N] ").strip().lower()
            if confirm not in ("y", "yes"):
                print("  已取消。")
                sys.exit(0)

    _FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _FIXTURE_PATH.write_text(current_json, encoding="utf-8")
    print(f"\n  ✅ 基线已更新: {_FIXTURE_PATH.relative_to(_ROOT)}")
    print("  请将此文件加入版本控制（git add）。")

    # --- 审计日志：并发写保护的追加 ---
    changelog_entry = {
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "operator": getpass.getuser(),
        "added": sorted(added),
        "removed": sorted(removed),
        "commit_sha": os.environ.get("EASYXT_COMMIT_SHA", "unknown"),
        "build_version": os.environ.get("EASYXT_BUILD_VERSION", "dev"),
    }
    _append_changelog_safe(changelog_entry)
    print(f"  📋 审计记录已追加: {_CHANGELOG_PATH.relative_to(_ROOT)}")


def _append_changelog_safe(entry: dict) -> None:
    """原子追加一条 changelog 记录：文件锁（O_CREAT|O_EXCL）+ 全局 atomic rename。

    适用于并发 CI 场景：多个作业同时运行本脚本时不会出现覆盖或文件损坏。
    """
    _CHANGELOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_path = _CHANGELOG_PATH.with_suffix(".lock")
    # --- 获取文件锁：尝试创建独占文件，最多等待 10 秒 ---
    deadline = time.monotonic() + 10.0
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except FileExistsError:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"等待 changelog 文件锁超时（10s），请手动删除: {lock_path}"
                ) from None
            time.sleep(0.1)
    try:
        log_data: list = (
            json.loads(_CHANGELOG_PATH.read_text(encoding="utf-8"))
            if _CHANGELOG_PATH.exists()
            else []
        )
        log_data.append(entry)
        new_content = json.dumps(log_data, indent=2, ensure_ascii=False)
        # 写入同目录临时文件（和目标同分区，保证 os.replace 原子）
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=_CHANGELOG_PATH.parent, suffix=".tmp"
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp_path, str(_CHANGELOG_PATH))
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    finally:
        try:
            lock_path.unlink()
        except OSError:
            pass


if __name__ == "__main__":
    main()

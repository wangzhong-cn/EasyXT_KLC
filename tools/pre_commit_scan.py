#!/usr/bin/env python
"""
pre_commit_scan.py — 轻量预提交凭据扫描
==========================================
只扫描 git staged diff 中的新增行（+开头），不扫全量文件，
速度极快（毫秒级），无第三方依赖。

安装方法（仅需运行一次）：
  python tools/pre_commit_scan.py --install

手动扫描当前 staged changes：
  python tools/pre_commit_scan.py

扫描指定 diff 文件：
  python tools/pre_commit_scan.py --diff-file some.patch

扫描指定 commit 范围（CI 使用）：
  python tools/pre_commit_scan.py --range HEAD~1..HEAD

退出码：
  0 = 无凭据泄露
  1 = 发现潜在凭据（阻断 commit / CI）
  2 = 运行时错误（git 未初始化等，不阻断 commit 以避免干扰）
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# 凭据检测规则（轻量版，与 p0_gate_check.py 中 KNOWN_CREDENTIALS 保持一致）
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CredRule:
    name: str
    pattern: re.Pattern
    hint: str


_RULES: list[CredRule] = [
    CredRule(
        name="硬编码密码赋值",
        pattern=re.compile(
            # 匹配 Python 赋值 (key = "val") 和 JSON 键值对 ("key": "val")
            r'(?:password|passwd|pwd|secret|api_key|apikey|token|auth_token)'
            r'[\s"\']*(?:=|:)\s*["\'][^"\']{4,}["\']',
            re.IGNORECASE,
        ),
        hint="使用环境变量 os.environ['KEY'] 或配置文件外读取，不要把凭据写进代码",
    ),
    CredRule(
        name="URL 内嵌凭据",
        pattern=re.compile(
            r'(?:https?|ftp)://[^\s:@/]+:[^\s@/]{4,}@',
            re.IGNORECASE,
        ),
        hint="把凭据从 URL 中移除，通过安全渠道传递",
    ),
    CredRule(
        name="私钥头部标志",
        pattern=re.compile(
            r'-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----',
        ),
        hint="私钥不能提交到代码库，添加到 .gitignore",
    ),
    CredRule(
        name="AWS 访问密钥",
        pattern=re.compile(r'\bAKIA[0-9A-Z]{16}\b'),
        hint="立即通过 AWS Console 吊销泄漏的密钥",
    ),
    CredRule(
        name="通达信账户密码字段",
        pattern=re.compile(
            r'(?:xt_acc_pwd|account_password|trade_password)\s*=\s*["\'][^"\']{4,}["\']',
            re.IGNORECASE,
        ),
        hint="通达信账户密码应从加密配置文件读取，不得硬编码",
    ),
]

# 允许跳过扫描的行标记（与 p0_gate_check.py allowlist 机制互补）
_NOSCAN_MARKERS = ("# noscan", "# no-scan", "# noqa: credential", "# nosec")

# ─────────────────────────────────────────────────────────────────────────────
# 核心扫描逻辑
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Finding:
    file: str
    line_no: int       # diff 内行号（近似，非源文件行号）
    rule: str
    snippet: str
    hint: str


def scan_diff_text(diff_text: str) -> list[Finding]:
    """扫描 unified diff 文本中的新增行（+ 开头，非 +++ 文件头）。"""
    findings: list[Finding] = []
    current_file = "<unknown>"
    new_line_no = 0  # 目标文件行号追踪

    for raw_line in diff_text.splitlines():
        # 解析文件名
        if raw_line.startswith("+++ b/"):
            current_file = raw_line[6:]
            new_line_no = 0
            continue
        if raw_line.startswith("+++ "):
            current_file = raw_line[4:]
            new_line_no = 0
            continue
        # 解析 hunk 头部 @@ -a,b +c,d @@
        if raw_line.startswith("@@"):
            m = re.search(r'\+(\d+)', raw_line)
            if m:
                new_line_no = int(m.group(1)) - 1  # 下一行递增
            continue
        # 上下文行 / 删除行：只跟踪行号
        if not raw_line.startswith("+"):
            if not raw_line.startswith("-"):
                new_line_no += 1
            continue

        # 新增行
        new_line_no += 1
        line = raw_line[1:]  # 去掉前缀 +

        # 跳过 noscan 注释
        if any(marker in line.lower() for marker in _NOSCAN_MARKERS):
            continue

        for rule in _RULES:
            if rule.pattern.search(line):
                findings.append(Finding(
                    file=current_file,
                    line_no=new_line_no,
                    rule=rule.name,
                    snippet=line.strip()[:120],
                    hint=rule.hint,
                ))
                break  # 同一行只报最高优先级规则

    return findings


def _get_staged_diff() -> tuple[str, int]:
    """获取 git diff --cached 输出；返回 (diff_text, returncode)。"""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        return result.stdout, result.returncode
    except FileNotFoundError:
        return "", 127  # git 不可用


def _get_range_diff(commit_range: str) -> tuple[str, int]:
    """获取指定 commit 范围的 diff；用于 CI 扫描。"""
    try:
        result = subprocess.run(
            ["git", "diff", commit_range],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
        )
        return result.stdout, result.returncode
    except FileNotFoundError:
        return "", 127


# ─────────────────────────────────────────────────────────────────────────────
# 安装 pre-commit hook
# ─────────────────────────────────────────────────────────────────────────────

def _build_hook_content(python_path: str) -> str:
    """
    生成 pre-commit hook 脚本内容。
    将安装时的 Python 解释器绝对路径内嵌到 hook 中，
    避免不同机器/虚拟环境里 "python" 命令指向错误解释器。
    可通过 EASYXT_PYTHON 环境变量在运行时覆盖（方便 CI 容器替换）。
    """
    # Windows 反斜杠 → Unix 正斜杠，供 sh 脚本使用
    safe_path = python_path.replace("\\", "/")
    return textwrap.dedent(f"""\
        #!/bin/sh
        # pre-commit hook: 凭据扫描（由 tools/pre_commit_scan.py --install 生成）
        # 安装时 Python: {python_path}
        # 覆盖方法: export EASYXT_PYTHON=/path/to/python
        PYTHON="${{EASYXT_PYTHON:-{safe_path}}}"
        "$PYTHON" "$(git rev-parse --show-toplevel)/tools/pre_commit_scan.py"
        if [ $? -ne 0 ]; then
            echo "[BLOCKED] 发现潜在凭据泄露，提交被阻断。"
            echo "         如需强制跳过（不推荐）: git commit --no-verify"
            exit 1
        fi
    """)


def install_hook() -> int:
    """将 pre-commit hook 写入 .git/hooks/pre-commit。"""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            capture_output=True, text=True,
        )
        git_dir = result.stdout.strip()
    except FileNotFoundError:
        print("[ERROR] git 命令不可用，无法安装 hook。", file=sys.stderr)
        return 2

    hooks_dir = Path(git_dir) / "hooks"
    hooks_dir.mkdir(exist_ok=True)
    hook_file = hooks_dir / "pre-commit"

    if hook_file.exists():
        existing = hook_file.read_text(encoding="utf-8")
        if "pre_commit_scan.py" in existing:
            print("[SKIP] pre-commit hook 已存在（含 pre_commit_scan.py），无需重复安装。")
            return 0
        # 追加：选用当前解释器路径，支持 EASYXT_PYTHON 覆盖
        safe_path = sys.executable.replace("\\", "/")
        with hook_file.open("a", encoding="utf-8") as f:
            f.write(
                f"\n# ─── 凭据扫描（追加，安装时 Python: {sys.executable}）───\n"
                f'PYTHON="${{EASYXT_PYTHON:-{safe_path}}}"\n'
                f'"$PYTHON" "$(git rev-parse --show-toplevel)/tools/pre_commit_scan.py"\n'
            )
        print(f"[OK] 已将凭据扫描追加到现有 hook: {hook_file}")
    else:
        hook_file.write_text(_build_hook_content(sys.executable), encoding="utf-8")
        # Windows 下跳过 chmod；Unix 下设置可执行位
        try:
            import stat
            hook_file.chmod(hook_file.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        except Exception:
            pass
        print(f"[OK] pre-commit hook 已安装: {hook_file}")
        print("     下次 git commit 时将自动扫描 staged 变更中的凭据。")

    return 0


# ─────────────────────────────────────────────────────────────────────────────
# 输出与主入口
# ─────────────────────────────────────────────────────────────────────────────

def print_findings(findings: list[Finding]) -> None:
    print(f"\n[pre_commit_scan] 发现 {len(findings)} 处潜在凭据泄露：\n")
    for f in findings:
        print(f"  {f.file}:{f.line_no}  [{f.rule}]")
        print(f"    代码: {f.snippet}")
        print(f"    建议: {f.hint}")
        print()


def main() -> int:
    # Windows PowerShell / CP936 管道修复
    stdout_reconfigure = getattr(sys.stdout, "reconfigure", None)
    if callable(stdout_reconfigure):
        stdout_reconfigure(encoding="utf-8")
    stderr_reconfigure = getattr(sys.stderr, "reconfigure", None)
    if callable(stderr_reconfigure):
        stderr_reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser(
        description="预提交凭据扫描：仅扫描 staged diff 新增行",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--install", action="store_true", help="安装 .git/hooks/pre-commit")
    ap.add_argument("--diff-file", metavar="FILE", help="扫描指定 patch 文件（-=stdin）")
    ap.add_argument("--range", metavar="REF..REF", help="扫描指定 commit 范围（CI 使用）")
    ap.add_argument("--quiet", action="store_true", help="无泄露时不输出任何内容")
    args = ap.parse_args()

    if args.install:
        return install_hook()

    # 获取 diff 内容
    if args.diff_file:
        if args.diff_file == "-":
            diff_text = sys.stdin.read()
        else:
            diff_text = Path(args.diff_file).read_text(encoding="utf-8", errors="replace")
        rc = 0
    elif args.range:
        diff_text, rc = _get_range_diff(args.range)
    else:
        diff_text, rc = _get_staged_diff()

    if rc == 127:
        # git 不可用（如 CI 镜像未安装 git），不阻断流程
        print("[WARN] git 不可用，跳过凭据扫描。", file=sys.stderr)
        return 2
    if not diff_text.strip():
        if not args.quiet:
            print("[pre_commit_scan] 无 staged 变更，跳过扫描。")
        return 0

    findings = scan_diff_text(diff_text)
    if not findings:
        if not args.quiet:
            print("[pre_commit_scan] OK — staged diff 未发现凭据泄露。")
        return 0

    print_findings(findings)
    print(
        "[BLOCKED] 请在提交前清理上述凭据。\n"
        "  如确认是测试占位值，可在行尾添加 # noscan 跳过该行。\n"
        "  如需批量豁免，在 tools/p0_gate_check.py 的 ALLOWLIST 中添加 AllowEntry。",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())

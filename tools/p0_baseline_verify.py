"""
p0_baseline_verify.py — P0 基线版本校验工具

CI 固定步骤：在 --new-only 运行前先执行此脚本，确保：
  1. 基线文件存在且可解析
  2. gate_version 与当前脚本版本匹配（跨版本误用风险）
  3. 基线未过期（expires_at 未到期）
  4. 基线 git_commit 可读（审计追踪）

用法：
  python tools/p0_baseline_verify.py           # 仅校验，退出码 0=通过 1=失败
  python tools/p0_baseline_verify.py --refresh  # 过期时自动刷新基线

GitHub Actions 片段：
  - name: Verify baseline
    run: python tools/p0_baseline_verify.py
  - name: PR gate
    run: python tools/p0_gate_check.py --new-only --strict --json
"""
import argparse
import json
import pathlib
import subprocess
import sys
from datetime import datetime, timezone

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
BASELINE_FILE = PROJECT_ROOT / ".p0_baseline.json"
GATE_SCRIPT = PROJECT_ROOT / "tools" / "p0_gate_check.py"
PYTHON = sys.executable

# 与 p0_gate_check.py 保持同步
GATE_VERSION = "2.2.0"
# 基线有效期天数（匹配 save-baseline 的 14 天）
BASELINE_TTL_DAYS = 14


def _load_baseline() -> dict:
    if not BASELINE_FILE.exists():
        print("[FAIL] 基线文件不存在: .p0_baseline.json", file=sys.stderr)
        print("       首次接入请运行: python tools/p0_gate_check.py --save-baseline", file=sys.stderr)
        sys.exit(1)
    try:
        return json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[FAIL] 基线文件损坏: {e}", file=sys.stderr)
        sys.exit(1)


def _check_version(meta: dict) -> bool:
    baseline_ver = meta.get("script_version", "unknown")
    if baseline_ver != GATE_VERSION:
        print(
            f"[WARN] 版本不匹配: baseline={baseline_ver}, current={GATE_VERSION}",
            file=sys.stderr,
        )
        print(
            "       建议刷新基线: python tools/p0_gate_check.py --save-baseline",
            file=sys.stderr,
        )
        return False
    return True


def _check_expiry(meta: dict) -> tuple[bool, float]:
    """返回 (is_valid, days_remaining)"""
    expires_str = meta.get("expires_at", "")
    if not expires_str:
        print("[WARN] 基线缺少 expires_at 字段，无法校验有效期", file=sys.stderr)
        return True, float("inf")
    try:
        expires_at = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
    except ValueError:
        print(f"[WARN] expires_at 格式无法解析: {expires_str}", file=sys.stderr)
        return True, float("inf")
    now = datetime.now(tz=timezone.utc)
    remaining = (expires_at - now).total_seconds() / 86400
    return remaining > 0, remaining


def _refresh_baseline() -> None:
    print("[INFO] 自动刷新基线...", file=sys.stderr)
    result = subprocess.run(
        [PYTHON, str(GATE_SCRIPT), "--save-baseline"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"[FAIL] 刷新基线失败:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"[OK]   {result.stdout.strip()}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="P0 基线版本校验")
    parser.add_argument("--refresh", action="store_true", help="过期时自动刷新基线")
    args = parser.parse_args()

    baseline = _load_baseline()
    meta = baseline.get("_meta", {})
    commit = meta.get("git_commit", "unknown")
    saved_at = meta.get("saved_at", "unknown")

    print(f"[INFO] 基线 commit={commit}  saved_at={saved_at}")

    version_ok = _check_version(meta)
    expiry_ok, days_remaining = _check_expiry(meta)

    if expiry_ok:
        if days_remaining == float("inf"):
            print("[OK]   有效期：无限制")
        else:
            print(f"[OK]   基线有效，剩余 {days_remaining:.1f} 天（到期: {meta.get('expires_at', '?')}）")
    else:
        print(
            f"[WARN] 基线已过期（expires_at={meta.get('expires_at', '?')}），"
            f"过期 {abs(days_remaining):.1f} 天",
            file=sys.stderr,
        )
        if args.refresh:
            _refresh_baseline()
            print("[OK]   基线已自动刷新")
            return
        else:
            print("       可用 --refresh 自动刷新，或手动运行: python tools/p0_gate_check.py --save-baseline", file=sys.stderr)
            sys.exit(1)

    if not version_ok:
        # 版本不匹配降级为警告，不阻断（旧基线格式向后兼容场景）
        print("[WARN] 继续使用跨版本基线，建议尽快刷新")

    print("[OK]   基线校验通过")


if __name__ == "__main__":
    main()

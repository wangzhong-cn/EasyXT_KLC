#!/usr/bin/env python3
"""
ci_gate_summary.py — CI 统一 gate 报告入口

依次调用所有 gate 脚本，汇总结果写入 logs/gate_summary_<timestamp>.json。
发布评审只看这一份报告。

用法：
  python tools/ci_gate_summary.py                             # staged 模式（pre-push 钩子）
  python tools/ci_gate_summary.py --phase 0 --tests-passed   # 含阶段退出条件
  python tools/ci_gate_summary.py --coverage-xml coverage.xml --phase 0 --tests-passed
  python tools/ci_gate_summary.py --report-only              # 只报告，不阻断

退出码：
  0 — 所有 gate 通过
  1 — 至少一个阻断级 gate 失败
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_GATE_VERSION = "1.4.0"


def _get_commit_sha() -> str:
    """获取当前 git HEAD commit sha（前 12 位），失败返回 'unknown'。"""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            text=True, cwd=str(_ROOT), timeout=5,
        )
        return out.strip()
    except Exception:
        return "unknown"


def _get_env_tag() -> str:
    """从环境变量读取 CI 环境标签，默认 'local'。"""
    return os.environ.get("CI_ENV", os.environ.get("CI_ENVIRONMENT_NAME", "local"))

_SUMMARY_DIR = _ROOT / "logs"
_SUMMARY_DIR.mkdir(exist_ok=True)


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _run(cmd: list[str], timeout: int = 120) -> tuple[int, str]:
    """运行子进程，返回 (returncode, combined_output)。"""
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(_ROOT),
            timeout=timeout,
        )
        output = (proc.stdout + proc.stderr).strip()
        return proc.returncode, output
    except subprocess.TimeoutExpired:
        return 1, f"[TIMEOUT] 子进程超时（{timeout}s）: {' '.join(cmd)}"
    except Exception as exc:
        return 1, str(exc)


def _load_audit_state() -> dict:
    """读取审计链连续失败状态文件。"""
    state_file = _ROOT / "logs" / "audit_chain_state.json"
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"consecutive_failures": 0, "last_failure_at": None, "escalated": False}


# ── Gate 运行器 ───────────────────────────────────────────────────────────────

def run_change_level_gate(report_only: bool) -> dict:
    """运行变更分级 gate（check_change_level.py）。"""
    cmd = [sys.executable, str(_ROOT / "tools" / "check_change_level.py")]
    if report_only:
        cmd.append("--report-only")
    rc, output = _run(cmd)
    level = "UNKNOWN"
    # 从输出中提取等级
    for line in output.splitlines():
        if "整体等级：" in line:
            parts = line.split("整体等级：")
            if len(parts) > 1:
                level = parts[1].strip().split()[0]
    return {
        "passed": rc == 0,
        "level": level,
        "output": output[-500:] if len(output) > 500 else output,
    }


def run_thread_lifecycle_gate(report_only: bool) -> dict:
    """运行线程生命周期 gate（check_thread_lifecycle.py）。"""
    cmd = [sys.executable, str(_ROOT / "tools" / "check_thread_lifecycle.py")]
    if not report_only:
        cmd.append("--strict")
    rc, output = _run(cmd)
    violations = 0
    for line in output.splitlines():
        m = re.search(r"发现\s*(\d+)\s*个", line)
        if m:
            violations = int(m.group(1))
            break
    # 解析 delta 机器可读摘要行
    delta_info: dict = {}
    for line in output.splitlines():
        m = re.search(r"\[DELTA\]\s+prev=(\d+)\s+curr=(\d+)\s+new=(\d+)\s+fixed=(\d+)", line)
        if m:
            delta_info = {
                "prev_violations": int(m.group(1)),
                "new_violations_this_build": int(m.group(3)),
                "fixed_violations_this_build": int(m.group(4)),
            }
            break
    return {
        "passed": rc == 0,
        "violations": violations,
        **delta_info,
        "output": output[-300:] if len(output) > 300 else output,
    }


def run_phase_exit_gate(
    phase: int | None,
    coverage_xml: str,
    tests_passed: bool,
    report_only: bool,
    allow_waiver: bool = False,
) -> dict:
    """运行阶段退出 gate（check_phase_exit.py）。如果 phase 为 None 则跳过。"""
    if phase is None:
        return {"passed": True, "skipped": True, "detail": "未指定 --phase，跳过"}
    cmd = [
        sys.executable,
        str(_ROOT / "tools" / "check_phase_exit.py"),
        "--phase", str(phase),
        "--coverage-xml", coverage_xml,
    ]
    if tests_passed:
        cmd.append("--tests-passed")
    if report_only:
        cmd.append("--report-only")
    if allow_waiver:
        cmd.append("--allow-waiver")
    rc, output = _run(cmd)
    return {
        "passed": rc == 0,
        "phase": phase,
        "output": output[-800:] if len(output) > 800 else output,
    }


def run_bare_except_gate(report_only: bool) -> dict:
    """运行 bare-except 检查 gate（check_bare_except.py）。"""
    script = _ROOT / "tools" / "check_bare_except.py"
    if not script.exists():
        return {"passed": True, "skipped": True, "detail": "check_bare_except.py 不存在，跳过"}
    cmd = [sys.executable, str(script)]
    rc, output = _run(cmd)
    return {
        "passed": rc == 0,
        "output": output[-300:] if len(output) > 300 else output,
    }


def collect_audit_chain_gate() -> dict:
    """读取审计链 state 文件，返回当前连续失败状态。"""
    state = _load_audit_state()
    escalated = state.get("escalated", False)
    consecutive_n = state.get("consecutive_failures", 0)
    return {
        "passed": not escalated,
        "consecutive_failures": consecutive_n,
        "escalated": escalated,
        "last_failure_at": state.get("last_failure_at"),
        "detail": (
            f"已升级为[阻断级]（连续失败 {consecutive_n} 次）" if escalated
            else f"正常（连续失败 {consecutive_n} 次，未触发阻断）"
        ),
    }


def run_license_compliance_gate(report_only: bool) -> dict:
    """运行 HC-4 许可证合规检查（check_license_compliance.py）。"""
    script = _ROOT / "tools" / "check_license_compliance.py"
    if not script.exists():
        return {"passed": True, "skipped": True, "detail": "check_license_compliance.py 不存在，跳过"}
    cmd = [sys.executable, str(script)]
    if not report_only:
        cmd.append("--strict")
    rc, output = _run(cmd)
    violations = output.count("[违规]")
    warnings = output.count("[警告]")
    return {
        "passed": rc == 0,
        "violations": violations,
        "warnings": warnings,
        "output": output[-400:] if len(output) > 400 else output,
    }


def run_ledger_chain_gate(report_only: bool) -> dict:
    """
    运行台账审计链验证 gate（verify_governance_ledger.py）。

    验链脚本退出码语义：
      0 — 链完整
      1 — 链断裂 / 索引 SHA-256 不匹配（告警，建议阻断）
      2 — 台账文件不存在（首次运行正常，视为通过）
    """
    script = _ROOT / "tools" / "verify_governance_ledger.py"
    if not script.exists():
        return {"passed": True, "skipped": True, "detail": "verify_governance_ledger.py 不存在，跳过",
                "warnings_count": 0, "warnings": []}
    cmd = [sys.executable, str(script), "--check-archive-index"]
    rc, output = _run(cmd)
    # 退出码 2 = 台账不存在，视为通过（首次运行）
    passed = rc in (0, 2)
    # 从脚本输出中提取 [WARN][type] 行，解析语义类型（支持后续趋势分析）
    warn_lines = [ln for ln in output.splitlines() if "[WARN][" in ln]
    warnings_list: list[dict] = []
    for ln in warn_lines:
        m = re.search(r"\[WARN\]\[(\w+)\]", ln)
        warn_type = m.group(1) if m else "unknown"
        warnings_list.append({"type": warn_type, "message": ln.strip()})
    # 按类型聚合统计警告数量（下游看板可直接消费，无需二次解析）
    warning_types_count: dict[str, int] = {}
    for w in warnings_list:
        wt = w["type"]
        warning_types_count[wt] = warning_types_count.get(wt, 0) + 1
    return {
        "passed": passed,
        "exit_code": rc,
        "detail": "台账不存在（首次运行）" if rc == 2 else ("链完整" if rc == 0 else "链断裂/索引异常"),
        "output": output[-500:] if len(output) > 500 else output,
        "warnings_count": len(warnings_list),
        "warnings": warnings_list,
        "warning_types_count": warning_types_count,
    }


def run_cross_source_consistency_gate(report_only: bool) -> dict:
    script = _ROOT / "tools" / "check_cross_source_consistency.py"
    if not script.exists():
        return {"passed": True, "skipped": True, "detail": "check_cross_source_consistency.py 不存在，跳过"}
    cmd = [
        sys.executable,
        str(script),
        "--json",
        "--sample-size",
        "20",
        "--max-alert-ratio",
        "0.25",
    ]
    if not report_only:
        cmd.append("--strict")
    rc, output = _run(cmd)
    parsed: dict = {}
    try:
        parsed = json.loads(output)
    except Exception:
        m = re.search(r"CROSS_SOURCE_CONSISTENCY_JSON=(\{.*\})", output, re.S)
        if m:
            try:
                parsed = json.loads(m.group(1))
            except Exception:
                parsed = {}
    summary = parsed.get("summary", {}) if isinstance(parsed, dict) else {}
    thresholds = parsed.get("thresholds", {}) if isinstance(parsed, dict) else {}
    return {
        "passed": rc == 0,
        "detail": parsed.get("reason", "unknown") if isinstance(parsed, dict) else "unknown",
        "pair_alert_ratio": summary.get("pair_alert_ratio", 0.0),
        "symbol_alert_ratio": summary.get("symbol_alert_ratio", 0.0),
        "sampled_symbols": summary.get("sampled_symbols", 0),
        "pair_totals": summary.get("pair_totals", {}),
        "thresholds": thresholds,
        "output": output[-500:] if len(output) > 500 else output,
    }


def _write_md_report(report_data: dict, path: "Path") -> None:
    """将 gate 汇总报告写入 Markdown 格式，便于 CI 产物展示。"""
    lines: list[str] = []
    ts = report_data.get("run_at", "")
    sha = report_data.get("commit_sha", "?")
    env = report_data.get("env", "?")
    pipeline = report_data.get("pipeline_id", "") or "N/A"
    version = report_data.get("gate_version", "?")
    overall = report_data.get("overall_ok", False)

    lines.append(f"# CI Gate 汇总报告")
    lines.append("")
    lines.append(f"| 字段 | 值 |")
    lines.append("|---|---|")
    lines.append(f"| 运行时间 | {ts} |")
    lines.append(f"| Commit | `{sha}` |")
    lines.append(f"| 环境 | {env} |")
    lines.append(f"| Pipeline | {pipeline} |")
    lines.append(f"| Gate 版本 | {version} |")
    warnings_count = report_data.get("warnings_count", 0)
    lines.append(f"| 整体结论 | {'**PASS ✅**' if overall else '**FAIL ❌**'} |")
    if warnings_count > 0:
        lines.append(f"| 非阻断告警 | ⚠️ {warnings_count} 个 warning（不阻断，请关注） |")
    lines.append("")
    lines.append("## 各 Gate 結果")
    lines.append("")
    lines.append("| Gate | 结果 | 备注 |")
    lines.append("|---|---|---|")
    _names = {
        "change_level":       "变更分级",
        "thread_lifecycle":   "线程生命周期",
        "bare_except":        "bare-except",
        "phase_exit":         "阶段退出条件",
        "audit_chain":        "审计链",
        "license_compliance": "HC-4 许可证",
        "ledger_chain":       "台账审计链验证",
        "cross_source_consistency": "跨源一致性抽检",
    }
    for key, gdata in report_data.get("gates", {}).items():
        passed = gdata.get("passed", True)
        skipped = gdata.get("skipped", False)
        status = "跳过" if skipped else ("✅ PASS" if passed else "❌ FAIL")
        note = ""
        if key == "change_level" and not skipped:
            note = f"level={gdata.get('level', '?')}"
        elif key == "thread_lifecycle" and not skipped:
            v = gdata.get('violations', 0)
            new_v = gdata.get('new_violations_this_build', 0)
            fixed_v = gdata.get('fixed_violations_this_build', 0)
            note = f"violations={v}"
            if new_v > 0:
                note += f" ↑new={new_v}"
            if fixed_v > 0:
                note += f" ↓fixed={fixed_v}"
        elif key == "audit_chain":
            note = gdata.get("detail", "")
        elif key == "license_compliance" and not skipped:
            note = f"violations={gdata.get('violations', 0)}, warnings={gdata.get('warnings', 0)}"
        elif key == "ledger_chain" and not skipped:
            note = gdata.get("detail", "")
            wc = gdata.get("warnings_count", 0)
            if wc > 0:
                wtc = gdata.get("warning_types_count", {})
                if wtc:
                    type_detail = ", ".join(
                        f"{t}×{c}"
                        for t, c in sorted(wtc.items(), key=lambda x: -x[1])
                    )
                    note += f" ⚠️ {wc} warning(s): {type_detail}"
                else:
                    note += f" ⚠️ {wc} warning(s)"
        elif key == "cross_source_consistency" and not skipped:
            note = (
                f"sampled={gdata.get('sampled_symbols', 0)}, "
                f"pair_alert_ratio={gdata.get('pair_alert_ratio', 0.0):.4f}, "
                f"symbol_alert_ratio={gdata.get('symbol_alert_ratio', 0.0):.4f}"
            )
        elif skipped:
            note = gdata.get("detail", "")
        name = _names.get(key, key)
        lines.append(f"| {name} | {status} | {note} |")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _check_waiver_status() -> None:
    """读取豁免文件，在报告头部输出倒计时告警或活跃状态。"""
    waiver_file = _ROOT / "tools" / "coverage_waiver.json"
    if not waiver_file.exists():
        return
    try:
        w = json.loads(waiver_file.read_text(encoding="utf-8"))
    except Exception:
        return
    if not w.get("enabled", False):
        return
    expires_str = w.get("expires", "")
    try:
        expires = date.fromisoformat(expires_str)
        remaining = (expires - date.today()).days
    except Exception:
        return
    if remaining < 0:
        print(f"  ⚠️  [WAIVER_EXPIRED] 覆盖率豁免已过期（{expires_str}），硬目标已自动恢复")
    elif remaining <= 3:
        print(
            f"  ⚠️  [WAIVER_EXPIRING] 覆盖率豁免还剩 {remaining} 天到期（{expires_str}），"
            f"请立即提升覆盖率至硬目标！"
        )
    else:
        try:
            target_str = f"{float(w.get('effective_target', 0)):.0%}"
        except (TypeError, ValueError):
            target_str = str(w.get("effective_target", "?"))
        aid = w.get("approval_id", "N/A")
        print(f"  [WAIVER_ACTIVE] 豁免有效（目标 {target_str}，单号 {aid}，剩余 {remaining} 天）")


# ── 主逻辑 ────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="CI 统一 gate 报告入口")
    parser.add_argument("--phase", type=int, choices=[0, 1, 2],
                        help="要检查的阶段退出条件（可选）")
    parser.add_argument("--coverage-xml", default=str(_ROOT / "coverage.xml"),
                        help="coverage.xml 路径（默认项目根 coverage.xml）")
    parser.add_argument("--tests-passed", action="store_true",
                        help="上游 pytest 步骤已通过（exit 0）")
    parser.add_argument("--report-only", action="store_true",
                        help="只输出报告，不阻断（exit 0）")
    parser.add_argument("--allow-waiver", action="store_true",
                        help="允许覆盖率豁免文件生效（覆盖率临时降级）")
    args = parser.parse_args()

    run_at = datetime.now()
    print(f"\n{'='*64}")
    print(f"  CI 统一 Gate 检查  {run_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*64}")
    _check_waiver_status()

    # ── 依次运行各 gate ──────────────────────────────────────────────────────
    gates: dict[str, dict] = {}

    print("[1/8] 变更分级检查...")
    gates["change_level"] = run_change_level_gate(args.report_only)

    print("[2/8] 线程生命周期检查...")
    gates["thread_lifecycle"] = run_thread_lifecycle_gate(args.report_only)

    print("[3/8] bare-except 检查...")
    gates["bare_except"] = run_bare_except_gate(args.report_only)

    print("[4/8] 阶段退出条件检查...")
    gates["phase_exit"] = run_phase_exit_gate(
        args.phase, args.coverage_xml, args.tests_passed, args.report_only,
        allow_waiver=getattr(args, "allow_waiver", False),
    )

    print("[5/8] 审计链状态读取...")
    gates["audit_chain"] = collect_audit_chain_gate()

    print("[6/8] HC-4 许可证合规检查...")
    gates["license_compliance"] = run_license_compliance_gate(args.report_only)

    print("[7/8] 台账审计链验证...")
    gates["ledger_chain"] = run_ledger_chain_gate(args.report_only)

    print("[8/8] 跨源一致性抽检...")
    gates["cross_source_consistency"] = run_cross_source_consistency_gate(args.report_only)

    # ── 汇总 ─────────────────────────────────────────────────────────────────
    overall_ok = all(g.get("passed", True) for g in gates.values())

    # Delta 回归置顶告警（new_violations > 0 时无论是否阻断，均置顶输出并写入报告）
    tl_data = gates.get("thread_lifecycle", {})
    _new_v = tl_data.get("new_violations_this_build", 0)
    if _new_v and _new_v > 0:
        print(
            f"\n🔴 [REGRESSION ALERT] 本次构建新引入 {_new_v} 个线程违规！"
            f"请立即修复，已写入 gate 报告，治理台账将同步记录。"
        )

    # ── 写入 JSON 报告 ────────────────────────────────────────────────────────
    ts = run_at.strftime("%Y%m%d_%H%M%S")
    pipeline_id = re.sub(
        r"[^\w\-]", "-",
        str(os.environ.get("CI_PIPELINE_ID", os.environ.get("GITHUB_RUN_ID", "local"))) or "local",
    )
    sha = _get_commit_sha()
    report_path = _SUMMARY_DIR / f"{pipeline_id}_{sha}_{ts}_gate_summary.json"
    # 汇总所有 gate 的非阻断 warnings（漏记告警等）
    all_warnings: list = []
    for _gdata in gates.values():
        w = _gdata.get("warnings", [])
        if isinstance(w, list):
            all_warnings.extend(w)
        elif isinstance(w, dict):
            all_warnings.append(w)
        elif isinstance(w, str):
            all_warnings.append({"type": "text", "message": w})
    report_data = {
        "gate_version": _GATE_VERSION,
        "commit_sha": _get_commit_sha(),
        "env": _get_env_tag(),
        "pipeline_id": os.environ.get("CI_PIPELINE_ID", os.environ.get("GITHUB_RUN_ID", "")),
        "run_at": run_at.isoformat(timespec="seconds"),
        "overall_ok": overall_ok,
        "warnings_count": len(all_warnings),
        "warnings": all_warnings,
        "gates": gates,
    }
    try:
        report_path.write_text(
            json.dumps(report_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\n  报告已写入：{report_path.relative_to(_ROOT)}")
    except OSError as exc:
        print(f"\n  [警告] 报告写入失败：{exc}")

    # ── 同时写入 Markdown 文件（便于 CI 产物展示） ───────────────────────────────
    md_path = report_path.with_suffix(".md")
    try:
        _write_md_report(report_data, md_path)
        print(f"  Markdown 已写入：{md_path.relative_to(_ROOT)}")
    except OSError as exc:
        print(f"  [警告] Markdown 写入失败：{exc}")

    # ── 打印摘要表 ────────────────────────────────────────────────────────────
    print(f"\n{'─'*64}")
    print(f"  {'Gate':<28} {'状态':<10} 备注")
    print(f"{'─'*64}")
    _names = {
        "change_level":    "变更分级",
        "thread_lifecycle": "线程生命周期",
        "bare_except":     "bare-except 检查",
        "phase_exit":      f"阶段退出条件 Phase {args.phase}" if args.phase is not None else "阶段退出条件",
        "audit_chain":     "审计链状态",
        "license_compliance": "HC-4 许可证合规",
        "ledger_chain":    "台账审计链验证",
        "cross_source_consistency": "跨源一致性抽检",
    }
    for key, gdata in gates.items():
        passed = gdata.get("passed", True)
        skipped = gdata.get("skipped", False)
        status = "跳过" if skipped else ("✓ PASS" if passed else "✗ FAIL")
        note = ""
        if key == "change_level" and not skipped:
            note = f"level={gdata.get('level', '?')}"
        elif key == "thread_lifecycle" and not skipped:
            v = gdata.get('violations', 0)
            new_v = gdata.get('new_violations_this_build', 0)
            fixed_v = gdata.get('fixed_violations_this_build', 0)
            note = f"violations={v}"
            if new_v > 0:
                note += f" ↑new={new_v}"
            if fixed_v > 0:
                note += f" ↓fixed={fixed_v}"
        elif key == "audit_chain":
            note = gdata.get("detail", "")
        elif key == "phase_exit" and skipped:
            note = gdata.get("detail", "")
        elif key == "license_compliance" and not skipped:
            note = f"violations={gdata.get('violations', 0)}, warnings={gdata.get('warnings', 0)}"
        elif key == "ledger_chain" and not skipped:
            note = gdata.get("detail", "")
            wc = gdata.get("warnings_count", 0)
            if wc > 0:
                wtc = gdata.get("warning_types_count", {})
                if wtc:
                    top_type, top_cnt = max(wtc.items(), key=lambda x: x[1])
                    note += f" ⚠{wc}w({top_type}×{top_cnt})"
                else:
                    note += f" ⚠{wc}w"
        elif key == "cross_source_consistency" and not skipped:
            note = (
                f"sampled={gdata.get('sampled_symbols', 0)} "
                f"pair_alert={gdata.get('pair_alert_ratio', 0.0):.4f} "
                f"symbol_alert={gdata.get('symbol_alert_ratio', 0.0):.4f}"
            )
        print(f"  {_names[key]:<28} {status:<10} {note}")

    print(f"{'─'*64}")
    print(f"  整体结论：{'PASS ✓' if overall_ok else 'FAIL ✗'}")
    if all_warnings:
        print(f"  ⚠️  Warnings: {len(all_warnings)} 个（不阻断，请关注慢性漏记问题）")
    print(f"{'='*64}\n")

    if args.report_only:
        return 0
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())

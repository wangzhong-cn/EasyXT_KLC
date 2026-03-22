"""
p0_trend_update.py — P0 门禁趋势看板追加工具

每次夜间巡检后运行，将本轮结果追加到趋势历史文件中，
并刷新 artifacts/p0_trend_latest.md（管理层看板）。

用法：
  python tools/p0_trend_update.py                           # 读取最新门禁结果追加
  python tools/p0_trend_update.py --gate-json <file>        # 指定门禁 JSON 文件
  python tools/p0_trend_update.py --keep-days 60            # 保留最近 N 天记录（默认 60）

Nightly 配置示例（GitHub Actions）：
  - name: Run gate
    run: python tools/p0_gate_check.py --strict --json > artifacts/p0_metrics_latest.json
  - name: Update trend
    run: python tools/p0_trend_update.py
  - name: Commit trend
    run: |
      git add artifacts/p0_trend_latest.md artifacts/p0_trend_history.json
      git diff --staged --quiet || git commit -m "chore: nightly P0 trend update"
"""
import argparse
import json
import pathlib
import subprocess
import sys
from datetime import datetime, timedelta, timezone

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
TREND_HISTORY = ARTIFACTS_DIR / "p0_trend_history.json"
TREND_DASHBOARD = ARTIFACTS_DIR / "p0_trend_latest.md"

DEFAULT_GATE_JSON = ARTIFACTS_DIR / "p0_metrics_latest.json"
DEFAULT_GOV_JSON = ARTIFACTS_DIR / "governance_metrics_latest.json"
DEFAULT_KEEP_DAYS = 60


def _contract_health(valid: bool, version: int, error: str) -> str:
    if not bool(valid):
        return "BROKEN"
    if int(version or 0) <= 0:
        return "BROKEN"
    if str(error or "").strip():
        return "BROKEN"
    return "HEALTHY"


def _load_gate(gate_json: pathlib.Path) -> dict:
    if not gate_json.exists():
        print(f"[FAIL] 门禁结果文件不存在: {gate_json}", file=sys.stderr)
        sys.exit(1)
    return json.loads(gate_json.read_text(encoding="utf-8"))


def _load_history() -> list:
    if not TREND_HISTORY.exists():
        return []
    try:
        return json.loads(TREND_HISTORY.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def _save_history(history: list, keep_days: int) -> list:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=keep_days)
    pruned = [
        r for r in history
        if datetime.fromisoformat(r["ts"].replace("Z", "+00:00")) >= cutoff
    ]
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    TREND_HISTORY.write_text(json.dumps(pruned, indent=2, ensure_ascii=False), encoding="utf-8")
    return pruned


def _make_row(gate: dict, ts: str) -> dict:
    period_validation_obj = gate.get("period_validation_detail")
    period_validation_detail: dict = period_validation_obj if isinstance(period_validation_obj, dict) else {}
    gate_contract_valid = bool(gate.get("gate_contract_valid", False))
    gate_contract_version = int(gate.get("gate_contract_version", 0) or 0)
    gate_contract_error = str(gate.get("gate_contract_error", "") or "")
    p0b_obj = gate.get("p0b_concurrent_detail")
    p0b_detail: dict = p0b_obj if isinstance(p0b_obj, dict) else {}
    api_smoke_obj = gate.get("api_smoke_detail")
    api_smoke_detail: dict = api_smoke_obj if isinstance(api_smoke_obj, dict) else {}
    row: dict = {
        "ts": ts,
        "strict_gate_pass": gate.get("strict_gate_pass", False),
        "P0_open": gate.get("P0_open_count", -1),
        "ach": gate.get("active_critical_high", -1),
        "allowlist_total": gate.get("allowlist_total", 0),
        "allowlist_expired": gate.get("allowlist_expired", 0),
        "allowlist_due_90d": gate.get("allowlist_due_90d", 0),
        "period_validation_status": str(period_validation_detail.get("status") or ""),
        "period_validation_failed_items": int(period_validation_detail.get("failed_items", 0) or 0),
        "gate_contract_valid": gate_contract_valid,
        "gate_contract_version": gate_contract_version,
        "gate_contract_error": gate_contract_error,
        "gate_contract_rag": str(gate.get("gate_contract_rag", "") or ""),
        "gate_detail_tag": str(gate.get("gate_detail_tag", "") or ""),
        "contract_health": _contract_health(gate_contract_valid, gate_contract_version, gate_contract_error),
        "p0b_concurrent_status": str(p0b_detail.get("status") or ""),
        "p0b_concurrent_failed_items": int(p0b_detail.get("failed_items", 0) or 0),
        "p0b_concurrent_message": str(p0b_detail.get("message") or ""),
        "api_smoke_status": str(api_smoke_detail.get("status") or ""),
        "api_smoke_failed_items": int(api_smoke_detail.get("failed_items", 0) or 0),
        "api_smoke_message": str(api_smoke_detail.get("message") or ""),
        "checks": {},
    }
    checks = gate.get("checks", [])
    if isinstance(checks, list):
        for c in checks:
            if not isinstance(c, dict):
                continue
            name = str(c.get("name") or "")
            if not name:
                continue
            viol = c.get("violations", [])
            row["checks"][name] = {
                "status": c.get("status", "?"),
                "violations": len(viol) if isinstance(viol, list) else 0,
            }
    return row


def _merge_governance(row: dict, governance: dict | None) -> dict:
    gov = governance or {}
    step6 = gov.get("step6_validation", {}) if isinstance(gov, dict) else {}
    row["step6_total"] = int(step6.get("total", 0) or 0)
    row["step6_sampled"] = int(step6.get("sampled", 0) or 0)
    row["step6_skipped"] = int(step6.get("skipped", 0) or 0)
    row["step6_hard_failed"] = int(step6.get("hard_failed", 0) or 0)
    row["step6_hard_fail_rate"] = float(step6.get("hard_fail_rate", 0.0) or 0.0)
    sla = gov.get("sla", {}) if isinstance(gov, dict) else {}
    row["canary_shadow_write_enabled"] = bool(sla.get("canary_shadow_write_enabled", False))
    row["canary_shadow_only"] = bool(sla.get("canary_shadow_only", True))
    impact = gov.get("strategy_impact", {}) if isinstance(gov, dict) else {}
    row["strategy_impact_available"] = bool(impact.get("available", False))
    row["strategy_impact_gate_pass"] = bool(impact.get("gate_pass", True))
    delta = impact.get("delta", {}) if isinstance(impact.get("delta"), dict) else {}
    row["strategy_impact_delta_return"] = float(delta.get("annualized_return_pct", 0.0) or 0.0)
    row["strategy_impact_delta_mdd"] = float(delta.get("max_drawdown_pct", 0.0) or 0.0)
    row["strategy_impact_sharpe_sign_changed"] = bool(delta.get("sharpe_sign_changed", False))
    return row


def _render_dashboard(history: list) -> str:
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    latest = history[-1] if history else {}
    sgp = latest.get("strict_gate_pass", False)
    ach = latest.get("ach", -1)
    po = latest.get("P0_open", -1)
    pv_failed = int(latest.get("period_validation_failed_items", 0) or 0)
    p0b_status = str(latest.get("p0b_concurrent_status") or "missing")
    p0b_failed = int(latest.get("p0b_concurrent_failed_items", 0) or 0)
    api_smoke_status = str(latest.get("api_smoke_status") or "missing")
    api_smoke_failed = int(latest.get("api_smoke_failed_items", 0) or 0)
    gate_contract_valid = bool(latest.get("gate_contract_valid", False))
    gate_contract_version = int(latest.get("gate_contract_version", 0) or 0)
    gate_contract_error = str(latest.get("gate_contract_error", "") or "")
    gate_contract_rag = str(latest.get("gate_contract_rag", "") or "")
    gate_detail_tag = str(latest.get("gate_detail_tag", "") or "")
    contract_health = str(latest.get("contract_health") or _contract_health(gate_contract_valid, gate_contract_version, gate_contract_error))

    # 趋势列表（最近 14 条）
    recent = history[-14:]
    trend_rows = ""
    for r in recent:
        date = r["ts"][:10]
        gate_ok = "✅" if r.get("strict_gate_pass") else "❌"
        contract_ok = "✅" if bool(r.get("gate_contract_valid", False)) else "❌"
        p0b_sym = "✅" if str(r.get("p0b_concurrent_status") or "") == "pass" else "❌"
        api_smoke_sym = "✅" if str(r.get("api_smoke_status") or "") == "pass" else "❌"
        contract_health_item = str(r.get("contract_health") or _contract_health(bool(r.get("gate_contract_valid", False)), int(r.get("gate_contract_version", 0) or 0), str(r.get("gate_contract_error", "") or "")))
        trend_rows += (
            f"| {date} | {gate_ok} | {r.get('P0_open', '?')} | {r.get('ach', '?')} "
            f"| {r.get('period_validation_failed_items', 0)} | {p0b_sym} | {api_smoke_sym} | {contract_ok} | {contract_health_item} |\n"
        )

    # 各检查项状态（取最新一条）
    check_rows = ""
    if latest.get("checks"):
        for name, info in latest["checks"].items():
            sym = "✅" if info["status"] == "pass" else "❌"
            check_rows += f"| {name} | {sym} | {info['violations']} |\n"

    overall_sym = "✅ PASS" if sgp else "❌ FAIL"

    md = f"""# P0 门禁趋势看板

> 最后更新: {now_str}

## 当前状态

| 指标 | 值 |
|---|---|
| strict_gate_pass | **{overall_sym}** |
| P0_open_count | {po} |
| active_critical_high | {ach} |
| period_validation_failed_items | {pv_failed} |
| p0b_concurrent_status | {p0b_status} |
| p0b_concurrent_failed_items | {p0b_failed} |
| api_smoke_status | {api_smoke_status} |
| api_smoke_failed_items | {api_smoke_failed} |
| gate_contract_valid | {'✅ True' if gate_contract_valid else '❌ False'} |
| gate_contract_version | {gate_contract_version} |
| gate_contract_rag | {gate_contract_rag or 'N/A'} |
| gate_contract_error | {gate_contract_error or 'N/A'} |
| gate_detail_tag | {gate_detail_tag or 'N/A'} |
| contract_health | **{contract_health}** |
| 历史记录条数 | {len(history)} |

## 最近 14 次巡检

| 日期 | 门禁 | P0_open | active_crit_high | period_validation_failed | p0b_concurrent | api_smoke | gate_contract_ok | contract_health |
|---|---|---|---|---|---|---|---|---|
{trend_rows}
## 最新各检查项状态

| 检查项 | 状态 | 活跃违规 |
|---|---|---|
{check_rows}
## 说明

- **strict_gate_pass** = `P0_open_count == 0 AND active_critical_high == 0`
- 数据来源: `artifacts/p0_trend_history.json`（保留最近 {DEFAULT_KEEP_DAYS} 天）
- 夜间巡检命令: `python tools/p0_gate_check.py --strict --json`
- 趋势更新命令: `python tools/p0_trend_update.py`

---
_此文件由自动化脚本生成，请勿手动编辑_
"""
    return md


def main() -> None:
    parser = argparse.ArgumentParser(description="P0 趋势看板更新")
    parser.add_argument("--gate-json", type=pathlib.Path, default=DEFAULT_GATE_JSON,
                        help="门禁结果 JSON 文件路径")
    parser.add_argument("--governance-json", type=pathlib.Path, default=DEFAULT_GOV_JSON,
                        help="governance_jobs 输出 JSON 文件路径（可选）")
    parser.add_argument("--keep-days", type=int, default=DEFAULT_KEEP_DAYS,
                        help=f"历史保留天数（默认 {DEFAULT_KEEP_DAYS}）")
    parser.add_argument("--run-governance-jobs", action="store_true")
    parser.add_argument("--strict-sla", action="store_true")
    parser.add_argument("--strict-dead-letter", action="store_true")
    parser.add_argument("--duckdb-path", type=str, default=None)
    args = parser.parse_args()

    if args.run_governance_jobs:
        cmd = [sys.executable, str(PROJECT_ROOT / "tools" / "governance_jobs.py"), "--job", "all"]
        if args.strict_sla:
            cmd.append("--strict-sla")
        if args.strict_dead_letter:
            cmd.append("--strict-dead-letter")
        if args.duckdb_path:
            cmd.extend(["--duckdb-path", args.duckdb_path])
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            print("[FAIL] governance_jobs 执行失败，终止趋势更新", file=sys.stderr)
            sys.exit(result.returncode)
        try:
            ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
            args.governance_json.write_text((result.stdout or "").strip() + "\n", encoding="utf-8")
        except Exception as e:
            print(f"[WARN] 写 governance latest 失败: {e}", file=sys.stderr)

    gate = _load_gate(args.gate_json)
    governance = None
    if args.governance_json.exists():
        try:
            governance = json.loads(args.governance_json.read_text(encoding="utf-8"))
        except Exception:
            governance = None
    history = _load_history()

    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = _make_row(gate, ts)
    row = _merge_governance(row, governance)
    history.append(row)

    pruned = _save_history(history, args.keep_days)

    # 刷新看板
    md = _render_dashboard(pruned)
    TREND_DASHBOARD.write_text(md, encoding="utf-8")

    sgp = gate.get("strict_gate_pass", False)
    print("[OK] 趋势看板已更新: artifacts/p0_trend_latest.md")
    print(f"     本次: strict_gate_pass={sgp}  P0_open={gate.get('P0_open_count')}  ach={gate.get('active_critical_high')}")
    print(f"     合约健康: {row.get('contract_health', 'BROKEN')}  gate_contract_valid={row.get('gate_contract_valid', False)}  gate_contract_version={row.get('gate_contract_version', 0)}")
    print(f"     历史记录: {len(history)} 条（保留最近 {args.keep_days} 天）")


if __name__ == "__main__":
    main()

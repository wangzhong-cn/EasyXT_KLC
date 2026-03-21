from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from tools.strategy_impact_gate import evaluate_strategy_impact
except Exception:
    from strategy_impact_gate import evaluate_strategy_impact

try:
    from tools.gate4_perf_gate import check_regression as _perf_check_regression
    from tools.gate4_perf_gate import run_sample as _perf_run_sample
except Exception:
    from gate4_perf_gate import check_regression as _perf_check_regression  # type: ignore[no-redef]
    from gate4_perf_gate import run_sample as _perf_run_sample  # type: ignore[no-redef]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"
DEFAULT_OUTPUT = ARTIFACTS_DIR / "gate_pipeline_latest.json"
DEFAULT_GATE4_OUTPUT = ARTIFACTS_DIR / "strategy_impact_latest.json"


@dataclass
class StepResult:
    name: str
    passed: bool
    rc: int
    detail: str
    command: list[str]
    output_tail: str


def _run_cmd(command: list[str], cwd: Path | None = None, env: dict[str, str] | None = None) -> tuple[int, str]:
    proc = subprocess.run(
        command,
        cwd=str(cwd or PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    merged = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return int(proc.returncode), merged


def _tail(text: str, n: int = 2000) -> str:
    if len(text) <= n:
        return text
    return text[-n:]


def _pytest_step(name: str, test_file: str) -> StepResult:
    cmd = [sys.executable, "-m", "pytest", test_file, "-q", "--tb=short"]
    rc, out = _run_cmd(cmd)
    return StepResult(
        name=name,
        passed=rc == 0,
        rc=rc,
        detail=f"{test_file} {'passed' if rc == 0 else 'failed'}",
        command=cmd,
        output_tail=_tail(out),
    )


def _gate3_step() -> StepResult:
    cmd = [sys.executable, str(PROJECT_ROOT / "tools" / "p0_gate_check.py"), "--strict", "--json"]
    rc, out = _run_cmd(cmd)
    fallback_used = False
    if (
        "period_validation_report.jsonl" in str(out)
        and os.environ.get("EASYXT_GATE3_AUTO_BUILD_PERIOD_VALIDATION", "1").lower() in ("1", "true")
    ):
        fallback_used = True
        _run_cmd(
            [
                sys.executable,
                str(PROJECT_ROOT / "tools" / "run_period_validation.py"),
                "--json",
            ]
        )
        rc, out = _run_cmd(cmd)
    if "period_validation_report.jsonl" in str(out):
        release_env = os.environ.get("EASYXT_RELEASE_ENV", "preprod").strip().lower()
        if release_env == "prod":
            # prod 禁止静默 fallback：报告缺失直接阻断，不降级 FAIL_BLOCK
            pass
        else:
            fallback_used = True
            env = os.environ.copy()
            env["EASYXT_PERIOD_VALIDATION_FAIL_BLOCK"] = "0"
            rc, out = _run_cmd(cmd, env=env)
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(out.strip() or "{}")
    except Exception:
        payload = {}
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS_DIR / "p0_metrics_latest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    strict_gate_pass = bool(payload.get("strict_gate_pass", False))
    contract_health = str(payload.get("contract_health", "") or "")
    passed = rc == 0 and strict_gate_pass and contract_health == "HEALTHY"
    detail = (
        f"strict_gate_pass={strict_gate_pass} contract_health={contract_health} "
        f"P0_open_count={int(payload.get('P0_open_count', 0) or 0)} "
        f"period_validation_fallback={str(fallback_used).lower()}"
    )
    return StepResult(
        name="Gate3-ReleaseHealth",
        passed=passed,
        rc=0 if passed else (rc or 1),
        detail=detail,
        command=cmd,
        output_tail=_tail(out),
    )


def _gate4_step(require_available: bool) -> StepResult:
    # ── Strategy impact ──────────────────────────────────────────────────────
    baseline = Path(os.environ.get("EASYXT_STRATEGY_IMPACT_BASELINE", str(PROJECT_ROOT / "artifacts" / "strategy_impact_baseline.json")))
    if not baseline.is_absolute():
        baseline = PROJECT_ROOT / baseline
    result = evaluate_strategy_impact(
        baseline_path=baseline,
        delta_return_threshold=float(os.environ.get("EASYXT_STRATEGY_IMPACT_DELTA_RETURN", "3.0") or 3.0),
        delta_mdd_threshold=float(os.environ.get("EASYXT_STRATEGY_IMPACT_DELTA_MDD", "1.5") or 1.5),
        enforce_sharpe_sign=(os.environ.get("EASYXT_STRATEGY_IMPACT_ENFORCE_SHARPE_SIGN", "true").lower() in ("1", "true")),
    )
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_GATE4_OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    available = bool(result.get("available", False))
    impact_gate_pass = bool(result.get("gate_pass", True))
    strategy_passed = (available and impact_gate_pass) or ((not available) and (not require_available))
    strategy_detail = (
        f"available={available} gate_pass={impact_gate_pass} "
        f"require_available={require_available} reason={result.get('reason', '')}"
    )

    # ── Performance gate ─────────────────────────────────────────────────────
    perf_baseline_path = Path(
        os.environ.get("EASYXT_GATE4_PERF_BASELINE", str(ARTIFACTS_DIR / "gate4_perf_baseline.json"))
    )
    if not perf_baseline_path.is_absolute():
        perf_baseline_path = PROJECT_ROOT / perf_baseline_path
    perf_threshold = float(os.environ.get("EASYXT_GATE4_PERF_THRESHOLD", "0.20") or 0.20)
    perf_n_runs = int(os.environ.get("EASYXT_GATE4_PERF_N_RUNS", "50") or 50)
    perf_latest_path = ARTIFACTS_DIR / "gate4_perf_latest.json"
    allow_bootstrap = os.environ.get("EASYXT_GATE4_ALLOW_BOOTSTRAP", "1").strip() == "1"

    if not perf_baseline_path.exists():
        if not allow_bootstrap:
            # 生产模式：基线缺失时明确失败，而非静默写入
            perf_passed = False
            perf_detail = "perf=baseline_missing bootstrap_disabled(EASYXT_GATE4_ALLOW_BOOTSTRAP=0)"
            perf_latest_path.parent.mkdir(parents=True, exist_ok=True)
            perf_latest_path.write_text(
                json.dumps({"status": "baseline_missing_bootstrap_disabled", "perf_passed": False},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        else:
            # 首次运行：自动采样并写入基线，不阻断
            sample_data = _perf_run_sample(n_runs=perf_n_runs)
            perf_baseline_path.parent.mkdir(parents=True, exist_ok=True)
            perf_baseline_path.write_text(json.dumps(sample_data, ensure_ascii=False, indent=2), encoding="utf-8")
            perf_latest_path.write_text(json.dumps(
                {"status": "baseline_bootstrapped", "sample": sample_data}, ensure_ascii=False, indent=2
            ), encoding="utf-8")
            perf_passed = True
            perf_detail = "perf=baseline_bootstrapped regressions=0"
    else:
        current_data = _perf_run_sample(n_runs=perf_n_runs)
        try:
            baseline_data = json.loads(perf_baseline_path.read_text(encoding="utf-8"))
        except Exception:
            baseline_data = {}
        reg = _perf_check_regression(current_data, baseline_data, threshold=perf_threshold)
        perf_passed = bool(reg.get("gate_pass", True))
        reg_count = int(reg.get("regression_count", 0))
        perf_detail = f"perf_regressions={reg_count} threshold={perf_threshold}"
        perf_latest_path.write_text(
            json.dumps({"status": "regression_blocked" if not perf_passed else "pass",
                        "sample": current_data, **reg}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    passed = strategy_passed and perf_passed
    detail = f"{strategy_detail} | {perf_detail}"
    return StepResult(
        name="Gate4-StrategyImpact",
        passed=passed,
        rc=0 if passed else 1,
        detail=detail,
        command=["internal:evaluate_strategy_impact+perf"],
        output_tail=_tail(json.dumps(
            {**result, "perf_passed": perf_passed, "perf_detail": perf_detail}, ensure_ascii=False
        )),
    )


def run_gate_pipeline(*, gate4_require_available: bool = False) -> dict[str, Any]:
    ordered: list[StepResult] = []
    ordered.append(_pytest_step("Gate2-StrategyRegistration", "tests/test_gate2_strategy_registration.py"))
    if not ordered[-1].passed:
        return _build_output(ordered)
    ordered.append(_pytest_step("Gate1-DataQuality", "tests/test_gate1_data_quality.py"))
    if not ordered[-1].passed:
        return _build_output(ordered)
    ordered.append(_pytest_step("RuntimeFuse-Realtime", "tests/test_main_window_realtime_fuse.py"))
    if not ordered[-1].passed:
        return _build_output(ordered)
    ordered.append(_gate3_step())
    if not ordered[-1].passed:
        return _build_output(ordered)
    ordered.append(_gate4_step(require_available=gate4_require_available))
    return _build_output(ordered)


def _build_output(results: list[StepResult]) -> dict[str, Any]:
    passed = all(s.passed for s in results)
    failed_step = next((s.name for s in results if not s.passed), "")
    return {
        "generated_at": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall_passed": passed,
        "failed_step": failed_step,
        "steps": [asdict(s) for s in results],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--gate4-require-available", action="store_true")
    args = parser.parse_args()

    report = run_gate_pipeline(gate4_require_available=bool(args.gate4_require_available))

    out = Path(args.output)
    if not out.is_absolute():
        out = PROJECT_ROOT / out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(f"[gate_pipeline] overall_passed={report['overall_passed']} failed_step={report['failed_step'] or 'none'}")
        print(f"[gate_pipeline] written={out}")

    return 0 if bool(report["overall_passed"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())

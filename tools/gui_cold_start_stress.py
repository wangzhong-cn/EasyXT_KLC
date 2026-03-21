from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class StartResult:
    run_index: int
    mode: str
    ok: bool
    started: bool
    reason: str
    return_code: int | None
    duration_sec: float
    output_tail: str


def _plan_modes(runs: int) -> list[str]:
    runs = max(1, int(runs))
    half = runs // 2
    modes = ["trading"] * half + ["non_trading"] * (runs - half)
    if not modes:
        modes = ["non_trading"]
    return modes


def _build_env(mode: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["EASYXT_ENABLE_QMT_ONLINE"] = env.get("EASYXT_ENABLE_QMT_ONLINE", "0")
    env["EASYXT_ENABLE_ACTIVE_PROBE"] = env.get("EASYXT_ENABLE_ACTIVE_PROBE", "0")
    env["EASYXT_ENABLE_BROKER_WARMUP"] = env.get("EASYXT_ENABLE_BROKER_WARMUP", "0")
    env["EASYXT_HEALTH_IMPORT_EASYXT"] = env.get("EASYXT_HEALTH_IMPORT_EASYXT", "0")
    env["EASYXT_ENABLE_PROBE_SUBPROCESS_ISOLATION"] = env.get("EASYXT_ENABLE_PROBE_SUBPROCESS_ISOLATION", "1")
    env["EASYXT_ENABLE_QTWEBENGINE_SAFE_CACHE"] = env.get("EASYXT_ENABLE_QTWEBENGINE_SAFE_CACHE", "1")
    env["EASYXT_RT_XTDATA_ONLY"] = "1" if mode == "trading" else "0"
    return env


def _tail_text(text: str, max_chars: int = 1800) -> str:
    value = str(text or "")
    if len(value) <= max_chars:
        return value
    return value[-max_chars:]


def _run_once(
    python_exe: str,
    repo_root: Path,
    mode: str,
    run_index: int,
    warmup_sec: float,
    startup_timeout_sec: float,
) -> StartResult:
    cmd = [python_exe, str((repo_root / "gui_app" / "main_window.py").resolve())]
    env = _build_env(mode)
    started_ts = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    merged = ""
    deadline = time.perf_counter() + startup_timeout_sec
    ok_marker = False
    while time.perf_counter() < deadline:
        if proc.poll() is not None:
            break
        line = proc.stdout.readline() if proc.stdout is not None else ""
        if line:
            merged += line
            if "reason=workspace_ready" in line or "[BACKTEST_ENGINE]" in line or "HEALTH][post-lazy]" in line:
                ok_marker = True
                break
        else:
            time.sleep(0.05)
    if proc.poll() is not None:
        duration = time.perf_counter() - started_ts
        rest = ""
        if proc.stdout is not None:
            rest = proc.stdout.read() or ""
        merged = merged + rest
        return StartResult(
            run_index=run_index,
            mode=mode,
            ok=False,
            started=False,
            reason="process_exited_early",
            return_code=proc.returncode,
            duration_sec=duration,
            output_tail=_tail_text(merged),
        )
    wait_deadline = time.perf_counter() + max(0.5, warmup_sec)
    while time.perf_counter() < wait_deadline and proc.poll() is None:
        if proc.stdout is not None:
            line = proc.stdout.readline()
            if line:
                merged += line
        time.sleep(0.03)
    if proc.poll() is not None:
        duration = time.perf_counter() - started_ts
        rest = ""
        if proc.stdout is not None:
            rest = proc.stdout.read() or ""
        merged = merged + rest
        return StartResult(
            run_index=run_index,
            mode=mode,
            ok=False,
            started=ok_marker,
            reason="crashed_during_warmup",
            return_code=proc.returncode,
            duration_sec=duration,
            output_tail=_tail_text(merged),
        )
    try:
        proc.terminate()
        proc.wait(timeout=4)
    except Exception:
        try:
            proc.kill()
            proc.wait(timeout=2)
        except Exception:
            pass
    duration = time.perf_counter() - started_ts
    rest = ""
    if proc.stdout is not None:
        rest = proc.stdout.read() or ""
    merged = merged + rest
    text = merged.lower()
    if "assertion failed:" in text or "bsonobj.cpp" in text:
        return StartResult(
            run_index=run_index,
            mode=mode,
            ok=False,
            started=ok_marker,
            reason="native_assert_detected",
            return_code=proc.returncode,
            duration_sec=duration,
            output_tail=_tail_text(merged),
        )
    return StartResult(
        run_index=run_index,
        mode=mode,
        ok=True,
        started=ok_marker,
        reason="stable_window_passed",
        return_code=proc.returncode,
        duration_sec=duration,
        output_tail=_tail_text(merged),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--warmup-sec", type=float, default=4.0)
    parser.add_argument("--startup-timeout-sec", type=float, default=25.0)
    parser.add_argument("--python", type=str, default=sys.executable)
    parser.add_argument("--output", type=str, default="artifacts/gui_cold_start_stress_latest.json")
    args = parser.parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    modes = _plan_modes(int(args.runs))
    results: list[StartResult] = []
    for idx, mode in enumerate(modes, start=1):
        r = _run_once(
            python_exe=args.python,
            repo_root=repo_root,
            mode=mode,
            run_index=idx,
            warmup_sec=float(args.warmup_sec),
            startup_timeout_sec=float(args.startup_timeout_sec),
        )
        results.append(r)
        print(
            f"[run {idx:02d}/{len(modes)}] mode={mode} ok={r.ok} reason={r.reason} rc={r.return_code} dur={r.duration_sec:.2f}s"
        )
    ok_count = sum(1 for r in results if r.ok)
    fail_count = len(results) - ok_count
    trading_total = sum(1 for r in results if r.mode == "trading")
    trading_ok = sum(1 for r in results if r.mode == "trading" and r.ok)
    non_total = sum(1 for r in results if r.mode == "non_trading")
    non_ok = sum(1 for r in results if r.mode == "non_trading" and r.ok)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "runs": len(results),
        "pass_count": ok_count,
        "fail_count": fail_count,
        "pass_rate": round(ok_count / max(1, len(results)), 6),
        "trading": {"runs": trading_total, "pass": trading_ok, "fail": trading_total - trading_ok},
        "non_trading": {"runs": non_total, "pass": non_ok, "fail": non_total - non_ok},
        "overall_ok": fail_count == 0,
        "results": [asdict(r) for r in results],
    }
    out_path = (repo_root / args.output).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {out_path}")
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

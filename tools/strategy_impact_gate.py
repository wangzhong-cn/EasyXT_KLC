from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = PROJECT_ROOT / "strategies" / "results"
DEFAULT_BASELINE_PATH = PROJECT_ROOT / "artifacts" / "strategy_impact_baseline.json"


def _sign(v: float, eps: float = 1e-9) -> int:
    if v > eps:
        return 1
    if v < -eps:
        return -1
    return 0


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_latest_stage1_result(results_dir: Path) -> dict[str, Any] | None:
    if not results_dir.exists():
        return None
    files = sorted(results_dir.glob("stage1_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return None
    return _load_json(files[-1])


def _load_baseline(path: Path) -> dict[str, Any] | None:
    payload = _load_json(path)
    if not isinstance(payload, dict):
        return None
    return payload


def evaluate_strategy_impact(
    *,
    baseline_path: Path = DEFAULT_BASELINE_PATH,
    results_dir: Path = RESULTS_DIR,
    delta_return_threshold: float = 3.0,
    delta_mdd_threshold: float = 1.5,
    enforce_sharpe_sign: bool = True,
) -> dict[str, Any]:
    current = _load_latest_stage1_result(results_dir)
    baseline_payload = _load_baseline(baseline_path)
    if not current or not baseline_payload:
        return {
            "available": False,
            "gate_pass": True,
            "reason": "baseline_or_current_missing",
            "delta_return_threshold": float(delta_return_threshold),
            "delta_mdd_threshold": float(delta_mdd_threshold),
            "enforce_sharpe_sign": bool(enforce_sharpe_sign),
            "comparisons": [],
        }
    baselines = baseline_payload.get("strategies", [])
    if not isinstance(baselines, list):
        baselines = []
    strategy = str(current.get("strategy", "") or "")
    symbol = str(current.get("symbol", "") or "")
    matched = None
    for b in baselines:
        if not isinstance(b, dict):
            continue
        if str(b.get("strategy", "")) == strategy and str(b.get("symbol", "")) == symbol:
            matched = b
            break
    if matched is None:
        return {
            "available": False,
            "gate_pass": True,
            "reason": "baseline_not_matched",
            "strategy": strategy,
            "symbol": symbol,
            "delta_return_threshold": float(delta_return_threshold),
            "delta_mdd_threshold": float(delta_mdd_threshold),
            "enforce_sharpe_sign": bool(enforce_sharpe_sign),
            "comparisons": [],
        }
    bm = current.get("backtest_metrics", {}) if isinstance(current.get("backtest_metrics"), dict) else {}
    cur_ret = float(bm.get("annualized_return_pct", 0.0) or 0.0)
    cur_mdd = float(bm.get("max_drawdown_pct", 0.0) or 0.0)
    cur_sharpe = float(bm.get("sharpe_ratio", 0.0) or 0.0)
    base_ret = float(matched.get("annualized_return_pct", 0.0) or 0.0)
    base_mdd = float(matched.get("max_drawdown_pct", 0.0) or 0.0)
    base_sharpe = float(matched.get("sharpe_ratio", 0.0) or 0.0)
    d_ret = cur_ret - base_ret
    d_mdd = cur_mdd - base_mdd
    sharpe_sign_changed = _sign(cur_sharpe) != _sign(base_sharpe)
    checks = {
        "delta_return_pass": abs(d_ret) <= float(delta_return_threshold),
        "delta_mdd_pass": abs(d_mdd) <= float(delta_mdd_threshold),
        "sharpe_sign_pass": (not enforce_sharpe_sign) or (not sharpe_sign_changed),
    }
    gate_pass = bool(checks["delta_return_pass"] and checks["delta_mdd_pass"] and checks["sharpe_sign_pass"])
    return {
        "available": True,
        "gate_pass": gate_pass,
        "strategy": strategy,
        "symbol": symbol,
        "delta_return_threshold": float(delta_return_threshold),
        "delta_mdd_threshold": float(delta_mdd_threshold),
        "enforce_sharpe_sign": bool(enforce_sharpe_sign),
        "current": {
            "annualized_return_pct": cur_ret,
            "max_drawdown_pct": cur_mdd,
            "sharpe_ratio": cur_sharpe,
        },
        "baseline": {
            "annualized_return_pct": base_ret,
            "max_drawdown_pct": base_mdd,
            "sharpe_ratio": base_sharpe,
        },
        "delta": {
            "annualized_return_pct": d_ret,
            "max_drawdown_pct": d_mdd,
            "sharpe_sign_changed": sharpe_sign_changed,
        },
        "checks": checks,
        "comparisons": [
            {
                "metric": "annualized_return_pct",
                "current": cur_ret,
                "baseline": base_ret,
                "delta": d_ret,
                "threshold_abs": float(delta_return_threshold),
                "pass": checks["delta_return_pass"],
            },
            {
                "metric": "max_drawdown_pct",
                "current": cur_mdd,
                "baseline": base_mdd,
                "delta": d_mdd,
                "threshold_abs": float(delta_mdd_threshold),
                "pass": checks["delta_mdd_pass"],
            },
            {
                "metric": "sharpe_sign",
                "current": _sign(cur_sharpe),
                "baseline": _sign(base_sharpe),
                "delta": None,
                "threshold_abs": None,
                "pass": checks["sharpe_sign_pass"],
            },
        ],
    }

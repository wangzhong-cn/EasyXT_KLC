from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE = PROJECT_ROOT / "artifacts" / "strategy_impact_baseline.json"
DEFAULT_RESULTS = PROJECT_ROOT / "strategies" / "results"
LEDGER_PATH = PROJECT_ROOT / "logs" / "strategy_impact_baseline_ledger.jsonl"


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _commit_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short=12", "HEAD"],
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=5,
        ).strip()
    except Exception:
        return "unknown"


def _hash_text(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_latest_stage1(results_dir: Path) -> dict[str, Any]:
    files = sorted(results_dir.glob("stage1_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise RuntimeError(f"未找到 Stage1 结果文件: {results_dir}")
    payload = _load_json(files[-1])
    if payload is None:
        raise RuntimeError(f"Stage1 结果文件不可解析: {files[-1]}")
    return payload


def _upsert_strategy_entry(
    baseline: dict[str, Any],
    *,
    strategy: str,
    symbol: str,
    annualized_return_pct: float,
    max_drawdown_pct: float,
    sharpe_ratio: float,
) -> dict[str, Any]:
    strategies = baseline.get("strategies", [])
    if not isinstance(strategies, list):
        strategies = []
    replaced = False
    for i, row in enumerate(strategies):
        if not isinstance(row, dict):
            continue
        if str(row.get("strategy", "")) == strategy and str(row.get("symbol", "")) == symbol:
            strategies[i] = {
                "strategy": strategy,
                "symbol": symbol,
                "annualized_return_pct": float(annualized_return_pct),
                "max_drawdown_pct": float(max_drawdown_pct),
                "sharpe_ratio": float(sharpe_ratio),
            }
            replaced = True
            break
    if not replaced:
        strategies.append(
            {
                "strategy": strategy,
                "symbol": symbol,
                "annualized_return_pct": float(annualized_return_pct),
                "max_drawdown_pct": float(max_drawdown_pct),
                "sharpe_ratio": float(sharpe_ratio),
            }
        )
    baseline["strategies"] = strategies
    return baseline


def _append_ledger(record: dict[str, Any], path: Path | None = None) -> None:
    p = path or LEDGER_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="策略影响 baseline 生命周期管理")
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--approver", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--from-latest-stage1", action="store_true")
    parser.add_argument("--strategy", default="")
    parser.add_argument("--symbol", default="")
    parser.add_argument("--annualized-return-pct", type=float, default=0.0)
    parser.add_argument("--max-drawdown-pct", type=float, default=0.0)
    parser.add_argument("--sharpe-ratio", type=float, default=0.0)
    args = parser.parse_args()

    baseline = _load_json(args.baseline) or {"schema_version": "strategy-impact/v1", "strategies": []}
    before_txt = json.dumps(baseline, ensure_ascii=False, sort_keys=True)

    if args.from_latest_stage1:
        latest = _load_latest_stage1(args.results_dir)
        bm = latest.get("backtest_metrics", {}) if isinstance(latest.get("backtest_metrics"), dict) else {}
        strategy = str(latest.get("strategy", "") or "")
        symbol = str(latest.get("symbol", "") or "")
        annualized_return_pct = float(bm.get("annualized_return_pct", 0.0) or 0.0)
        max_drawdown_pct = float(bm.get("max_drawdown_pct", 0.0) or 0.0)
        sharpe_ratio = float(bm.get("sharpe_ratio", 0.0) or 0.0)
    else:
        strategy = str(args.strategy or "")
        symbol = str(args.symbol or "")
        annualized_return_pct = float(args.annualized_return_pct)
        max_drawdown_pct = float(args.max_drawdown_pct)
        sharpe_ratio = float(args.sharpe_ratio)
    if not strategy or not symbol:
        raise SystemExit("strategy/symbol 不能为空（或使用 --from-latest-stage1）")

    baseline = _upsert_strategy_entry(
        baseline,
        strategy=strategy,
        symbol=symbol,
        annualized_return_pct=annualized_return_pct,
        max_drawdown_pct=max_drawdown_pct,
        sharpe_ratio=sharpe_ratio,
    )
    baseline["_meta"] = {
        "updated_at": _utc_now(),
        "approval_id": args.approval_id,
        "approver": args.approver,
        "reason_excerpt": str(args.reason)[:120],
        "commit_sha": _commit_sha(),
    }
    args.baseline.parent.mkdir(parents=True, exist_ok=True)
    out_txt = json.dumps(baseline, ensure_ascii=False, indent=2) + "\n"
    args.baseline.write_text(out_txt, encoding="utf-8")
    after_hash = _hash_text(json.dumps(baseline, ensure_ascii=False, sort_keys=True))
    before_hash = _hash_text(before_txt)
    _append_ledger(
        {
            "ts": _utc_now(),
            "approval_id": args.approval_id,
            "approver": args.approver,
            "reason_excerpt": str(args.reason)[:120],
            "strategy": strategy,
            "symbol": symbol,
            "annualized_return_pct": annualized_return_pct,
            "max_drawdown_pct": max_drawdown_pct,
            "sharpe_ratio": sharpe_ratio,
            "baseline_path": str(args.baseline),
            "before_hash": before_hash,
            "after_hash": after_hash,
            "commit_sha": _commit_sha(),
        }
    )
    print(f"[OK] baseline updated: {args.baseline}")
    print(f"[OK] ledger appended: {LEDGER_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

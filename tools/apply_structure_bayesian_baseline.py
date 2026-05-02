"""批量计算并回填结构 Bayesian baseline。

示例：
    conda run --no-capture-output -n myenv python tools/apply_structure_bayesian_baseline.py
    conda run --no-capture-output -n myenv python tools/apply_structure_bayesian_baseline.py \
        --posterior-output out/structure_posterior.csv --no-writeback
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from data_manager.structure_bayesian_baseline import StructureBayesianBaseline
from data_manager.structure_dataset_builder import StructureDatasetBuilder


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="计算并回填 structure_analyze 的 Bayesian baseline 区间")
    parser.add_argument("--db-path", default="", help="可选 DuckDB 路径；为空时使用默认主库")
    parser.add_argument(
        "--group-by",
        action="append",
        default=None,
        help="分桶字段，可重复传入；默认 interval + direction",
    )
    parser.add_argument(
        "--group-strategy",
        choices=["fixed", "adaptive"],
        default="fixed",
        help="分桶策略：fixed=固定分桶，adaptive=样本不足时自动回退到更宽桶",
    )
    parser.add_argument("--min-observations", type=int, default=3, help="adaptive 模式下每桶最小样本数")
    parser.add_argument("--alpha-prior", type=float, default=1.0, help="Beta 先验 alpha")
    parser.add_argument("--beta-prior", type=float, default=1.0, help="Beta 先验 beta")
    parser.add_argument("--credible-level", type=float, default=0.95, help="可信区间置信水平")
    parser.add_argument("--code", default="", help="可选过滤股票代码")
    parser.add_argument("--interval", default="", help="可选过滤周期")
    parser.add_argument("--direction", default="", choices=["", "up", "down"], help="可选过滤方向")
    parser.add_argument(
        "--status",
        action="append",
        default=None,
        choices=["active", "closed", "reversed"],
        help="可选过滤结构状态，可重复传入",
    )
    parser.add_argument(
        "--signal-type",
        action="append",
        default=None,
        choices=["LONG", "SHORT", "EXIT", "HOLD"],
        help="可选过滤最新信号类型，可重复传入",
    )
    parser.add_argument("--no-writeback", action="store_true", help="只计算 posterior，不写回 structure_analyze")
    parser.add_argument("--posterior-output", default="", help="可选 posterior 输出路径（.csv/.parquet）")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    builder = StructureDatasetBuilder(duckdb_path=args.db_path or None)
    baseline = StructureBayesianBaseline(dataset_builder=builder)
    group_by = tuple(args.group_by or ["interval", "direction"])

    dataset = builder.build_dataset(
        code=args.code,
        interval=args.interval,
        direction=args.direction,
        statuses=args.status,
        signal_types=args.signal_type,
    )
    posterior = baseline.fit(
        dataset,
        group_by=group_by,
        group_strategy=args.group_strategy,
        min_observations=args.min_observations,
        alpha_prior=args.alpha_prior,
        beta_prior=args.beta_prior,
        credible_level=args.credible_level,
    )

    if args.posterior_output:
        out_path = Path(args.posterior_output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        suffix = out_path.suffix.lower()
        if suffix == ".csv":
            posterior.to_csv(out_path, index=False, encoding="utf-8-sig")
        elif suffix == ".parquet":
            posterior.to_parquet(out_path, index=False)
        else:
            raise ValueError("posterior_output 后缀必须是 .csv 或 .parquet")

    updated = 0
    if not args.no_writeback:
        updated = baseline.writeback_structure_bounds(
            dataset,
            group_by=group_by,
            group_strategy=args.group_strategy,
            min_observations=args.min_observations,
            alpha_prior=args.alpha_prior,
            beta_prior=args.beta_prior,
            credible_level=args.credible_level,
        )

    print(
        "Bayesian baseline 完成: "
        f"groups={len(posterior)} dataset_rows={len(dataset)} updated={updated}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
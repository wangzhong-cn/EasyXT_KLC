"""导出七层结构语料到 CSV / Parquet。

示例：
    conda run --no-capture-output -n myenv python tools/export_structure_dataset.py \
        --output out/structure_dataset.csv --code 000001.SZ --status active
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from data_manager.structure_dataset_builder import StructureDatasetBuilder


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="导出七层结构语料（structure/analyze/audit/signal）")
    parser.add_argument("--output", required=True, help="输出文件路径，后缀必须是 .csv 或 .parquet")
    parser.add_argument("--db-path", default="", help="可选 DuckDB 路径；为空时使用默认主库")
    parser.add_argument("--code", default="", help="过滤股票代码，如 000001.SZ")
    parser.add_argument("--interval", default="", help="过滤周期，如 1d")
    parser.add_argument("--direction", default="", choices=["", "up", "down"], help="过滤结构方向")
    parser.add_argument(
        "--status",
        action="append",
        default=None,
        choices=["active", "closed", "reversed"],
        help="过滤结构状态，可重复传入",
    )
    parser.add_argument(
        "--signal-type",
        action="append",
        default=None,
        choices=["LONG", "SHORT", "EXIT", "HOLD"],
        help="过滤最新信号类型，可重复传入",
    )
    parser.add_argument("--limit", type=int, default=None, help="可选返回上限")
    parser.add_argument("--offset", type=int, default=0, help="可选偏移量")
    parser.add_argument("--order-desc", action="store_true", help="按 created_at 倒序导出")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    builder = StructureDatasetBuilder(duckdb_path=args.db_path or None)
    build_kwargs = {
        "code": args.code,
        "interval": args.interval,
        "direction": args.direction,
        "statuses": args.status,
        "signal_types": args.signal_type,
        "limit": args.limit,
        "offset": args.offset,
        "order_desc": args.order_desc,
    }
    dataset = builder.build_dataset(**build_kwargs)
    out_path = builder.export_dataset(
        Path(args.output),
        dataset=dataset,
        **build_kwargs,
    )
    print(f"导出完成: {out_path} rows={len(dataset)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
tools/migrate_qmt_db.py — 审计表口径统一迁移工具

决策（2026-03-10 定稿）
─────────────────────────────────────────────────────────────────────────────
  ✅  data_ingestion_status  — 唯一主路径（canonical audit table）
       写入方：data_manager/unified_data_interface.py
              gui_app/widgets/local_data_manager_widget.py
       血缘字段：schema_version / ingest_run_id / raw_hash / source_event_time
       规范文档：docs/lineage_spec.md

  🔍  data_governance        — 独立表，用途不同（不是当前表的旧版）
       schema: id/stock_code/period/event_type/start_date/end_date/
               status/details/path_a_hash/path_b_hash/residue_ratio/created_at
       语义：数据源 A/B 对应关系追踪（十二则），与 lineage 血缘跟踪是不同层次
       处理方式：保留原表，不尝试替换为视图（schema 不兼容）
       目标 DB 另行创建 data_ingestion_status—两表并存且相互独立
─────────────────────────────────────────────────────────────────────────────

用法：
  # 查看现状（不改动）
  python tools/migrate_qmt_db.py --db "D:/QMT_KLineChart/data/easyxt/stock_data.ddb" --dry-run

  # 执行迁移（创建 data_ingestion_status + 兼容视图）
  python tools/migrate_qmt_db.py --db "D:/QMT_KLineChart/data/easyxt/stock_data.ddb"
"""

from __future__ import annotations

import argparse
import sys

# ─────────────────────────────────────────────────────────────────────────────
# Canonical schema — 与 data_manager/unified_data_interface.py 保持一致
# 更新 schema 时两处必须同步，并运行 lineage_backfill.py 对存量行补列
# ─────────────────────────────────────────────────────────────────────────────
CANONICAL_DDL = """
CREATE TABLE IF NOT EXISTS data_ingestion_status (
    stock_code       VARCHAR NOT NULL,
    period           VARCHAR NOT NULL,
    start_date       TIMESTAMP,
    end_date         TIMESTAMP,
    source           VARCHAR,
    status           VARCHAR,
    record_count     INTEGER,
    error_message    VARCHAR,
    last_updated     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schema_version   VARCHAR DEFAULT '1.0',
    ingest_run_id    VARCHAR,
    raw_hash         VARCHAR,
    source_event_time TIMESTAMP,
    PRIMARY KEY (stock_code, period)
)
""".strip()


def _get_tables(con) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return [r[0] for r in rows]


def _get_columns(con, table_name: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
        [table_name],
    ).fetchall()
    return [r[0] for r in rows]


def migrate(db_path: str, dry_run: bool) -> int:
    try:
        import duckdb
    except ImportError:
        print("[ERROR] duckdb 未安装，请先执行: pip install duckdb", file=sys.stderr)
        return 1

    prefix = "[DRY-RUN] " if dry_run else ""
    print(f"[INFO] 打开数据库: {db_path}")
    try:
        con = duckdb.connect(db_path, read_only=dry_run)
    except Exception as e:
        print(f"[ERROR] 无法打开数据库: {e}", file=sys.stderr)
        return 1

    tables = _get_tables(con)
    print(f"\n── 当前表/视图清单 ─────────────────────────────────────────")
    for t in sorted(tables):
        print(f"  {t}")

    has_ingestion = "data_ingestion_status" in tables
    has_governance = "data_governance" in tables

    print(f"\n── 主表状态 ─────────────────────────────────────────────────")
    if has_ingestion:
        cols = _get_columns(con, "data_ingestion_status")
        row_count_row = con.execute("SELECT COUNT(*) FROM data_ingestion_status").fetchone()
        row_count = int(row_count_row[0]) if row_count_row else 0
        print(f"  data_ingestion_status  存在  列数={len(cols)}  行数={row_count}")
    else:
        print("  data_ingestion_status  ❌ 不存在 → 将创建")

    print(f"\n── 遗留表状态 ───────────────────────────────────────────────")
    if has_governance:
        gov_cols = _get_columns(con, "data_governance")
        gov_count_row = con.execute("SELECT COUNT(*) FROM data_governance").fetchone()
        gov_count = int(gov_count_row[0]) if gov_count_row else 0
        print(f"  data_governance        存在  列数={len(gov_cols)}  行数={gov_count}")
        print(f"  列名: {gov_cols}")
    else:
        print("  data_governance        不存在（无需处理）")

    print(f"\n── 迁移操作 ─────────────────────────────────────────────────")

    # Step 1: 创建主表（若不存在）
    if not has_ingestion:
        print(f"{prefix}Step 1: 创建 data_ingestion_status（canonical schema）")
        if not dry_run:
            con.execute(CANONICAL_DDL)
            print("  [OK] data_ingestion_status 已创建")
    else:
        print("  Step 1: data_ingestion_status 已存在，跳过创建")

    # Step 2: 检查 data_governance 是否迁移到 data_ingestion_status
    # 注意：QMT DB 的 data_governance 与 data_ingestion_status schema 完全不同，
    # 是独立用途的表（A/B 对应关系），不应满足抒合，保留原表不展动
    if has_governance:
        print("  Step 2: data_governance 是独立表（slug 不同，不屝试替换）— 已保留")
        print("  → 两表并存:尹路径 lineage 血缘跟踪，吐 data_governance 跟踪数据源对应关系")
    else:
        print("  Step 2: data_governance 不存在，无需处理")

    print(f"\n── 迁移{'预览' if dry_run else '完成'} ───────────────────────────────────────────")
    if dry_run:
        print("  （上述为预览，未做任何改动。去掉 --dry-run 参数后执行正式迁移）")
    else:
        print("  [OK] 创建 data_ingestion_status（canonical lineage 表）")
        print("  [OK] data_governance 保留不动（独立用途，两表并存）")
        print("\n  下一步:")
        print("  1. 运行 lineage_backfill.py --db 对新创建的表补充血缘字段")
        print("  2. 确认 unified_data_interface.py 写入路径指向本 DB，新数据将自动写入 data_ingestion_status")

    con.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="审计表口径统一迁移工具 — data_governance → data_ingestion_status",
    )
    parser.add_argument("--db", required=True, help="DuckDB 数据库路径")
    parser.add_argument(
        "--dry-run", action="store_true", dest="dry_run",
        help="仅预览，不执行任何改动",
    )
    args = parser.parse_args()
    return migrate(args.db, args.dry_run)


if __name__ == "__main__":
    sys.exit(main())

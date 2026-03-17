#!/usr/bin/env python3
"""
血缘字段回填脚本 — data_ingestion_status 历史记录补齐

对已有行补充默认血缘字段：
  schema_version   = '1.0'
  ingest_run_id    = '<backfill-{rowid}>'
  raw_hash         = 'backfill'
  source_event_time = NULL (无法回溯历史数据的事件时间，可空原因: HIST_BACKFILL)

用法：
  python tools/lineage_backfill.py [--db PATH] [--dry-run] [--report PATH]

退出码:
  0 = 成功
  1 = 失败

审计报告：
  默认写到 artifacts/lineage_backfill_report.md
  可通过 --report 指定其他路径
"""
from __future__ import annotations

import argparse
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------
DB_DEFAULT = pathlib.Path("cache/stock_data.duckdb")
REPORT_DEFAULT = pathlib.Path("artifacts/lineage_backfill_report.md")
VALIDATION_SQL = """\
SELECT
    COUNT(*)               AS total_rows,
    COUNT(schema_version)  AS sv_filled,
    COUNT(ingest_run_id)   AS run_id_filled,
    COUNT(raw_hash)        AS hash_filled,
    COUNT(source_event_time) AS set_filled
FROM data_ingestion_status;
"""


def _open_db(db_path: pathlib.Path) -> Any:
    try:
        import duckdb  # type: ignore[import-untyped]
        return duckdb.connect(str(db_path))  # type: ignore[no-any-return]
    except Exception as exc:
        print(f"[ERROR] 无法打开数据库 {db_path}: {exc}")
        sys.exit(1)


def _check_columns(con: Any) -> list[str]:
    """返回 data_ingestion_status 现有列名；若表不存在则返回空列表。"""
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name = 'data_ingestion_status'"
        ).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


def _ensure_lineage_columns(con: Any, dry_run: bool) -> None:
    """确保 4 个血缘列存在（DDL 迁移 — 幂等）。"""
    columns = set(_check_columns(con))
    pairs = [
        ("schema_version",    "VARCHAR DEFAULT '1.0'"),
        ("ingest_run_id",     "VARCHAR"),
        ("raw_hash",          "VARCHAR"),
        ("source_event_time", "TIMESTAMP"),
    ]
    for col, col_def in pairs:
        if col not in columns:
            sql = f"ALTER TABLE data_ingestion_status ADD COLUMN {col} {col_def}"
            if dry_run:
                print(f"  [DRY-RUN] {sql}")
            else:
                try:
                    con.execute(sql)
                    print(f"  [OK] 添加列: {col}")
                except Exception as e:
                    print(f"  [WARN] 添加列 {col} 失败: {e}")


def _backfill_nulls(con: Any, dry_run: bool) -> int:
    """用默认值回填 NULL 行，返回更新行数。"""
    # schema_version
    sv_sql = "UPDATE data_ingestion_status SET schema_version = '1.0' WHERE schema_version IS NULL"
    # ingest_run_id：用 rowid 确保每行唯一
    rid_sql = (
        "UPDATE data_ingestion_status SET ingest_run_id = 'backfill-' || CAST(rowid AS VARCHAR)"
        " WHERE ingest_run_id IS NULL"
    )
    # raw_hash
    rh_sql = "UPDATE data_ingestion_status SET raw_hash = 'backfill' WHERE raw_hash IS NULL"

    # 每个 UPDATE 对应的列名和统计 SQL（静态字符串，无外部输入）
    update_ops = [
        (sv_sql,  "schema_version",
         "SELECT COUNT(*) FROM data_ingestion_status WHERE schema_version IS NULL"),
        (rid_sql, "ingest_run_id",
         "SELECT COUNT(*) FROM data_ingestion_status WHERE ingest_run_id IS NULL"),
        (rh_sql,  "raw_hash",
         "SELECT COUNT(*) FROM data_ingestion_status WHERE raw_hash IS NULL"),
    ]

    total = 0
    for sql, col, count_sql in update_ops:
        if dry_run:
            print(f"  [DRY-RUN] {sql[:80]}...")
        else:
            try:
                null_count_row = con.execute(count_sql).fetchone()
                cnt = null_count_row[0] if null_count_row else 0
                con.execute(sql)
                total += cnt
                print(f"  [OK] 已更新 {cnt} 行 ← {col}")
            except Exception as e:
                print(f"  [WARN] 更新失败: {e}")
    return total


def _collect_validation(con: Any) -> dict[str, int]:
    """返回校验指标字典。"""
    try:
        row = con.execute(VALIDATION_SQL).fetchone()
        if row:
            total, sv, rid, rh, set_ = row
            return {
                "total_rows": total,
                "sv_filled": sv,
                "run_id_filled": rid,
                "hash_filled": rh,
                "set_filled": set_,
                "set_null": total - set_,
            }
    except Exception as e:
        print(f"  [ERROR] 校验查询失败: {e}")
    return {}


def _print_validation(metrics: dict[str, int]) -> None:
    """打印校验指标到控制台。"""
    if not metrics:
        return
    total   = metrics["total_rows"]
    sv      = metrics["sv_filled"]
    rid     = metrics["run_id_filled"]
    rh      = metrics["hash_filled"]
    set_    = metrics["set_filled"]
    set_null = metrics["set_null"]
    print("\n── 校验结果 ────────────────────────────────────")
    print(f"  total_rows              = {total}")
    print(f"  schema_version 已填充   = {sv}/{total}")
    print(f"  ingest_run_id  已填充   = {rid}/{total}")
    print(f"  raw_hash       已填充   = {rh}/{total}")
    print(f"  source_event_time 非空  = {set_}/{total}  (历史行 NULL = {set_null}，原因: HIST_BACKFILL)")
    if total > 0 and sv == total and rid == total and rh == total:
        print("  [PASS] 三项强制血缘字段全部非空")
    elif total == 0:
        print("  [INFO] 表为空，无需回填")
    else:
        print("  [WARN] 仍有 NULL，请检查")
    print("───────────────────────────────────────────────")


def _write_report(
    report_path: pathlib.Path,
    db_path: pathlib.Path,
    metrics: dict[str, int],
    dry_run: bool,
    updated_ops: int,
) -> None:
    """将回填审计报告写到 Markdown 文件。"""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    total    = metrics.get("total_rows", 0)
    sv       = metrics.get("sv_filled", 0)
    rid      = metrics.get("run_id_filled", 0)
    rh       = metrics.get("hash_filled", 0)
    set_     = metrics.get("set_filled", 0)
    set_null = metrics.get("set_null", 0)
    fill_rate = f"{set_ * 100.0 / total:.1f}%" if total > 0 else "N/A"
    pass_flag = "[PASS]" if (total > 0 and sv == total and rid == total and rh == total) else "[WARN]"

    report_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 血缘字段回填审计报告",
        "",
        f"> 执行时间: {now}  ",
        f"> 数据库路径: `{db_path}`  ",
        f"> dry_run: `{dry_run}`  ",
        "",
        "## 汇总",
        "",
        f"| 指标 | 值 |",
        f"|------|-----|",
        f"| 总行数 | {total} |",
        f"| schema_version 已填充 | {sv} |",
        f"| ingest_run_id 已填充 | {rid} |",
        f"| raw_hash 已填充 | {rh} |",
        f"| source_event_time 非空 | {set_} |",
        f"| source_event_time NULL (HIST_BACKFILL) | {set_null} |",
        f"| source_event_time 非空率 | {fill_rate} |",
        f"| 本次更新操作数 | {updated_ops} |",
        f"| 门禁结论 | {pass_flag} |",
        "",
        "## 说明",
        "",
        "- `source_event_time` 保留 NULL 为预期行为：历史回填行无法追溯原始业务事件时间，",
        "  可空原因代码 = `HIST_BACKFILL`，见 `docs/lineage_spec.md §四`。",
        "- 三项强制字段（schema_version / ingest_run_id / raw_hash）必须全部非空。",
        "- 验证 SQL 见 `tools/lineage_backfill.py`。",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[OK] 审计报告已写入: {report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="血缘字段回填脚本")
    parser.add_argument("--db", default=str(DB_DEFAULT), help="DuckDB 文件路径")
    parser.add_argument("--dry-run", action="store_true", help="只打印 SQL，不执行")
    parser.add_argument("--report", default=str(REPORT_DEFAULT), help="审计报告输出路径")
    args = parser.parse_args()

    db_path = pathlib.Path(args.db)
    report_path = pathlib.Path(args.report)

    if not db_path.exists():
        print(f"[INFO] 数据库不存在，跳过回填: {db_path}")
        return 0

    print(f"[INFO] 打开数据库: {db_path}")
    con = _open_db(db_path)

    # 1. 确保列存在
    print("\n── Step 1: DDL 迁移 ────────────────────────────")
    _ensure_lineage_columns(con, args.dry_run)

    # 2. 回填 NULL
    print("\n── Step 2: 回填历史记录 ─────────────────────────")
    updated = _backfill_nulls(con, args.dry_run)
    if not args.dry_run:
        print(f"  汇总: 共更新 {updated} 次（列 × 行）")

    # 3. 校验 & 报告
    metrics: dict[str, int] = {}
    if not args.dry_run:
        metrics = _collect_validation(con)
        _print_validation(metrics)
        _write_report(report_path, db_path, metrics, args.dry_run, updated)

    con.close()
    print("\n[OK] 血缘字段回填完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())

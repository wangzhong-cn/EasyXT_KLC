"""
pipeline_health.py — 数据管道健康报告
=====================================
汇总 DuckDB、因子注册中心、DataSourceRegistry、回填调度器等各子系统的状态，
生成结构化 JSON 报告，可作为监控端点或 CLI 快速诊断工具。

CLI 用法::

    python -m data_manager.pipeline_health          # 打印 JSON 报告
    python -m data_manager.pipeline_health --quiet  # 仅在非健康时输出，适合 cron

程序化用法::

    from data_manager.pipeline_health import PipelineHealth
    report = PipelineHealth().report()          # dict
    ok = report["overall_healthy"]              # bool
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

log = logging.getLogger(__name__)

# 模块级导入（供 mock.patch 定位），各自带 fallback
try:
    from data_manager.duckdb_connection_pool import get_db_manager
except Exception:  # pragma: no cover
    get_db_manager = None  # type: ignore[assignment]

try:
    import data_manager.builtin_factors  # noqa: F401
    from data_manager.factor_registry import factor_registry
except Exception:  # pragma: no cover
    factor_registry = None  # type: ignore[assignment]

try:
    from data_manager.unified_data_interface import UnifiedDataInterface
except Exception:  # pragma: no cover
    UnifiedDataInterface = None  # type: ignore[assignment]


class PipelineHealth:
    """聚合各子系统健康快照，产出一份可机读的 JSON 报告。"""

    def report(self) -> dict[str, Any]:
        """运行全部健康检查，返回汇总字典。"""
        checks: dict[str, Any] = {}

        checks["duckdb"] = self._check_duckdb()
        checks["factor_registry"] = self._check_factor_registry()
        checks["datasource_registry"] = self._check_datasource_registry()
        checks["backfill_scheduler"] = self._check_backfill_scheduler()

        all_ok = all(v.get("healthy", False) for v in checks.values())
        return {
            "overall_healthy": all_ok,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "checks": checks,
        }

    # ── Individual checks ────────────────────────────────────────────────────

    def _check_duckdb(self) -> dict[str, Any]:
        try:
            mgr = get_db_manager()
            ping_df = mgr.execute_read_query("SELECT 1 AS ping")
            tables_df = mgr.execute_read_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            )
            ping_ok = not ping_df.empty and int(ping_df.iloc[0, 0]) == 1
            tables = sorted(tables_df["table_name"].tolist()) if not tables_df.empty else []
            return {
                "healthy": ping_ok,
                "tables": tables,
                "table_count": len(tables),
                "path": os.environ.get("EASYXT_DUCKDB_PATH", "(default)"),
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def _check_factor_registry(self) -> dict[str, Any]:
        try:
            all_factors = factor_registry.list_all()
            cats: dict[str, int] = {}
            for f in all_factors:
                cat = f.get("category", "unknown") if isinstance(f, dict) else getattr(f, "category", "unknown")
                cats[str(cat)] = cats.get(str(cat), 0) + 1
            return {
                "healthy": len(all_factors) > 0,
                "total_factors": len(all_factors),
                "by_category": cats,
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def _check_datasource_registry(self) -> dict[str, Any]:
        try:
            udi = UnifiedDataInterface(
                duckdb_path=os.environ.get("EASYXT_DUCKDB_PATH", ""),
            )
            udi.connect(read_only=True)
            h = udi.data_registry.get_health_summary() if hasattr(udi, "data_registry") else {}
            metrics = udi.data_registry.get_metrics() if hasattr(udi, "data_registry") else {}
            udi.close()
            healthy_sources = sum(1 for v in h.values() if v.get("available", False))
            return {
                "healthy": len(h) > 0,
                "source_count": len(h),
                "healthy_source_count": healthy_sources,
                "sources": h,
                "metrics": metrics,
            }
        except Exception as e:
            return {"healthy": False, "error": str(e)}

    def _check_backfill_scheduler(self) -> dict[str, Any]:
        try:
            from data_manager.history_backfill_scheduler import HistoryBackfillScheduler
            sched = HistoryBackfillScheduler.__new__(HistoryBackfillScheduler)
            # 只做基础属性检测，不真正启动调度器
            return {
                "healthy": True,
                "class_available": True,
            }
        except ImportError:
            return {"healthy": True, "class_available": False, "note": "scheduler not installed"}
        except Exception as e:
            return {"healthy": False, "error": str(e)}


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def _main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="EasyXT 数据管道健康检查")
    parser.add_argument("--quiet", action="store_true", help="仅在不健康时输出")
    parser.add_argument("--indent", type=int, default=2, help="JSON 缩进（0 = 压缩）")
    args = parser.parse_args()

    report = PipelineHealth().report()
    if not args.quiet or not report["overall_healthy"]:
        print(json.dumps(report, ensure_ascii=False, indent=args.indent or None))

    raise SystemExit(0 if report["overall_healthy"] else 1)


if __name__ == "__main__":
    _main()

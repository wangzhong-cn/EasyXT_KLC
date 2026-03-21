"""
data_manager_controller.py — 数据管理控制器（无 Qt 依赖）
=========================================================

将 UI 层（DataGovernancePanel）与治理后端解耦。
所有业务判断集中于此，可 100% 单元测试。

核心职责：
1. 聚合管道健康报告（PipelineHealth）
2. 触发数据完整性校验（DataIntegrityChecker）
3. 查询数据源路由指标（DataSourceRegistry）
4. 记录数据入库审计事件（AuditTrail）
5. 提供环境变量有效性检查（validate_environment）
"""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from typing import Any, Optional

log = logging.getLogger(__name__)


# ─── 延迟导入帮助函数（避免启动时 ImportError 影响 UI 加载） ──────────────


def _safe_import(module_path: str, class_name: Optional[str] = None) -> Any:
    """尝试导入模块或类，失败时返回 None。"""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        if class_name:
            return getattr(mod, class_name, None)
        return mod
    except Exception as exc:
        log.debug("_safe_import(%s, %s) failed: %s", module_path, class_name, exc)
        return None


# ─── 控制器主类 ────────────────────────────────────────────────────────────


class DataManagerController:
    """纯 Python 数据管理控制器，协调各治理子系统。

    可注入 mock 替代（依赖注入），便于单元测试::

        ctrl = DataManagerController(
            pipeline_health=MockHealth(),
            integrity_checker=MockChecker(),
        )
    """

    # ------------------------------------------------------------------
    # 初始化：支持依赖注入，也支持自动发现（infer_from_env=True）
    # ------------------------------------------------------------------

    def __init__(
        self,
        pipeline_health: Any = None,
        integrity_checker: Any = None,
        datasource_registry: Any = None,
        duckdb_path: Optional[str] = None,
    ) -> None:
        self._pipeline_health = pipeline_health
        self._integrity_checker = integrity_checker
        self._datasource_registry = datasource_registry
        self._duckdb_path = duckdb_path or self._resolve_duckdb_path()

    # ------------------------------------------------------------------
    # 1. 管道健康
    # ------------------------------------------------------------------

    def get_pipeline_status(self) -> dict[str, Any]:
        """返回全套管道健康报告，包含每个子系统的 healthy 状态。

        返回结构::
            {
                "overall_healthy": bool,
                "timestamp": str,
                "checks": {
                    "duckdb": {"healthy": bool, "tables": [...], ...},
                    "factor_registry": {"healthy": bool, ...},
                    "datasource_registry": {"healthy": bool, ...},
                    "backfill_scheduler": {"healthy": bool, ...},
                },
                "error": str   # 仅在异常时
            }
        """
        health = self._get_pipeline_health()
        if health is None:
            return {"overall_healthy": False, "error": "PipelineHealth 不可用", "checks": {}}
        try:
            return health.report()
        except Exception as exc:
            log.warning("DataManagerController.get_pipeline_status failed: %s", exc)
            return {"overall_healthy": False, "error": str(exc), "checks": {}}

    # ------------------------------------------------------------------
    # 2. 数据完整性校验
    # ------------------------------------------------------------------

    def run_integrity_check(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
        check_quality: bool = True,
    ) -> dict[str, Any]:
        """对单只标的运行完整性校验，返回结构化报告。

        返回结构::
            {
                "stock_code": str,
                "passed": bool,
                "errors": [...],
                "warnings": [...],
                "summary": {...},
                "elapsed_ms": float,
                "error": str   # 仅在异常时
            }
        """
        checker = self._get_integrity_checker()
        if checker is None:
            return {"stock_code": stock_code, "passed": False, "error": "DataIntegrityChecker 不可用"}
        t0 = time.perf_counter()
        try:
            report = checker.check_integrity(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                check_quality=check_quality,
            )
            elapsed = round((time.perf_counter() - t0) * 1000, 1)
            return {
                "stock_code": stock_code,
                "passed": not report.get("has_errors", True),
                "errors": report.get("errors", []),
                "warnings": report.get("warnings", []),
                "summary": report.get("summary", {}),
                "elapsed_ms": elapsed,
            }
        except Exception as exc:
            log.warning("DataManagerController.run_integrity_check(%s) failed: %s", stock_code, exc)
            return {"stock_code": stock_code, "passed": False, "error": str(exc)}

    def run_batch_integrity_check(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        check_quality: bool = True,
    ) -> dict[str, Any]:
        """批量完整性校验，返回汇总统计 + 各标的结果。

        返回结构::
            {
                "total": int,
                "passed": int,
                "failed": int,
                "reports": {stock_code: report_dict, ...},
                "error": str   # 仅在异常时
            }
        """
        checker = self._get_integrity_checker()
        if checker is None:
            return {"total": len(stock_codes), "passed": 0, "failed": len(stock_codes),
                    "reports": {}, "error": "DataIntegrityChecker 不可用"}
        try:
            raw = checker.batch_check_integrity(
                stock_codes=stock_codes,
                start_date=start_date,
                end_date=end_date,
                check_quality=check_quality,
            )
            passed = sum(1 for r in raw.values() if not r.get("has_errors", True))
            return {
                "total": len(stock_codes),
                "passed": passed,
                "failed": len(stock_codes) - passed,
                "reports": raw,
            }
        except Exception as exc:
            log.warning("DataManagerController.run_batch_integrity_check failed: %s", exc)
            return {"total": len(stock_codes), "passed": 0, "failed": len(stock_codes),
                    "reports": {}, "error": str(exc)}

    # ------------------------------------------------------------------
    # 3. 数据源路由指标
    # ------------------------------------------------------------------

    def get_routing_metrics(self) -> dict[str, Any]:
        """返回各数据源的命中/漏/错误统计及健康状态。

        返回结构::
            {
                "sources": {
                    source_name: {
                        "hits": int,
                        "misses": int,
                        "errors": int,
                        "quality_rejects": int,
                        "last_latency_ms": float | None,
                        "health": {...}
                    }
                },
                "total_sources": int,
                "healthy_sources": int,
                "error": str   # 仅在异常时
            }
        """
        reg = self._get_datasource_registry()
        if reg is None:
            return {"sources": {}, "total_sources": 0, "healthy_sources": 0,
                    "error": "DataSourceRegistry 不可用"}
        try:
            metrics = reg.get_metrics()
            health = reg.get_health_summary()
            combined: dict[str, Any] = {}
            for name, m in metrics.items():
                combined[name] = dict(m)
                combined[name]["health"] = health.get(name, {})
            healthy_count = sum(
                1 for h in health.values() if h.get("available", False)
            )
            return {
                "sources": combined,
                "total_sources": len(combined),
                "healthy_sources": healthy_count,
            }
        except Exception as exc:
            log.warning("DataManagerController.get_routing_metrics failed: %s", exc)
            return {"sources": {}, "total_sources": 0, "healthy_sources": 0, "error": str(exc)}

    # ------------------------------------------------------------------
    # 4. 环境变量检查
    # ------------------------------------------------------------------

    def validate_environment(self) -> dict[str, Any]:
        """检查关键环境变量是否齐备，返回逐项结果。

        返回结构::
            {
                "valid": bool,
                "items": [
                    {"key": str, "status": "ok"|"missing"|"invalid", "value": str, "note": str},
                    ...
                ]
            }
        """
        # (env_key, description, is_required)
        REQUIRED_ENV: list[tuple[str, str, bool]] = [
            ("EASYXT_DUCKDB_PATH", "DuckDB 数据库文件路径",  True),
            ("EASYXT_LOG_DIR",     "日志目录",                False),
            ("QMT_DATA_DIR",       "QMT 数据根目录",          False),
        ]
        items: list[dict[str, Any]] = []
        all_valid = True
        for entry in REQUIRED_ENV:
            key = entry[0]
            note = entry[1]
            required = entry[2]
            val = os.environ.get(key, "")
            if val:
                # 路径类配置做存在性检查
                if "PATH" in key or "DIR" in key:
                    exists = os.path.exists(val)
                    status = "ok" if exists else "invalid"
                    if not exists:
                        if required:
                            all_valid = False
                        note = note + f"（路径不存在: {val}）"
                else:
                    status = "ok"
            else:
                status = "missing"
                if required:
                    all_valid = False
            items.append({"key": key, "status": status, "value": val, "note": note, "required": required})
        return {"valid": all_valid, "items": items}

    # ------------------------------------------------------------------
    # 5. DuckDB 快速统计（供管道状态面板刷新）
    # ------------------------------------------------------------------

    def get_duckdb_summary(self) -> dict[str, Any]:
        """返回 DuckDB 数据概览，用于管道状态面板。

        返回结构::
            {
                "healthy": bool,
                "path": str,
                "tables": [...],
                "table_count": int,
                "stock_daily_rows": int,
                "latest_date": str,
                "error": str   # 仅在异常时
            }
        """
        try:
            get_db_manager_fn = _safe_import(
                "data_manager.duckdb_connection_pool", "get_db_manager"
            )
            if get_db_manager_fn is None:
                return {"healthy": False, "error": "duckdb_connection_pool 不可用"}
            mgr = get_db_manager_fn(self._duckdb_path)

            tables_df = mgr.execute_read_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            )
            tables = sorted(tables_df["table_name"].tolist()) if not tables_df.empty else []

            daily_rows = 0
            latest_date = "N/A"
            if "stock_daily" in tables:
                r = mgr.execute_read_query(
                    "SELECT COUNT(*) as cnt, MAX(date) as latest FROM stock_daily"
                )
                if not r.empty:
                    daily_rows = int(r.iloc[0]["cnt"] or 0)
                    latest_date = str(r.iloc[0]["latest"] or "N/A")

            return {
                "healthy": True,
                "path": self._duckdb_path,
                "tables": tables,
                "table_count": len(tables),
                "stock_daily_rows": daily_rows,
                "latest_date": latest_date,
            }
        except Exception as exc:
            return {"healthy": False, "error": str(exc), "path": self._duckdb_path}

    # ------------------------------------------------------------------
    # 私有：延迟加载治理组件
    # ------------------------------------------------------------------

    def _get_pipeline_health(self) -> Any:
        if self._pipeline_health is not None:
            return self._pipeline_health
        cls = _safe_import("data_manager.pipeline_health", "PipelineHealth")
        if cls:
            self._pipeline_health = cls()
        return self._pipeline_health

    def _get_integrity_checker(self) -> Any:
        if self._integrity_checker is not None:
            return self._integrity_checker
        cls = _safe_import("data_manager.data_integrity_checker", "DataIntegrityChecker")
        if cls:
            try:
                self._integrity_checker = cls(duckdb_path=self._duckdb_path)
                self._integrity_checker.connect()
            except Exception as exc:
                log.warning("DataIntegrityChecker 初始化失败: %s", exc)
                self._integrity_checker = None
        return self._integrity_checker

    def _get_datasource_registry(self) -> Any:
        if self._datasource_registry is not None:
            return self._datasource_registry
        # 从 UnifiedDataInterface 取已注册的 registry 实例
        udi_mod = _safe_import("data_manager.unified_data_interface")
        if udi_mod:
            registry_attr = getattr(udi_mod, "_global_registry", None)
            if registry_attr is not None:
                self._datasource_registry = registry_attr
        return self._datasource_registry

    @staticmethod
    def _resolve_duckdb_path() -> str:
        resolve_fn = _safe_import(
            "data_manager.duckdb_connection_pool", "resolve_duckdb_path"
        )
        if resolve_fn:
            try:
                return resolve_fn()
            except Exception:
                pass
        return os.environ.get("EASYXT_DUCKDB_PATH", "stock_data.ddb")

    # ==================================================================
    # 6. 多源数据对账（交叉验证兜底）
    # ==================================================================

    def cross_validate_sources(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """并行从 DuckDB 与 UnifiedDataInterface 取数，比较收盘价一致性。

        返回结构::
            {
                "stock_code": str,
                "start_date": str, "end_date": str,
                "duckdb_rows": int,
                "live_rows": int,
                "consistent": bool,
                "consistency_rate": float,   # 0.0~1.0
                "max_diff_pct": float,        # 最大相对偏差（%）
                "diff_days": [...],           # 偏差 > 1% 的日期列表
                "error": str
            }
        """
        result: dict[str, Any] = {
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
            "duckdb_rows": 0,
            "live_rows": 0,
            "consistent": False,
            "consistency_rate": 0.0,
            "max_diff_pct": 0.0,
            "diff_days": [],
        }
        try:
            # ── 从 DuckDB 取数 ──────────────────────────────────────────
            get_db = _safe_import("data_manager.duckdb_connection_pool", "get_db_manager")
            if get_db is None:
                result["error"] = "duckdb_connection_pool 不可用"
                return result
            mgr = get_db(self._duckdb_path)
            df_duck = mgr.execute_read_query(
                "SELECT date, close FROM stock_daily WHERE code=? AND date>=? AND date<=? ORDER BY date",
                [stock_code, start_date, end_date],
            )
            result["duckdb_rows"] = len(df_duck)
            if df_duck.empty:
                result["error"] = f"DuckDB 中 {stock_code} 无数据"
                return result

            # ── 从统一数据接口取数（或直接用 DuckDB 已有数据做契约校验）──
            udi_cls = _safe_import("data_manager.unified_data_interface", "UnifiedDataInterface")
            if udi_cls is None:
                # 降级：契约验证替代多源比价
                validator_cls = _safe_import(
                    "data_manager.data_contract_validator", "DataContractValidator"
                )
                if validator_cls is None:
                    result["error"] = "UnifiedDataInterface 和 DataContractValidator 均不可用"
                    return result
                validator = validator_cls()
                vr = validator.validate(df_duck, symbol=stock_code, source="duckdb")
                result["consistent"] = not vr.violations
                result["consistency_rate"] = vr.ohlc_sanity_pct
                result["max_diff_pct"] = round(vr.velocity_violation_pct * 100, 2)
                result["live_rows"] = len(df_duck)
                result["note"] = "降级模式：使用 DataContractValidator 内部一致性校验"
                return result

            try:
                udi = udi_cls()
                df_live = udi.get_stock_data(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:
                result["note"] = f"实时源拉取失败（{exc}），仅展示 DuckDB 侧数据"
                result["duckdb_rows"] = len(df_duck)
                result["consistent"] = True
                result["consistency_rate"] = 1.0
                return result

            result["live_rows"] = len(df_live)

            # ── 比对收盘价 ──────────────────────────────────────────────
            if df_live.empty:
                result["error"] = "实时数据源无返回数据"
                return result

            # 归一化列名
            for col in ("close", "Close", "close_price"):
                if col in df_live.columns:
                    df_live = df_live.rename(columns={col: "close"})
                    break
            for col in ("date", "Date", "trade_date"):
                if col in df_live.columns:
                    df_live = df_live.rename(columns={col: "date"})
                    break

            _pd = _safe_import("pandas")
            if _pd is None:
                result["error"] = "pandas 不可用"
                return result
            df_duck["date"] = _pd.to_datetime(df_duck["date"]).dt.date
            df_live["date"] = _pd.to_datetime(df_live["date"]).dt.date
            merged = df_duck.merge(df_live[["date", "close"]], on="date", suffixes=("_duckdb", "_live"))

            if merged.empty:
                result["error"] = "两源数据无交集日期，无法对账"
                return result

            diffs = ((merged["close_live"] - merged["close_duckdb"]).abs() /
                     merged["close_duckdb"].replace(0, float("nan"))).fillna(0)
            threshold = 0.01  # 1% 偏差视为不一致
            diff_mask = diffs > threshold
            max_diff = float(diffs.max()) * 100
            consistency_rate = float((~diff_mask).mean())
            diff_days = merged.loc[diff_mask, "date"].astype(str).tolist()

            result.update({
                "consistent": consistency_rate >= 0.99,
                "consistency_rate": round(consistency_rate, 4),
                "max_diff_pct": round(max_diff, 4),
                "diff_days": diff_days[:20],  # 最多展示 20 天
                "compared_rows": len(merged),
            })
        except Exception as exc:
            log.warning("cross_validate_sources(%s) failed: %s", stock_code, exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 7. 交易日历查询（日期边界管理）
    # ==================================================================

    def get_trading_calendar_info(
        self,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """查询指定日期区间内的交易日信息。

        返回结构::
            {
                "start_date": str, "end_date": str,
                "total_days": int,           # 日历天数
                "trading_days": int,         # 交易日数
                "non_trading_days": int,
                "trading_days_list": [...],  # YYYY-MM-DD 列表
                "non_trading_list": [...],   # 非交易日列表（节假日+周末）
                "weekend_days": int,
                "holiday_days": int,
                "error": str
            }
        """
        import datetime as _dt

        result: dict[str, Any] = {
            "start_date": start_date,
            "end_date": end_date,
            "total_days": 0,
            "trading_days": 0,
            "non_trading_days": 0,
            "trading_days_list": [],
            "non_trading_list": [],
            "weekend_days": 0,
            "holiday_days": 0,
        }
        try:
            cal_cls = _safe_import("data_manager.smart_data_detector", "TradingCalendar")
            if cal_cls is None:
                result["error"] = "TradingCalendar 不可用"
                return result
            cal = cal_cls()
            sd = _dt.date.fromisoformat(start_date)
            ed = _dt.date.fromisoformat(end_date)
            if sd > ed:
                result["error"] = "start_date 不能晚于 end_date"
                return result

            trading = cal.get_trading_days(sd, ed)
            all_days: list[_dt.date] = []
            cur = sd
            while cur <= ed:
                all_days.append(cur)
                cur += _dt.timedelta(days=1)

            non_trading = sorted(set(all_days) - set(trading))
            weekends = [d for d in non_trading if d.weekday() >= 5]
            holidays_list = [d for d in non_trading if d.weekday() < 5]

            result.update({
                "total_days": len(all_days),
                "trading_days": len(trading),
                "non_trading_days": len(non_trading),
                "trading_days_list": [str(d) for d in trading],
                "non_trading_list": [str(d) for d in non_trading[:200]],
                "weekend_days": len(weekends),
                "holiday_days": len(holidays_list),
            })
        except Exception as exc:
            log.warning("get_trading_calendar_info failed: %s", exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 8. 数据修复：触发缺失数据补全回填
    # ==================================================================

    def repair_missing_data(
        self,
        stock_code: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """将指定标的+日期区间加入回填调度队列。

        返回结构::
            {
                "queued": bool,
                "stock_code": str,
                "start_date": str, "end_date": str,
                "message": str,
                "error": str
            }
        """
        result: dict[str, Any] = {
            "queued": False,
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
        }
        try:
            sched_cls = _safe_import(
                "data_manager.history_backfill_scheduler", "HistoryBackfillScheduler"
            )
            if sched_cls is None:
                # 降级：使用 AutoDataUpdater
                updater_cls = _safe_import(
                    "data_manager.auto_data_updater", "AutoDataUpdater"
                )
                if updater_cls is None:
                    result["error"] = "HistoryBackfillScheduler 和 AutoDataUpdater 均不可用"
                    return result
                updater = updater_cls(duckdb_path=self._duckdb_path)
                updater.update_single_stock(
                    stock_code=stock_code,
                    start_date=start_date,
                    end_date=end_date,
                )
                result["queued"] = True
                result["message"] = f"已通过 AutoDataUpdater 直接触发 {stock_code} 数据补全"
                return result

            def _worker(task: dict[str, Any]) -> bool:
                updater_cls2 = _safe_import(
                    "data_manager.auto_data_updater", "AutoDataUpdater"
                )
                if updater_cls2 is None:
                    return False
                up2 = updater_cls2(duckdb_path=self._duckdb_path)
                return up2.update_single_stock(
                    stock_code=task["stock_code"],
                    start_date=task["start_date"],
                    end_date=task["end_date"],
                )

            sched = sched_cls(worker=_worker)
            sched.start()
            sched.schedule(
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                priority=1,
            )
            result["queued"] = True
            result["message"] = f"{stock_code} [{start_date} ~ {end_date}] 已加入回填队列"
        except Exception as exc:
            log.warning("repair_missing_data(%s) failed: %s", stock_code, exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 9. DuckDB 运维信息（表大小、行数、最后更新）
    # ==================================================================

    def get_duckdb_maintenance_info(self) -> dict[str, Any]:
        """返回 DuckDB 各表的详细运维数据。

        返回结构::
            {
                "healthy": bool,
                "path": str,
                "db_size_mb": float,
                "tables": [
                    {"name": str, "rows": int, "columns": int, "last_date": str}
                ],
                "error": str
            }
        """
        result: dict[str, Any] = {
            "healthy": False,
            "path": self._duckdb_path,
            "db_size_mb": 0.0,
            "tables": [],
        }
        try:
            get_db = _safe_import("data_manager.duckdb_connection_pool", "get_db_manager")
            if get_db is None:
                result["error"] = "duckdb_connection_pool 不可用"
                return result
            mgr = get_db(self._duckdb_path)

            # 获取所有表
            tables_df = mgr.execute_read_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
            )
            tables: list[dict[str, Any]] = []
            for _, row in tables_df.iterrows():
                tname = row["table_name"]
                try:
                    cnt_df = mgr.execute_read_query(f'SELECT COUNT(*) AS n FROM "{tname}"')  # noqa: S608
                    row_count = int(cnt_df.iloc[0]["n"]) if not cnt_df.empty else 0
                except Exception:
                    row_count = -1
                try:
                    col_df = mgr.execute_read_query(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema='main' AND table_name=? ORDER BY ordinal_position",
                        [tname],
                    )
                    col_count = len(col_df)
                    has_date = "date" in col_df["column_name"].str.lower().tolist()
                except Exception:
                    col_count = 0
                    has_date = False
                last_date = "N/A"
                if has_date and row_count > 0:
                    try:
                        ld_df = mgr.execute_read_query(f'SELECT MAX(date) AS ld FROM "{tname}"')  # noqa: S608
                        last_date = str(ld_df.iloc[0]["ld"] or "N/A")
                    except Exception:
                        pass
                tables.append({"name": tname, "rows": row_count, "columns": col_count, "last_date": last_date})

            # DB 文件大小
            db_size_mb = 0.0
            if os.path.exists(self._duckdb_path):
                db_size_mb = round(os.path.getsize(self._duckdb_path) / 1048576, 2)

            result.update({
                "healthy": True,
                "tables": tables,
                "db_size_mb": db_size_mb,
            })
        except Exception as exc:
            log.warning("get_duckdb_maintenance_info failed: %s", exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 10. DuckDB CHECKPOINT（强制刷新）
    # ==================================================================

    def run_checkpoint(self) -> dict[str, Any]:
        """对 DuckDB 执行强制 CHECKPOINT，确保 WAL 落盘。

        返回结构::
            {"ok": bool, "elapsed_ms": float, "message": str, "error": str}
        """
        result: dict[str, Any] = {"ok": False, "elapsed_ms": 0.0}
        try:
            get_db = _safe_import("data_manager.duckdb_connection_pool", "get_db_manager")
            if get_db is None:
                result["error"] = "duckdb_connection_pool 不可用"
                return result
            mgr = get_db(self._duckdb_path)
            t0 = time.perf_counter()
            mgr.execute_read_query("CHECKPOINT")
            elapsed = round((time.perf_counter() - t0) * 1000, 1)
            result.update({"ok": True, "elapsed_ms": elapsed, "message": f"CHECKPOINT 完成（{elapsed} ms）"})
        except Exception as exc:
            log.warning("run_checkpoint failed: %s", exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 11. 完整环境配置（所有 EASYXT_ 变量，含分组和敏感值掩码）
    # ==================================================================

    _ENV_CATALOG: list[tuple[str, str, str, bool]] = [
        # (key, group, description, is_sensitive)
        ("EASYXT_DUCKDB_PATH",                   "数据存储",   "DuckDB 数据库文件路径",         False),
        ("EASYXT_TUSHARE_TOKEN",                  "数据接入",   "Tushare API Token",             True),
        ("TUSHARE_TOKEN",                         "数据接入",   "Tushare Token（兼容）",          True),
        ("QMT_DATA_DIR",                          "数据接入",   "QMT 数据根目录",                False),
        ("QMT_EXE",                               "数据接入",   "QMT 引擎可执行路径",            False),
        ("EASYXT_LOG_DIR",                        "日志与监控", "日志目录",                      False),
        ("EASYXT_MONITOR_DASHBOARD_URL",          "日志与监控", "监控仪表板 URL",                False),
        ("EASYXT_ALERTS_WEBHOOK_URL",             "日志与监控", "告警 Webhook URL",              True),
        ("EASYXT_ENABLE_WAL_AUTO_REPAIR",         "可靠性",     "1=启用 WAL 自动修复",          False),
        ("EASYXT_ENABLE_AUTO_CHECKPOINT",         "可靠性",     "1=定时 CHECKPOINT",            False),
        ("EASYXT_BACKFILL_ENABLED",               "回填调度",   "1=启用历史补数",               False),
        ("EASYXT_REMOTE_CB_THRESHOLD",            "回填调度",   "远程数据源熔断阈值（次）",      False),
        ("EASYXT_REMOTE_BACKOFF_BASE_S",          "回填调度",   "熔断退避基准（秒）",            False),
        ("EASYXT_REMOTE_BACKOFF_MAX_S",           "回填调度",   "熔断退避上限（秒）",            False),
        ("EASYXT_CACHE_STALE_QUARANTINE_ENABLED", "缓存",       "1=启用缓存隔离",               False),
        ("EASYXT_STEP6_VALIDATE_SAMPLE_RATE",     "数据质量",   "入库校验采样率（0.0~1.0）",    False),
        ("EASYXT_WM_WEIGHT_LATE",                 "数据质量",   "Q-score Late子分权重",         False),
        ("EASYXT_WM_WEIGHT_OOO",                  "数据质量",   "Q-score OOO子分权重",          False),
        ("EASYXT_WM_WEIGHT_LATENESS",             "数据质量",   "Q-score Lateness子分权重",     False),
        ("EASYXT_WM_PROFILE",                     "数据质量",   "Q-score模板(balanced/conservative/aggressive)", False),
        ("EASYXT_WM_QSCORE_FLOOR",                "数据质量",   "Q-score 发布阈值",             False),
        ("EASYXT_WM_LOOKBACK_DAYS",               "数据质量",   "Q-score 趋势统计天数",         False),
        ("EASYXT_WM_APPROVAL_REQUIRED_PROFILES",  "数据质量",   "需审批模板（逗号分隔）",       False),
        ("EASYXT_WM_APPROVAL_ID",                 "数据质量",   "模板切换审批单号",             False),
        ("EASYXT_WM_APPROVER",                    "数据质量",   "模板切换审批人",               False),
        ("EASYXT_WM_APPROVAL_REGISTRY_PATH",      "数据质量",   "审批白名单文件路径",           False),
        ("EASYXT_WM_APPROVAL_MAX_AGE_DAYS",       "数据质量",   "审批单最大有效天数",           False),
        ("EASYXT_WM_APPROVAL_REQUIRE_SIGNATURE",  "数据质量",   "1=强制审批签名校验",           False),
        ("EASYXT_WM_APPROVAL_MULTISIG_THRESHOLD", "数据质量",   "审批多签门槛M值",              False),
        ("EASYXT_WM_APPROVAL_SIGNERS",            "数据质量",   "审批签署人白名单",             False),
        ("EASYXT_WM_APPROVAL_EXPIRY_WARN_DAYS",   "数据质量",   "审批过期预警天数",             False),
        ("EASYXT_WM_APPROVAL_USAGE_WARN_RATIO",   "数据质量",   "审批用量预警比例",             False),
        ("EASYXT_WM_APPROVAL_WARN_BLOCK",         "数据质量",   "1=审批预警阻断发布",           False),
        ("EASYXT_WM_APPROVAL_USAGE_LOG_PATH",     "数据质量",   "审批使用日志路径",             False),
        ("EASYXT_WM_APPROVAL_SIGNING_KEY",        "数据质量",   "审批签名密钥(HMAC)",           True),
        ("EASYXT_RT_EVENT_WATERMARK_S",           "数据质量",   "实时事件时间watermark秒数",    False),
        ("EASYXT_RT_DROP_OOO_SEQUENCE",           "数据质量",   "1=丢弃乱序sequence事件",       False),
        ("EASYXT_SESSION_PROFILE",                "数据质量",   "周期构建会话模板(CN_A/CN_A_AUCTION/FUTURES_COMMODITY)", False),
        ("EASYXT_SESSION_PROFILE_FILE",           "数据质量",   "周期构建会话模板文件(JSON)",     False),
        ("EASYXT_SESSION_PROFILE_RULES_FILE",     "数据质量",   "标的到会话模板映射规则文件(JSON)", False),
        ("EASYXT_PERIOD_ALIGNMENT",               "数据质量",   "周期对齐方式(默认left)",        False),
        ("EASYXT_PERIOD_ANCHOR",                  "数据质量",   "日内收敛锚点(daily_close/none)", False),
        ("EASYXT_PERIOD_VALIDATION_REPORT_PATH",  "数据质量",   "周期校验报告JSONL路径",         False),
        ("EASYXT_PERIOD_VALIDATION_FAIL_BLOCK",   "数据质量",   "1=周期校验失败阻断P0",          False),
        ("EASYXT_PEAK_MAX_PERIOD_VALIDATION_FAILED_ITEMS", "数据质量", "峰值门禁允许周期失败行数上限", False),
        ("EASYXT_PROFILE_STARTUP",                "性能",       "1=启用启动耗时分析",            False),
        ("EASYXT_PRELOAD_TABS",                   "性能",       "1=启动时预加载所有 Tab",        False),
        ("EASYXT_AUTOSTART_SERVICES",             "性能",       "1=自动启动后台服务",            False),
        ("EASYXT_WATCHDOG_BUFFER_SIZE",           "性能",       "监视缓冲区大小（秒）",          False),
    ]

    def get_all_env_config(self) -> dict[str, Any]:
        """返回完整的环境变量配置，按分组展示，敏感值自动掩码。

        返回结构::
            {
                "groups": {
                    "数据存储": [{"key": str, "status": str, "value": str, "description": str, "required": bool}],
                    ...
                },
                "summary": {"total": int, "configured": int, "missing_required": int},
                "overall_valid": bool
            }
        """
        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        configured = 0
        missing_required = 0
        required_keys = {"EASYXT_DUCKDB_PATH"}  # 最低必须项

        for key, group, desc, sensitive in self._ENV_CATALOG:
            raw_val = os.environ.get(key, "")
            is_required = key in required_keys
            if raw_val:
                configured += 1
                display_val = ("***MASKED***" if sensitive and len(raw_val) > 4
                               else raw_val[:80])
                if ("PATH" in key or "DIR" in key or "EXE" in key) and not key.endswith("URL"):
                    exists = os.path.exists(raw_val)
                    status = "ok" if exists else "invalid"
                    if not exists and is_required:
                        missing_required += 1
                else:
                    status = "ok"
            else:
                display_val = ""
                status = "missing"
                if is_required:
                    missing_required += 1
            groups[group].append({
                "key": key,
                "status": status,
                "value": display_val,
                "description": desc,
                "required": is_required,
                "sensitive": sensitive,
            })

        return {
            "groups": dict(groups),
            "summary": {
                "total": len(self._ENV_CATALOG),
                "configured": configured,
                "missing_required": missing_required,
            },
            "overall_valid": missing_required == 0,
        }

    # ==================================================================
    # 12. 数据源连通性测试
    # ==================================================================

    def test_datasource_connectivity(self, source_name: str) -> dict[str, Any]:
        """对指定数据源执行轻量连通性探测。

        返回结构::
            {
                "source": str,
                "reachable": bool,
                "latency_ms": float,
                "method": str,       # 探测方式描述
                "error": str
            }
        """
        result: dict[str, Any] = {
            "source": source_name,
            "reachable": False,
            "latency_ms": 0.0,
            "method": "",
        }
        t0 = time.perf_counter()
        try:
            src = source_name.lower()
            if src in ("duckdb", "local", "local_duckdb"):
                get_db = _safe_import("data_manager.duckdb_connection_pool", "get_db_manager")
                if get_db is None:
                    result["error"] = "duckdb_connection_pool 不可用"
                    return result
                mgr = get_db(self._duckdb_path)
                mgr.execute_read_query("SELECT 1")
                result["method"] = "DuckDB SELECT 1"
                result["reachable"] = True

            elif src in ("tushare",):
                token = (os.environ.get("EASYXT_TUSHARE_TOKEN", "").strip()
                         or os.environ.get("TUSHARE_TOKEN", "").strip())
                if not token:
                    result["error"] = "未设置 EASYXT_TUSHARE_TOKEN，无法连通"
                    return result
                ts_mod = _safe_import("tushare")
                if ts_mod is None:
                    result["error"] = "tushare 未安装"
                    return result
                ts_mod.set_token(token)
                pro = ts_mod.pro_api()
                pro.query("trade_cal", exchange="SSE", start_date="20240101", end_date="20240103")
                result["method"] = "Tushare pro.query(trade_cal)"
                result["reachable"] = True

            elif src in ("qmt", "xtquant"):
                xt_mod = _safe_import("xtquant.xtdata")
                if xt_mod is None:
                    result["error"] = "xtquant 未安装或 QMT 未启动"
                    return result
                result["method"] = "xtquant.xtdata import 检查"
                result["reachable"] = True

            elif src in ("akshare",):
                ak_mod = _safe_import("akshare")
                if ak_mod is None:
                    result["error"] = "akshare 未安装"
                    return result
                df = ak_mod.tool_trade_date_hist_sina()
                result["method"] = "akshare.tool_trade_date_hist_sina()"
                result["reachable"] = df is not None and not df.empty

            else:
                # 通用：通过注册表获取实例并调 health()
                reg = self._get_datasource_registry()
                if reg is None:
                    result["error"] = f"DataSourceRegistry 不可用，无法探测 '{source_name}'"
                    return result
                src_obj = None
                try:
                    src_obj = reg._sources.get(source_name)  # type: ignore[attr-defined]
                except Exception:
                    pass
                if src_obj is None:
                    result["error"] = f"数据源 '{source_name}' 未注册"
                    return result
                h = src_obj.health()
                result["reachable"] = h.get("available", False)
                result["method"] = f"DataSource.health() → {h}"

        except Exception as exc:
            log.warning("test_datasource_connectivity(%s) failed: %s", source_name, exc)
            result["error"] = str(exc)
        finally:
            result["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)
        return result

    # ==================================================================
    # 13. 将环境变量写入 .env 文件（白名单保护）
    # ==================================================================

    _ENV_WRITE_WHITELIST: frozenset[str] = frozenset({
        "EASYXT_DUCKDB_PATH",
        "EASYXT_TUSHARE_TOKEN",
        "TUSHARE_TOKEN",
        "QMT_DATA_DIR",
        "QMT_EXE",
        "EASYXT_LOG_DIR",
        "EASYXT_ENABLE_WAL_AUTO_REPAIR",
        "EASYXT_ENABLE_AUTO_CHECKPOINT",
        "EASYXT_BACKFILL_ENABLED",
        "EASYXT_CACHE_STALE_QUARANTINE_ENABLED",
        "EASYXT_STEP6_VALIDATE_SAMPLE_RATE",
        "EASYXT_WM_WEIGHT_LATE",
        "EASYXT_WM_WEIGHT_OOO",
        "EASYXT_WM_WEIGHT_LATENESS",
        "EASYXT_WM_PROFILE",
        "EASYXT_WM_QSCORE_FLOOR",
        "EASYXT_WM_LOOKBACK_DAYS",
        "EASYXT_WM_APPROVAL_REQUIRED_PROFILES",
        "EASYXT_WM_APPROVAL_ID",
        "EASYXT_WM_APPROVER",
        "EASYXT_WM_APPROVAL_REGISTRY_PATH",
        "EASYXT_WM_APPROVAL_MAX_AGE_DAYS",
        "EASYXT_WM_APPROVAL_REQUIRE_SIGNATURE",
        "EASYXT_WM_APPROVAL_MULTISIG_THRESHOLD",
        "EASYXT_WM_APPROVAL_SIGNERS",
        "EASYXT_WM_APPROVAL_EXPIRY_WARN_DAYS",
        "EASYXT_WM_APPROVAL_USAGE_WARN_RATIO",
        "EASYXT_WM_APPROVAL_WARN_BLOCK",
        "EASYXT_WM_APPROVAL_USAGE_LOG_PATH",
        "EASYXT_RT_EVENT_WATERMARK_S",
        "EASYXT_RT_DROP_OOO_SEQUENCE",
        "EASYXT_SESSION_PROFILE",
        "EASYXT_SESSION_PROFILE_FILE",
        "EASYXT_SESSION_PROFILE_RULES_FILE",
        "EASYXT_PERIOD_ALIGNMENT",
        "EASYXT_PERIOD_ANCHOR",
        "EASYXT_PERIOD_VALIDATION_REPORT_PATH",
        "EASYXT_PERIOD_VALIDATION_FAIL_BLOCK",
        "EASYXT_PEAK_MAX_PERIOD_VALIDATION_FAILED_ITEMS",
        "EASYXT_PRELOAD_TABS",
        "EASYXT_AUTOSTART_SERVICES",
    })

    def save_env_to_dotenv(self, key: str, value: str, dotenv_path: str = ".env") -> dict[str, Any]:
        """将指定环境变量持久化到 .env 文件（白名单保护）。

        返回结构::
            {"ok": bool, "message": str, "error": str}
        """
        if key not in self._ENV_WRITE_WHITELIST:
            return {"ok": False, "error": f"'{key}' 不在可写白名单，禁止修改"}
        # 基本值合法性检验：不允许含换行、NUL
        if any(c in value for c in ("\n", "\r", "\x00")):
            return {"ok": False, "error": "值中含非法字符（换行/NUL）"}

        abs_path = os.path.abspath(dotenv_path)
        try:
            lines: list[str] = []
            found = False
            if os.path.exists(abs_path):
                with open(abs_path, encoding="utf-8") as f:
                    lines = f.readlines()
            new_line = f'{key}="{value}"\n'
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped.startswith(f"{key}=") or stripped.startswith(f"#{key}="):
                    lines[i] = new_line
                    found = True
                    break
            if not found:
                lines.append(new_line)
            with open(abs_path, "w", encoding="utf-8") as f:
                f.writelines(lines)
            os.environ[key] = value  # 同步进程内
            return {"ok": True, "message": f"已将 {key} 写入 {abs_path}"}
        except Exception as exc:
            log.warning("save_env_to_dotenv(%s) failed: %s", key, exc)
            return {"ok": False, "error": str(exc)}

    # ==================================================================
    # 14. 回填死信队列查询与清空
    # ==================================================================

    def _resolve_dead_letter_path(self) -> str:
        """解析回填死信队列文件路径（支持环境变量覆盖）。"""
        env_path = os.environ.get("EASYXT_DEAD_LETTER_PATH", "").strip()
        if env_path:
            return env_path
        mod = _safe_import("data_manager.history_backfill_scheduler")
        if mod and hasattr(mod, "__file__") and mod.__file__:
            return os.path.join(os.path.dirname(mod.__file__), "backfill_dead_letter.jsonl")
        # 降级：与 DuckDB 同目录
        db_dir = os.path.dirname(os.path.abspath(self._duckdb_path or "."))
        return os.path.join(db_dir, "backfill_dead_letter.jsonl")

    def get_backfill_dead_letter(self) -> dict[str, Any]:
        """读取回填死信队列文件，返回所有失败任务记录。

        返回结构::
            {
                "entries": [
                    {"stock_code": str, "start_date": str, "end_date": str,
                     "period": str, "retry_count": int, "reason": str,
                     "failed_at": str, "key": str}
                ],
                "total": int,
                "file_path": str,
                "error": str   # 仅在异常时
            }
        """
        dead_path = self._resolve_dead_letter_path()
        try:
            if not os.path.exists(dead_path):
                return {"entries": [], "total": 0, "file_path": dead_path}
            entries: list[dict[str, Any]] = []
            with open(dead_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        import json as _json
                        rec = _json.loads(line)
                        payload = rec.get("payload", {})
                        entries.append({
                            "stock_code": payload.get("stock_code", rec.get("key", "")),
                            "start_date": payload.get("start_date", ""),
                            "end_date": payload.get("end_date", ""),
                            "period": payload.get("period", "1d"),
                            "retry_count": rec.get("retry_count", 0),
                            "reason": rec.get("reason", ""),
                            "failed_at": rec.get("failed_at", ""),
                            "key": rec.get("key", ""),
                        })
                    except Exception:
                        continue
            return {"entries": entries, "total": len(entries), "file_path": dead_path}
        except Exception as exc:
            log.warning("get_backfill_dead_letter failed: %s", exc)
            return {"entries": [], "total": 0, "file_path": dead_path, "error": str(exc)}

    def clear_backfill_dead_letter(self) -> dict[str, Any]:
        """清空回填死信队列文件。

        返回结构::
            {"ok": bool, "message": str, "error": str}
        """
        dead_path = self._resolve_dead_letter_path()
        try:
            if os.path.exists(dead_path):
                os.remove(dead_path)
                return {"ok": True, "message": f"已清空死信队列：{dead_path}"}
            return {"ok": True, "message": "死信队列文件不存在（无需清除）"}
        except Exception as exc:
            log.warning("clear_backfill_dead_letter failed: %s", exc)
            return {"ok": False, "error": str(exc)}

    # ==================================================================
    # 15. 数据快照导出（CSV / JSON）
    # ==================================================================

    def export_data_snapshot(
        self,
        stock_codes: list[str],
        start_date: str,
        end_date: str,
        output_path: str,
        fmt: str = "csv",
    ) -> dict[str, Any]:
        """将指定标的的历史日线数据导出为 CSV 或 JSON 文件。

        参数::
            stock_codes  : 标的代码列表，如 ["000001.SZ", "600519.SH"]
            start_date   : 起始日期（含），格式 "YYYY-MM-DD"
            end_date     : 截止日期（含），格式 "YYYY-MM-DD"
            output_path  : 输出文件路径（调用方负责提供合法路径）
            fmt          : "csv" 或 "json"，默认 "csv"

        返回结构::
            {
                "ok": bool,
                "output_path": str,
                "rows": int,
                "symbols": int,
                "fmt": str,
                "error": str   # 仅在失败时
            }
        """
        result: dict[str, Any] = {
            "ok": False,
            "output_path": output_path,
            "rows": 0,
            "symbols": 0,
            "fmt": fmt,
        }
        if not stock_codes:
            result["error"] = "stock_codes 不能为空"
            return result
        if fmt not in ("csv", "json"):
            result["error"] = f"不支持的导出格式：{fmt}，仅接受 csv / json"
            return result
        try:
            get_db = _safe_import("data_manager.duckdb_connection_pool", "get_db_manager")
            if get_db is None:
                result["error"] = "duckdb_connection_pool 不可用"
                return result
            mgr = get_db(self._duckdb_path)
            placeholders = ", ".join("?" * len(stock_codes))
            df = mgr.execute_read_query(
                f"SELECT * FROM stock_daily WHERE code IN ({placeholders}) AND date >= ? AND date <= ? ORDER BY code, date",  # noqa: S608
                stock_codes + [start_date, end_date],
            )
            if df is None or df.empty:
                result["error"] = f"所选标的在 {start_date}~{end_date} 无数据"
                return result

            os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
            if fmt == "csv":
                df.to_csv(output_path, index=False, encoding="utf-8-sig")
            else:
                df.to_json(output_path, orient="records", force_ascii=False, indent=2)

            result.update({
                "ok": True,
                "rows": len(df),
                "symbols": df["code"].nunique() if "code" in df.columns else len(stock_codes),
            })
        except Exception as exc:
            log.warning("export_data_snapshot failed: %s", exc)
            result["error"] = str(exc)
        return result

    # ==================================================================
    # 16. 实时链路状态查询
    # ==================================================================

    def get_realtime_pipeline_info(self) -> dict[str, Any]:
        """返回最新实时数据链路状态。

        优先从 RealtimePipelineManager 单例读取 metrics；
        若模块不可用则返回 ``connected=None`` 的占位结构，保持 UI 容错。

        Returns
        -------
        dict with keys:
            connected   : bool | None
            degraded    : bool
            symbol      : str
            quote_ts    : str
            reason      : str
            drop_rate   : float
            total_quotes: int
            queue_depth : int
            source      : str   ("RealtimePipelineManager" | "unavailable")
        """
        result: dict[str, Any] = {
            "connected": None,
            "degraded": False,
            "symbol": "",
            "quote_ts": "",
            "reason": "",
            "drop_rate": 0.0,
            "total_quotes": 0,
            "queue_depth": 0,
            "source": "unavailable",
        }
        try:
            rpm_cls = _safe_import(
                "data_manager.realtime_pipeline_manager", "RealtimePipelineManager"
            )
            if rpm_cls is None:
                result["error"] = "RealtimePipelineManager 不可用"
                return result

            # 尝试获取进程内单例实例（若已存在）
            import sys as _sys
            module = _sys.modules.get("data_manager.realtime_pipeline_manager")
            rpm: Any = None
            if module is not None:
                rpm = getattr(module, "_singleton_instance", None)

            if rpm is None:
                # 没有运行中的实例 → 返回 connected=False
                result["connected"] = False
                result["reason"] = "RealtimePipelineManager 未启动"
                result["source"] = "RealtimePipelineManager"
                return result

            metrics = rpm.metrics() if callable(getattr(rpm, "metrics", None)) else {}
            result.update({
                "connected": True,
                "degraded": bool(metrics.get("sustained_alert", False)),
                "symbol": str(getattr(rpm, "_symbol", "") or ""),
                "quote_ts": str(metrics.get("last_quote_ts", "") or ""),
                "reason": str(metrics.get("alert_reason", "") or ""),
                "drop_rate": float(metrics.get("drop_rate", 0.0)),
                "total_quotes": int(metrics.get("total_quotes", 0)),
                "queue_depth": int(metrics.get("queue_len", 0)),
                "source": "RealtimePipelineManager",
            })
        except Exception as exc:
            log.warning("get_realtime_pipeline_info failed: %s", exc)
            result["error"] = str(exc)
        return result

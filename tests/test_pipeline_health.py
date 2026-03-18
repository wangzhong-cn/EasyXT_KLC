"""
tests/test_pipeline_health.py
单元测试：data_manager.pipeline_health.PipelineHealth

使用 unittest.mock 隔离所有外部依赖（DuckDB、因子注册表、数据源注册表、
调度器），保证测试在无数据库环境下快速、无副作用地执行。
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from data_manager.pipeline_health import PipelineHealth


# ---------------------------------------------------------------------------
# 辅助：构造 Mock 对象
# ---------------------------------------------------------------------------


def _mock_db_manager(ping_ok: bool = True, table_names: list[str] | None = None):
    """模拟 DuckDBConnectionManager（execute_read_query 返回 DataFrame）。"""
    if table_names is None:
        table_names = ["ohlcv_daily", "factor_values"]

    mgr = MagicMock()

    def _execute(sql):
        if "SELECT 1" in sql:
            if ping_ok:
                return pd.DataFrame({"ping": [1]})
            return pd.DataFrame()  # 空 → ping 失败
        # information_schema 查询
        return pd.DataFrame({"table_name": table_names})

    mgr.execute_read_query.side_effect = _execute
    return mgr


def _mock_factor_registry(n_factors: int = 5):
    registry = MagicMock()
    factors = []
    categories = ["momentum", "volatility", "value"]
    for i in range(n_factors):
        f = MagicMock()
        f.name = f"factor_{i}"
        f.category = categories[i % len(categories)]
        factors.append(f)
    registry.list_all.return_value = factors
    return registry


def _mock_udi(source_count: int = 3):
    """模拟 UnifiedDataInterface 实例：让 data_registry 属性返回受控数据。"""
    registry_mock = MagicMock()
    registry_mock.get_health_summary.return_value = {
        f"src_{i}": {"available": True} for i in range(source_count)
    }
    registry_mock.get_metrics.return_value = {
        f"src_{i}": {"hits": i, "misses": 0, "errors": 0}
        for i in range(source_count)
    }

    udi = MagicMock()
    udi.data_registry = registry_mock
    return udi


# ---------------------------------------------------------------------------
# 1. 整体结构
# ---------------------------------------------------------------------------


class TestReportStructure:
    """report() 返回的字典结构必须符合约定。"""

    def test_report_has_required_keys(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert "overall_healthy" in report
        assert "timestamp" in report
        assert "checks" in report

    def test_checks_contains_all_subsystems(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        checks = report["checks"]
        for key in ("duckdb", "factor_registry", "datasource_registry", "backfill_scheduler"):
            assert key in checks, f"missing check: {key}"

    def test_each_check_has_healthy_field(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        for name, check in report["checks"].items():
            assert "healthy" in check, f"check '{name}' missing 'healthy' field"


# ---------------------------------------------------------------------------
# 2. DuckDB 检查
# ---------------------------------------------------------------------------


class TestDuckDbCheck:
    def test_healthy_when_ping_returns_1(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager(ping_ok=True)),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["checks"]["duckdb"]["healthy"] is True

    def test_table_count_correct(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager",
                  return_value=_mock_db_manager(table_names=["t1", "t2", "t3"])),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["checks"]["duckdb"]["table_count"] == 3

    def test_unhealthy_when_ping_empty(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager(ping_ok=False)),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["checks"]["duckdb"]["healthy"] is False

    def test_unhealthy_on_exception(self):
        broken_mgr = MagicMock()
        broken_mgr.execute_read_query.side_effect = RuntimeError("connection refused")
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=broken_mgr),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        check = report["checks"]["duckdb"]
        assert check["healthy"] is False
        assert "error" in check


# ---------------------------------------------------------------------------
# 3. 因子注册表检查
# ---------------------------------------------------------------------------


class TestFactorRegistryCheck:
    def test_healthy_with_factors(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry(n_factors=5)),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        check = report["checks"]["factor_registry"]
        assert check["healthy"] is True
        assert check["total_factors"] == 5

    def test_unhealthy_when_no_factors(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry(n_factors=0)),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["checks"]["factor_registry"]["healthy"] is False


# ---------------------------------------------------------------------------
# 4. overall_healthy 聚合
# ---------------------------------------------------------------------------


class TestOverallHealthy:
    def test_true_when_all_pass(self):
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry(n_factors=3)),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["overall_healthy"] is True

    def test_false_when_duckdb_fails(self):
        broken = MagicMock()
        broken.execute_read_query.side_effect = RuntimeError("db down")
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=broken),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry(n_factors=3)),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        assert report["overall_healthy"] is False


# ---------------------------------------------------------------------------
# 5. 因子注册表异常路径
# ---------------------------------------------------------------------------


class TestFactorRegistryException:
    """覆盖 _check_factor_registry 的 except 分支（lines 118-119）。"""

    def test_factor_registry_list_all_raises(self):
        broken_registry = MagicMock()
        broken_registry.list_all.side_effect = RuntimeError("registry unavailable")
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", broken_registry),
            patch("data_manager.pipeline_health.UnifiedDataInterface", return_value=_mock_udi()),
        ):
            report = PipelineHealth().report()

        check = report["checks"]["factor_registry"]
        assert check["healthy"] is False
        assert "error" in check


# ---------------------------------------------------------------------------
# 6. 数据源注册表异常路径
# ---------------------------------------------------------------------------


class TestDatasourceRegistryException:
    """覆盖 _check_datasource_registry 的 except 分支（line 152）。"""

    def test_udi_constructor_raises(self):
        broken_udi_cls = MagicMock(side_effect=RuntimeError("udi init failed"))
        with (
            patch("data_manager.pipeline_health.get_db_manager", return_value=_mock_db_manager()),
            patch("data_manager.pipeline_health.factor_registry", _mock_factor_registry()),
            patch("data_manager.pipeline_health.UnifiedDataInterface", broken_udi_cls),
        ):
            report = PipelineHealth().report()

        check = report["checks"]["datasource_registry"]
        assert check["healthy"] is False
        assert "error" in check

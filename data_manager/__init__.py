"""
数据管理模块
提供便捷的数据管理功能
"""

import sys
from pathlib import Path
from typing import Any, Optional

# 尝试添加101因子平台路径（可选）
factor_platform_path = Path(__file__).parents[1] / "101因子" / "101因子分析平台" / "src"
if str(factor_platform_path) not in sys.path:
    # Append to end to avoid shadowing core modules like data_manager
    sys.path.append(str(factor_platform_path))

# 本地模块 - 始终可用
from data_manager.duckdb_connection_pool import DuckDBConnectionManager, resolve_duckdb_path
from data_manager.unified_data_interface import UnifiedDataInterface
from data_manager.factor_registry import (
    FactorRegistry,
    FactorDefinition,
    FactorComputeEngine,
    FactorStorage,
    factor_registry,
    factor_compute_engine,
    make_factor_storage,
)
# 自动注册内置因子（导入即触发）
try:
    import data_manager.builtin_factors as _builtin_factors  # noqa: F401
except Exception:
    pass

# 可观测性模块
from data_manager.pipeline_health import PipelineHealth
from data_manager.datasource_registry import DataSourceRegistry

# 尝试从101因子平台导入（可选，失败不影响核心功能）
LocalDataManager: Any = None
MetadataDB: Any = None
ParquetStorage: Any = None
DUCKDB_AVAILABLE: bool = True
DuckDBStorage: Any = None
DuckDBDataManager: Any = None
HybridDataManager: Any = None

try:
    if factor_platform_path.exists():
        import importlib.util

        factor_manager_init = factor_platform_path / "data_manager" / "__init__.py"
        if factor_manager_init.exists():
            module_name = "factor_data_manager"
            spec = importlib.util.spec_from_file_location(
                module_name,
                str(factor_manager_init),
                submodule_search_locations=[str(factor_platform_path / "data_manager")],
            )
            if spec is not None and spec.loader is not None:
                dm_module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = dm_module
                spec.loader.exec_module(dm_module)

                LocalDataManager = getattr(dm_module, "LocalDataManager", None)
                MetadataDB = getattr(dm_module, "MetadataDB", None)
                ParquetStorage = getattr(dm_module, "ParquetStorage", None)
                DUCKDB_AVAILABLE = getattr(dm_module, "DUCKDB_AVAILABLE", False)
                DuckDBStorage = getattr(dm_module, "DuckDBStorage", None)
                DuckDBDataManager = getattr(dm_module, "DuckDBDataManager", None)
                HybridDataManager = getattr(dm_module, "HybridDataManager", None)
except ImportError:
    pass  # 101因子平台不可用，使用本地模块

__all__ = [
    # 本地模块
    "DuckDBConnectionManager",
    "resolve_duckdb_path",
    "UnifiedDataInterface",
    "validate_environment",
    # 因子引擎
    "FactorRegistry",
    "FactorDefinition",
    "FactorComputeEngine",
    "FactorStorage",
    "factor_registry",
    "factor_compute_engine",
    "make_factor_storage",
    # 101因子平台模块（可能为None）
    "LocalDataManager",
    "MetadataDB",
    "ParquetStorage",
    "DuckDBStorage",
    "DuckDBDataManager",
    "HybridDataManager",
    "DUCKDB_AVAILABLE",
]


def validate_environment(raise_on_error: bool = False) -> dict[str, str]:
    """
    启动时环境完整性校验。

    检查项：
      - EASYXT_DUCKDB_PATH 父目录是否可写（可创建 + 写文件探测）
      - 数值型 env 变量格式有效性（CB 阈值、退避时间）
      - QMT_EXE 路径存在性（仅 WARN，不阻断，离线模式可忽略）

    Args:
        raise_on_error: True 时，遇到首个 ERROR 级问题立即抛出 RuntimeError。

    Returns:
        {检查名称: "OK" | "WARN: ..." | "ERROR: ..."}
    """
    import os
    from pathlib import Path

    results: dict[str, str] = {}

    # ── 1. EASYXT_DUCKDB_PATH 父目录可写 ─────────────────────────────────────
    duckdb_path_str = os.environ.get("EASYXT_DUCKDB_PATH", "")
    if duckdb_path_str:
        parent = Path(duckdb_path_str).parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
            probe = parent / ".easyxt_write_probe"
            try:
                probe.touch()
                probe.unlink()
                results["EASYXT_DUCKDB_PATH"] = "OK"
            except OSError as exc:
                msg = f"ERROR: 父目录 '{parent}' 无写权限: {exc}"
                results["EASYXT_DUCKDB_PATH"] = msg
                if raise_on_error:
                    raise RuntimeError(msg)
        except OSError as exc:
            msg = f"ERROR: 无法创建父目录 '{parent}': {exc}"
            results["EASYXT_DUCKDB_PATH"] = msg
            if raise_on_error:
                raise RuntimeError(msg)
    else:
        results["EASYXT_DUCKDB_PATH"] = "WARN: 未设置，将使用默认路径"

    # ── 2. 整数型 env 变量格式 ────────────────────────────────────────────────
    _int_vars: dict[str, tuple[int, int]] = {
        "EASYXT_REMOTE_CB_THRESHOLD": (1, 100),
    }
    for var, (lo, hi) in _int_vars.items():
        raw = os.environ.get(var, "")
        if raw:
            try:
                val = int(raw)
                if lo <= val <= hi:
                    results[var] = "OK"
                else:
                    results[var] = f"WARN: 值 {val} 超出建议范围 [{lo}, {hi}]"
            except ValueError:
                msg = f"ERROR: '{raw}' 不是有效整数"
                results[var] = msg
                if raise_on_error:
                    raise RuntimeError(f"{var}: {msg}")
        else:
            results[var] = "OK"  # 未设置时代码使用内置默认值

    # ── 3. 浮点型 env 变量格式 ────────────────────────────────────────────────
    _float_vars: dict[str, tuple[float, float]] = {
        "EASYXT_REMOTE_BACKOFF_BASE_S": (0.1, 60.0),
        "EASYXT_REMOTE_BACKOFF_MAX_S": (1.0, 3600.0),
    }
    for var, (lo, hi) in _float_vars.items():
        raw = os.environ.get(var, "")
        if raw:
            try:
                val = float(raw)
                if lo <= val <= hi:
                    results[var] = "OK"
                else:
                    results[var] = f"WARN: 值 {val} 超出建议范围 [{lo}, {hi}]"
            except ValueError:
                msg = f"ERROR: '{raw}' 不是有效浮点数"
                results[var] = msg
                if raise_on_error:
                    raise RuntimeError(f"{var}: {msg}")
        else:
            results[var] = "OK"

    # ── 4. QMT_EXE 存在性（仅 WARN，离线模式可忽略）─────────────────────────
    qmt_exe = os.environ.get("QMT_EXE", "")
    if qmt_exe:
        if Path(qmt_exe).exists():
            results["QMT_EXE"] = "OK"
        else:
            results["QMT_EXE"] = f"WARN: QMT 执行文件不存在: '{qmt_exe}'（离线模式可忽略）"
    else:
        results["QMT_EXE"] = "WARN: 未设置 QMT_EXE，将使用自动探测"

    # ── 5. Tushare token 非空检查 ─────────────────────────────────────────────
    _tushare_token = (
        os.environ.get("EASYXT_TUSHARE_TOKEN", "").strip()
        or os.environ.get("TUSHARE_TOKEN", "").strip()
    )
    if _tushare_token:
        results["EASYXT_TUSHARE_TOKEN"] = "OK"
    else:
        results["EASYXT_TUSHARE_TOKEN"] = "WARN: 未设置 Tushare token，Tushare 日线备源将不可用"

    # ── 6. AKShare 可用性探测 ─────────────────────────────────────────────────
    try:
        import importlib as _il
        _il.import_module("akshare")
        results["AKSHARE"] = "OK"
    except ImportError:
        results["AKSHARE"] = "WARN: akshare 未安装，AKShare 兜底数据源不可用"
    except Exception as _ak_exc:
        results["AKSHARE"] = f"WARN: akshare 探测异常: {_ak_exc}"

    return results

"""
数据管理模块
提供便捷的数据管理功能，兼容101因子分析平台
"""

import sys
from pathlib import Path

# 添加101因子平台路径
factor_platform_path = Path(__file__).parents[1] / "101因子" / "101因子分析平台" / "src"
if str(factor_platform_path) not in sys.path:
    sys.path.insert(0, str(factor_platform_path))

try:
    import importlib.util

    factor_manager_init = factor_platform_path / "data_manager" / "__init__.py"
    if not factor_manager_init.exists():
        raise ImportError("101因子平台 data_manager 未找到")

    module_name = "factor_data_manager"
    spec = importlib.util.spec_from_file_location(
        module_name,
        str(factor_manager_init),
        submodule_search_locations=[str(factor_platform_path / "data_manager")]
    )
    if spec is None or spec.loader is None:
        raise ImportError("无法加载 101因子平台 data_manager")

    dm_module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = dm_module
    spec.loader.exec_module(dm_module)

    LocalDataManager = getattr(dm_module, 'LocalDataManager', None)
    MetadataDB = getattr(dm_module, 'MetadataDB', None)
    ParquetStorage = getattr(dm_module, 'ParquetStorage', None)
    DUCKDB_AVAILABLE = getattr(dm_module, 'DUCKDB_AVAILABLE', False)

    DuckDBStorage = getattr(dm_module, 'DuckDBStorage', None)
    DuckDBDataManager = getattr(dm_module, 'DuckDBDataManager', None)
    HybridDataManager = getattr(dm_module, 'HybridDataManager', None)

    if LocalDataManager is None:
        raise ImportError("LocalDataManager 未导入")

except ImportError as e:
    raise ImportError(
        f"无法从101因子分析平台导入数据管理类: {e}\n"
        f"请确保 101因子/101因子分析平台/src 目录存在且包含必要的数据管理模块"
    )

__all__ = [
    'LocalDataManager',
    'MetadataDB',
    'ParquetStorage',
    'DuckDBStorage',
    'DuckDBDataManager',
    'HybridDataManager',
    'DUCKDB_AVAILABLE'
]

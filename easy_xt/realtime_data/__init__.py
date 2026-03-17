"""
实时数据模块

提供统一的实时数据接口，支持多数据源和缓存功能。
"""

from .providers import BaseDataProvider, ThsDataProvider

try:
    from .providers import EastmoneyDataProvider
except Exception:
    EastmoneyDataProvider = None
from .unified_api import UnifiedDataAPI

try:
    from .providers import TdxDataProvider
except Exception:
    TdxDataProvider = None
from .api_server import RealtimeDataAPIServer
from .cache import CacheConfig, MemoryCache, RedisCache
from .cache import CacheManager as DataCacheManager
from .config.settings import RealtimeDataConfig
from .config_manager import CacheConfig as ConfigCacheConfig
from .config_manager import (
    ConfigLevel,
    ConfigManager,
    DataProviderConfig,
    MonitorConfig,
    SchedulerConfig,
    WebSocketConfig,
    get_config,
    get_provider_config,
    set_config,
)
from .error_handler import (
    CircuitBreaker,
    CircuitBreakerState,
    ErrorCategory,
    ErrorHandler,
    ErrorInfo,
    ErrorSeverity,
    RecoveryAction,
    RecoveryStrategy,
    handle_async_errors,
    handle_error,
    handle_errors,
    record_success,
)
from .push_service import RealtimeDataPushService

__all__ = [
    'UnifiedDataAPI',
    'BaseDataProvider',
    'ThsDataProvider',
    'TdxDataProvider',
    'EastmoneyDataProvider',
    'ConfigManager',
    'ConfigLevel',
    'DataProviderConfig',
    'ConfigCacheConfig',
    'WebSocketConfig',
    'MonitorConfig',
    'SchedulerConfig',
    'get_config',
    'set_config',
    'get_provider_config',
    'RealtimeDataConfig',
    'RealtimeDataPushService',
    'RealtimeDataAPIServer',
    'DataCacheManager',
    'MemoryCache',
    'RedisCache',
    'CacheConfig',
    'ErrorHandler',
    'ErrorInfo',
    'RecoveryAction',
    'CircuitBreaker',
    'ErrorSeverity',
    'ErrorCategory',
    'RecoveryStrategy',
    'CircuitBreakerState',
    'handle_errors',
    'handle_async_errors',
    'handle_error',
    'record_success'
]

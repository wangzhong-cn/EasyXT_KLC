#!/usr/bin/env python3
"""
缓存管理器 - 多级缓存系统

功能：
1. 内存 LRU 缓存 - 小数据
2. 磁盘缓存 - 大数据（Parquet/Feather）
3. 缓存键设计：symbol × period × adjust_type × [start,end] × schema_hash
4. TTL + 事件驱动失效

使用示例：
    from core.cache_manager import cache_manager, CacheLevel

    # 缓存数据
    cache_manager.set("stock_data", key, df, level=CacheLevel.MEMORY)

    # 获取缓存
    df = cache_manager.get("stock_data", key)

    # 清除缓存
    cache_manager.invalidate("stock_data", key)
"""

import hashlib
import json
import logging
import pickle
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any, Optional

import pandas as pd


class CacheLevel:
    """缓存级别"""

    MEMORY = "memory"  # 内存缓存
    DISK = "disk"  # 磁盘缓存
    BOTH = "both"  # 两级缓存


class LRUCache:
    """LRU 缓存实现"""

    def __init__(self, max_size: int = 100):
        self._cache = OrderedDict()
        self._max_size = max_size
        self._logger = logging.getLogger(__name__)

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        if key in self._cache:
            # 移到末尾（最近使用）
            self._cache.move_to_end(key)
            return self._cache[key]
        return None

    def set(self, key: str, value: Any):
        """设置缓存"""
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self._max_size:
                # 删除最旧的
                self._cache.popitem(last=False)
        self._cache[key] = value

    def delete(self, key: str):
        """删除缓存"""
        if key in self._cache:
            del self._cache[key]

    def clear(self):
        """清空缓存"""
        self._cache.clear()

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)


class CacheManager:
    """
    缓存管理器 - 单例模式

    功能：
    1. 内存 LRU 缓存
    2. 磁盘缓存（支持大数据）
    3. 缓存键生成
    4. TTL 过期
    5. 事件驱动失效
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        self._logger = logging.getLogger(__name__)

        # 配置
        self._memory_cache_size = 200  # 内存缓存最大条目数
        self._disk_cache_dir = Path("./cache")
        self._disk_cache_dir.mkdir(exist_ok=True)
        self._default_ttl = 3600  # 默认 TTL 1小时

        # 缓存存储
        self._memory_cache = LRUCache(max_size=self._memory_cache_size)
        self._cache_metadata = {}  # key -> {created_at, ttl, size}

        self._logger.info("CacheManager initialized")

    def _generate_cache_key(self, namespace: str, **kwargs) -> str:
        """生成缓存键"""
        # 按固定顺序序列化
        keys = sorted(kwargs.keys())
        values = [str(kwargs[k]) for k in keys]
        key_str = f"{namespace}:" + ":".join(values)

        # 添加 schema hash（可选）
        if "schema" in kwargs:
            schema_str = json.dumps(kwargs["schema"], sort_keys=True)
            schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:8]
            key_str += f":{schema_hash}"

        return key_str

    def get(
        self,
        namespace: str,
        key: str,
        level: str = CacheLevel.MEMORY,
    ) -> Optional[Any]:
        """获取缓存"""

        # 内存缓存
        if level in (CacheLevel.MEMORY, CacheLevel.BOTH):
            cache_key = f"{namespace}:{key}"
            value = self._memory_cache.get(cache_key)
            if value is not None:
                # 检查 TTL
                meta = self._cache_metadata.get(cache_key)
                if meta and self._is_expired(meta):
                    self._memory_cache.delete(cache_key)
                    del self._cache_metadata[cache_key]
                else:
                    self._logger.debug(f"Memory cache hit: {cache_key}")
                    return value

        # 磁盘缓存
        if level in (CacheLevel.DISK, CacheLevel.BOTH):
            value = self._load_from_disk(namespace, key)
            if value is not None:
                # 加载到内存缓存
                if level == CacheLevel.BOTH:
                    self._memory_cache.set(f"{namespace}:{key}", value)
                return value

        return None

    def set(
        self,
        namespace: str,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        level: str = CacheLevel.MEMORY,
    ):
        """设置缓存"""
        cache_key = f"{namespace}:{key}"
        ttl = ttl or self._default_ttl

        # 内存缓存
        if level in (CacheLevel.MEMORY, CacheLevel.BOTH):
            self._memory_cache.set(cache_key, value)
            self._cache_metadata[cache_key] = {
                "created_at": time.time(),
                "ttl": ttl,
                "size": self._estimate_size(value),
            }
            self._logger.debug(f"Memory cache set: {cache_key}")

        # 磁盘缓存
        if level in (CacheLevel.DISK, CacheLevel.BOTH):
            self._save_to_disk(namespace, key, value)
            self._logger.debug(f"Disk cache set: {namespace}:{key}")

    def invalidate(self, namespace: str, key: Optional[str] = None):
        """清除缓存"""
        cache_key = f"{namespace}:{key}" if key else None

        if key:
            # 清除指定键
            if cache_key is None:
                return
            self._memory_cache.delete(cache_key)
            self._cache_metadata.pop(cache_key, None)
            self._delete_from_disk(namespace, key)
            self._logger.info(f"Cache invalidated: {namespace}:{key}")
        else:
            # 清除整个命名空间
            keys_to_delete = [
                k for k in self._memory_cache._cache.keys() if k.startswith(f"{namespace}:")
            ]
            for k in keys_to_delete:
                self._memory_cache.delete(k)
                self._cache_metadata.pop(k, None)
            self._logger.info(f"Cache namespace invalidated: {namespace}")

    def _is_expired(self, meta: dict) -> bool:
        """检查是否过期"""
        if meta["ttl"] <= 0:  # TTL <= 0 表示永不过期
            return False
        return (time.time() - meta["created_at"]) > meta["ttl"]

    def _estimate_size(self, obj: Any) -> int:
        """估算对象大小（字节）"""
        try:
            return len(pickle.dumps(obj))
        except Exception:
            return 0

    def _get_disk_cache_path(self, namespace: str, key: str) -> Path:
        """获取磁盘缓存路径"""
        # 使用 key 的 hash 作为文件名
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self._disk_cache_dir / namespace / f"{key_hash}.parquet"

    def _save_to_disk(self, namespace: str, key: str, value: Any):
        """保存到磁盘"""
        try:
            path = self._get_disk_cache_path(namespace, key)
            path.parent.mkdir(parents=True, exist_ok=True)

            if isinstance(value, pd.DataFrame):
                value.to_parquet(path, index=False)
            else:
                # 其他对象用 pickle
                with open(path, "wb") as f:
                    pickle.dump(value, f)
        except Exception as e:
            self._logger.warning(f"Failed to save to disk cache: {e}")

    def _load_from_disk(self, namespace: str, key: str) -> Optional[Any]:
        """从磁盘加载"""
        try:
            path = self._get_disk_cache_path(namespace, key)
            if not path.exists():
                return None

            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            else:
                with open(path, "rb") as f:
                    return pickle.load(f)
        except Exception as e:
            self._logger.warning("Failed to load from disk cache: %s", e)
            return None

    def _delete_from_disk(self, namespace: str, key: str):
        """从磁盘删除"""
        try:
            path = self._get_disk_cache_path(namespace, key)
            if path.exists():
                path.unlink()
        except Exception as e:
            self._logger.warning("Failed to delete from disk cache: %s", e)

    def get_stats(self) -> dict:
        """获取缓存统计"""
        total_memory = sum(meta["size"] for meta in self._cache_metadata.values())
        return {
            "memory_items": len(self._memory_cache),
            "metadata_items": len(self._cache_metadata),
            "total_memory_bytes": total_memory,
            "disk_cache_dir": str(self._disk_cache_dir),
        }

    def clear_all(self):
        """清空所有缓存"""
        self._memory_cache.clear()
        self._cache_metadata.clear()

        # 清空磁盘缓存
        import shutil

        if self._disk_cache_dir.exists():
            shutil.rmtree(self._disk_cache_dir)
            self._disk_cache_dir.mkdir(parents=True, exist_ok=True)

        self._logger.info("All caches cleared")


# 全局单例
cache_manager = CacheManager()

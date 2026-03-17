"""
统一数据接口

提供统一的数据访问接口，屏蔽不同数据源的差异。
"""

import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from dataclasses import dataclass
from typing import Any, Optional

from .providers.base_provider import BaseDataProvider

try:
    from .providers.tdx_provider import TdxDataProvider
except Exception:
    TdxDataProvider = None
from .cache import CacheManager
from .cache.cache_strategy import CacheConfig, CacheType
from .config import RealtimeDataConfig

try:
    from .providers.eastmoney_provider import EastmoneyDataProvider
except Exception:
    EastmoneyDataProvider = None
from .providers.ths_provider import ThsDataProvider


@dataclass
class DataSourceStatus:
    """数据源状态"""
    name: str
    connected: bool
    available: bool
    last_update: float
    error_count: int
    response_time: float


class UnifiedDataAPI:
    """统一数据接口

    整合多个数据源，提供统一的数据访问接口。
    支持数据源优先级、故障切换、负载均衡等功能。
    """

    def __init__(self, config: Optional[RealtimeDataConfig] = None):
        """初始化统一数据接口

        Args:
            config: 配置对象
        """
        self.config = config or RealtimeDataConfig()
        self.logger = logging.getLogger(__name__)

        # 初始化缓存管理器
        cache_settings = self.config.get_cache_config()
        cache_enabled = cache_settings.get('enabled', True)
        if cache_enabled:
            cache_config = self._build_cache_config(cache_settings)
            self.cache_manager: Optional[CacheManager] = CacheManager(cache_config)
            self.logger.info("缓存管理器已启用")
        else:
            self.cache_manager = None
            self.logger.info("缓存管理器已禁用")

        # 初始化数据源提供者
        self.providers: dict[str, BaseDataProvider] = {}
        self._init_providers()

        # 数据源状态跟踪
        self.source_status: dict[str, DataSourceStatus] = {}
        self._init_status_tracking()

        # 配置参数
        self.max_workers = getattr(self.config, 'max_workers', 3)
        self.timeout = getattr(self.config, 'timeout', 5)  # Fix 66: 从 10s 降到 5s，减少全部超时累计时间
        self.retry_count = getattr(self.config, 'retry_count', 2)

        # 运行时质量控制（自动切源）
        self._quality_window_seconds = int(
            float(os.environ.get("EASYXT_SOURCE_QUALITY_WINDOW_S", "120"))
        )
        self._latency_switch_threshold_ms = float(
            os.environ.get("EASYXT_SOURCE_SWITCH_LATENCY_MS", "800")
        )
        self._min_success_rate = float(
            os.environ.get("EASYXT_SOURCE_MIN_SUCCESS_RATE", "0.8")
        )
        self._disable_cooldown_s = int(
            float(os.environ.get("EASYXT_SOURCE_DISABLE_COOLDOWN_S", "60"))
        )
        self._quality_events: dict[str, deque[tuple[float, float, bool]]] = defaultdict(deque)
        self._source_penalty: dict[str, float] = defaultdict(float)
        self._source_disabled_until: dict[str, float] = defaultdict(float)
        routing_config = self.config.get_source_routing_config()
        self.source_priority = list(routing_config.get("source_priority") or [])
        self.max_staleness_ms = float(routing_config.get("max_staleness_ms") or 0.0)

        # SWR (Stale-While-Revalidate) 快照缓存：先返回旧快照，后台静默刷新
        self._swr_lock = threading.Lock()
        self._swr_stale_ttl_s = float(os.environ.get("EASYXT_SWR_STALE_TTL_S", "3.0"))
        self._swr_max_age_s = float(os.environ.get("EASYXT_SWR_MAX_AGE_S", "30.0"))
        # key → (timestamp, data, is_refreshing)
        self._swr_snapshot: dict[str, tuple[float, list, bool]] = {}

        # 错误预算 SLO 追踪（5 分钟滑动窗口）
        self._slo_p99_threshold_ms = float(os.environ.get("EASYXT_SLO_P99_MS", "1200.0"))
        self._slo_availability_target = float(os.environ.get("EASYXT_SLO_AVAILABILITY", "0.995"))
        self._slo_violations: dict[str, int] = defaultdict(int)
        self._slo_window: dict[str, deque] = defaultdict(deque)  # (ts, success, latency_ms)

    def _init_providers(self):
        """初始化数据源提供者"""
        try:
            # 通达信数据源
            if self.config.is_provider_enabled('tdx') and TdxDataProvider is not None:
                tdx_config = self.config.get_provider_config('tdx')
                self.providers['tdx'] = TdxDataProvider(tdx_config)
                self.logger.info("通达信数据源初始化成功")
            elif self.config.is_provider_enabled('tdx'):
                self.logger.warning("通达信数据源不可用，缺少依赖 pytdx")

            # 同花顺数据源
            if self.config.is_provider_enabled('ths'):
                ths_config = self.config.get_provider_config('ths')
                self.providers['ths'] = ThsDataProvider(ths_config)
                self.logger.info("同花顺数据源初始化成功")

            # 东方财富数据源
            if self.config.is_provider_enabled('eastmoney'):
                if EastmoneyDataProvider is None:
                    self.logger.warning("东方财富数据源不可用，缺少依赖 requests")
                else:
                    em_config = self.config.get_provider_config('eastmoney')
                    self.providers['eastmoney'] = EastmoneyDataProvider(em_config)
                    self.logger.info("东方财富数据源初始化成功")

        except Exception as e:
            self.logger.error(f"初始化数据源提供者失败: {e}")

    def _init_status_tracking(self):
        """初始化状态跟踪"""
        for name, provider in self.providers.items():
            self.source_status[name] = DataSourceStatus(
                name=name,
                connected=False,
                available=False,
                last_update=0,
                error_count=0,
                response_time=0
            )

    def _build_cache_config(self, settings: dict[str, Any]) -> CacheConfig:
        backend = settings.get("backend", "memory")
        if backend == "redis":
            cache_type = CacheType.REDIS
        elif backend == "hybrid":
            cache_type = CacheType.HYBRID
        else:
            cache_type = CacheType.MEMORY
        return CacheConfig(
            cache_type=cache_type,
            max_size=int(settings.get("max_size", 1000)),
            default_ttl=int(settings.get("ttl", 300)),
            redis_host=settings.get("redis_host", "localhost"),
            redis_port=int(settings.get("redis_port", 6379)),
            redis_db=int(settings.get("redis_db", 0)),
            redis_password=settings.get("redis_password")
        )

    def connect_all(self) -> dict[str, bool]:
        """并行连接所有数据源

        Returns:
            Dict[str, bool]: 各数据源连接结果
        """
        started_at = time.monotonic()
        results = {}

        def _connect_one(name: str, provider):
            try:
                start_time = time.time()
                success = provider.connect()
                response_time = time.time() - start_time
                return name, success, response_time, None
            except Exception as e:
                return name, False, 0.0, e

        # Fix 67: 整体超时上限 = 单源超时 + 2s 余量，避免 ThreadPoolExecutor 永久阻塞
        overall_timeout = self.timeout + 2
        pool = ThreadPoolExecutor(max_workers=len(self.providers) or 1)
        futures = {}
        timed_out = False
        try:
            futures = {
                pool.submit(_connect_one, name, provider): name
                for name, provider in self.providers.items()
            }
            try:
                for future in as_completed(futures, timeout=overall_timeout):
                    try:
                        name, success, response_time, err = future.result(timeout=1)
                    except Exception as exc:
                        name = futures[future]
                        success, response_time, err = False, 0.0, exc
                    results[name] = success

                    status = self.source_status.get(name)
                    if status is None:
                        continue
                    status.connected = success
                    status.available = success
                    status.last_update = time.time()
                    status.response_time = response_time

                    if success:
                        status.error_count = 0
                        self.logger.info(f"{name}数据源连接成功，响应时间: {response_time:.2f}s")
                    else:
                        status.error_count += 1
                        if err:
                            self.logger.error(f"{name}数据源连接异常: {err}")
                        else:
                            self.logger.warning(f"{name}数据源连接失败")
            except TimeoutError:
                timed_out = True
                # 整体超时，将未完成的数据源标记为失败
                for fut, pname in futures.items():
                    if pname not in results:
                        results[pname] = False
                        st = self.source_status.get(pname)
                        if st:
                            st.connected = False
                            st.available = False
                            st.error_count += 1
                        self.logger.warning(f"{pname}数据源连接超时（整体超时 {overall_timeout}s）")
        finally:
            if timed_out:
                for fut in futures:
                    fut.cancel()
            # 所有 future 均已完成或已取消，无需阻塞等待线程池退出
            pool.shutdown(wait=False, cancel_futures=True)
            elapsed = time.monotonic() - started_at
            if elapsed > overall_timeout:
                self.logger.warning(
                    f"connect_all连接预算超限: elapsed={elapsed:.2f}s budget={overall_timeout:.2f}s"
                )
            else:
                self.logger.info(
                    f"connect_all连接预算: elapsed={elapsed:.2f}s budget={overall_timeout:.2f}s"
                )

        return results

    def disconnect_all(self):
        """断开所有数据源连接"""
        for name, provider in self.providers.items():
            try:
                provider.disconnect()
                status = self.source_status[name]
                status.connected = False
                status.available = False
                self.logger.info(f"{name}数据源已断开连接")
            except Exception as e:
                self.logger.error(f"断开{name}数据源连接失败: {e}")

    def get_available_providers(self, data_type: Optional[str] = None) -> list[str]:
        """获取可用的数据源列表

        Args:
            data_type: 数据类型筛选

        Returns:
            List[str]: 可用数据源名称列表
        """
        available = []

        now = time.time()
        for name, provider in self.providers.items():
            status = self.source_status[name]

            # 运行时降级窗口内，临时禁用此数据源
            if now < self._source_disabled_until.get(name, 0):
                continue

            # 检查基本可用性
            if not (status.connected and status.available):
                continue
            if self.max_staleness_ms > 0 and status.last_update > 0:
                if now - status.last_update > self.max_staleness_ms / 1000.0:
                    continue

            # 检查数据类型支持
            if data_type:
                provider_info = provider.get_provider_info()
                supported_types = provider_info.get('supported_data_types', [])

                if data_type == 'realtime_quotes':
                    if '实时行情' not in supported_types:
                        continue
                elif data_type == 'hot_stocks':
                    if name not in ['ths', 'eastmoney']:  # 只有同花顺和东方财富支持
                        continue
                elif data_type == 'concept_data':
                    if name not in ['ths', 'eastmoney']:
                        continue

            available.append(name)

        # 按优先级和响应时间排序
        def _priority_index(name: str) -> int:
            if not self.source_priority:
                return 9999
            try:
                return self.source_priority.index(name)
            except ValueError:
                return 9999

        available.sort(key=lambda x: (
            _priority_index(x),
            self.source_status[x].error_count,
            self._source_penalty.get(x, 0.0),
            self.source_status[x].response_time
        ))

        return available

    def report_source_quality(
        self,
        source_name: str,
        latency_ms: float = 0.0,
        success: bool = True,
    ) -> None:
        """上报运行时质量，并在质量恶化时触发临时切源。"""
        if source_name not in self.providers:
            return

        now = time.time()
        event_q = self._quality_events[source_name]
        event_q.append((now, max(0.0, float(latency_ms)), bool(success)))
        cutoff = now - max(10, self._quality_window_seconds)
        while event_q and event_q[0][0] < cutoff:
            event_q.popleft()

        if not event_q:
            self._source_penalty[source_name] = 0.0
            return

        latencies = [x[1] for x in event_q if x[1] > 0]
        success_rate = sum(1 for x in event_q if x[2]) / float(len(event_q))
        p95_latency = 0.0
        if latencies:
            ordered = sorted(latencies)
            idx = min(int(len(ordered) * 0.95), len(ordered) - 1)
            p95_latency = ordered[idx]

        penalty = 0.0
        if p95_latency > 0:
            penalty += p95_latency / 1000.0
        if success_rate < 1.0:
            penalty += (1.0 - success_rate) * 5.0
        self._source_penalty[source_name] = penalty

        if p95_latency >= self._latency_switch_threshold_ms or success_rate < self._min_success_rate:
            self._source_disabled_until[source_name] = now + self._disable_cooldown_s
            self.source_status[source_name].available = False
            self.logger.warning(
                "数据源%s触发临时切换: p95=%.2fms success_rate=%.2f cooldown=%ss",
                source_name,
                p95_latency,
                success_rate,
                self._disable_cooldown_s,
            )
        elif now >= self._source_disabled_until.get(source_name, 0):
            self.source_status[source_name].available = True

        # SLO 错误预算追踪（5 分钟滑动窗口）
        slo_q = self._slo_window[source_name]
        slo_q.append((now, bool(success), max(0.0, float(latency_ms))))
        slo_cutoff = now - 300.0
        while slo_q and slo_q[0][0] < slo_cutoff:
            slo_q.popleft()
        if not success or latency_ms > self._slo_p99_threshold_ms:
            self._slo_violations[source_name] += 1

    def _fetch_quotes_parallel_race(
        self, codes: list[str], preferred_source: Optional[str] = None
    ) -> list[dict[str, Any]]:
        """对冲并行竞速：所有可用源同时发起请求，取最先返回的成功结果。
        timeout 由 self.timeout 控制，慢源超时后自动丢弃。
        """
        available = self.get_available_providers('realtime_quotes')
        if not available:
            self.logger.error("所有数据源都无法获取实时行情")
            return []

        if preferred_source and preferred_source in available:
            available.remove(preferred_source)
            available.insert(0, preferred_source)

        result_q: queue.Queue = queue.Queue()

        def _try(name: str) -> None:
            try:
                start = time.time()
                quotes = self.providers[name].get_realtime_quotes(codes)
                latency_ms = (time.time() - start) * 1000.0
                status = self.source_status[name]
                status.response_time = latency_ms / 1000.0
                if quotes:
                    status.last_update = time.time()
                    status.error_count = 0
                    self.report_source_quality(name, latency_ms, True)
                    result_q.put((name, quotes))
                else:
                    self.report_source_quality(name, latency_ms, False)
            except Exception:
                self.report_source_quality(name, 0.0, False)
                self.source_status[name].error_count += 1
                if self.source_status[name].error_count >= 3:
                    self.source_status[name].available = False

        threads = [
            threading.Thread(target=_try, args=(n,), daemon=True)
            for n in available
        ]
        for t in threads:
            t.start()

        try:
            source_name, quotes = result_q.get(timeout=self.timeout)
            self.logger.debug("[对冲竞速] 赢家: %s，%d条行情", source_name, len(quotes))
            if self.cache_manager:
                ck = f"quotes_{preferred_source or 'auto'}_{'_'.join(sorted(codes))}"
                self.cache_manager.set(ck, quotes, "realtime_quotes")
            return quotes
        except queue.Empty:
            self.logger.warning(
                "[对冲竞速] 全部超时 timeout=%ss sources=%s", self.timeout, available
            )
            return []

    def _background_refresh_quotes(
        self, codes: list[str], preferred_source: Optional[str], swr_key: str
    ) -> None:
        """SWR 后台静默刷新：更新快照后清除 is_refreshing 标志。"""
        try:
            result = self._fetch_quotes_parallel_race(codes, preferred_source)
            if result:
                with self._swr_lock:
                    self._swr_snapshot[swr_key] = (time.time(), result, False)
                return
        except Exception:
            pass
        finally:
            # 无论成功与否，清除刷新标志
            with self._swr_lock:
                entry = self._swr_snapshot.get(swr_key)
                if entry and entry[2]:
                    self._swr_snapshot[swr_key] = (entry[0], entry[1], False)

    def get_slo_stats(self) -> dict[str, dict[str, Any]]:
        """返回各数据源过去 5 分钟的 SLO 合规统计。

        字段说明:
            requests: 窗口内请求总数
            availability: 成功率（0~1）
            availability_ok: 是否满足目标可用率
            p99_ms: P99 延迟（毫秒）
            p99_ok: 是否满足 P99 目标
            total_violations: 生命周期累计违规次数
        """
        stats: dict[str, dict[str, Any]] = {}
        now = time.time()
        slo_cutoff = now - 300.0
        for name in self.providers:
            raw = self._slo_window.get(name)
            window = [e for e in (raw or []) if e[0] >= slo_cutoff]
            total = len(window)
            if total == 0:
                stats[name] = {
                    "requests": 0, "availability": None,
                    "availability_ok": None, "p99_ms": None,
                    "p99_ok": None, "total_violations": self._slo_violations.get(name, 0),
                }
                continue
            ok_cnt = sum(1 for e in window if e[1])
            availability = ok_cnt / total
            latencies = sorted(e[2] for e in window if e[2] > 0)
            p99_ms = latencies[min(int(len(latencies) * 0.99), len(latencies) - 1)] if latencies else 0.0
            stats[name] = {
                "requests": total,
                "availability": round(availability, 4),
                "availability_ok": availability >= self._slo_availability_target,
                "p99_ms": round(p99_ms, 1),
                "p99_ok": p99_ms <= self._slo_p99_threshold_ms,
                "total_violations": self._slo_violations.get(name, 0),
            }
        return stats

    def get_realtime_quotes(self, codes: list[str],
                          preferred_source: Optional[str] = None) -> list[dict[str, Any]]:
        """获取实时行情数据（SWR + 对冲竞速双层优化）。

        策略：
        1. 快照足够新鲜（< stale_ttl）→ 直接返回
        2. 快照略旧但未超最大容忍期 → 立即返回旧快照 + 后台刷新
        3. 无快照或快照过旧 → 同步对冲并行竞速
        """
        swr_key = f"swr_quotes_{'_'.join(sorted(codes))}"
        now = time.time()

        with self._swr_lock:
            snapshot = self._swr_snapshot.get(swr_key)

        if snapshot:
            snap_ts, snap_data, snap_refreshing = snapshot
            age_s = now - snap_ts
            if age_s < self._swr_stale_ttl_s:
                return snap_data  # 足够新鲜
            if age_s < self._swr_max_age_s:
                if not snap_refreshing:
                    # 标记后台刷新中，异步更新
                    with self._swr_lock:
                        self._swr_snapshot[swr_key] = (snap_ts, snap_data, True)
                    threading.Thread(
                        target=self._background_refresh_quotes,
                        args=(codes, preferred_source, swr_key),
                        daemon=True,
                    ).start()
                return snap_data  # 先返回旧快照

        # 无快照或超最大容忍期：同步对冲竞速
        result = self._fetch_quotes_parallel_race(codes, preferred_source)
        if result:
            with self._swr_lock:
                self._swr_snapshot[swr_key] = (time.time(), result, False)
        return result

    def _get_akshare_realtime_quotes(self, codes: list[str]) -> list[dict[str, Any]]:
        try:
            import akshare as ak
        except Exception:
            return []
        try:
            from easy_xt.utils import StockCodeUtils
        except Exception:
            StockCodeUtils = None
        normalized: list[str] = []
        for code in codes:
            if StockCodeUtils is None:
                normalized.append(str(code))
            else:
                normalized.append(StockCodeUtils.normalize_code(str(code)))
        symbol_map: dict[str, str] = {}
        symbols: list[str] = []
        for code in normalized:
            symbol = str(code).split('.')[0]
            symbols.append(symbol)
            symbol_map[symbol] = code
        try:
            df = ak.stock_zh_a_spot_em()
        except Exception:
            return []
        if df is None or df.empty:
            return []
        df = df[df["代码"].isin(symbols)]
        if df.empty:
            return []
        quotes: list[dict[str, Any]] = []
        for row in df.itertuples(index=False):
            symbol = getattr(row, "代码", "")
            name = getattr(row, "名称", "")
            price = float(getattr(row, "最新价", 0) or 0)
            last_close = float(getattr(row, "昨收", 0) or 0)
            open_price = float(getattr(row, "今开", 0) or 0)
            high = float(getattr(row, "最高", 0) or 0)
            low = float(getattr(row, "最低", 0) or 0)
            change = float(getattr(row, "涨跌额", 0) or 0)
            change_pct = float(getattr(row, "涨跌幅", 0) or 0)
            volume = int(getattr(row, "成交量", 0) or 0)
            turnover = float(getattr(row, "成交额", 0) or 0)
            code = symbol_map.get(symbol, "")
            if not code:
                if str(symbol).startswith("6"):
                    code = f"{symbol}.SH"
                elif str(symbol).startswith(("8", "4")):
                    code = f"{symbol}.BJ"
                else:
                    code = f"{symbol}.SZ"
            if last_close > 0 and change == 0:
                change = round(price - last_close, 2)
            if last_close > 0 and change_pct == 0:
                change_pct = round((price - last_close) / last_close * 100, 2)
            quotes.append({
                "code": code,
                "name": name,
                "price": price,
                "last_close": last_close,
                "change": change,
                "change_pct": change_pct,
                "volume": volume,
                "turnover": turnover,
                "high": high,
                "low": low,
                "open": open_price,
                "timestamp": int(time.time()),
                "source": "akshare"
            })
        return quotes

    def get_hot_stocks(self, count: int = 50, market: str = 'all',
                      data_type: str = '大家都在看',
                      preferred_source: Optional[str] = None) -> list[dict[str, Any]]:
        """获取热门股票数据

        Args:
            market: 市场类型
            count: 获取数量
            data_type: 数据类型

        Returns:
            List[Dict]: 热门股票数据
        """
        # 尝试从缓存获取
        if self.cache_manager:
            cache_key = f"hot_stocks_{market}_{count}_{data_type}"
            cached_result = self.cache_manager.get(cache_key, "hot_stocks")
            if cached_result:
                self.logger.debug(f"从缓存获取热门股票: {count}个")
                return cached_result

        # 优先使用同花顺，备选东方财富
        available_sources = self.get_available_providers('hot_stocks')
        if preferred_source and preferred_source in available_sources:
            available_sources.remove(preferred_source)
            available_sources.insert(0, preferred_source)

        # 同花顺优先
        if 'ths' in available_sources:
            available_sources.remove('ths')
            available_sources.insert(0, 'ths')

        for source_name in available_sources:
            try:
                provider = self.providers[source_name]

                if source_name == 'ths':
                    # 同花顺热股排行
                    hot_stocks = provider.get_hot_stock_rank(data_type, count)
                elif source_name == 'eastmoney':
                    # 东方财富热门股票
                    hot_stocks = provider.get_hot_stocks(market, count)
                else:
                    continue

                if hot_stocks:
                    # 缓存结果
                    if self.cache_manager:
                        cache_key = f"hot_stocks_{market}_{count}_{data_type}"
                        self.cache_manager.set(cache_key, hot_stocks, "hot_stocks")

                    self.logger.info(f"从{source_name}成功获取{len(hot_stocks)}条热门股票")
                    return hot_stocks

            except Exception as e:
                self.logger.warning(f"从{source_name}获取热门股票失败: {e}")

        return []

    def get_concept_data(self, count: int = 50,
                         preferred_source: Optional[str] = None) -> list[dict[str, Any]]:
        """获取概念数据

        Args:
            count: 获取数量

        Returns:
            List[Dict]: 概念数据
        """
        # 尝试从缓存获取
        if self.cache_manager:
            cache_key = f"concept_data_{count}"
            cached_result = self.cache_manager.get(cache_key, "concept_data")
            if cached_result:
                self.logger.debug(f"从缓存获取概念数据: {count}个")
                return cached_result

        # 优先使用同花顺，备选东方财富
        available_sources = self.get_available_providers('concept_data')
        if preferred_source and preferred_source in available_sources:
            available_sources.remove(preferred_source)
            available_sources.insert(0, preferred_source)

        # 同花顺优先
        if 'ths' in available_sources:
            available_sources.remove('ths')
            available_sources.insert(0, 'ths')

        for source_name in available_sources:
            try:
                provider = self.providers[source_name]

                if source_name == 'ths':
                    # 同花顺概念排行
                    concepts = provider.get_concept_rank(count)
                elif source_name == 'eastmoney':
                    # 东方财富概念板块
                    concepts = provider.get_sector_data('concept')
                else:
                    continue

                if concepts:
                    # 缓存结果
                    if self.cache_manager:
                        cache_key = f"concept_data_{count}"
                        self.cache_manager.set(cache_key, concepts, "concept_data")

                    self.logger.info(f"从{source_name}成功获取{len(concepts)}条概念数据")
                    return concepts

            except Exception as e:
                self.logger.warning(f"从{source_name}获取概念数据失败: {e}")

        return []

    def get_market_status(self) -> dict[str, Any]:
        """获取市场状态

        Returns:
            Dict: 市场状态信息
        """
        # 优先使用通达信，备选东方财富
        available_sources = self.get_available_providers()

        # 通达信优先
        if 'tdx' in available_sources:
            available_sources.remove('tdx')
            available_sources.insert(0, 'tdx')

        for source_name in available_sources:
            try:
                provider = self.providers[source_name]

                get_market_status = getattr(provider, "get_market_status", None)
                if callable(get_market_status):
                    status = get_market_status()
                    if isinstance(status, dict) and status:
                        self.logger.info(f"从{source_name}成功获取市场状态")
                        return status

            except Exception as e:
                self.logger.warning(f"从{source_name}获取市场状态失败: {e}")

        return {
            'market_status': 'unknown',
            'timestamp': int(time.time()),
            'source': 'unified_api'
        }

    def get_multi_source_data(self, codes: list[str]) -> dict[str, list[dict[str, Any]]]:
        """并行获取多数据源数据

        Args:
            codes: 股票代码列表

        Returns:
            Dict: 各数据源的数据结果
        """
        results: dict[str, list[dict[str, Any]]] = {}
        available_sources = self.get_available_providers('realtime_quotes')

        if not available_sources:
            return results

        # 使用线程池并行获取
        executor = ThreadPoolExecutor(max_workers=min(len(available_sources), self.max_workers))
        future_to_source = {}
        try:
            # 提交任务
            for source_name in available_sources:
                provider = self.providers[source_name]
                future = executor.submit(provider.get_realtime_quotes, codes)
                future_to_source[future] = source_name

            # 收集结果
            try:
                for future in as_completed(future_to_source, timeout=self.timeout):
                    source_name = future_to_source[future]
                    try:
                        data = future.result(timeout=0.5)
                        if data:
                            results[source_name] = data
                            self.logger.info(f"{source_name}并行获取数据成功: {len(data)}条")
                    except Exception as e:
                        self.logger.warning(f"{source_name}并行获取数据失败: {e}")
            except TimeoutError:
                for future, source_name in future_to_source.items():
                    if not future.done():
                        future.cancel()
                        self.logger.warning(f"{source_name}并行获取数据超时（上限 {self.timeout}s）")
        finally:
            # 所有 future 均已完成或已取消，无需阻塞等待线程池退出
            executor.shutdown(wait=False, cancel_futures=True)

        return results

    def get_source_status(self) -> dict[str, dict[str, Any]]:
        """获取所有数据源状态

        Returns:
            Dict: 数据源状态信息
        """
        status_info = {}

        for name, status in self.source_status.items():
            provider = self.providers.get(name)
            provider_info = provider.get_provider_info() if provider else {}

            status_info[name] = {
                'name': status.name,
                'connected': status.connected,
                'available': status.available,
                'last_update': status.last_update,
                'error_count': status.error_count,
                'response_time': status.response_time,
                'provider_info': provider_info
            }

        return status_info

    def health_check(self) -> dict[str, Any]:
        """健康检查

        Returns:
            Dict: 健康状态信息
        """
        total_sources = len(self.providers)
        available_sources = len(self.get_available_providers())

        health_status = {
            'overall_status': 'healthy' if available_sources > 0 else 'unhealthy',
            'total_sources': total_sources,
            'available_sources': available_sources,
            'availability_rate': available_sources / total_sources if total_sources > 0 else 0,
            'timestamp': int(time.time()),
            'sources': self.get_source_status()
        }

        return health_status


# 导出类供外部使用
__all__ = ['UnifiedDataAPI', 'DataSourceStatus']

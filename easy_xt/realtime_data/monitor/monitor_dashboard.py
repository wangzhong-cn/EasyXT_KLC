"""
监控仪表板

提供Web界面展示监控数据和告警信息。
"""

import json
import logging
import os
import weakref
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, Optional, cast

import aiohttp_cors
from aiohttp import WSMsgType, web

from .alert_manager import AlertLevel, AlertManager, AlertRule, AlertStatus
from .api_monitor import APIMonitor
from .data_source_monitor import DataSourceMonitor
from .metrics_collector import MetricPoint, MetricsCollector
from .system_monitor import SystemMonitor

logger = logging.getLogger(__name__)


class MonitorDashboard:
    """监控仪表板"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8081,
        enable_cors: bool = True,
        ui_config: Optional[dict[str, Any]] = None,
        config_file: Optional[str] = None,
    ):
        """初始化监控仪表板

        Args:
            host: 监听地址
            port: 监听端口
            enable_cors: 是否启用CORS
        """
        self.host = host
        self.port = port
        self.enable_cors = enable_cors
        self.ui_config = ui_config or {}
        self.config_file = config_file

        # 监控组件
        self.system_monitor: Optional[SystemMonitor] = None
        self.data_source_monitor: Optional[DataSourceMonitor] = None
        self.api_monitor: Optional[APIMonitor] = None
        self.alert_manager: Optional[AlertManager] = None
        self.metrics_collector: Optional[MetricsCollector] = None

        # Web应用
        self.app = None
        self.runner = None
        self.site = None

        # WebSocket连接管理
        self.websocket_connections: weakref.WeakSet = weakref.WeakSet()
        self._alerts_rollups: list[dict[str, Any]] = []
        self._alerts_rollups_max = 200

        logger.info(f"监控仪表板初始化完成，地址: {host}:{port}")

    def register_system_monitor(self, system_monitor: SystemMonitor):
        """注册系统监控器"""
        self.system_monitor = system_monitor

    def register_data_source_monitor(self, data_source_monitor: DataSourceMonitor):
        """注册数据源监控器"""
        self.data_source_monitor = data_source_monitor

    def register_api_monitor(self, api_monitor: APIMonitor):
        """注册API监控器"""
        self.api_monitor = api_monitor

    def register_alert_manager(self, alert_manager: AlertManager):
        """注册告警管理器"""
        self.alert_manager = alert_manager

    def register_metrics_collector(self, metrics_collector: MetricsCollector):
        """注册指标收集器"""
        self.metrics_collector = metrics_collector

    async def start(self):
        """启动仪表板服务"""
        if self.app:
            logger.warning("监控仪表板已在运行")
            return

        # 创建Web应用
        self.app = web.Application()

        # 配置CORS
        if self.enable_cors:
            aiohttp_cors.setup(
                self.app,
                defaults={
                    "*": aiohttp_cors.ResourceOptions(
                        allow_credentials=True,
                        expose_headers="*",
                        allow_headers="*",
                        allow_methods="*",
                    )
                },
            )

        # 注册路由
        self._setup_routes()

        # 启动服务器
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()

        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()

        logger.info(f"监控仪表板已启动: http://{self.host}:{self.port}")

    async def stop(self):
        """停止仪表板服务"""
        if self.site:
            await self.site.stop()

        if self.runner:
            await self.runner.cleanup()

        self.app = None
        self.runner = None
        self.site = None

        logger.info("监控仪表板已停止")

    def _setup_routes(self):
        """设置路由"""
        app = self.app
        if app is None:
            return
        # 静态文件
        app.router.add_get("/", self._handle_index)
        app.router.add_get("/dashboard", self._handle_dashboard)

        # API路由
        app.router.add_get("/api/health", self._handle_health)
        app.router.add_get("/api/overview", self._handle_overview)

        # 系统监控API
        app.router.add_get("/api/system/current", self._handle_system_current)
        app.router.add_get("/api/system/history", self._handle_system_history)
        app.router.add_get("/api/system/stats", self._handle_system_stats)

        # 数据源监控API
        app.router.add_get("/api/datasources/status", self._handle_datasources_status)
        app.router.add_get("/api/datasources/stats", self._handle_datasources_stats)

        # API监控API
        app.router.add_get("/api/api/stats", self._handle_api_stats)
        app.router.add_get("/api/api/endpoints", self._handle_api_endpoints)
        app.router.add_get("/api/api/errors", self._handle_api_errors)

        # 告警API
        app.router.add_get("/api/alerts/active", self._handle_alerts_active)
        app.router.add_get("/api/alerts/history", self._handle_alerts_history)
        app.router.add_get("/api/alerts/stats", self._handle_alerts_stats)
        app.router.add_post("/api/alerts/resolve", self._handle_alerts_resolve)
        app.router.add_get("/api/alerts/rollup", self._handle_alerts_rollup)
        app.router.add_post("/api/alerts/ingest", self._handle_alerts_ingest)

        # UI配置API
        app.router.add_get("/api/dashboard/ui-config", self._handle_ui_config_get)
        app.router.add_post("/api/dashboard/ui-config", self._handle_ui_config_update)

        # 降级专用统计API
        app.router.add_get("/api/alerts/degrade/stats", self._handle_degrade_stats)
        app.router.add_get("/api/alerts/degrade/events", self._handle_degrade_events)

        # 指标API
        app.router.add_get("/api/metrics/names", self._handle_metrics_names)
        app.router.add_get("/api/metrics/query", self._handle_metrics_query)

        # WebSocket
        app.router.add_get("/ws", self._handle_websocket)

    async def _handle_index(self, request):
        """首页"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>EasyXT 监控仪表板</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                .header { background: #f0f0f0; padding: 20px; border-radius: 5px; }
                .nav { margin: 20px 0; }
                .nav a { margin-right: 20px; text-decoration: none; color: #007bff; }
                .nav a:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>EasyXT 实时数据监控系统</h1>
                <p>监控系统性能、数据源状态、API指标和告警信息</p>
            </div>

            <div class="nav">
                <a href="/dashboard">监控仪表板</a>
                <a href="/api/health">健康检查</a>
                <a href="/api/overview">系统概览</a>
            </div>

            <h2>API 端点</h2>
            <ul>
                <li><a href="/api/system/current">当前系统指标</a></li>
                <li><a href="/api/datasources/status">数据源状态</a></li>
                <li><a href="/api/api/stats">API统计</a></li>
                <li><a href="/api/alerts/active">活跃告警</a></li>
                <li><a href="/api/metrics/names">指标名称列表</a></li>
                <li><a href="/api/alerts/degrade/stats">降级统计</a></li>
                <li><a href="/api/alerts/degrade/events">降级事件详情</a></li>
            </ul>
        </body>
        </html>
        """
        delay_ms = int(self.ui_config.get("source_scroll_release_delay_ms", 200))
        html = html.replace("__SOURCE_SCROLL_RELEASE_DELAY_MS__", str(delay_ms))
        return web.Response(text=html, content_type="text/html")

    async def _handle_dashboard(self, request):
        """仪表板页面"""
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>监控仪表板</title>
            <meta charset="utf-8">
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
                .dashboard { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                .card h3 { margin-top: 0; color: #333; }
                .metric { display: flex; justify-content: space-between; margin: 10px 0; }
                .metric-name { font-weight: bold; }
                .metric-value { color: #007bff; }
                .status-online { color: #28a745; }
                .status-offline { color: #dc3545; }
                .alert-critical { background: #f8d7da; border-left: 4px solid #dc3545; }
                .alert-warning { background: #fff3cd; border-left: 4px solid #ffc107; }
                .refresh-btn { background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
            </style>
        </head>
        <body>
            <h1>EasyXT 监控仪表板</h1>
            <button class="refresh-btn" onclick="refreshData()">刷新数据</button>

            <div class="dashboard" id="dashboard">
                <div class="card">
                    <h3>系统状态</h3>
                    <div id="system-metrics">加载中...</div>
                </div>

                <div class="card">
                    <h3>数据源状态</h3>
                    <div id="datasource-status">加载中...</div>
                </div>

                <div class="card">
                    <h3>数据源质量指标</h3>
                    <div id="datasource-metrics">加载中...</div>
                    <div id="datasource-trends" style="margin-top: 10px;">
                        <div style="font-size: 12px; color: #666; margin-bottom: 6px;">数据源趋势</div>
                        <div id="datasource-trends-list"></div>
                    </div>
                </div>

                <div class="card">
                    <h3>API统计</h3>
                    <div id="api-stats">加载中...</div>
                </div>

                <div class="card">
                    <h3>活跃告警</h3>
                    <div id="active-alerts">加载中...</div>
                </div>

                <div class="card">
                    <h3>降级统计</h3>
                    <div id="degrade-stats">加载中...</div>
                </div>

                <div class="card">
                    <h3>过期源告警历史</h3>
                    <div style="display:flex; gap:8px; margin-bottom:8px; align-items:center;">
                        <div style="position:relative; flex:1;">
                            <input id="stale-source-filter" placeholder="全部源" style="width:100%; padding:4px 6px; font-size:12px;" oninput="renderSourceDropdown()" onkeydown="handleSourceKeydown(event)" onblur="hideSourceDropdown()" />
                            <div id="stale-source-dropdown" style="position:absolute; top:28px; left:0; right:0; background:white; border:1px solid #ddd; max-height:160px; overflow-y:auto; display:none; z-index:10;"></div>
                        </div>
                        <button class="refresh-btn" style="padding:4px 8px;" onclick="toggleSourceDropdown()">▼</button>
                        <select id="stale-duration" style="padding:4px 6px; font-size:12px;">
                            <option value="1h">1h</option>
                            <option value="6h">6h</option>
                            <option value="24h" selected>24h</option>
                            <option value="7d">7d</option>
                        </select>
                        <button class="refresh-btn" style="padding:4px 8px;" onclick="updateStaleAlerts()">筛选</button>
                    </div>
                    <div id="stale-alerts">加载中...</div>
                </div>

                <div class="card">
                    <h3>前端配置</h3>
                    <div style="font-size: 12px; color: #666; margin-bottom: 6px;">滚动释放延迟</div>
                    <div style="display:flex; gap:8px; align-items:center;">
                        <input id="scroll-delay-input" type="range" min="50" max="1000" step="50" value="200" style="flex:1;" oninput="updateScrollDelayDisplay()" />
                        <span id="scroll-delay-value">200ms</span>
                        <button class="refresh-btn" style="padding:4px 8px;" onclick="applyScrollDelayConfig()">保存</button>
                        <button class="refresh-btn" style="padding:4px 8px; display:none;" id="scroll-delay-copy" onclick="copyAndShowTerminalGuide()">复制+终端指南</button>
                        <button class="refresh-btn" style="padding:4px 8px; display:none;" id="scroll-delay-retry" onclick="retrySaveAfterCommand()">已执行命令，重试保存</button>
                    </div>
                    <div id="scroll-delay-notice" style="font-size: 12px; margin-top: 6px; display:none;"></div>
                    <div id="scroll-delay-guide" style="font-size:12px; margin-top:6px; display:none; background:#f8f9fa; border:1px solid #e9ecef; border-radius:4px; padding:8px;"></div>
                </div>
            </div>

            <script>
                let SOURCE_SCROLL_RELEASE_DELAY_MS = __SOURCE_SCROLL_RELEASE_DELAY_MS__;
                let sourceCandidates = [];
                let sourceGroups = { stale: [], other: [] };
                let sourceActiveIndex = -1;
                let sourceVisibleItems = [];
                let sourceDropdownOutsideBound = false;
                let sourceDropdownVisible = false;
                let sourceScrollDelayValue = SOURCE_SCROLL_RELEASE_DELAY_MS;

                async function fetchData(url) {
                    try {
                        const response = await fetch(url);
                        return await response.json();
                    } catch (error) {
                        console.error('获取数据失败:', error);
                        return null;
                    }
                }

                function formatMetric(name, value, unit = '') {
                    return `<div class="metric"><span class="metric-name">${name}</span><span class="metric-value">${value}${unit}</span></div>`;
                }

                function updateScrollDelayDisplay() {
                    const input = document.getElementById('scroll-delay-input');
                    const label = document.getElementById('scroll-delay-value');
                    if (!input || !label) {
                        return;
                    }
                    label.textContent = `${input.value}ms`;
                }

                let scrollDelayAdviceText = '';

                function showScrollDelayNotice(message, level, adviceText = '') {
                    const notice = document.getElementById('scroll-delay-notice');
                    const copyBtn = document.getElementById('scroll-delay-copy');
                    const retryBtn = document.getElementById('scroll-delay-retry');
                    if (!notice) {
                        return;
                    }
                    scrollDelayAdviceText = adviceText || '';
                    notice.textContent = message;
                    if (level === 'error') {
                        notice.style.color = '#dc3545';
                    } else if (level === 'success') {
                        notice.style.color = '#28a745';
                    } else {
                        notice.style.color = '#666';
                    }
                    notice.style.display = 'block';
                    if (copyBtn) {
                        copyBtn.style.display = scrollDelayAdviceText ? 'inline-block' : 'none';
                    }
                    if (retryBtn) {
                        retryBtn.style.display = scrollDelayAdviceText ? 'inline-block' : 'none';
                    }
                    setTimeout(() => {
                        notice.style.display = 'none';
                        if (copyBtn) {
                            copyBtn.style.display = 'none';
                        }
                        if (retryBtn) {
                            retryBtn.style.display = 'none';
                        }
                    }, 3000);
                }

                function renderTerminalGuide(commandText) {
                    const guide = document.getElementById('scroll-delay-guide');
                    if (!guide) {
                        return;
                    }
                    if (!commandText) {
                        guide.style.display = 'none';
                        guide.innerHTML = '';
                        return;
                    }
                    const escaped = commandText.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
                    guide.innerHTML = `PowerShell 操作步骤：<br>1) 打开终端并进入项目目录<br>2) 粘贴并执行命令<br>3) 执行后点击保存重试<br><pre style="margin:6px 0 0 0; padding:6px; background:#fff; border:1px solid #ddd; border-radius:4px; white-space:pre-wrap;">${escaped}</pre>`;
                    guide.style.display = 'block';
                }

                async function copyAndShowTerminalGuide() {
                    if (!scrollDelayAdviceText) {
                        return;
                    }
                    try {
                        await navigator.clipboard.writeText(scrollDelayAdviceText);
                        showScrollDelayNotice('命令已复制', 'success', scrollDelayAdviceText);
                        renderTerminalGuide(scrollDelayAdviceText);
                    } catch (error) {
                        console.error('复制命令失败:', error);
                        showScrollDelayNotice('复制失败，请手动复制', 'error', scrollDelayAdviceText);
                        renderTerminalGuide(scrollDelayAdviceText);
                    }
                }

                function retrySaveAfterCommand() {
                    applyScrollDelayConfig();
                }

                async function applyScrollDelayConfig() {
                    const input = document.getElementById('scroll-delay-input');
                    if (!input) {
                        return;
                    }
                    sourceScrollDelayValue = Number(input.value || SOURCE_SCROLL_RELEASE_DELAY_MS);
                    try {
                        const response = await fetch('/api/dashboard/ui-config', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ source_scroll_release_delay_ms: sourceScrollDelayValue })
                        });
                        if (!response.ok) {
                            showScrollDelayNotice('保存失败，请稍后重试', 'error');
                            return;
                        }
                        const result = await response.json();
                        if (result && result.persisted) {
                            showScrollDelayNotice('保存成功', 'success');
                            renderTerminalGuide('');
                            const retryBtn = document.getElementById('scroll-delay-retry');
                            if (retryBtn) {
                                retryBtn.style.display = 'none';
                            }
                        } else {
                            let reason = '保存成功但未持久化';
                            if (result && result.persist_error === 'permission_denied') {
                                reason = '保存成功但未持久化（权限不足）';
                            } else if (result && result.persist_error === 'file_locked') {
                                reason = '保存成功但未持久化（文件锁定）';
                            } else if (result && result.persist_error === 'path_not_found') {
                                reason = '保存成功但未持久化（路径不存在）';
                            }
                            if (result && result.persist_advice) {
                                reason = `${reason}，${result.persist_advice}`;
                            }
                            showScrollDelayNotice(reason, 'info', result && result.persist_advice ? result.persist_advice : '');
                            renderTerminalGuide(result && result.persist_advice ? result.persist_advice : '');
                        }
                    } catch (error) {
                        console.error('保存前端配置失败:', error);
                        showScrollDelayNotice('保存失败，请检查网络', 'error');
                    }
                }

                async function updateSystemMetrics() {
                    const data = await fetchData('/api/system/current');
                    const container = document.getElementById('system-metrics');

                    if (data) {
                        container.innerHTML =
                            formatMetric('CPU使用率', data.cpu_percent?.toFixed(1), '%') +
                            formatMetric('内存使用率', data.memory_percent?.toFixed(1), '%') +
                            formatMetric('磁盘使用率', data.disk_percent?.toFixed(1), '%') +
                            formatMetric('进程数', data.process_count);
                    } else {
                        container.innerHTML = '<div>无法获取系统指标</div>';
                    }
                }

                function initFrontendConfigPanel() {
                    const input = document.getElementById('scroll-delay-input');
                    const label = document.getElementById('scroll-delay-value');
                    if (!input || !label) {
                        return;
                    }
                    input.value = String(SOURCE_SCROLL_RELEASE_DELAY_MS);
                    sourceScrollDelayValue = SOURCE_SCROLL_RELEASE_DELAY_MS;
                    label.textContent = `${SOURCE_SCROLL_RELEASE_DELAY_MS}ms`;
                }

                async function updateDataSourceStatus() {
                    const data = await fetchData('/api/datasources/status');
                    const container = document.getElementById('datasource-status');

                    if (data && typeof data === 'object') {
                        let html = '';
                        for (const [name, status] of Object.entries(data)) {
                            const statusClass = status.status === 'online' ? 'status-online' : 'status-offline';
                            html += `<div class="metric"><span class="metric-name">${name}</span><span class="metric-value ${statusClass}">${status.status || '未知'}</span></div>`;
                        }
                        container.innerHTML = html || '<div>暂无数据源</div>';
                    } else {
                        container.innerHTML = '<div>无法获取数据源状态</div>';
                    }
                }

                function latestBySource(points) {
                    const latest = {};
                    if (!Array.isArray(points)) {
                        return latest;
                    }
                    for (const p of points) {
                        const source = p.tags?.source || 'unknown';
                        const ts = new Date(p.timestamp || 0).getTime();
                        if (!latest[source] || ts > latest[source].ts) {
                            latest[source] = { ts, value: p.value };
                        }
                    }
                    return latest;
                }

                async function updateDataSourceMetrics() {
                    const availability = await fetchData('/api/metrics/query?metric=datasource.availability&duration=10m');
                    const responseTime = await fetchData('/api/metrics/query?metric=datasource.response_time_ms&duration=10m');
                    const staleness = await fetchData('/api/metrics/query?metric=datasource.stale_flag&duration=10m');
                    const container = document.getElementById('datasource-metrics');
                    const availMap = latestBySource(availability);
                    const rtMap = latestBySource(responseTime);
                    const staleMap = latestBySource(staleness);
                    const sources = new Set([...Object.keys(availMap), ...Object.keys(rtMap)]);
                    if (sources.size === 0) {
                        container.innerHTML = '<div>暂无指标数据</div>';
                        return;
                    }
                    let html = '';
                    sources.forEach((name) => {
                        const avail = availMap[name]?.value ?? null;
                        const rt = rtMap[name]?.value ?? null;
                        const stale = staleMap[name]?.value ?? 0;
                        const statusClass = avail === 1 ? 'status-online' : 'status-offline';
                        const statusText = avail === null ? '未知' : (avail === 1 ? '在线' : '离线');
                        const rtText = rt === null ? '--' : `${Number(rt).toFixed(1)}ms`;
                        const staleText = stale >= 1 ? '过期' : '正常';
                        const staleColor = stale >= 1 ? '#dc3545' : '#28a745';
                        html += `<div class="metric"><span class="metric-name">${name}</span><span class="metric-value ${statusClass}">${statusText} / ${rtText} / <span style="color:${staleColor}">${staleText}</span></span></div>`;
                    });
                    container.innerHTML = html;
                }

                function drawSparkline(svgId, points, color) {
                    const svg = document.getElementById(svgId);
                    if (!svg) {
                        return;
                    }
                    const width = svg.clientWidth || 280;
                    const height = svg.clientHeight || 60;
                    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
                    if (!points || points.length === 0) {
                        svg.innerHTML = '';
                        return;
                    }
                    const sorted = points.slice().sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
                    const values = sorted.map(p => Number(p.value || 0));
                    const min = Math.min(...values);
                    const max = Math.max(...values);
                    const span = max - min || 1;
                    const step = width / Math.max(sorted.length - 1, 1);
                    let d = '';
                    sorted.forEach((p, i) => {
                        const x = i * step;
                        const y = height - ((Number(p.value || 0) - min) / span) * height;
                        d += `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)} `;
                    });
                    svg.innerHTML = `<path d="${d.trim()}" fill="none" stroke="${color}" stroke-width="2"/>`;
                }

                function groupBySource(points) {
                    const grouped = {};
                    if (!Array.isArray(points)) {
                        return grouped;
                    }
                    for (const p of points) {
                        const source = p.tags?.source || 'unknown';
                        if (!grouped[source]) {
                            grouped[source] = [];
                        }
                        grouped[source].push(p);
                    }
                    return grouped;
                }

                async function updateDataSourceTrends() {
                    const availability = await fetchData('/api/metrics/query?metric=datasource.availability&duration=1h');
                    const responseTime = await fetchData('/api/metrics/query?metric=datasource.response_time_ms&duration=1h');
                    const availGroups = groupBySource(availability || []);
                    const rtGroups = groupBySource(responseTime || []);
                    const sources = new Set([...Object.keys(availGroups), ...Object.keys(rtGroups)]);
                    const list = document.getElementById('datasource-trends-list');
                    if (sources.size === 0) {
                        list.innerHTML = '<div style="color:#999;">暂无趋势数据</div>';
                        return;
                    }
                    let html = '';
                    sources.forEach((name) => {
                        const availId = `trend-availability-${name}`;
                        const rtId = `trend-response-${name}`;
                        html += `
                            <div style="margin-bottom: 8px; padding: 6px; border: 1px solid #f0f0f0; border-radius: 4px;">
                                <div style="font-size: 12px; color: #333; margin-bottom: 4px;">${name}</div>
                                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 6px;">
                                    <div>
                                        <div style="font-size: 11px; color: #666; margin-bottom: 2px;">可用性</div>
                                        <svg id="${availId}" width="100%" height="40"></svg>
                                    </div>
                                    <div>
                                        <div style="font-size: 11px; color: #666; margin-bottom: 2px;">响应时间</div>
                                        <svg id="${rtId}" width="100%" height="40"></svg>
                                    </div>
                                </div>
                            </div>
                        `;
                    });
                    list.innerHTML = html;
                    sources.forEach((name) => {
                        drawSparkline(`trend-availability-${name}`, availGroups[name] || [], '#28a745');
                        drawSparkline(`trend-response-${name}`, rtGroups[name] || [], '#007bff');
                    });
                }

                async function updateApiStats() {
                    const data = await fetchData('/api/api/stats');
                    const container = document.getElementById('api-stats');

                    if (data && data.overall_stats) {
                        const stats = data.overall_stats;
                        container.innerHTML =
                            formatMetric('总请求数', stats.total_requests) +
                            formatMetric('成功率', stats.success_rate?.toFixed(1), '%') +
                            formatMetric('平均响应时间', stats.avg_response_time?.toFixed(1), 'ms') +
                            formatMetric('每秒请求数', stats.requests_per_second?.toFixed(1));
                    } else {
                        container.innerHTML = '<div>无法获取API统计</div>';
                    }
                }

                async function updateActiveAlerts() {
                    const data = await fetchData('/api/alerts/active');
                    const container = document.getElementById('active-alerts');

                    if (data && Array.isArray(data)) {
                        if (data.length === 0) {
                            container.innerHTML = '<div style="color: #28a745;">暂无活跃告警</div>';
                        } else {
                            let html = '';
                            data.slice(0, 5).forEach(alert => {
                                const alertClass = alert.rule_name === 'data_source_stale' ? 'alert-critical' : (alert.level === 'critical' ? 'alert-critical' : 'alert-warning');
                                const badge = alert.rule_name === 'data_source_stale' ? '<span style="color:#dc3545; font-weight:bold;">[过期]</span> ' : '';
                                html += `<div class="${alertClass}" style="padding: 10px; margin: 5px 0; border-radius: 4px;">
                                    ${badge}<strong>${alert.title}</strong><br>
                                    <small>${alert.message}</small>
                                </div>`;
                            });
                            container.innerHTML = html;
                        }
                    } else {
                        container.innerHTML = '<div>无法获取告警信息</div>';
                    }
                }

                function updateSourceFilterOptions(sources, preferSource, staleSources = []) {
                    const input = document.getElementById('stale-source-filter');
                    if (!input) {
                        return;
                    }
                    const current = input.value || '';
                    const unique = Array.from(new Set(sources)).filter(Boolean).sort();
                    const staleSet = new Set(staleSources);
                    const staleOrdered = staleSources.filter(s => unique.includes(s));
                    const otherOrdered = unique.filter(s => !staleSet.has(s));
                    sourceGroups = {
                        stale: staleOrdered,
                        other: otherOrdered
                    };
                    if (preferSource && unique.includes(preferSource)) {
                        sourceCandidates = [preferSource, ...unique.filter(s => s !== preferSource)];
                    } else {
                        sourceCandidates = unique;
                    }
                    if (!current && preferSource && sourceCandidates.includes(preferSource)) {
                        input.value = preferSource;
                        return;
                    }
                    if (sourceCandidates.includes(current)) {
                        input.value = current;
                    }
                    if (!sourceDropdownOutsideBound) {
                        document.addEventListener('mousedown', closeSourceDropdownIfOutside);
                        sourceDropdownOutsideBound = true;
                    }
                }

                function toggleSourceDropdown() {
                    const input = document.getElementById('stale-source-filter');
                    if (!input) {
                        return;
                    }
                    if (sourceDropdownVisible) {
                        const dropdown = document.getElementById('stale-source-dropdown');
                        if (dropdown) {
                            dropdown.style.display = 'none';
                        }
                        sourceDropdownVisible = false;
                        return;
                    }
                    input.focus();
                    renderSourceDropdown(true);
                }

                let sourceDropdownLocked = false;
                let sourceScrollLock = false;
                let sourceScrollReleaseTimer = null;

                function scheduleScrollRelease() {
                    if (sourceScrollReleaseTimer) {
                        clearTimeout(sourceScrollReleaseTimer);
                    }
                    sourceScrollReleaseTimer = setTimeout(() => {
                        sourceScrollLock = false;
                        sourceScrollReleaseTimer = null;
                    }, sourceScrollDelayValue || SOURCE_SCROLL_RELEASE_DELAY_MS);
                }

                function closeSourceDropdownIfOutside(event) {
                    if (sourceDropdownLocked || sourceScrollLock) {
                        return;
                    }
                    const container = document.getElementById('stale-source-dropdown');
                    const input = document.getElementById('stale-source-filter');
                    if (!container || !input) {
                        return;
                    }
                    const target = event.target;
                    const inside = container.contains(target) || input.contains(target);
                    if (!inside) {
                        container.style.display = 'none';
                        sourceDropdownVisible = false;
                    }
                }

                function hideSourceDropdown() {
                    const dropdown = document.getElementById('stale-source-dropdown');
                    if (!dropdown) {
                        return;
                    }
                    setTimeout(() => {
                        if (sourceDropdownLocked || sourceScrollLock) {
                            return;
                        }
                        dropdown.style.display = 'none';
                        sourceDropdownVisible = false;
                    }, 150);
                }

                function renderSourceDropdown(showAll = false) {
                    const dropdown = document.getElementById('stale-source-dropdown');
                    const input = document.getElementById('stale-source-filter');
                    if (!dropdown || !input) {
                        return;
                    }
                    const keyword = showAll ? '' : (input.value || '').toLowerCase();
                    const staleFiltered = sourceGroups.stale.filter(s => s.toLowerCase().includes(keyword));
                    const otherFiltered = sourceGroups.other.filter(s => s.toLowerCase().includes(keyword));
                    const combined = staleFiltered.concat(otherFiltered);
                    if (combined.length === 0) {
                        dropdown.style.display = 'none';
                        sourceDropdownVisible = false;
                        return;
                    }
                    sourceVisibleItems = combined;
                    if (sourceActiveIndex >= sourceVisibleItems.length) {
                        sourceActiveIndex = sourceVisibleItems.length - 1;
                    }
                    if (sourceActiveIndex < 0 && sourceVisibleItems.length > 0) {
                        sourceActiveIndex = 0;
                    }
                    const renderGroup = (title, items, offset) => {
                        if (items.length === 0) {
                            return '';
                        }
                        const header = `<div style="padding:4px 6px; font-size:11px; color:#666; background:#f7f7f7;">${title}</div>`;
                        const body = items.map((s, idx) => {
                            const active = (offset + idx) === sourceActiveIndex ? 'background:#eef3ff;' : '';
                            return `<div style="padding:4px 6px; cursor:pointer; ${active}" data-value="${s.replace(/"/g, '&quot;')}" data-index="${offset + idx}" onmouseover="highlightSourceIndex(this.dataset.index)" onmousedown="lockSourceDropdown(true)" onmouseup="lockSourceDropdown(false)" onclick="selectSourceFilter(this.dataset.value)">${s}</div>`;
                        }).join('');
                        return header + body;
                    };
                    const staleHtml = renderGroup('过期源', staleFiltered, 0);
                    const otherHtml = renderGroup('其他源', otherFiltered, staleFiltered.length);
                    dropdown.innerHTML = staleHtml + otherHtml;
                    dropdown.style.display = 'block';
                    sourceDropdownVisible = true;
                    ensureSourceVisible();
                    dropdown.onmousedown = () => {
                        sourceScrollLock = true;
                    };
                    dropdown.onmouseup = () => {
                        scheduleScrollRelease();
                    };
                    dropdown.onmouseleave = () => {
                        scheduleScrollRelease();
                    };
                    dropdown.onwheel = () => {
                        sourceScrollLock = true;
                        scheduleScrollRelease();
                    };
                    dropdown.onscroll = () => {
                        sourceScrollLock = true;
                        scheduleScrollRelease();
                    };
                    dropdown.ontouchstart = () => {
                        sourceScrollLock = true;
                    };
                    dropdown.ontouchmove = () => {
                        sourceScrollLock = true;
                        scheduleScrollRelease();
                    };
                    dropdown.ontouchend = () => {
                        scheduleScrollRelease();
                    };
                }

                function highlightSourceIndex(indexValue) {
                    const index = Number(indexValue);
                    if (!Number.isFinite(index)) {
                        return;
                    }
                    sourceActiveIndex = Math.max(0, Math.min(index, sourceVisibleItems.length - 1));
                    renderSourceDropdown();
                }

                function ensureSourceVisible() {
                    const dropdown = document.getElementById('stale-source-dropdown');
                    if (!dropdown) {
                        return;
                    }
                    const active = dropdown.querySelector(`[data-index="${sourceActiveIndex}"]`);
                    if (active && typeof active.scrollIntoView === 'function') {
                        active.scrollIntoView({ block: 'nearest' });
                    }
                }

                function selectSourceFilter(value) {
                    const input = document.getElementById('stale-source-filter');
                    const dropdown = document.getElementById('stale-source-dropdown');
                    if (!input || !dropdown) {
                        return;
                    }
                    input.value = value;
                    dropdown.style.display = 'none';
                    sourceDropdownLocked = false;
                    sourceDropdownVisible = false;
                }

                function lockSourceDropdown(locked) {
                    sourceDropdownLocked = locked === true;
                }

                function handleSourceKeydown(event) {
                    if (event.key === 'ArrowDown') {
                        event.preventDefault();
                        sourceActiveIndex = Math.min(sourceActiveIndex + 1, sourceVisibleItems.length - 1);
                        renderSourceDropdown();
                    } else if (event.key === 'ArrowUp') {
                        event.preventDefault();
                        sourceActiveIndex = Math.max(sourceActiveIndex - 1, 0);
                        renderSourceDropdown();
                    } else if (event.key === 'Enter') {
                        event.preventDefault();
                        if (sourceActiveIndex >= 0 && sourceActiveIndex < sourceVisibleItems.length) {
                            selectSourceFilter(sourceVisibleItems[sourceActiveIndex]);
                        }
                    } else if (event.key === 'Escape') {
                        event.preventDefault();
                        const dropdown = document.getElementById('stale-source-dropdown');
                        if (dropdown) {
                            dropdown.style.display = 'none';
                            sourceDropdownVisible = false;
                        }
                        sourceDropdownLocked = false;
                    }
                }

                async function updateSourceFilterCandidates() {
                    const [avail, rt, stale] = await Promise.all([
                        fetchData('/api/metrics/query?metric=datasource.availability&duration=24h'),
                        fetchData('/api/metrics/query?metric=datasource.response_time_ms&duration=24h'),
                        fetchData('/api/metrics/query?metric=datasource.stale_flag&duration=24h')
                    ]);
                    const metricSources = []
                        .concat((avail || []).map(a => a.tags?.source))
                        .concat((rt || []).map(a => a.tags?.source))
                        .concat((stale || []).map(a => a.tags?.source))
                        .filter(Boolean);
                    const staleFiltered = (stale || []).filter(s => Number(s.value || 0) >= 1);
                    const staleSorted = staleFiltered.slice().sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                    const staleSources = staleSorted.map(s => s.tags?.source).filter(Boolean);
                    const preferSource = staleSources.length > 0 ? staleSources[0] : '';
                    updateSourceFilterOptions(metricSources, preferSource, staleSources);
                }

                async function updateStaleAlerts() {
                    const source = document.getElementById('stale-source-filter')?.value || '';
                    const duration = document.getElementById('stale-duration')?.value || '24h';
                    const query = new URLSearchParams({
                        duration: duration,
                        limit: '50',
                        rule: 'data_source_stale'
                    });
                    if (source) {
                        query.append('source', source);
                    }
                    const data = await fetchData(`/api/alerts/history?${query.toString()}`);
                    const container = document.getElementById('stale-alerts');
                    if (data && Array.isArray(data)) {
                        const sorted = data.slice().sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                        const alertSources = sorted.map(a => a.tags?.source).filter(Boolean);
                        const preferSource = sorted.length > 0 ? (sorted[0].tags?.source || '') : '';
                        updateSourceFilterOptions(alertSources, preferSource, sourceGroups.stale);
                        const filtered = sorted.slice(0, 10);
                        if (filtered.length === 0) {
                            container.innerHTML = '<div style="color: #28a745;">暂无过期源告警</div>';
                            return;
                        }
                        let html = '';
                        filtered.forEach(alert => {
                            const time = new Date(alert.timestamp).toLocaleTimeString();
                            const tagText = alert.tags ? JSON.stringify(alert.tags) : '';
                            html += `<div style="font-size: 12px; margin: 4px 0; border-bottom: 1px solid #f5f5f5; padding: 4px;">
                                <div><strong>${time}</strong> ${alert.message || alert.title || '过期告警'}</div>
                                <div style="color:#666;">${tagText}</div>
                            </div>`;
                        });
                        container.innerHTML = html;
                    } else {
                        container.innerHTML = '<div>无法获取告警历史</div>';
                    }
                }

                async function refreshData() {
                    await Promise.all([
                        updateSystemMetrics(),
                        updateDataSourceStatus(),
                        updateDataSourceMetrics(),
                        updateDataSourceTrends(),
                        updateApiStats(),
                        updateActiveAlerts(),
                        updateDegradeStats(),
                        updateStaleAlerts(),
                        updateSourceFilterCandidates()
                    ]);
                }

                async function updateDegradeStats() {
                    const statsData = await fetchData('/api/alerts/degrade/stats');
                    const eventsData = await fetchData('/api/alerts/degrade/events?limit=5');
                    const container = document.getElementById('degrade-stats');

                    if (statsData) {
                        const triggered = statsData.triggered_from_rollup || 0;
                        const resolved = statsData.resolved_from_rollup || 0;
                        const avgTime = statsData.avg_resolution_time_seconds || 0;
                        const active = statsData.active_alerts || 0;

                        let statusColor = active > 0 ? '#dc3545' : '#28a745';
                        let statusText = active > 0 ? '降级中' : '正常';

                        let html = `
                            <div style="margin-bottom: 10px; padding: 8px; background: ${statusColor}; color: white; border-radius: 4px; text-align: center;">
                                <strong>${statusText}</strong>
                            </div>
                            ${formatMetric('触发次数', triggered)}
                            ${formatMetric('恢复次数', resolved)}
                            ${formatMetric('当前活跃', active)}
                            ${formatMetric('平均恢复时间', avgTime.toFixed(1), 's')}
                        `;

                        // 事件详情只从 /api/alerts/degrade/events 读取，避免双数据源漂移
                        if (eventsData && Array.isArray(eventsData) && eventsData.length > 0) {
                            html += '<div style="margin-top: 10px; border-top: 1px solid #eee; padding-top: 10px;"><strong>最近事件 (点击展开):</strong></div>';
                            html += '<div id="degrade-events-list" style="max-height: 200px; overflow-y: auto;">';
                            eventsData.forEach((event, idx) => {
                                const eventColor = event.status === 'active' ? '#dc3545' : '#28a745';
                                const time = new Date(event.timestamp).toLocaleTimeString();

                                html += `<div style="font-size: 12px; margin: 4px 0; border-bottom: 1px solid #f5f5f5; padding: 4px;">
                                    <div style="cursor: pointer; color: ${eventColor};" onclick="toggleDegradeEventDetail(${idx})">
                                        [${event.status}] ${time} - ${event.message || event.rule_name || '无描述'}
                                    </div>
                                    <div id="degrade-event-detail-${idx}" style="display: none; padding: 6px; background: #f9f9f9; margin-top: 4px; border-radius: 4px; font-size: 11px;">
                                        ${event.source !== undefined && event.source !== null ? `<div><strong>来源:</strong> ${event.source}</div>` : ''}
                                        ${event.value !== undefined && event.value !== null ? `<div><strong>值:</strong> ${event.value}</div>` : ''}
                                        ${event.threshold !== undefined && event.threshold !== null ? `<div><strong>阈值:</strong> ${event.threshold}</div>` : ''}
                                        ${event.tags && Object.keys(event.tags).length > 0 ? `<div><strong>标签:</strong> ${JSON.stringify(event.tags)}</div>` : ''}
                                        ${event.resolved_at !== undefined && event.resolved_at !== null ? `<div><strong>解决时间:</strong> ${new Date(event.resolved_at).toLocaleTimeString()}</div>` : ''}
                                    </div>
                                </div>`;
                            });
                            html += '</div>';
                        }

                        container.innerHTML = html;
                    } else {
                        container.innerHTML = '<div>无法获取降级统计</div>';
                    }
                }

                // 展开/收起事件详情
                function toggleDegradeEventDetail(idx) {
                    const detail = document.getElementById('degrade-event-detail-' + idx);
                    if (detail) {
                        detail.style.display = detail.style.display === 'none' ? 'block' : 'none';
                    }
                }

                // 初始加载和定时刷新
                initFrontendConfigPanel();
                refreshData();
                setInterval(refreshData, 30000); // 30秒刷新一次
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type="text/html")

    async def _handle_health(self, request):
        """健康检查"""
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(tz=_SH).isoformat(),
            "components": {},
        }

        # 检查各组件状态
        if self.system_monitor:
            health_status["components"]["system_monitor"] = "running"

        if self.data_source_monitor:
            health_status["components"]["data_source_monitor"] = "running"

        if self.api_monitor:
            health_status["components"]["api_monitor"] = "running"

        if self.alert_manager:
            health_status["components"]["alert_manager"] = "running"

        if self.metrics_collector:
            health_status["components"]["metrics_collector"] = "running"

        return web.json_response(health_status)

    async def _handle_overview(self, request):
        """系统概览"""
        overview = {
            "timestamp": datetime.now(tz=_SH).isoformat(),
            "system": None,
            "datasources": None,
            "api": None,
            "alerts": None,
        }

        # 系统概览
        if self.system_monitor:
            current = self.system_monitor.get_current_metrics()
            if current:
                overview["system"] = {
                    "cpu_percent": current.cpu_percent,
                    "memory_percent": current.memory_percent,
                    "disk_percent": current.disk_percent,
                    "process_count": current.process_count,
                }

        # 数据源概览
        if self.data_source_monitor:
            status = self.data_source_monitor.get_current_status()
            online_count = sum(
                1 for s in status.values() if isinstance(s, dict) and s.get("status") == "online"
            )
            overview["datasources"] = {
                "total": len(status),
                "online": online_count,
                "offline": len(status) - online_count,
            }

        # API概览
        if self.api_monitor:
            stats = self.api_monitor.get_overall_stats()
            overview["api"] = {
                "total_requests": stats.get("total_requests", 0),
                "success_rate": stats.get("success_rate", 0),
                "error_rate": stats.get("error_rate", 0),
                "avg_response_time": stats.get("avg_response_time", 0),
            }

        # 告警概览
        if self.alert_manager:
            active_alerts = self.alert_manager.get_active_alerts()
            overview["alerts"] = {
                "active_count": len(active_alerts),
                "critical_count": len([a for a in active_alerts if a.level.value == "critical"]),
                "warning_count": len([a for a in active_alerts if a.level.value == "warning"]),
            }

        return web.json_response(overview)

    async def _handle_system_current(self, request):
        """当前系统指标"""
        if not self.system_monitor:
            return web.json_response({"error": "系统监控器未注册"}, status=404)

        current = self.system_monitor.get_current_metrics()
        if current:
            return web.json_response(
                {
                    "timestamp": current.timestamp.isoformat(),
                    "cpu_percent": current.cpu_percent,
                    "memory_percent": current.memory_percent,
                    "memory_used": current.memory_used,
                    "memory_total": current.memory_total,
                    "disk_percent": current.disk_percent,
                    "disk_used": current.disk_used,
                    "disk_total": current.disk_total,
                    "network_sent": current.network_sent,
                    "network_recv": current.network_recv,
                    "process_count": current.process_count,
                    "load_average": current.load_average,
                }
            )

        return web.json_response({"error": "暂无系统指标数据"}, status=404)

    async def _handle_system_history(self, request):
        """系统历史指标"""
        if not self.system_monitor:
            return web.json_response({"error": "系统监控器未注册"}, status=404)

        # 获取查询参数
        duration_str = request.query.get("duration", "1h")
        try:
            if duration_str.endswith("h"):
                duration = timedelta(hours=int(duration_str[:-1]))
            elif duration_str.endswith("m"):
                duration = timedelta(minutes=int(duration_str[:-1]))
            else:
                duration = timedelta(hours=1)
        except ValueError:
            duration = timedelta(hours=1)

        history = self.system_monitor.get_metrics_history(duration)

        return web.json_response(
            [
                {
                    "timestamp": m.timestamp.isoformat(),
                    "cpu_percent": m.cpu_percent,
                    "memory_percent": m.memory_percent,
                    "disk_percent": m.disk_percent,
                    "network_sent": m.network_sent,
                    "network_recv": m.network_recv,
                }
                for m in history
            ]
        )

    async def _handle_system_stats(self, request):
        """系统统计信息"""
        if not self.system_monitor:
            return web.json_response({"error": "系统监控器未注册"}, status=404)

        return web.json_response(self.system_monitor.get_stats())

    async def _handle_datasources_status(self, request):
        """数据源状态"""
        if not self.data_source_monitor:
            return web.json_response({"error": "数据源监控器未注册"}, status=404)

        return web.json_response(self.data_source_monitor.get_current_status())

    async def _handle_datasources_stats(self, request):
        """数据源统计"""
        if not self.data_source_monitor:
            return web.json_response({"error": "数据源监控器未注册"}, status=404)

        return web.json_response(self.data_source_monitor.get_stats())

    async def _handle_api_stats(self, request):
        """API统计"""
        if not self.api_monitor:
            return web.json_response({"error": "API监控器未注册"}, status=404)

        return web.json_response(self.api_monitor.get_stats())

    async def _handle_api_endpoints(self, request):
        """API端点统计"""
        if not self.api_monitor:
            return web.json_response({"error": "API监控器未注册"}, status=404)

        return web.json_response(self.api_monitor.get_endpoint_stats())

    async def _handle_api_errors(self, request):
        """API错误请求"""
        if not self.api_monitor:
            return web.json_response({"error": "API监控器未注册"}, status=404)

        limit = int(request.query.get("limit", 10))
        return web.json_response(self.api_monitor.get_error_requests(limit))

    async def _handle_alerts_active(self, request):
        """活跃告警"""
        if not self.alert_manager:
            return web.json_response({"error": "告警管理器未注册"}, status=404)

        alerts = self.alert_manager.get_active_alerts()
        return web.json_response(
            [
                {
                    "id": alert.id,
                    "rule_name": alert.rule_name,
                    "level": alert.level.value,
                    "title": alert.title,
                    "message": alert.message,
                    "value": alert.value,
                    "threshold": alert.threshold,
                    "timestamp": alert.timestamp.isoformat(),
                    "source": alert.source,
                    "tags": alert.tags,
                }
                for alert in alerts
            ]
        )

    async def _handle_alerts_history(self, request):
        """告警历史"""
        if not self.alert_manager:
            return web.json_response({"error": "告警管理器未注册"}, status=404)

        limit = int(request.query.get("limit", 50))
        duration_str = request.query.get("duration", "24h")
        source_filter = request.query.get("source")
        rule_filter = request.query.get("rule")

        try:
            if duration_str.endswith("h"):
                duration = timedelta(hours=int(duration_str[:-1]))
            elif duration_str.endswith("d"):
                duration = timedelta(days=int(duration_str[:-1]))
            else:
                duration = timedelta(hours=24)
        except ValueError:
            duration = timedelta(hours=24)

        alerts = self.alert_manager.get_alert_history(duration, limit=limit)
        if rule_filter:
            alerts = [a for a in alerts if a.rule_name == rule_filter]
        if source_filter:
            alerts = [a for a in alerts if (a.tags or {}).get("source") == source_filter]
        return web.json_response(
            [
                {
                    "id": alert.id,
                    "rule_name": alert.rule_name,
                    "level": alert.level.value,
                    "title": alert.title,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "status": alert.status.value,
                    "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
                    "tags": alert.tags,
                }
                for alert in alerts
            ]
        )

    async def _handle_alerts_stats(self, request):
        """告警统计"""
        if not self.alert_manager:
            return web.json_response({"error": "告警管理器未注册"}, status=404)

        return web.json_response(self.alert_manager.get_alert_stats())

    async def _handle_alerts_resolve(self, request):
        """解决告警"""
        if not self.alert_manager:
            return web.json_response({"error": "告警管理器未注册"}, status=404)

        data = await request.json()
        alert_id = data.get("alert_id")

        if not alert_id:
            return web.json_response({"error": "缺少alert_id参数"}, status=400)

        self.alert_manager.resolve_alert(alert_id)
        return web.json_response({"success": True})

    async def _handle_alerts_rollup(self, request):
        return web.json_response(self._alerts_rollups)

    def _record_alert_metric(self, metric_name: str, value: float, tags: dict[str, str]):
        if not self.metrics_collector:
            return
        try:
            self.metrics_collector._add_metric_point(
                MetricPoint(
                    timestamp=datetime.now(tz=_SH),
                    metric_name=metric_name,
                    value=value,
                    tags=tags,
                    source="alerts_ingest",
                )
            )
        except Exception:
            pass

    async def _handle_alerts_ingest(self, request):
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)
        if not isinstance(payload, dict):
            return web.json_response({"error": "invalid_payload"}, status=400)

        alert_status = payload.get("status", "")
        if alert_status == "resolved" and self.alert_manager:
            rule_name = payload.get("rule_name", "")
            if rule_name:
                active_alerts = self.alert_manager.get_active_alerts()
                resolved_count = 0
                source = payload.get("source", "")
                tags = payload.get("tags") if isinstance(payload.get("tags"), dict) else {}
                tags = cast(dict[str, Any], tags)
                for alert in active_alerts:
                    if alert.rule_name == rule_name:
                        if source and (alert.source or "") != source:
                            continue
                        alert_tags = alert.tags or {}
                        if any(str(alert_tags.get(k, "")) != str(v) for k, v in tags.items()):
                            continue
                        self.alert_manager.resolve_alert(alert.id)
                        resolved_count += 1
                        logger.info(f"自动解决告警: {alert.id} - {rule_name}")

                # 记录到rollups
                resolve_record = {
                    "rule_name": rule_name,
                    "status": "resolved",
                    "message": payload.get("message", f"自动解决 {resolved_count} 个告警"),
                    "timestamp": datetime.now(tz=_SH).isoformat(),
                    "source": source or "alerts_ingest",
                    "tags": tags,
                    "resolved_count": resolved_count,
                }
                self._alerts_rollups.append(resolve_record)
                if len(self._alerts_rollups) > self._alerts_rollups_max:
                    self._alerts_rollups = self._alerts_rollups[-self._alerts_rollups_max :]
                metric_tags = {"rule": rule_name}
                metric_tags.update({f"tag_{k}": str(v) for k, v in tags.items()})
                self._record_alert_metric(
                    metric_name=f"alerts.{rule_name}.resolved",
                    value=float(resolved_count),
                    tags=metric_tags,
                )

                return web.json_response(
                    {"success": True, "action": "resolved", "resolved_count": resolved_count}
                )

        if self.alert_manager:
            rule_name = payload.get("rule_name", "")
            if rule_name:
                level_text = str(payload.get("level", "warning")).lower()
                level_map = {
                    "info": AlertLevel.INFO,
                    "warning": AlertLevel.WARNING,
                    "critical": AlertLevel.CRITICAL,
                }
                level = level_map.get(level_text, AlertLevel.WARNING)
                if rule_name not in self.alert_manager.rules:
                    self.alert_manager.update_rule(
                        AlertRule(
                            name=rule_name,
                            condition=payload.get("title") or rule_name,
                            level=level,
                            threshold=float(payload.get("threshold") or 1.0),
                            cooldown=300,
                            notification_channels=["webhook"],
                        )
                    )
                self.alert_manager.trigger_alert(
                    rule_name=rule_name,
                    title=payload.get("title") or rule_name,
                    message=payload.get("message") or "",
                    value=float(payload.get("value") or 0),
                    threshold=float(payload.get("threshold") or 0),
                    source=payload.get("source") or "alerts_ingest",
                    tags=payload.get("tags") if isinstance(payload.get("tags"), dict) else {},
                )
                metric_tags = {"rule": rule_name}
                payload_tags = payload.get("tags") if isinstance(payload.get("tags"), dict) else {}
                payload_tags = cast(dict[str, Any], payload_tags)
                metric_tags.update({f"tag_{k}": str(v) for k, v in payload_tags.items()})
                self._record_alert_metric(
                    metric_name=f"alerts.{rule_name}.triggered", value=1.0, tags=metric_tags
                )

        self._alerts_rollups.append(payload)
        if len(self._alerts_rollups) > self._alerts_rollups_max:
            self._alerts_rollups = self._alerts_rollups[-self._alerts_rollups_max :]
        if self.alert_manager:
            count = 0
            by_type = payload.get("by_type") or {}
            if isinstance(by_type, dict):
                count = int(by_type.get("REALTIME_DEGRADE", 0) or 0)
            if count > 0:
                self._record_alert_metric(
                    metric_name="alerts.realtime_degrade.rollup_count",
                    value=float(count),
                    tags={},
                )
                if "realtime_degrade" not in self.alert_manager.rules:
                    self.alert_manager.update_rule(
                        AlertRule(
                            name="realtime_degrade",
                            condition="实时链路降级事件",
                            level=AlertLevel.WARNING,
                            threshold=1.0,
                            cooldown=300,
                            notification_channels=["webhook"],
                        )
                    )
                tags = {}
                by_mode = payload.get("by_mode") or {}
                by_mode = cast(dict[str, Any], by_mode) if isinstance(by_mode, dict) else {}
                if isinstance(by_mode, dict):
                    tags = {f"mode_{k}": str(v) for k, v in by_mode.items()}
                self.alert_manager.trigger_alert(
                    rule_name="realtime_degrade",
                    title="实时链路降级",
                    message=f"检测到 {count} 条降级事件",
                    value=float(count),
                    threshold=1.0,
                    source="alerts_ingest",
                    tags=tags,
                )
        return web.json_response({"success": True, "action": "triggered"})

    async def _handle_ui_config_get(self, request):
        return web.json_response(
            {
                "source_scroll_release_delay_ms": int(
                    self.ui_config.get("source_scroll_release_delay_ms", 200)
                )
            }
        )

    async def _handle_ui_config_update(self, request):
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)

        delay_ms = payload.get("source_scroll_release_delay_ms")
        try:
            delay_ms = int(delay_ms)
        except Exception:
            return web.json_response({"error": "invalid_delay_ms"}, status=400)

        delay_ms = max(50, min(2000, delay_ms))
        self.ui_config["source_scroll_release_delay_ms"] = delay_ms

        persisted = False
        persist_error = None
        persist_advice = None
        if self.config_file:
            try:
                with open(self.config_file, encoding="utf-8") as f:
                    config_data = json.load(f)
            except Exception:
                config_data = {}
            if not isinstance(config_data, dict):
                config_data = {}
            dashboard_cfg = config_data.get("dashboard")
            if not isinstance(dashboard_cfg, dict):
                dashboard_cfg = {}
                config_data["dashboard"] = dashboard_cfg
            ui_cfg = dashboard_cfg.get("ui")
            if not isinstance(ui_cfg, dict):
                ui_cfg = {}
                dashboard_cfg["ui"] = ui_cfg
            ui_cfg["source_scroll_release_delay_ms"] = delay_ms
            try:
                with open(self.config_file, "w", encoding="utf-8") as f:
                    json.dump(config_data, f, indent=2, ensure_ascii=False)
                persisted = True
            except PermissionError:
                persisted = False
                persist_error = "permission_denied"
                persist_advice = f"执行: icacls \"{self.config_file}\" /grant %USERNAME%:F"
            except FileNotFoundError:
                persisted = False
                persist_error = "path_not_found"
                persist_advice = f"执行: mkdir \"{os.path.dirname(self.config_file)}\""
            except OSError:
                persisted = False
                persist_error = "file_locked"
                persist_advice = f"关闭占用进程或移除只读: attrib -r \"{self.config_file}\""
            except Exception:
                persisted = False
                persist_error = "unknown_error"
                persist_advice = f"检查磁盘可写与权限: icacls \"{self.config_file}\""
            if not persisted:
                logger.warning(
                    "UI配置持久化失败: error=%s advice=%s path=%s",
                    persist_error,
                    persist_advice,
                    self.config_file,
                )

        return web.json_response(
            {
                "success": True,
                "source_scroll_release_delay_ms": delay_ms,
                "persisted": persisted,
                "persist_error": persist_error,
                "persist_advice": persist_advice,
            }
        )

    def _build_degrade_events(self, limit: int = 50) -> list[dict[str, Any]]:
        """构建统一字段契约的降级事件列表。"""
        if not self.alert_manager:
            return []

        history = self.alert_manager.get_alert_history(duration=timedelta(hours=24), limit=2000)
        degrade_alerts = [a for a in history if a.rule_name == "realtime_degrade"]

        events: list[dict[str, Any]] = []
        for alert in degrade_alerts:
            events.append(
                {
                    "id": alert.id,
                    "rule_name": alert.rule_name,
                    "status": alert.status.value,
                    "timestamp": alert.timestamp.isoformat(),
                    "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
                    "message": alert.message,
                    "source": alert.source,
                    "value": alert.value,
                    "threshold": alert.threshold,
                    "tags": alert.tags or {},
                    "resolved_count": None,
                }
            )

        events = events[: max(0, int(limit))]
        return events

    async def _handle_degrade_stats(self, request):
        """降级专用统计"""
        if not self.alert_manager:
            return web.json_response({"error": "告警管理器未注册"}, status=404)

        # 获取realtime_degrade规则相关的告警统计
        history = self.alert_manager.get_alert_history(duration=timedelta(hours=24), limit=1000)

        # 筛选realtime_degrade相关的告警
        degrade_alerts = [a for a in history if a.rule_name == "realtime_degrade"]

        triggered_count = len([a for a in degrade_alerts if a.status == AlertStatus.ACTIVE])
        resolved_count = len([a for a in degrade_alerts if a.status == AlertStatus.RESOLVED])

        # 计算平均解决时间
        avg_resolution_time = 0.0
        resolved_alerts = [
            a for a in degrade_alerts if a.status == AlertStatus.RESOLVED and a.resolved_at
        ]
        if resolved_alerts:
            total_time = sum(
                (a.resolved_at - a.timestamp).total_seconds()
                for a in resolved_alerts
                if a.resolved_at
            )
            avg_resolution_time = total_time / len(resolved_alerts)

        # 从rollups中获取统计
        degrade_rollups = [
            r for r in self._alerts_rollups if r.get("rule_name") == "realtime_degrade"
        ]
        triggered_from_rollup = len([r for r in degrade_rollups if r.get("status") != "resolved"])
        resolved_from_rollup = len([r for r in degrade_rollups if r.get("status") == "resolved"])
        recent_events = self._build_degrade_events(limit=10)

        return web.json_response(
            {
                "rule_name": "realtime_degrade",
                "total_alerts": len(degrade_alerts),
                "active_alerts": triggered_count,
                "resolved_alerts": resolved_count,
                "triggered_from_rollup": triggered_from_rollup,
                "resolved_from_rollup": resolved_from_rollup,
                "avg_resolution_time_seconds": round(avg_resolution_time, 2),
                "recent_events": recent_events,
            }
        )

    async def _handle_degrade_events(self, request):
        """降级事件详情列表"""
        limit = int(request.query.get("limit", 50))
        return web.json_response(self._build_degrade_events(limit=limit))

    async def _handle_metrics_names(self, request):
        """指标名称列表"""
        if not self.metrics_collector:
            return web.json_response({"error": "指标收集器未注册"}, status=404)

        return web.json_response(self.metrics_collector.get_metric_names())

    async def _handle_metrics_query(self, request):
        """查询指标数据"""
        if not self.metrics_collector:
            return web.json_response({"error": "指标收集器未注册"}, status=404)

        metric_name = request.query.get("metric")
        if not metric_name:
            return web.json_response({"error": "缺少metric参数"}, status=400)

        # 解析时间范围
        duration_str = request.query.get("duration", "1h")
        try:
            if duration_str.endswith("h"):
                duration = timedelta(hours=int(duration_str[:-1]))
            elif duration_str.endswith("m"):
                duration = timedelta(minutes=int(duration_str[:-1]))
            else:
                duration = timedelta(hours=1)
        except ValueError:
            duration = timedelta(hours=1)

        end_time = datetime.now(tz=_SH)
        start_time = end_time - duration

        points = self.metrics_collector.query_metrics(metric_name, None, start_time, end_time)

        return web.json_response(
            [
                {"timestamp": p.timestamp.isoformat(), "value": p.value, "tags": p.tags}
                for p in points
            ]
        )

    async def _handle_websocket(self, request):
        """WebSocket连接处理"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # 添加到连接集合
        self.websocket_connections.add(ws)

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._handle_websocket_message(ws, data)
                    except json.JSONDecodeError:
                        await ws.send_str(json.dumps({"error": "无效的JSON格式"}))
                elif msg.type == WSMsgType.ERROR:
                    logger.error(f"WebSocket错误: {ws.exception()}")
        except Exception as e:
            logger.error(f"WebSocket处理异常: {e}")

        return ws

    async def _handle_websocket_message(self, ws, data):
        """处理WebSocket消息"""
        msg_type = data.get("type")

        if msg_type == "subscribe":
            # 订阅实时数据
            await ws.send_str(json.dumps({"type": "subscribed", "message": "已订阅实时数据推送"}))

        elif msg_type == "get_overview":
            # 获取系统概览
            overview = await self._get_overview_data()
            await ws.send_str(json.dumps({"type": "overview", "data": overview}))

        else:
            await ws.send_str(json.dumps({"error": f"未知的消息类型: {msg_type}"}))

    async def _get_overview_data(self):
        """获取概览数据"""
        # 这里可以复用 _handle_overview 的逻辑
        # 为了简化，直接返回基本信息
        return {"timestamp": datetime.now(tz=_SH).isoformat(), "status": "running"}

    async def broadcast_to_websockets(self, message: dict[str, Any]):
        """向所有WebSocket连接广播消息"""
        if not self.websocket_connections:
            return

        message_str = json.dumps(message)

        # 创建连接列表的副本，避免在迭代时修改
        connections = list(self.websocket_connections)

        for ws in connections:
            try:
                if not ws.closed:
                    await ws.send_str(message_str)
            except Exception as e:
                logger.error(f"WebSocket广播失败: {e}")

    def get_stats(self) -> dict[str, Any]:
        """获取仪表板统计信息"""
        return {
            "dashboard_info": {
                "host": self.host,
                "port": self.port,
                "running": self.app is not None,
                "websocket_connections": len(self.websocket_connections),
                "cors_enabled": self.enable_cors,
            },
            "registered_components": {
                "system_monitor": self.system_monitor is not None,
                "data_source_monitor": self.data_source_monitor is not None,
                "api_monitor": self.api_monitor is not None,
                "alert_manager": self.alert_manager is not None,
                "metrics_collector": self.metrics_collector is not None,
            },
        }

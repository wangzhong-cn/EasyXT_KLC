/**
 * treemap-bridge.js — EasyXT 行情热图 WebSocket JSON-RPC 2.0 客户端
 *
 * 依赖：ECharts v5.x (window.echarts)
 *
 * 使用方式（由 TreemapAdapter._build_html() 注入）：
 *   TreemapBridge.init(container, port);
 *
 * Python → JS 支持的方法：
 *   treemap.setData        设置板块/股票层级数据
 *   treemap.setFilter      按板块名过滤（空字符串表示全部）
 *   treemap.applyTheme     应用主题 { theme: 'dark'|'light' }
 *   treemap.resize         触发 ECharts resize
 *
 * JS → Python 事件：
 *   treemap.ready          初始化完成握手
 *   treemap.click          用户点击叶节点 { symbol, name, pct_change }
 *   treemap.hover          鼠标悬停 { symbol, name, pct_change }
 */

'use strict';

const TreemapBridge = (function () {
    // ── State ─────────────────────────────────────────────────────────────────
    let _chart = null;
    let _ws = null;
    let _port = 0;
    let _msgId = 0;
    let _pendingCalls = {};   // id → { resolve, reject, timer }
    let _rawData = null;      // 原始 Python 推送的数据（用于过滤还原）

    // ── 颜色映射 ──────────────────────────────────────────────────────────────
    // pct_change: -7% 以下深红 ↔ 0% 灰 ↔ +7% 以上深绿
    const COLOR_STOPS = [
        { pct: -7.0, color: [200,  40,  40] },
        { pct: -4.0, color: [180,  80,  80] },
        { pct: -2.0, color: [150, 100, 100] },
        { pct: -0.5, color: [100,  80,  80] },
        { pct:  0.0, color: [ 80,  80,  80] },
        { pct:  0.5, color: [ 70, 100,  70] },
        { pct:  2.0, color: [ 50, 150,  50] },
        { pct:  4.0, color: [ 40, 180,  40] },
        { pct:  7.0, color: [ 40, 200,  40] },
    ];

    function _lerpColor(t, c1, c2) {
        return [
            Math.round(c1[0] + (c2[0] - c1[0]) * t),
            Math.round(c1[1] + (c2[1] - c1[1]) * t),
            Math.round(c1[2] + (c2[2] - c1[2]) * t),
        ];
    }

    function _pctToColor(pct) {
        const stops = COLOR_STOPS;
        if (pct <= stops[0].pct) return stops[0].color;
        if (pct >= stops[stops.length - 1].pct) return stops[stops.length - 1].color;
        for (let i = 0; i < stops.length - 1; i++) {
            if (pct >= stops[i].pct && pct <= stops[i + 1].pct) {
                const t = (pct - stops[i].pct) / (stops[i + 1].pct - stops[i].pct);
                return _lerpColor(t, stops[i].color, stops[i + 1].color);
            }
        }
        return [80, 80, 80];
    }

    function _colorToCss(rgb, alpha) {
        return alpha != null
            ? `rgba(${rgb[0]},${rgb[1]},${rgb[2]},${alpha})`
            : `rgb(${rgb[0]},${rgb[1]},${rgb[2]})`;
    }

    // ── 图例色带 ──────────────────────────────────────────────────────────────
    function _buildLegendHtml() {
        const steps = 40;
        let gradParts = [];
        for (let i = 0; i <= steps; i++) {
            const pct = -7 + (14 * i / steps);
            const c = _pctToColor(pct);
            gradParts.push(`rgb(${c[0]},${c[1]},${c[2]}) ${(i / steps * 100).toFixed(1)}%`);
        }
        return `<div id="treemap-legend">
  <span class="legend-label">-7%</span>
  <div class="legend-bar" style="background: linear-gradient(to right, ${gradParts.join(',')})"></div>
  <span class="legend-label">+7%</span>
</div>`;
    }

    // ── ECharts option 构建 ────────────────────────────────────────────────────
    function _buildOption(sectors) {
        /*
         * sectors: [
         *   { name: "电子", children: [
         *       { symbol: "000001.SZ", name: "平安银行", value: 1234567890, pct_change: 2.34 },
         *       ...
         *   ]},
         *   ...
         * ]
         */
        const seriesData = sectors.map(sector => {
            const children = (sector.children || []).map(stock => {
                const pct = stock.pct_change || 0;
                const rgb = _pctToColor(pct);
                const displayPct = pct >= 0 ? `+${pct.toFixed(2)}%` : `${pct.toFixed(2)}%`;
                return {
                    name: stock.name || stock.symbol,
                    value: stock.value || 100,
                    pct_change: pct,
                    symbol: stock.symbol,
                    label: {
                        formatter: (params) => {
                            return `{name|${params.name}}\n{pct|${displayPct}}`;
                        },
                        rich: {
                            name: { fontSize: 11, color: '#fff', fontWeight: 'bold' },
                            pct:  { fontSize: 10, color: pct >= 0 ? '#90EE90' : '#FF9999' },
                        },
                    },
                    itemStyle: {
                        color: _colorToCss(rgb),
                        borderColor: '#1a1a2e',
                        borderWidth: 1,
                        gapWidth: 1,
                    },
                    emphasis: {
                        itemStyle: {
                            color: _colorToCss(rgb, 0.85),
                            borderColor: '#fff',
                            borderWidth: 2,
                        },
                    },
                };
            });

            // 板块颜色取子股平均涨跌
            const avgPct = children.length
                ? children.reduce((s, c) => s + c.pct_change, 0) / children.length
                : 0;
            const sectorRgb = _pctToColor(avgPct);

            return {
                name: sector.name,
                value: children.reduce((s, c) => s + c.value, 0),
                children: children,
                itemStyle: {
                    color: _colorToCss(sectorRgb, 0.3),
                    borderColor: '#333',
                    borderWidth: 2,
                    gapWidth: 2,
                },
                label: {
                    show: true,
                    formatter: sector.name,
                    color: '#ddd',
                    fontSize: 12,
                    fontWeight: 'bold',
                },
            };
        });

        return {
            backgroundColor: 'transparent',
            tooltip: {
                formatter: (info) => {
                    if (!info.data || info.data.children) return ''; // 板块级不显示 tooltip
                    const pct = info.data.pct_change || 0;
                    const sign = pct >= 0 ? '+' : '';
                    return [
                        `<b>${info.data.symbol || ''}</b> ${info.name}`,
                        `涨跌幅: <span style="color:${pct >= 0 ? '#90EE90':'#FF9999'}">${sign}${pct.toFixed(2)}%</span>`,
                    ].join('<br/>');
                },
            },
            series: [{
                type: 'treemap',
                id: 'easyxt-treemap',
                animationDurationUpdate: 300,
                roam: false,
                nodeClick: 'zoomToNode',
                zoomToNodeRatio: 0.618 * 0.618,
                breadcrumb: {
                    show: true,
                    bottom: 30,       // 在图例栏上方留空间
                    itemStyle: { color: '#333', borderColor: '#555' },
                    emphasis: { itemStyle: { color: '#444' } },
                    textStyle: { color: '#ccc' },
                },
                data: seriesData,
                levels: [
                    {   // 板块层
                        itemStyle: { borderColor: '#333', borderWidth: 2, gapWidth: 2 },
                        upperLabel: { show: true, height: 22, color: '#ddd', fontWeight: 'bold', fontSize: 12 },
                        emphasis: { itemStyle: { borderColor: '#aaa' } },
                    },
                    {   // 股票层
                        itemStyle: { borderColor: '#1a1a2e', borderWidth: 1, gapWidth: 1 },
                        emphasis: { itemStyle: { borderColor: '#fff' } },
                    },
                ],
            }],
        };
    }

    // ── WebSocket JSON-RPC 2.0 ────────────────────────────────────────────────
    function _send(msg) {
        if (_ws && _ws.readyState === WebSocket.OPEN) {
            _ws.send(JSON.stringify(msg));
        }
    }

    function _notifyPython(method, params) {
        _send({ jsonrpc: '2.0', method: method, params: params || {} });
    }

    function _handleMessage(raw) {
        let msg;
        try { msg = JSON.parse(raw); } catch { return; }

        // 服务端 → 客户端方法调用
        if (msg.method) {
            const params = msg.params || {};
            switch (msg.method) {
                case 'treemap.setData':
                    _handleSetData(params);
                    break;
                case 'treemap.setFilter':
                    _handleSetFilter(params);
                    break;
                case 'treemap.applyTheme':
                    // 主题暂用背景色区分，ECharts 暂不支持内置主题切换
                    break;
                case 'treemap.resize':
                    if (_chart) _chart.resize();
                    break;
                default:
                    console.warn('[TreemapBridge] 未知方法:', msg.method);
            }
            // 若是请求（有 id），回复 null result
            if (msg.id != null) {
                _send({ jsonrpc: '2.0', id: msg.id, result: null });
            }
        }
        // 服务端响应（call_sync 结果）
        else if (msg.id != null) {
            const pending = _pendingCalls[msg.id];
            if (pending) {
                clearTimeout(pending.timer);
                delete _pendingCalls[msg.id];
                if (msg.error) pending.reject(msg.error);
                else pending.resolve(msg.result);
            }
        }
    }

    function _handleSetData(params) {
        /*
         * params = {
         *   sectors: [ { name, children: [{symbol, name, value, pct_change}] } ]
         * }
         */
        _rawData = params.sectors || [];
        if (_chart) {
            _chart.setOption(_buildOption(_rawData), { replaceMerge: ['series'] });
        }
    }

    function _handleSetFilter(params) {
        /*
         * params = { sector: "电子" }  — 空字符串表示全行业
         */
        if (!_rawData) return;
        const sector = (params && params.sector) || '';
        const data = sector
            ? _rawData.filter(s => s.name === sector)
            : _rawData;
        if (_chart) {
            _chart.setOption(_buildOption(data), { replaceMerge: ['series'] });
        }
    }

    // ── ECharts 事件绑定 ─────────────────────────────────────────────────────
    function _bindChartEvents() {
        _chart.on('click', (params) => {
            if (!params.data || params.data.children) return; // 忽略板块层点击
            _notifyPython('treemap.click', {
                symbol:     params.data.symbol || '',
                name:       params.name || '',
                pct_change: params.data.pct_change || 0,
            });
        });

        _chart.on('mouseover', (params) => {
            if (!params.data || params.data.children) return;
            _notifyPython('treemap.hover', {
                symbol:     params.data.symbol || '',
                name:       params.name || '',
                pct_change: params.data.pct_change || 0,
            });
        });
    }

    // ── 初始化 ────────────────────────────────────────────────────────────────
    function init(container, port) {
        _port = port;

        // 1. 注入图例 HTML
        const legend = document.createElement('div');
        legend.innerHTML = _buildLegendHtml();
        document.body.appendChild(legend.firstChild);

        // 2. 初始化 ECharts
        _chart = echarts.init(container, null, { renderer: 'canvas' });

        // 响应窗口大小变化
        window.addEventListener('resize', () => { if (_chart) _chart.resize(); });

        // 3. 连接 WebSocket
        _ws = new WebSocket(`ws://127.0.0.1:${port}`);

        _ws.onopen = () => {
            console.log('[TreemapBridge] WS 连接成功，端口', port);
            // 绑定图表交互事件（只在 WS 就绪后绑定，确保事件能发出）
            _bindChartEvents();
            // 发送握手
            _notifyPython('treemap.ready', { version: '1.0' });
        };

        _ws.onmessage = (ev) => _handleMessage(ev.data);

        _ws.onerror = (err) => console.error('[TreemapBridge] WS 错误', err);

        _ws.onclose = () => console.warn('[TreemapBridge] WS 已断开');
    }

    return { init };
})();

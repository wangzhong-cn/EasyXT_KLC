/**
 * kline-bridge.js — EasyXT KLineChart v9.x WebSocket JSON-RPC 2.0 客户端
 *
 * 依赖：klinecharts v9.8.x (window.klinecharts)
 *
 * 使用方式（由 KLineChartAdapter._build_html() 注入）：
 *   KlineBridge.init(container, port);
 *
 * Python → JS 支持的方法（与 chart-bridge.js 协议兼容）：
 *   chart.setData         批量写入 K线（全量替换）→ chart.applyNewData()
 *   chart.updateBar       增量更新最后 bar     → chart.updateData()
 *   chart.setMarkers      买卖信号点           → Sprint 2 实现（当前 stub）
 *   chart.createIndicator KLineChart 内置指标  → chart.createIndicator()
 *   chart.removeIndicator 移除指标             → chart.removeIndicator()
 *   chart.addIndicator    Python端计算指标     → Sprint 2 前兼容 stub
 *   chart.updateIndicator Python端增量指标     → Sprint 2 前兼容 stub
 *   chart.applyTheme      应用主题             → chart.setStyles()
 *   chart.resize          尺寸调整             → chart.resize()
 *   chart.fitContent      适配可视范围         → chart.scrollToRealTime()
 *   chart.setSymbolText   顶栏标的名           → DOM 更新
 *   chart.setPeriodText   顶栏周期名           → DOM 更新
 *   chart.addDrawing      添加画线             → chart.createOverlay()
 *   chart.removeDrawing   删除画线             → chart.removeOverlay()
 *   chart.getDrawings     获取全部画线（同步） → 返回本地 meta 缓存
 *   chart.loadDrawings    批量恢复画线         → 清空 + createOverlay
 *   chart.takeScreenshot  截图（同步）         → chart.getConvertPictureUrl()
 *
 * JS → Python 事件：
 *   chart.ready           初始化完成握手
 *   chart.click           K线点击 {time, price}
 *   chart.crosshairMove   十字线移动 {time, price, open, high, low, close, volume}
 *   chart.rangeChanged    可视范围变化 {from, to}
 *   chart.drawingCreated  画线完成 {id, type, ...}
 *   chart.drawingUpdated  画线拖动调整 {id, ...}
 *   chart.drawingDeleted  画线删除 {id}
 *
 * ── 数据格式说明 ──────────────────────────────────────────────────────────────
 *   Python 侧发送 time 列（Unix 秒），_toBar() 转换为 timestamp（毫秒）
 *   KLineChart 格式: { timestamp(ms), open, high, low, close, volume }
 *
 * ── 内置 Overlay 类型映射 ─────────────────────────────────────────────────────
 *   hline    → horizontalStraightLine     rayLine    → rayLine
 *   tline    → segment                    straightLine → straightLine
 *   vline    → verticalStraightLine        priceLine  → priceLine
 *   hray     → horizontalRayLine           priceChannel → priceChannelLine
 *   hseg     → horizontalSegment           parallel   → parallelStraightLine
 *   vray     → verticalRayLine             fibonacci  → fibonacciLine
 *   vseg     → verticalSegment             annotation → simpleAnnotation
 *                                          tag        → simpleTag
 *
 *   chart.setTimezone    设置时区
 *   chart.setWatermark   设置水印文字
 */

'use strict';

const KlineBridge = (function () {

    // ── State ─────────────────────────────────────────────────────────────────
    let _chart = null;
    let _ws = null;
    let _port = 0;
    let _reconnectAttempts = 0;
    const MAX_RECONNECT = 5;
    const RECONNECT_DELAY_MS = 1000;

    // 画线元数据缓存（id → Python 原始参数），用于 chart.getDrawings 持久化
    let _drawingMeta = {};

    // 买卖信号标记 overlay ids（setMarkers 批量清理用）
    let _markerIds = [];

    // 侧边栏：成交明细行缓存（用于超出上限时清理旧行）
    let _tradesRows = [];
    const _MAX_TRADES = 60;

    // 画线类型 → KLineChart overlay 名称
    const OVERLAY_TYPE_MAP = {
        // ── 水平线族 ──
        hline:        'horizontalStraightLine',
        hray:         'horizontalRayLine',
        hseg:         'horizontalSegment',
        // ── 垂直线族 ──
        vline:        'verticalStraightLine',
        vray:         'verticalRayLine',
        vseg:         'verticalSegment',
        // ── 趋势线族 ──
        tline:        'segment',
        rayLine:      'rayLine',
        straightLine: 'straightLine',
        // ── 价格线 ──
        priceLine:    'priceLine',
        // ── 通道 / 平行 ──
        priceChannel: 'priceChannelLine',
        parallel:     'parallelStraightLine',
        // ── 斐波那契 ──
        fibonacci:    'fibonacciLine',
        // ── 标注 ──
        annotation:   'simpleAnnotation',
        tag:          'simpleTag',
    };

    // 每种 overlay 需要的 point 数量（用于 _serializeOverlay 泛化）
    const OVERLAY_POINT_COUNT = {
        hline: 1, hray: 1, hseg: 2,
        vline: 1, vray: 1, vseg: 2,
        tline: 2, rayLine: 2, straightLine: 2,
        priceLine: 2,
        priceChannel: 3, parallel: 3,
        fibonacci: 2,
        annotation: 1, tag: 1,
    };

    // ── Public API ────────────────────────────────────────────────────────────

    function init(container, port) {
        _port = port;
        _initChart(container);
        _connectWs(port);
    }

    // ── Chart Initialization ──────────────────────────────────────────────────

    function _initChart(container) {
        _chart = klinecharts.init(container, {
            styles: {
                grid: {
                    show: true,
                    horizontal: { show: true, size: 1, color: '#2B2F36', style: 'dashed', dashedValue: [4, 2] },
                    vertical:   { show: true, size: 1, color: '#2B2F36', style: 'dashed', dashedValue: [4, 2] },
                },
                candle: {
                    bar: {
                        upColor:             '#26a69a',
                        downColor:           '#ef5350',
                        noChangeColor:       '#888888',
                        upBorderColor:       '#26a69a',
                        downBorderColor:     '#ef5350',
                        noChangeBorderColor: '#888888',
                        upWickColor:         '#26a69a',
                        downWickColor:       '#ef5350',
                    },
                    priceMark: {
                        show: true,
                        high: { show: true, color: '#d8d9db', textSize: 10 },
                        low:  { show: true, color: '#d8d9db', textSize: 10 },
                        last: {
                            show: true,
                            upColor:   '#26a69a',
                            downColor: '#ef5350',
                            noChangeColor: '#888888',
                            line: { show: true, style: 'dashed', dashedValue: [4, 2], size: 1 },
                            text: { show: true, size: 11, paddingLeft: 4, paddingRight: 4, paddingTop: 2, paddingBottom: 2, borderRadius: 2 },
                        },
                    },
                },
                xAxis: {
                    axisLine: { show: true, color: '#3C434C', size: 1 },
                    tickText: { show: true, color: '#d8d9db', family: 'sans-serif', size: 11 },
                    tickLine: { show: true, size: 1, length: 3, color: '#3C434C' },
                },
                yAxis: {
                    axisLine: { show: true, color: '#3C434C', size: 1 },
                    tickText: { show: true, color: '#d8d9db', family: 'sans-serif', size: 11 },
                    tickLine: { show: true, size: 1, length: 3, color: '#3C434C' },
                },
                crosshair: {
                    show: true,
                    horizontal: {
                        show: true,
                        line: { show: true, style: 'dashed', dashedValue: [4, 2], size: 1, color: '#888888' },
                        text: { show: true, color: '#d8d9db', backgroundColor: '#374151', borderRadius: 2, paddingLeft: 4, paddingRight: 4, paddingTop: 2, paddingBottom: 2 },
                    },
                    vertical: {
                        show: true,
                        line: { show: true, style: 'dashed', dashedValue: [4, 2], size: 1, color: '#888888' },
                        text: { show: true, color: '#d8d9db', backgroundColor: '#374151', borderRadius: 2, paddingLeft: 4, paddingRight: 4, paddingTop: 2, paddingBottom: 2 },
                    },
                },
            },
        });

        if (!_chart) {
            console.error('[KlineBridge] klinecharts.init() 返回 null，容器不可见或 DOM 未就绪');
            return;
        }

        // 十字线变化 → Python
        if (klinecharts.ActionType) {
            _chart.subscribeAction(klinecharts.ActionType.OnCrosshairChange, function (data) {
                if (!data) return;
                const d = data.kLineData;
                if (!d) return;
                _emit('chart.crosshairMove', {
                    time:   Math.floor((d.timestamp || 0) / 1000),
                    price:  d.close,
                    open:   d.open,
                    high:   d.high,
                    low:    d.low,
                    close:  d.close,
                    volume: d.volume,
                });
            });

            // K线点击 → Python
            _chart.subscribeAction(klinecharts.ActionType.OnCandleBarClick, function (data) {
                if (!data || !data.kLineData) return;
                const d = data.kLineData;
                _emit('chart.click', {
                    time:  Math.floor((d.timestamp || 0) / 1000),
                    price: d.close,
                });
            });

            // 可视范围变化 → Python
            _chart.subscribeAction(klinecharts.ActionType.OnVisibleRangeChange, function (data) {
                if (!data) return;
                _emit('chart.rangeChanged', {
                    from: data.from ? Math.floor(data.from / 1000) : null,
                    to:   data.to   ? Math.floor(data.to   / 1000) : null,
                });
            });
        }

        // 自动跟随容器尺寸
        if (typeof ResizeObserver !== 'undefined') {
            const ro = new ResizeObserver(function () {
                if (_chart) _chart.resize();
            });
            ro.observe(container);
        }
    }

    // ── WebSocket ─────────────────────────────────────────────────────────────

    function _connectWs(port) {
        try {
            _ws = new WebSocket('ws://127.0.0.1:' + port);
        } catch (e) {
            console.error('[KlineBridge] WebSocket constructor failed:', e);
            return;
        }

        _ws.onopen = function () {
            _reconnectAttempts = 0;
            console.log('[KlineBridge] connected to ws port', port);
            _emit('chart.ready', {});
        };

        _ws.onmessage = function (evt) {
            var msg;
            try {
                msg = JSON.parse(evt.data);
            } catch (e) {
                console.error('[KlineBridge] invalid JSON:', evt.data);
                return;
            }
            _dispatch(msg);
        };

        _ws.onerror = function (e) { console.error('[KlineBridge] WS error:', e); };

        _ws.onclose = function () {
            _ws = null;
            if (_reconnectAttempts < MAX_RECONNECT) {
                _reconnectAttempts++;
                console.log('[KlineBridge] reconnect attempt', _reconnectAttempts);
                setTimeout(function () { _connectWs(port); }, RECONNECT_DELAY_MS);
            } else {
                console.warn('[KlineBridge] max reconnect attempts reached');
            }
        };
    }

    // ── Dispatch ──────────────────────────────────────────────────────────────

    function _dispatch(msg) {
        var method = msg.method;
        var params = msg.params || {};
        var msgId  = msg.id;

        var result = null;
        var error  = null;

        try {
            result = _handleMethod(method, params);
        } catch (e) {
            console.error('[KlineBridge] method error:', method, e);
            error = { code: -32603, message: String(e) };
        }

        // call（有 id）需要发回响应
        if (msgId !== undefined && msgId !== null) {
            var resp = error
                ? { jsonrpc: '2.0', id: msgId, error: error }
                : { jsonrpc: '2.0', id: msgId, result: result !== undefined ? result : null };
            _wsSend(JSON.stringify(resp));
        }
    }

    function _handleMethod(method, p) {
        if (!_chart) {
            console.warn('[KlineBridge] chart not ready, dropped:', method);
            return null;
        }

        switch (method) {

            // ── K线数据 ───────────────────────────────────────────────────────

            case 'chart.setData':
                if (p.bars && p.bars.length > 0) {
                    _chart.applyNewData(p.bars.map(_toBar), false);
                    if (p.fitContent !== false) {
                        _chart.scrollToRealTime();
                    }
                }
                break;

            case 'chart.updateBar':
                if (p.bar) {
                    _chart.updateData(_toBar(p.bar));
                }
                break;

            case 'chart.setMarkers': {
                // 使用 simpleAnnotation overlay 实现买卖信号标记
                // 先清除旧标记
                _clearMarkers();
                (p.markers || []).forEach(function (m) {
                    var id = 'marker_' + (m.time || Math.random().toString(36).slice(2));
                    var color = m.color || (m.position === 'belowBar' ? '#26a69a' : '#ef5350');
                    _chart.createOverlay({
                        id: id,
                        name: 'simpleAnnotation',
                        points: [{ timestamp: _secToMs(m.time), value: Number(m.price || 0) }],
                        styles: {
                            symbol: { type: m.shape === 'arrowDown' ? 'diamond' : 'triangle', color: color, size: 8 },
                            text: { color: color },
                        },
                        extendData: m.text || '',
                    });
                    _markerIds.push(id);
                });
                break;
            }

            // ── 时区 / 水印 ──────────────────────────────────────────────────

            case 'chart.setTimezone':
                if (p.timezone) {
                    _chart.setTimezone(p.timezone);
                }
                break;

            case 'chart.setWatermark':
                _setWatermark(p.text || '');
                break;

            // ── KLineChart 内置指标（Sprint 1 新增，Sprint 2 SubchartManager 使用）──

            case 'chart.createIndicator': {
                var indicatorName = p.name || p.indicator;
                if (!indicatorName) break;
                var isStack    = p.isStack !== undefined ? Boolean(p.isStack) : false;
                var paneOpts   = p.paneOptions
                    || (p.paneId ? { id: p.paneId, height: p.height || 80 } : undefined);
                var indicator  = p.calcParams
                    ? { name: indicatorName, calcParams: p.calcParams, shortName: p.shortName || indicatorName }
                    : indicatorName;
                _chart.createIndicator(indicator, isStack, paneOpts);
                break;
            }

            case 'chart.removeIndicator': {
                var paneId = p.paneId || p.id || 'candle_pane';
                var name   = p.name || undefined;
                _chart.removeIndicator(paneId, name);
                break;
            }

            // ── 旧式 Python 端计算指标（Sprint 2 前兼容 stub）────────────────────

            case 'chart.addIndicator':
                console.warn('[KlineBridge] chart.addIndicator: Python端计算指标在Sprint 2迁移至 chart.createIndicator');
                break;

            case 'chart.updateIndicator':
                // Sprint 2 后正式废弃
                break;

            // ── 主题 / 尺寸 ───────────────────────────────────────────────────

            case 'chart.applyTheme':
                if (p.theme) {
                    _chart.setStyles(p.theme);
                }
                break;

            case 'chart.resize':
                _chart.resize();
                break;

            case 'chart.fitContent':
                _chart.scrollToRealTime();
                break;

            // ── 顶栏标签 ──────────────────────────────────────────────────────

            case 'chart.setSymbolText':
                _setLabel('easyxt-symbol-label', p.text || '');
                break;

            case 'chart.setPeriodText':
                _setLabel('easyxt-period-label', p.text || '');
                break;

            // ── 画线 API ──────────────────────────────────────────────────────

            case 'chart.getDrawings':
                return Object.values(_drawingMeta);

            case 'chart.loadDrawings':
                // 清空现有 + 批量重建
                Object.keys(_drawingMeta).forEach(function (id) {
                    try { _chart.removeOverlay(id); } catch (e) { /* ignore */ }
                });
                _drawingMeta = {};
                (p.drawings || []).forEach(function (d) { _addDrawing(d); });
                break;

            case 'chart.addDrawing':
                return _addDrawing(p) ? { id: p.id } : null;

            case 'chart.startDraw':
                return _startDraw(p) ? { id: p.id } : null;

            case 'chart.removeDrawing':
                _removeDrawing(String(p.id));
                break;

            // ── 截图 ──────────────────────────────────────────────────────────

            // ── 侧边栏：五档行情 / 成交明细 / 关键数据 ─────────────────────────────

            case 'orderbook.update':
                _updateOrderbook(p);
                break;

            case 'trades.addTick':
                _addTradesTick(p);
                break;

            case 'stats.update':
                _updateStats(p);
                break;

            case 'chart.takeScreenshot': {
                try {
                    if (_chart.getConvertPictureUrl) {
                        return _chart.getConvertPictureUrl('jpeg', '#0c0d0f');
                    }
                    var canvas = document.querySelector('canvas');
                    return canvas ? canvas.toDataURL('image/png') : null;
                } catch (e) {
                    console.warn('[KlineBridge] screenshot failed:', e);
                    return null;
                }
            }

            default:
                console.log('[KlineBridge] unknown method:', method);
        }
        return null;
    }

    // ── Drawing API ───────────────────────────────────────────────────────────

    function _addDrawing(p) {
        if (!p || !p.id || !p.type) return null;
        var id      = String(p.id);
        var klcType = OVERLAY_TYPE_MAP[p.type] || p.type;
        var style   = p.style || {};
        var color   = style.color     || '#ef5350';
        var size    = style.lineWidth || 1;
        var dash    = _lineStyleName(style.lineStyle);

        var overlayDef = {
            id: id,
            name: klcType,
            styles: {
                line: { color: color, size: size, style: dash, dashedValue: [4, 2] },
            },
        };

        // 根据画线类型设置 points（KLineChart 需要 timestamp 毫秒）
        // 若未提供必需锚点参数，则进入交互绘制模式
        if (!_hasDrawingAnchors(p)) {
            return _startDraw(p);
        }
        if (p.points && Array.isArray(p.points) && p.points.length > 0) {
            // 通用多点格式：points: [{time, value}, ...]
            overlayDef.points = p.points.map(function (pt) {
                var kpt = {};
                if (pt.time  !== undefined && pt.time  !== null) kpt.timestamp = _secToMs(pt.time);
                if (pt.value !== undefined && pt.value !== null) kpt.value     = Number(pt.value);
                return kpt;
            });
        } else if (p.type === 'hline' || p.type === 'hray') {
            overlayDef.points = [{ value: Number(p.price) }];
        } else if (p.type === 'hseg') {
            overlayDef.points = [
                { timestamp: _secToMs(p.time1), value: Number(p.price) },
                { timestamp: _secToMs(p.time2), value: Number(p.price) },
            ];
        } else if (p.type === 'tline' || p.type === 'rayLine' || p.type === 'straightLine'
                   || p.type === 'priceLine' || p.type === 'fibonacci') {
            overlayDef.points = [
                { timestamp: _secToMs(p.time1), value: Number(p.price1) },
                { timestamp: _secToMs(p.time2), value: Number(p.price2) },
            ];
        } else if (p.type === 'vline' || p.type === 'vray') {
            overlayDef.points = [{ timestamp: _secToMs(p.time) }];
        } else if (p.type === 'vseg') {
            overlayDef.points = [
                { timestamp: _secToMs(p.time1) },
                { timestamp: _secToMs(p.time2) },
            ];
        } else if (p.type === 'priceChannel' || p.type === 'parallel') {
            overlayDef.points = [
                { timestamp: _secToMs(p.time1), value: Number(p.price1) },
                { timestamp: _secToMs(p.time2), value: Number(p.price2) },
                { timestamp: _secToMs(p.time3), value: Number(p.price3) },
            ];
        } else if (p.type === 'annotation' || p.type === 'tag') {
            overlayDef.points = [{ timestamp: _secToMs(p.time), value: Number(p.price) }];
            if (p.text) overlayDef.extendData = p.text;
        }

        // 用户完成画线时更新元数据 → Python
        overlayDef.onDrawEnd = function (overlay) {
            _drawingMeta[id] = _serializeOverlay(overlay, p.type, style, id);
            _emit('chart.drawingCreated', _drawingMeta[id]);
        };

        // 用户拖动调整时 → Python
        overlayDef.onPressedMoveEnd = function (overlay) {
            _drawingMeta[id] = _serializeOverlay(overlay, p.type, style, id);
            _emit('chart.drawingUpdated', _drawingMeta[id]);
        };

        try {
            _chart.createOverlay(overlayDef);
            // 程序化创建（来自 Python RPC）：立即存储元数据
            _drawingMeta[id] = Object.assign({}, p, { id: id });
        } catch (e) {
            console.warn('[KlineBridge] createOverlay failed:', klcType, e);
            return null;
        }

        return { id: id };
    }

    function _startDraw(p) {
        if (!p || !p.id || !p.type) return null;
        var id = String(p.id);
        var klcType = OVERLAY_TYPE_MAP[p.type] || p.type;
        var style = p.style || {};
        var color = style.color || '#ef5350';
        var size = style.lineWidth || 1;
        var dash = _lineStyleName(style.lineStyle);

        var overlayDef = {
            id: id,
            name: klcType,
            styles: {
                line: { color: color, size: size, style: dash, dashedValue: [4, 2] },
            },
            onDrawEnd: function (overlay) {
                _drawingMeta[id] = _serializeOverlay(overlay, p.type, style, id);
                _emit('chart.drawingCreated', _drawingMeta[id]);
            },
            onPressedMoveEnd: function (overlay) {
                _drawingMeta[id] = _serializeOverlay(overlay, p.type, style, id);
                _emit('chart.drawingUpdated', _drawingMeta[id]);
            },
        };

        try {
            _chart.createOverlay(overlayDef);
        } catch (e) {
            console.warn('[KlineBridge] startDraw failed:', klcType, e);
            return null;
        }
        return { id: id };
    }

    function _hasDrawingAnchors(p) {
        if (!p) return false;
        if (p.points && Array.isArray(p.points) && p.points.length > 0) return true;
        if ((p.type === 'hline' || p.type === 'hray') && p.price != null) return true;
        if (p.type === 'hseg' && p.price != null && p.time1 != null && p.time2 != null) return true;
        if (
            (p.type === 'tline' || p.type === 'rayLine' || p.type === 'straightLine' || p.type === 'priceLine' || p.type === 'fibonacci')
            && p.time1 != null && p.price1 != null && p.time2 != null && p.price2 != null
        ) return true;
        if ((p.type === 'vline' || p.type === 'vray') && p.time != null) return true;
        if (p.type === 'vseg' && p.time1 != null && p.time2 != null) return true;
        if ((p.type === 'priceChannel' || p.type === 'parallel') && p.time1 != null && p.price1 != null && p.time2 != null && p.price2 != null && p.time3 != null && p.price3 != null) return true;
        if ((p.type === 'annotation' || p.type === 'tag') && p.time != null && p.price != null) return true;
        return false;
    }

    function _removeDrawing(id) {
        if (!_drawingMeta[id]) return;
        try { _chart.removeOverlay(id); } catch (e) { /* ignore */ }
        delete _drawingMeta[id];
        _emit('chart.drawingDeleted', { id: id });
    }

    function _serializeOverlay(overlay, origType, origStyle, fallbackId) {
        var meta = {
            id:    overlay.id || fallbackId,
            type:  origType,
            style: origStyle,
        };
        var pts = overlay.points || [];
        // 通用多点序列化
        meta.points = pts.map(function (pt) {
            var s = {};
            if (pt.timestamp !== undefined && pt.timestamp !== null) s.time  = _msToSec(pt.timestamp);
            if (pt.value     !== undefined && pt.value     !== null) s.value = pt.value;
            return s;
        });
        // 同时保留旧格式兼容性
        if (origType === 'hline' || origType === 'hray') {
            meta.price = pts[0] ? pts[0].value : null;
        } else if (origType === 'tline' || origType === 'rayLine' || origType === 'straightLine'
                   || origType === 'priceLine' || origType === 'fibonacci') {
            meta.time1  = pts[0] ? _msToSec(pts[0].timestamp) : null;
            meta.price1 = pts[0] ? pts[0].value : null;
            meta.time2  = pts[1] ? _msToSec(pts[1].timestamp) : null;
            meta.price2 = pts[1] ? pts[1].value : null;
        } else if (origType === 'vline' || origType === 'vray') {
            meta.time = pts[0] ? _msToSec(pts[0].timestamp) : null;
        } else if (origType === 'priceChannel' || origType === 'parallel') {
            meta.time1  = pts[0] ? _msToSec(pts[0].timestamp) : null;
            meta.price1 = pts[0] ? pts[0].value : null;
            meta.time2  = pts[1] ? _msToSec(pts[1].timestamp) : null;
            meta.price2 = pts[1] ? pts[1].value : null;
            meta.time3  = pts[2] ? _msToSec(pts[2].timestamp) : null;
            meta.price3 = pts[2] ? pts[2].value : null;
        } else if (origType === 'annotation' || origType === 'tag') {
            meta.time  = pts[0] ? _msToSec(pts[0].timestamp) : null;
            meta.price = pts[0] ? pts[0].value : null;
        }
        return meta;
    }

    function _clearMarkers() {
        _markerIds.forEach(function (id) {
            try { _chart.removeOverlay(id); } catch (e) { /* ignore */ }
        });
        _markerIds = [];
    }

    function _setWatermark(text) {
        var el = document.getElementById('easyxt-watermark');
        if (el) {
            el.textContent = text;
        }
    }

    // ── Sidebar: 五档行情 ─────────────────────────────────────────────────────

    function _updateOrderbook(p) {
        var priceEl = document.getElementById('ob-price');
        if (priceEl && p.price != null) {
            priceEl.textContent = _fmtPrice(p.price);
            var pct = parseFloat(p.chg_pct);
            priceEl.className = isNaN(pct) ? '' : pct > 0 ? 'up' : pct < 0 ? 'dn' : '';
        }
        var maxVol = 1;
        for (var i = 1; i <= 5; i++) {
            maxVol = Math.max(maxVol, p['ask' + i + '_vol'] || 0, p['bid' + i + '_vol'] || 0);
        }
        for (var lv = 1; lv <= 5; lv++) {
            _setObRow('a', lv, p['ask' + lv], p['ask' + lv + '_vol'], maxVol);
            _setObRow('b', lv, p['bid' + lv], p['bid' + lv + '_vol'], maxVol);
        }
        var spEl = document.getElementById('ob-spread');
        if (spEl && p.ask1 != null && p.bid1 != null) {
            var sp = parseFloat(p.ask1) - parseFloat(p.bid1);
            spEl.textContent = '价差 ' + (isNaN(sp) ? '--' : _fmtPrice(sp));
        }
    }

    function _setObRow(side, lv, price, vol, maxVol) {
        var pfx = 'ob-' + side + lv;
        var pEl = document.getElementById(pfx + '-p');
        var vEl = document.getElementById(pfx + '-v');
        var bEl = document.getElementById(pfx + '-b');
        if (pEl) pEl.textContent = price != null ? _fmtPrice(price) : '--';
        if (vEl) vEl.textContent = vol   != null ? _fmtVol(vol)     : '--';
        if (bEl) bEl.style.width = (maxVol > 0 && vol != null) ? (Number(vol) / maxVol * 100).toFixed(1) + '%' : '0%';
    }

    // ── Sidebar: 成交明细 ─────────────────────────────────────────────────────

    function _addTradesTick(p) {
        var list = document.getElementById('tr-list');
        if (!list) return;
        var price = p.price != null ? _fmtPrice(p.price) : '--';
        var vol   = '--';
        var ts    = String(p.tick_time || p.time || '');
        if (ts.length > 8) ts = ts.slice(-8);
        // 根据价格与买一/卖一关系推断方向
        var dir = p.direction || '';
        if (!dir && p.ask1 != null && p.bid1 != null && p.price != null) {
            dir = parseFloat(p.price) >= parseFloat(p.ask1) ? 'B'
                : parseFloat(p.price) <= parseFloat(p.bid1) ? 'S' : '';
        }
        var cls = dir === 'B' ? 'up' : dir === 'S' ? 'dn' : '';
        var row = document.createElement('div');
        row.className = 'tr';
        row.innerHTML = '<span class="tt">' + ts + '</span>'
            + '<span class="tp ' + cls + '">' + price + '</span>'
            + '<span class="tv">' + vol + '</span>';
        if (list.firstChild) list.insertBefore(row, list.firstChild);
        else list.appendChild(row);
        _tradesRows.push(row);
        while (_tradesRows.length > _MAX_TRADES) {
            var old = _tradesRows.shift();
            if (old.parentNode) old.parentNode.removeChild(old);
        }
    }

    // ── Sidebar: 关键数据 ─────────────────────────────────────────────────────

    function _updateStats(p) {
        var fmtMap = {
            open: _fmtPrice, high: _fmtPrice, low: _fmtPrice, close: _fmtPrice,
        };
        ['open', 'high', 'low', 'close'].forEach(function (k) {
            var el = document.getElementById('st-' + k);
            if (el && p[k] != null) {
                el.textContent = _fmtPrice(p[k]);
                el.className = 'st-v';
            }
        });
        var chgEl = document.getElementById('st-chg_pct');
        if (chgEl && p.chg_pct != null) {
            var pct = parseFloat(p.chg_pct);
            chgEl.textContent = (pct > 0 ? '+' : '') + pct.toFixed(2) + '%';
            chgEl.className = 'st-v ' + (pct > 0 ? 'up' : pct < 0 ? 'dn' : '');
        }
        var volEl = document.getElementById('st-volume');
        if (volEl && p.volume != null) { volEl.textContent = _fmtVol(p.volume); volEl.className = 'st-v'; }
        var amtEl = document.getElementById('st-amount');
        if (amtEl && p.amount != null) {
            var a = Number(p.amount);
            amtEl.textContent = a >= 1e8 ? (a / 1e8).toFixed(2) + '亿' : a >= 1e4 ? (a / 1e4).toFixed(0) + '万' : String(Math.round(a));
            amtEl.className = 'st-v';
        }
        var trEl = document.getElementById('st-turnover');
        if (trEl && p.turnover != null) { trEl.textContent = parseFloat(p.turnover).toFixed(2) + '%'; trEl.className = 'st-v'; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Python 发送的 bar 格式（time 为 Unix 秒）→ KLineChart 需要的格式（timestamp 毫秒）。
     * 兼容：若入参已含 timestamp（ms），直接使用。
     */
    function _toBar(d) {
        var ts;
        if (typeof d.timestamp === 'number') {
            ts = d.timestamp;
        } else if (typeof d.time === 'number') {
            ts = d.time > 1e10 ? d.time : d.time * 1000;
        } else if (typeof d.time === 'string') {
            // "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS" → 毫秒时间戳
            ts = new Date(d.time.replace(' ', 'T')).getTime();
        } else {
            ts = 0;
        }
        return {
            timestamp: ts,
            open:   Number(d.open),
            high:   Number(d.high),
            low:    Number(d.low),
            close:  Number(d.close),
            volume: Number(d.volume || 0),
        };
    }

    /** Unix 秒（数字或 "YYYY-MM-DD" 字符串）→ 毫秒 */
    function _secToMs(val) {
        if (val === null || val === undefined) return 0;
        if (typeof val === 'number') return val > 1e10 ? val : val * 1000;
        var n = Number(val);
        if (!isNaN(n)) return n > 1e10 ? n : n * 1000;
        return new Date(val).getTime();
    }

    /** 毫秒 → Unix 秒 */
    function _msToSec(ms) {
        return (ms === null || ms === undefined) ? null : Math.floor(Number(ms) / 1000);
    }

    /** 量能格式化：≥1万 → 'X.X万'，否则整数 */
    function _fmtVol(v) {
        var n = Number(v);
        if (isNaN(n) || n === 0) return '--';
        return n >= 10000 ? (n / 10000).toFixed(1) + '万' : String(Math.round(n));
    }

    /** 价格格式化：≥100 保留2位，否则3位 */
    function _fmtPrice(v) {
        var f = parseFloat(v);
        if (isNaN(f)) return '--';
        return f >= 100 ? f.toFixed(2) : f.toFixed(3);
    }

    /** lineStyle 数字（LWC 约定）→ KLineChart 线型字符串 */
    function _lineStyleName(lineStyle) {
        if (!lineStyle) return 'solid';
        // 0=solid, 1=dotted, 2=dashed, 3=large-dashed, 4=sparse-dotted
        if (lineStyle === 0) return 'solid';
        if (lineStyle === 1 || lineStyle === 4) return 'dotted';
        return 'dashed';
    }

    // ── Emitter ───────────────────────────────────────────────────────────────

    function _emit(event, params) {
        if (_ws && _ws.readyState === WebSocket.OPEN) {
            _wsSend(JSON.stringify({ jsonrpc: '2.0', method: event, params: params }));
        }
    }

    function _wsSend(msg) {
        try {
            _ws.send(msg);
        } catch (e) {
            console.warn('[KlineBridge] send failed:', e);
        }
    }

    function _setLabel(id, text) {
        var el = document.getElementById(id);
        if (el) el.textContent = text;
    }

    return { init: init };

})();

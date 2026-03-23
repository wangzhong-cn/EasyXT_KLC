/**
 * chart-bridge.js — EasyXT 原生图表 WebSocket JSON-RPC 2.0 客户端
 *
 * 依赖：lightweight-charts v4.x / v5.x (window.LightweightCharts)
 *
 * 使用方式（由 NativeLwcChartAdapter._build_html() 注入）：
 *   ChartBridge.init(container, port);
 *
 * Python → JS 支持的方法：
 *   chart.setData, chart.updateBar, chart.setMarkers,
 *   chart.addIndicator, chart.updateIndicator, chart.removeIndicator,
 *   chart.applyTheme, chart.resize, chart.fitContent,
 *   chart.setSymbolText, chart.setPeriodText,
 *   chart.addDrawing, chart.removeDrawing,
 *   chart.getDrawings (需响应), chart.takeScreenshot (需响应),
 *   chart.loadDrawings
 *
 * JS → Python 事件：
 *   chart.ready, chart.click, chart.crosshairMove, chart.rangeChanged,
 *   chart.drawingCreated, chart.drawingUpdated, chart.drawingDeleted
 */

'use strict';

const ChartBridge = (function () {
    // ── State ─────────────────────────────────────────────────────────────────
    let _chart = null;
    let _candleSeries = null;
    let _volSeries = null;
    let _indicators = {};   // id → { series, pane }
    let _drawings = {};     // id → drawing metadata
    let _ws = null;
    let _port = 0;
    let _reconnectAttempts = 0;
    const MAX_RECONNECT = 5;
    const RECONNECT_DELAY_MS = 1000;

    // ── Public API ────────────────────────────────────────────────────────────

    function init(container, port) {
        _port = port;
        _initChart(container);
        _connectWs(port);
    }

    // ── Chart Initialization ──────────────────────────────────────────────────

    function _initChart(container) {
        _chart = LightweightCharts.createChart(container, {
            layout: {
                background: { color: '#0c0d0f' },
                textColor: '#d8d9db',
                attributionLogo: true,   // Apache 2.0 合规：保留 TradingView 归因
            },
            grid: {
                vertLines: { color: '#2B2F36' },
                horzLines: { color: '#2B2F36' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#3C434C',
                scaleMargins: { top: 0.1, bottom: 0.2 },
            },
            timeScale: {
                borderColor: '#3C434C',
                timeVisible: true,
                secondsVisible: false,
            },
            width: container.clientWidth || 800,
            height: container.clientHeight || 600,
        });

        _candleSeries = _chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a',
        });

        // 十字线事件 → Python
        _chart.subscribeCrosshairMove((param) => {
            if (!param || !param.time) return;
            const bar = param.seriesData && param.seriesData.get(_candleSeries);
            _emit('chart.crosshairMove', {
                time: param.time,
                price: bar ? bar.close : null,
                open: bar ? bar.open : null,
                high: bar ? bar.high : null,
                low: bar ? bar.low : null,
                close: bar ? bar.close : null,
            });
        });

        // 点击事件 → Python
        _chart.subscribeClick((param) => {
            if (!param || !param.time) return;
            const bar = param.seriesData && param.seriesData.get(_candleSeries);
            _emit('chart.click', {
                time: param.time,
                price: bar ? bar.close : null,
            });
        });

        // 可视范围变化 → Python
        _chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
            if (!range) return;
            _emit('chart.rangeChanged', { from: range.from, to: range.to });
        });

        // 自适应尺寸
        const ro = new ResizeObserver(() => {
            if (_chart) {
                _chart.resize(container.clientWidth, container.clientHeight);
            }
        });
        ro.observe(container);
    }

    // ── WebSocket ─────────────────────────────────────────────────────────────

    function _connectWs(port) {
        try {
            _ws = new WebSocket(`ws://127.0.0.1:${port}`);
        } catch (e) {
            console.error('[ChartBridge] WebSocket constructor failed:', e);
            return;
        }

        _ws.onopen = () => {
            _reconnectAttempts = 0;
            console.log('[ChartBridge] connected to ws port', port);
            // 握手确认：通知 Python 图表已就绪
            _emit('chart.ready', {});
        };

        _ws.onmessage = (evt) => {
            let msg;
            try {
                msg = JSON.parse(evt.data);
            } catch (e) {
                console.error('[ChartBridge] invalid JSON:', evt.data);
                return;
            }
            _dispatch(msg);
        };

        _ws.onerror = (e) => console.error('[ChartBridge] WS error:', e);

        _ws.onclose = () => {
            _ws = null;
            if (_reconnectAttempts < MAX_RECONNECT) {
                _reconnectAttempts++;
                console.log('[ChartBridge] reconnect attempt', _reconnectAttempts);
                setTimeout(() => _connectWs(port), RECONNECT_DELAY_MS);
            } else {
                console.warn('[ChartBridge] max reconnect attempts reached');
            }
        };
    }

    // ── Dispatch ──────────────────────────────────────────────────────────────

    function _dispatch(msg) {
        let method = msg.method;
        let params = msg.params || {};
        let chartId = 'main';
        let paneId = null;
        if (method === 'chart.rpc' && params && Number(params.v) === 1) {
            method = params.method || '';
            chartId = params.chart_id || 'main';
            paneId = params.pane_id || null;
            params = params.payload || {};
        }
        const msgId = msg.id;  // 有 id 表示需要响应

        let result = null;
        let error = null;

        try {
            result = _handleMethod(method, params, chartId, paneId);
        } catch (e) {
            console.error('[ChartBridge] method error:', method, e);
            error = { code: -32603, message: String(e) };
        }

        // 若是 call（有 id），发回响应
        if (msgId !== undefined && msgId !== null) {
            const resp = error
                ? { jsonrpc: '2.0', id: msgId, error }
                : { jsonrpc: '2.0', id: msgId, result: result !== undefined ? result : null };
            _wsSend(JSON.stringify(resp));
        }
    }

    function _handleMethod(method, p, chartId, paneId) {
        switch (method) {

            case 'chart.setData':
                if (_candleSeries && p.bars) {
                    _candleSeries.setData(p.bars);
                    if (p.fitContent !== false) {
                        _chart.timeScale().fitContent();
                    }
                }
                break;

            case 'chart.updateBar':
                if (_candleSeries && p.bar) {
                    _candleSeries.update(p.bar);
                }
                break;

            case 'chart.setMarkers':
                if (_candleSeries && p.markers) {
                    _candleSeries.setMarkers(p.markers);
                }
                break;

            case 'chart.addIndicator': {
                // 目前使用 LineSeries 统一渲染（Stage 2 后期可换 Custom Series）
                if (!p.id || !p.data) break;
                const series = _chart.addLineSeries({
                    color: (p.style && p.style.color) || '#2196f3',
                    lineWidth: (p.style && p.style.lineWidth) || 1,
                    priceScaleId: p.pane || 'right',
                    lastValueVisible: false,
                    priceLineVisible: false,
                });
                // 提取 value 列（第一个非 time 列）
                const valueKey = p.style && p.style.valueKey
                    ? p.style.valueKey
                    : Object.keys(p.data[0] || {}).find(k => k !== 'time') || 'value';
                const lineData = p.data
                    .filter(d => d[valueKey] !== null && d[valueKey] !== undefined)
                    .map(d => ({ time: d.time, value: Number(d[valueKey]) }));
                series.setData(lineData);
                _indicators[p.id] = { series, valueKey };
                break;
            }

            case 'chart.updateIndicator': {
                const ind = _indicators[p.id];
                if (ind && p.bar) {
                    const val = p.bar[ind.valueKey];
                    if (val !== null && val !== undefined) {
                        ind.series.update({ time: p.bar.time, value: Number(val) });
                    }
                }
                break;
            }

            case 'chart.removeIndicator': {
                const ind = _indicators[p.id];
                if (ind) {
                    try { _chart.removeSeries(ind.series); } catch (e) { /* ignore */ }
                    delete _indicators[p.id];
                }
                break;
            }

            case 'chart.applyTheme':
                if (_chart && p.theme) {
                    _chart.applyOptions({ layout: p.theme });
                }
                break;

            case 'chart.resize':
                if (_chart && p.width && p.height) {
                    _chart.resize(Number(p.width), Number(p.height));
                }
                break;

            case 'chart.fitContent':
                if (_chart) _chart.timeScale().fitContent();
                break;

            case 'chart.setSymbolText':
                // 更新顶部标题（若存在 #symbol-label 元素）
                _setLabel('easyxt-symbol-label', p.text || '');
                break;

            case 'chart.setPeriodText':
                _setLabel('easyxt-period-label', p.text || '');
                break;

            case 'chart.getDrawings':
                // 返回所有画线元数据（供 Python 持久化）
                return Object.values(_drawings).map(_drawingMeta);

            case 'chart.loadDrawings':
                // 批量恢复画线（页面加载时由 Python 调用）
                (_drawings_clear_all());
                (p.drawings || []).forEach(d => _addDrawing(d));
                break;

            case 'chart.addDrawing':
                // 添加单条画线，返回 id 供 Python 记录
                return _addDrawing(p) ? { id: p.id } : null;

            case 'chart.removeDrawing':
                _removeDrawing(p.id);
                break;

            case 'chart.takeScreenshot': {
                // 返回 canvas toDataURL (base64 PNG)
                try {
                    const canvas = document.querySelector('canvas');
                    return canvas ? canvas.toDataURL('image/png') : null;
                } catch (e) {
                    return null;
                }
            }

            default:
                console.log('[ChartBridge] unknown method:', method, 'chartId=', chartId, 'paneId=', paneId);
        }
        return null;
    }

    // ── Drawing API ───────────────────────────────────────────────────────────

    function _drawingMeta(d) {
        // 返回纯数据字段（剥离内部 _priceLine/_primitive 引用）
        const { _priceLine, _primitive, ...meta } = d;
        return meta;
    }

    function _drawings_clear_all() {
        Object.keys(_drawings).forEach(id => _removeDrawing(id));
    }

    function _addDrawing(p) {
        if (!p || !p.id || !p.type) return null;
        const id = String(p.id);
        const type = p.type;          // 'hline' | 'tline' | 'vline'
        const style = p.style || {};
        const color = style.color || '#ef5350';
        const lineWidth = style.lineWidth || 1;
        const lineStyle = style.lineStyle !== undefined ? style.lineStyle : 0;

        const drawing = Object.assign({}, p, { id });

        if (type === 'hline') {
            // 水平价格线 — 使用 LightweightCharts 原生 createPriceLine
            drawing._priceLine = _candleSeries.createPriceLine({
                price: Number(p.price),
                color,
                lineWidth,
                lineStyle,
                axisLabelVisible: p.axisLabel !== false,
                title: p.title || '',
            });
        } else if (type === 'tline') {
            // 趋势线：两点连线
            drawing._primitive = _makeTrendLinePrimitive(
                p.time1, p.price1, p.time2, p.price2, color, lineWidth, lineStyle
            );
            _candleSeries.attachPrimitive(drawing._primitive);
        } else if (type === 'vline') {
            // 垂直时间线
            drawing._primitive = _makeVerticalLinePrimitive(
                p.time, color, lineWidth, lineStyle
            );
            _candleSeries.attachPrimitive(drawing._primitive);
        }

        _drawings[id] = drawing;
        _emit('chart.drawingCreated', _drawingMeta(drawing));
        return drawing;
    }

    function _removeDrawing(id) {
        const drawing = _drawings[String(id)];
        if (!drawing) return;
        if (drawing._priceLine && _candleSeries) {
            try { _candleSeries.removePriceLine(drawing._priceLine); } catch (e) { /* ignore */ }
        }
        if (drawing._primitive && _candleSeries) {
            try { _candleSeries.detachPrimitive(drawing._primitive); } catch (e) { /* ignore */ }
        }
        delete _drawings[String(id)];
        _emit('chart.drawingDeleted', { id });
    }

    function _makeTrendLinePrimitive(time1, price1, time2, price2, color, lineWidth, lineStyle) {
        let _c = null, _s = null;
        const renderer = {
            draw(target) {
                if (!_c || !_s) return;
                const x1 = _c.timeScale().timeToCoordinate(time1);
                const x2 = _c.timeScale().timeToCoordinate(time2);
                const y1 = _s.priceToCoordinate(price1);
                const y2 = _s.priceToCoordinate(price2);
                if (x1 == null || x2 == null || y1 == null || y2 == null) return;
                target.useBitmapCoordinateSpace(({ context: ctx, horizontalPixelRatio: hR, verticalPixelRatio: vR }) => {
                    ctx.save();
                    ctx.beginPath();
                    ctx.strokeStyle = color;
                    ctx.lineWidth = lineWidth * hR;
                    _applyLineDash(ctx, lineStyle, lineWidth * hR);
                    ctx.moveTo(x1 * hR, y1 * vR);
                    ctx.lineTo(x2 * hR, y2 * vR);
                    ctx.stroke();
                    ctx.restore();
                });
            }
        };
        return {
            attached({ chart, series }) { _c = chart; _s = series; },
            detached() { _c = null; _s = null; },
            paneViews() { return [{ renderer() { return renderer; } }]; },
        };
    }

    function _makeVerticalLinePrimitive(time, color, lineWidth, lineStyle) {
        let _c = null;
        const renderer = {
            draw(target) {
                if (!_c) return;
                const x = _c.timeScale().timeToCoordinate(time);
                if (x == null) return;
                target.useBitmapCoordinateSpace(({ context: ctx, horizontalPixelRatio: hR, verticalPixelRatio: vR }) => {
                    ctx.save();
                    ctx.beginPath();
                    ctx.strokeStyle = color;
                    ctx.lineWidth = lineWidth * hR;
                    _applyLineDash(ctx, lineStyle, lineWidth * hR);
                    ctx.moveTo(x * hR, 0);
                    ctx.lineTo(x * hR, ctx.canvas.height);
                    ctx.stroke();
                    ctx.restore();
                });
            }
        };
        return {
            attached({ chart }) { _c = chart; },
            detached() { _c = null; },
            paneViews() { return [{ renderer() { return renderer; } }]; },
        };
    }

    function _applyLineDash(ctx, lineStyle, scaledWidth) {
        // lineStyle: 0=solid, 1=dotted, 2=dashed, 3=large-dashed, 4=sparse-dotted
        const u = Math.max(1, Math.round(scaledWidth));
        switch (lineStyle) {
            case 1:  ctx.setLineDash([u, u * 2]);      break;  // dotted
            case 2:  ctx.setLineDash([u * 4, u * 2]);  break;  // dashed
            case 3:  ctx.setLineDash([u * 8, u * 2]);  break;  // large-dashed
            case 4:  ctx.setLineDash([u, u * 6]);      break;  // sparse-dotted
            default: ctx.setLineDash([]);               break;  // solid
        }
    }

    // ── Emitter ───────────────────────────────────────────────────────────────

    function _emit(event, params) {
        if (_ws && _ws.readyState === WebSocket.OPEN) {
            if (!params || typeof params !== 'object') {
                params = {};
            }
            if (params.chart_id === undefined) {
                params.chart_id = 'main';
            }
            _wsSend(JSON.stringify({ jsonrpc: '2.0', method: event, params }));
        }
    }

    function _wsSend(msg) {
        try {
            _ws.send(msg);
        } catch (e) {
            console.warn('[ChartBridge] send failed:', e);
        }
    }

    function _setLabel(id, text) {
        let el = document.getElementById(id);
        if (el) el.textContent = text;
    }

    return { init };
})();

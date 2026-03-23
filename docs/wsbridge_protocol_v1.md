# WsBridge 协议 v1（多图路由）

## 目标
- 为多图表分屏提供稳定的消息路由能力
- 保持对旧消息（`method + params`）兼容
- 为后续 `chart_id`、`pane_id` 分流提供统一信封

## 信封格式
Python 通过 JSON-RPC 发送：

```json
{
  "jsonrpc": "2.0",
  "method": "chart.rpc",
  "params": {
    "v": 1,
    "type": "chart.rpc",
    "chart_id": "main",
    "pane_id": "orderbook",
    "method": "orderbook.update",
    "payload": {
      "symbol": "000988.SZ"
    },
    "seq": 101,
    "ts_ms": 1774259000000,
    "source": "python"
  }
}
```

## 字段说明
- `v`: 协议版本，当前固定 `1`
- `type`: 消息类型，当前固定 `chart.rpc`
- `chart_id`: 图实例路由键，默认 `main`
- `pane_id`: 面板路由键，可选（如 `orderbook/trades/stats`）
- `method`: 业务方法名（如 `chart.setData`、`orderbook.update`）
- `payload`: 业务参数
- `seq`: 发送序号（单进程自增）
- `ts_ms`: 毫秒时间戳
- `source`: 来源标记（`python/js`）

## 兼容策略
- JS 端若收到旧格式（`method != chart.rpc`），按历史逻辑执行
- JS 端若收到新格式（`method == chart.rpc && params.v == 1`），先解包再路由
- JS -> Python 事件默认附加 `chart_id=main`（若调用方未指定）

## 当前落地点
- Python:
  - `KLineChartAdapter.notify_orderbook(..., chart_id='main', pane_id='orderbook')`
  - `KLineChartAdapter.notify_trades_tick(..., chart_id='main', pane_id='trades')`
  - `KLineChartAdapter.notify_stats(..., chart_id='main', pane_id='stats')`
- JS:
  - `kline-bridge.js` 支持 `chart.rpc` 解包与回退
  - `chart-bridge.js` 支持 `chart.rpc` 解包与回退

## Phase 3 扩展建议
- 多图实例注册表：`Map<chart_id, chartInstance>`
- `_handleMethod(method, payload, chartId, paneId)` 路由到指定实例
- 事件回传统一携带 `chart_id`，支持同步组（A/B）策略

#!/usr/bin/env python3
"""
订单转换器（easyxt -> qka/miniqmt 订单字段规范）
- 仅做转换，不直接执行下发；执行由策略或执行器调用适配层方法完成
"""
from typing import Any


class OrderConverter:
    """订单字段转换工具"""

    @staticmethod
    def target_positions_to_qka_orders(diff_result: dict[str, list[dict]], price_map: dict[str, float],
                                       price_type: str = 'market') -> list[dict[str, Any]]:
        """
        将持仓差异结果转换为 qka 订单列表
        Args:
            diff_result: PositionDiffer.compare_positions(...) 的结果
            price_map: 代码到价格的映射（若为限价单）
            price_type: 'market' 或 'limit'
        Returns:
            List[Dict]: qka 订单列表（示例字段）
                {
                  'code': '000001.XSHE',
                  'direction': 'BUY'/'SELL',
                  'order_type': 'MARKET'/'LIMIT',
                  'price': 12.35,  # 限价时必填
                  'volume': 1000
                }
        """
        orders: list[dict[str, Any]] = []
        order_type = 'MARKET' if price_type == 'market' else 'LIMIT'

        def _make(code: str, direction: str, volume: int) -> dict[str, Any]:
            order = {
                'code': code,
                'direction': direction,
                'order_type': order_type,
                'volume': int(volume)
            }
            if order_type == 'LIMIT':
                order['price'] = float(price_map.get(code, 0.0))
            return order

        def _normalize_code(value: Any) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text if text else None

        def _normalize_volume(value: Any) -> int | None:
            try:
                vol = int(value)
                return vol if vol > 0 else None
            except Exception:
                return None

        def _normalize_signed_int(value: Any) -> int | None:
            try:
                return int(value)
            except Exception:
                return None

        # 需要买入的：全部生成 BUY 订单
        for pos in diff_result.get('to_buy', []):
            code = _normalize_code(pos.get('code') or pos.get('symbol'))
            volume = _normalize_volume(pos.get('volume') or pos.get('quantity'))
            if code and volume:
                orders.append(_make(code, 'BUY', volume))

        # 需要卖出的：SELL 到 0
        for pos in diff_result.get('to_sell', []):
            code = _normalize_code(pos.get('code') or pos.get('symbol'))
            current_volume = _normalize_volume(pos.get('volume') or pos.get('quantity'))
            if code and current_volume:
                orders.append(_make(code, 'SELL', current_volume))

        # 需要调整的：正数为 BUY，负数为 SELL
        for pos in diff_result.get('to_adjust', []):
            code = _normalize_code(pos.get('code') or pos.get('symbol'))
            adj = _normalize_signed_int(pos.get('adjust_quantity') or pos.get('adjust_volume'))
            if adj is None or code is None:
                continue
            adj_volume = _normalize_volume(abs(adj))
            if adj_volume is None:
                continue
            if adj > 0:
                orders.append(_make(code, 'BUY', adj_volume))
            elif adj < 0:
                orders.append(_make(code, 'SELL', adj_volume))

        return orders

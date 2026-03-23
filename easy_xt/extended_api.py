"""
EasyXT扩展API模块
基于参考项目qmt_trader的完整功能封装
包含从随机ID生成到个股权重检查的所有API
"""

import logging
import math
import random
import string
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_SH = ZoneInfo('Asia/Shanghai')
from typing import Any, Optional

import pandas as pd

from .data_api import DataAPI
from .data_types import ConnectionError
from .enhanced_indicators import EnhancedIndicators
from .trade_api import TradeAPI
from .utils import TimeUtils


class ExtendedAPI:
    """扩展API类，提供完整的交易和数据分析功能"""

    def __init__(self):
        self.data_api = DataAPI()
        self.trade_api = TradeAPI()
        self.indicators = EnhancedIndicators(self.data_api)
        self._connected_data = False
        self._connected_trade = False

    def init_data(self) -> bool:
        """初始化数据服务"""
        self._connected_data = self.data_api.connect()
        return self._connected_data

    def init_trade(self, userdata_path: str, session_id: Optional[str] = None) -> bool:
        """初始化交易服务"""
        self._connected_trade = self.trade_api.connect(userdata_path, session_id)
        return self._connected_trade

    def add_account(self, account_id: str, account_type: str = 'STOCK') -> bool:
        """添加交易账户"""
        return self.trade_api.add_account(account_id, account_type)

    # ==================== 2. 随机ID生成 ====================

    def generate_random_id(self, length: int = 8, prefix: str = '') -> str:
        """
        生成随机ID

        Args:
            length: ID长度
            prefix: 前缀

        Returns:
            str: 随机ID
        """
        chars = string.ascii_letters + string.digits
        random_part = ''.join(random.choice(chars) for _ in range(length))
        return f"{prefix}{random_part}"

    def generate_order_id(self) -> str:
        """生成订单ID"""
        timestamp = str(int(time.time() * 1000))[-8:]  # 取时间戳后8位
        random_part = ''.join(random.choice(string.digits) for _ in range(4))
        return f"ORD{timestamp}{random_part}"

    def generate_strategy_id(self, strategy_name: str = '') -> str:
        """生成策略ID"""
        if strategy_name:
            prefix = ''.join(c for c in strategy_name if c.isalnum())[:4].upper()
        else:
            prefix = 'STG'
        return self.generate_random_id(8, prefix)

    # ==================== 3. 时间相关功能 ====================

    def get_current_timestamp(self) -> int:
        """获取当前时间戳（毫秒）"""
        return int(time.time() * 1000)

    def get_current_time_str(self, format_str: str = '%Y-%m-%d %H:%M:%S') -> str:
        """获取当前时间字符串"""
        return datetime.now(tz=_SH).strftime(format_str)

    def is_trading_time(self) -> bool:
        """检查是否为交易时间"""
        now = datetime.now(tz=_SH)
        weekday = now.weekday()

        # 周末不交易
        if weekday >= 5:
            return False

        current_time = now.time()

        # 上午交易时间 9:30-11:30
        morning_start = datetime.strptime('09:30', '%H:%M').time()
        morning_end = datetime.strptime('11:30', '%H:%M').time()

        # 下午交易时间 13:00-15:00
        afternoon_start = datetime.strptime('13:00', '%H:%M').time()
        afternoon_end = datetime.strptime('15:00', '%H:%M').time()

        return (morning_start <= current_time <= morning_end) or \
               (afternoon_start <= current_time <= afternoon_end)

    def get_next_trading_day(self, date: Optional[str] = None) -> str:
        """获取下一个交易日"""
        if date is None:
            start_date = datetime.now(tz=_SH)
        else:
            start_date = datetime.strptime(TimeUtils.normalize_date(date), '%Y%m%d')

        next_day = start_date + timedelta(days=1)

        # 跳过周末
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)

        return next_day.strftime('%Y%m%d')

    # ==================== 4. 委托状态检查 ====================

    def check_unfilled_orders(self, account_id: str) -> pd.DataFrame:
        """检查没有成交的当日委托"""
        if not self._connected_trade:
            raise ConnectionError("交易服务未连接")

        orders = self.trade_api.get_orders(account_id)
        if orders is None or orders.empty:
            return pd.DataFrame()

        # 筛选未成交或部分成交的委托
        unfilled = orders[orders['order_status'].isin(['未报', '已报', '部成'])]
        return unfilled

    def check_order_status(self, account_id: str, order_id: int) -> dict[str, Any]:
        """检查指定委托状态"""
        orders = self.trade_api.get_orders(account_id)
        if orders is None or orders.empty:
            return {}

        order_info = orders[orders['order_id'] == order_id]
        if order_info.empty:
            return {}

        return order_info.iloc[0].to_dict()

    def get_order_fill_ratio(self, account_id: str, order_id: int) -> float:
        """获取委托成交比例"""
        order_info = self.check_order_status(account_id, order_id)
        if not order_info:
            return 0.0

        order_volume = order_info.get('order_volume', 0)
        traded_volume = order_info.get('traded_volume', 0)

        if order_volume == 0:
            return 0.0

        return traded_volume / order_volume

    # ==================== 5. 持仓管理 ====================

    def get_position_by_code(self, account_id: str, code: str) -> dict[str, Any]:
        """获取指定股票持仓"""
        positions = self.trade_api.get_positions(account_id, code)
        if positions is None or positions.empty:
            return {}

        return positions.iloc[0].to_dict()

    def get_total_position_value(self, account_id: str) -> float:
        """获取总持仓市值"""
        positions = self.trade_api.get_positions(account_id)
        if positions is None or positions.empty:
            return 0.0

        return positions['market_value'].sum()

    def get_position_profit_loss(self, account_id: str, code: Optional[str] = None) -> float:
        """获取持仓盈亏"""
        if code:
            position = self.get_position_by_code(account_id, code)
            return position.get('profit_loss', 0.0)
        else:
            positions = self.trade_api.get_positions(account_id)
            if positions is None or positions.empty:
                return 0.0
            return positions['profit_loss'].sum()

    # ==================== 6. 资金管理 ====================

    def get_available_cash(self, account_id: str) -> float:
        """获取可用资金"""
        asset = self.trade_api.get_account_asset(account_id)
        if asset is None:
            return 0.0
        return asset.get('cash', 0.0)

    def get_frozen_cash(self, account_id: str) -> float:
        """获取冻结资金"""
        asset = self.trade_api.get_account_asset(account_id)
        if asset is None:
            return 0.0
        return asset.get('frozen_cash', 0.0)

    def get_total_asset(self, account_id: str) -> float:
        """获取总资产"""
        asset = self.trade_api.get_account_asset(account_id)
        if asset is None:
            return 0.0
        return asset.get('total_asset', 0.0)

    def calculate_buying_power(self, account_id: str, code: str) -> int:
        """计算股票最大可买数量"""
        available_cash = self.get_available_cash(account_id)
        if available_cash <= 0:
            return 0

        # 获取当前价格
        current_price = self.data_api.get_current_price([code])
        if current_price is None or current_price.empty:
            return 0

        price = current_price.iloc[0]['price']
        if price <= 0:
            return 0

        # 计算可买手数（100股为一手）
        max_shares = int(available_cash / price)
        return (max_shares // 100) * 100

    # ==================== 7. 价格分析 ====================

    def get_price_change(self, code: str, period: str = '1d', count: int = 2) -> dict[str, float]:
        """获取价格变化信息"""
        data = self.data_api.get_price([code], period=period, count=count)
        if data is None or len(data) < 2:
            return {}

        latest = data.iloc[-1]
        previous = data.iloc[-2]

        change = latest['close'] - previous['close']
        change_pct = (change / previous['close']) * 100 if previous['close'] > 0 else 0

        return {
            'current_price': latest['close'],
            'previous_price': previous['close'],
            'change': change,
            'change_pct': change_pct,
            'high': latest['high'],
            'low': latest['low'],
            'volume': latest['volume']
        }

    def get_price_range(self, code: str, period: str = '1d', count: int = 20) -> dict[str, float]:
        """获取价格区间信息"""
        data = self.data_api.get_price([code], period=period, count=count)
        if data is None or data.empty:
            return {}

        return {
            'max_price': data['high'].max(),
            'min_price': data['low'].min(),
            'avg_price': data['close'].mean(),
            'current_price': data.iloc[-1]['close'],
            'volatility': data['close'].std()
        }

    # ==================== 8. 技术指标 ====================

    def calculate_ma(self, code: str, period: int = 20, ma_period: str = '1d') -> float:
        """计算移动平均线"""
        data = self.data_api.get_price([code], period=ma_period, count=period)
        if data is None or len(data) < period:
            return 0.0

        return data['close'].tail(period).mean()

    def calculate_rsi(self, code: str, period: int = 14, data_period: str = '1d') -> float:
        """计算RSI指标"""
        data = self.data_api.get_price([code], period=data_period, count=period + 1)
        if data is None or len(data) < period + 1:
            return 50.0

        closes = data['close']
        deltas = closes.diff()

        gains = deltas.where(deltas > 0, 0)
        losses = -deltas.where(deltas < 0, 0)

        avg_gain = gains.rolling(window=period).mean().iloc[-1]
        avg_loss = losses.rolling(window=period).mean().iloc[-1]

        if avg_loss == 0:
            return 100.0

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    def calculate_bollinger_bands(self, code: str, period: int = 20, std_dev: float = 2.0) -> dict[str, float]:
        """计算布林带"""
        data = self.data_api.get_price([code], period='1d', count=period)
        if data is None or len(data) < period:
            return {}

        closes = data['close'].tail(period)
        ma = closes.mean()
        std = closes.std()

        return {
            'upper_band': ma + (std_dev * std),
            'middle_band': ma,
            'lower_band': ma - (std_dev * std),
            'current_price': closes.iloc[-1]
        }

    def calculate_macd(self, code: str, fast_period: int = 12, slow_period: int = 26,
                      signal_period: int = 9, data_period: str = '1d') -> dict[str, float]:
        """
        计算MACD指标

        Args:
            code: 股票代码
            fast_period: 快线周期，默认12
            slow_period: 慢线周期，默认26
            signal_period: 信号线周期，默认9
            data_period: 数据周期

        Returns:
            Dict: 包含MACD、信号线、柱状图的字典
        """
        return self.indicators.calculate_macd(code, fast_period, slow_period, signal_period, data_period)

    def calculate_kdj(self, code: str, k_period: int = 9, d_period: int = 3,
                     j_period: int = 3, data_period: str = '1d') -> dict[str, float]:
        """
        计算KDJ指标

        Args:
            code: 股票代码
            k_period: K值计算周期，默认9
            d_period: D值平滑周期，默认3
            j_period: J值计算周期，默认3
            data_period: 数据周期

        Returns:
            Dict: 包含K、D、J值的字典
        """
        return self.indicators.calculate_kdj(code, k_period, d_period, j_period, data_period)

    def calculate_enhanced_rsi(self, code: str, period: int = 14,
                              data_period: str = '1d') -> dict[str, float]:
        """
        计算增强版RSI指标

        Args:
            code: 股票代码
            period: 计算周期
            data_period: 数据周期

        Returns:
            Dict: 增强的RSI信息
        """
        return self.indicators.calculate_rsi_enhanced(code, period, data_period)

    def calculate_enhanced_bollinger_bands(self, code: str, period: int = 20, std_dev: float = 2.0,
                                          data_period: str = '1d') -> dict[str, float]:
        """
        计算增强版布林带

        Args:
            code: 股票代码
            period: 计算周期
            std_dev: 标准差倍数
            data_period: 数据周期

        Returns:
            Dict: 增强的布林带信息
        """
        return self.indicators.calculate_boll_enhanced(code, period, std_dev, data_period)

    def get_comprehensive_technical_signal(self, code: str) -> dict[str, str]:
        """
        获取综合技术信号

        Args:
            code: 股票代码

        Returns:
            Dict: 综合信号分析
        """
        return self.indicators.get_comprehensive_signal(code)

    # ==================== 9. 风险控制 ====================

    def check_position_limit(self, account_id: str, code: str, volume: int, max_position_ratio: float = 0.3) -> bool:
        """检查持仓限制"""
        total_asset = self.get_total_asset(account_id)
        if total_asset <= 0:
            return False

        # 获取当前价格
        current_price = self.data_api.get_current_price([code])
        if current_price is None or current_price.empty:
            return False

        price = current_price.iloc[0]['price']
        order_value = volume * price

        # 获取当前持仓
        current_position = self.get_position_by_code(account_id, code)
        current_value = current_position.get('market_value', 0.0)

        total_position_value = current_value + order_value
        position_ratio = total_position_value / total_asset

        return position_ratio <= max_position_ratio

    def calculate_stop_loss_price(self, entry_price: float, stop_loss_pct: float = 0.05) -> float:
        """计算止损价格"""
        return entry_price * (1 - stop_loss_pct)

    def calculate_take_profit_price(self, entry_price: float, take_profit_pct: float = 0.1) -> float:
        """计算止盈价格"""
        return entry_price * (1 + take_profit_pct)

    def check_risk_metrics(self, account_id: str) -> dict[str, Any]:
        """检查风险指标"""
        total_asset = self.get_total_asset(account_id)
        position_value = self.get_total_position_value(account_id)
        available_cash = self.get_available_cash(account_id)

        if total_asset <= 0:
            return {}

        position_ratio = position_value / total_asset
        cash_ratio = available_cash / total_asset

        return {
            'total_asset': total_asset,
            'position_value': position_value,
            'available_cash': available_cash,
            'position_ratio': position_ratio,
            'cash_ratio': cash_ratio,
            'leverage': position_value / available_cash if available_cash > 0 else 0
        }

    # ==================== 10. 批量操作 ====================

    def batch_get_current_prices(self, codes: list[str]) -> pd.DataFrame:
        """批量获取当前价格"""
        return self.data_api.get_current_price(codes)

    def batch_calculate_ma(self, codes: list[str], period: int = 20) -> dict[str, float]:
        """批量计算移动平均线"""
        results = {}
        for code in codes:
            try:
                ma = self.calculate_ma(code, period)
                results[code] = ma
            except Exception:
                results[code] = 0.0
        return results

    def batch_check_signals(self, codes: list[str], short_ma: int = 5, long_ma: int = 20) -> dict[str, str]:
        """批量检查交易信号"""
        results = {}
        for code in codes:
            try:
                short_ma_value = self.calculate_ma(code, short_ma)
                long_ma_value = self.calculate_ma(code, long_ma)

                if short_ma_value > long_ma_value:
                    signal = 'buy'
                elif short_ma_value < long_ma_value:
                    signal = 'sell'
                else:
                    signal = 'hold'

                results[code] = signal
            except Exception as _sig_err:
                logging.getLogger(__name__).warning("信号计算异常 %s: %s", code, _sig_err)
                results[code] = 'hold'

        return results

    def batch_calculate_technical_indicators(self, codes: list[str],
                                          indicators: Optional[list[str]] = None) -> dict[str, dict]:
        """
        批量计算技术指标

        Args:
            codes: 股票代码列表
            indicators: 指标列表，默认计算所有指标

        Returns:
            Dict: 各股票的指标结果
        """
        return self.indicators.batch_calculate_indicators(codes, indicators)

    def batch_get_comprehensive_signals(self, codes: list[str]) -> dict[str, dict]:
        """
        批量获取综合技术信号

        Args:
            codes: 股票代码列表

        Returns:
            Dict: 各股票的综合信号
        """
        results = {}
        for code in codes:
            try:
                results[code] = self.get_comprehensive_technical_signal(code)
            except Exception as e:
                results[code] = {'error': str(e), 'final_signal': 'hold'}

        return results

    # ==================== 11. 数据统计 ====================

    def get_trading_statistics(self, account_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> dict[str, Any]:
        """获取交易统计信息"""
        if start_date is None:
            start_date = (datetime.now(tz=_SH) - timedelta(days=30)).strftime('%Y%m%d')
        if end_date is None:
            end_date = datetime.now(tz=_SH).strftime('%Y%m%d')

        # 获取历史成交
        trades = self.trade_api.get_trades(account_id)
        if trades is None or trades.empty:
            return {}

        buy_trades = trades[trades['trade_type'] == '买入']
        sell_trades = trades[trades['trade_type'] == '卖出']

        return {
            'total_trades': len(trades),
            'buy_trades': len(buy_trades),
            'sell_trades': len(sell_trades),
            'total_volume': trades['volume'].sum(),
            'total_amount': trades['amount'].sum(),
            'avg_trade_amount': trades['amount'].mean(),
            'trading_days': pd.to_datetime(trades['time'], errors='coerce').map(lambda x: x.date() if pd.notna(x) else None).nunique() if 'time' in trades.columns else 0
        }

    def calculate_portfolio_performance(self, account_id: str, benchmark_code: str = '000300.SH') -> dict[str, float]:
        """计算投资组合表现"""
        # 获取当前资产
        current_asset = self.get_total_asset(account_id)

        # 这里需要历史资产数据来计算收益率
        # 简化实现，返回基本指标
        positions = self.trade_api.get_positions(account_id)
        if positions is None or positions.empty:
            return {}

        total_profit_loss = positions['profit_loss'].sum()
        total_cost = positions['market_value'].sum() - total_profit_loss

        return_rate = (total_profit_loss / total_cost) * 100 if total_cost > 0 else 0

        return {
            'total_return': total_profit_loss,
            'return_rate': return_rate,
            'current_asset': current_asset,
            'position_count': len(positions)
        }

    # ==================== 12. 市场数据分析 ====================

    def get_market_overview(self, market: str = 'A股') -> dict[str, Any]:
        """获取市场概览"""
        # 获取主要指数
        index_codes = {
            'A股': ['000001.SH', '399001.SZ', '399006.SZ'],  # 上证指数、深证成指、创业板指
            'SH': ['000001.SH'],  # 上证指数
            'SZ': ['399001.SZ']   # 深证成指
        }

        codes = index_codes.get(market, ['000001.SH'])

        overview = {}
        for code in codes:
            try:
                price_info = self.get_price_change(code)
                overview[code] = price_info
            except Exception:
                continue

        return overview

    def get_sector_performance(self, sector_codes: list[str]) -> pd.DataFrame:
        """获取板块表现"""
        results = []

        for code in sector_codes:
            try:
                price_info = self.get_price_change(code)
                if price_info:
                    results.append({
                        'code': code,
                        'current_price': price_info['current_price'],
                        'change': price_info['change'],
                        'change_pct': price_info['change_pct'],
                        'volume': price_info['volume']
                    })
            except Exception:
                continue

        return pd.DataFrame(results)

    # ==================== 13. 智能交易 ====================

    def smart_buy(self, account_id: str, code: str, target_amount: float,
                  max_price_impact: float = 0.02) -> list[int]:
        """智能买入（分批下单）"""
        if not self.check_position_limit(account_id, code, 0):
            return []

        # 获取当前价格
        current_price_data = self.data_api.get_current_price([code])
        if current_price_data is None or current_price_data.empty:
            return []

        current_price = current_price_data.iloc[0]['price']
        total_volume = int(target_amount / current_price)

        # 分批下单，每批最多不超过总量的30%
        batch_size = max(100, (total_volume // 3) // 100 * 100)  # 确保是100的倍数
        order_ids = []

        remaining_volume = total_volume
        while remaining_volume >= 100:
            volume = min(batch_size, remaining_volume)

            order_id = self.trade_api.buy(account_id, code, volume, current_price, 'limit')
            if order_id:
                order_ids.append(order_id)

            remaining_volume -= volume
            time.sleep(0.5)  # 间隔0.5秒

        return order_ids

    def smart_sell(self, account_id: str, code: str, sell_ratio: float = 1.0) -> list[int]:
        """智能卖出（分批卖出）"""
        position = self.get_position_by_code(account_id, code)
        if not position:
            return []

        total_volume = int(position['volume'] * sell_ratio)
        if total_volume < 100:
            return []

        # 获取当前价格
        current_price_data = self.data_api.get_current_price([code])
        if current_price_data is None or current_price_data.empty:
            return []

        current_price = current_price_data.iloc[0]['price']

        # 分批卖出
        batch_size = max(100, (total_volume // 3) // 100 * 100)
        order_ids = []

        remaining_volume = total_volume
        while remaining_volume >= 100:
            volume = min(batch_size, remaining_volume)

            order_id = self.trade_api.sell(account_id, code, volume, current_price, 'limit')
            if order_id:
                order_ids.append(order_id)

            remaining_volume -= volume
            time.sleep(0.5)

        return order_ids

    # ==================== 14. 策略回测 ====================

    def simple_backtest(self, code: str, strategy_func, start_date: str, end_date: str,
                       initial_capital: float = 100000) -> dict[str, Any]:
        """简单回测框架"""
        # 获取历史数据
        data = self.data_api.get_price([code], start=start_date, end=end_date, period='1d')
        if data is None or data.empty:
            return {}

        capital = initial_capital
        position = 0
        trades = []

        for i in range(len(data)):
            current_data = data.iloc[:i+1]  # 当前及之前的数据
            if len(current_data) < 20:  # 需要足够的历史数据
                continue

            signal = strategy_func(current_data)
            current_price = current_data.iloc[-1]['close']

            if signal == 'buy' and position == 0 and capital > current_price * 100:
                # 买入
                shares = int(capital / current_price / 100) * 100
                cost = shares * current_price
                capital -= cost
                position = shares
                trades.append({
                    'date': current_data.iloc[-1]['time'],
                    'action': 'buy',
                    'price': current_price,
                    'shares': shares,
                    'capital': capital
                })

            elif signal == 'sell' and position > 0:
                # 卖出
                proceeds = position * current_price
                capital += proceeds
                trades.append({
                    'date': current_data.iloc[-1]['time'],
                    'action': 'sell',
                    'price': current_price,
                    'shares': position,
                    'capital': capital
                })
                position = 0

        # 计算最终收益
        final_value = capital + (position * data.iloc[-1]['close'] if position > 0 else 0)
        total_return = (final_value - initial_capital) / initial_capital * 100

        return {
            'initial_capital': initial_capital,
            'final_value': final_value,
            'total_return': total_return,
            'trades': trades,
            'trade_count': len(trades)
        }

    # ==================== 15. 组合管理 ====================

    def rebalance_portfolio(self, account_id: str, target_weights: dict[str, float]) -> dict[str, list[int]]:
        """投资组合再平衡"""
        total_asset = self.get_total_asset(account_id)
        if total_asset <= 0:
            return {}

        results = {}

        for code, target_weight in target_weights.items():
            target_value = total_asset * target_weight

            # 获取当前持仓
            current_position = self.get_position_by_code(account_id, code)
            current_value = current_position.get('market_value', 0.0)

            diff_value = target_value - current_value

            if abs(diff_value) < 1000:  # 差异小于1000元，不调整
                continue

            # 获取当前价格
            current_price_data = self.data_api.get_current_price([code])
            if current_price_data is None or current_price_data.empty:
                continue

            current_price = current_price_data.iloc[0]['price']

            if diff_value > 0:
                # 需要买入
                volume = int(diff_value / current_price / 100) * 100
                if volume >= 100:
                    order_ids = self.smart_buy(account_id, code, diff_value)
                    results[code] = order_ids
            else:
                # 需要卖出
                sell_value = abs(diff_value)
                volume = int(sell_value / current_price / 100) * 100
                current_volume = current_position.get('volume', 0)

                if volume >= 100 and volume <= current_volume:
                    sell_ratio = volume / current_volume
                    order_ids = self.smart_sell(account_id, code, sell_ratio)
                    results[code] = order_ids

        return results

    # ==================== 16. 数据导出 ====================

    def export_positions_to_csv(self, account_id: str, filename: Optional[str] = None) -> str:
        """导出持仓到CSV"""
        positions = self.trade_api.get_positions(account_id)
        if positions is None or positions.empty:
            return ""

        if filename is None:
            filename = f"positions_{account_id}_{datetime.now(tz=_SH).strftime('%Y%m%d_%H%M%S')}.csv"

        positions.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename

    def export_trades_to_csv(self, account_id: str, start_date: Optional[str] = None,
                            end_date: Optional[str] = None, filename: Optional[str] = None) -> str:
        """导出成交记录到CSV"""
        trades = self.trade_api.get_trades(account_id)
        if trades is not None and not trades.empty and 'time' in trades.columns and (start_date or end_date):
            time_series = pd.to_datetime(trades['time'], errors='coerce')
            if start_date:
                parsed_start = pd.to_datetime(start_date, errors='coerce')
                if pd.notna(parsed_start):
                    trades = trades[time_series >= parsed_start]
            if end_date:
                parsed_end = pd.to_datetime(end_date, errors='coerce')
                if pd.notna(parsed_end):
                    trades = trades[time_series <= parsed_end]
        if trades is None or trades.empty:
            return ""

        if filename is None:
            filename = f"trades_{account_id}_{datetime.now(tz=_SH).strftime('%Y%m%d_%H%M%S')}.csv"

        trades.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename

    # ==================== 17. 监控和报警 ====================

    def monitor_price_alerts(self, alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """价格监控报警"""
        triggered_alerts = []

        for alert in alerts:
            code = alert['code']
            alert_type = alert['type']  # 'above', 'below'
            target_price = alert['price']

            try:
                current_price_data = self.data_api.get_current_price([code])
                if current_price_data is None or current_price_data.empty:
                    continue

                current_price = current_price_data.iloc[0]['price']

                triggered = False
                if alert_type == 'above' and current_price >= target_price:
                    triggered = True
                elif alert_type == 'below' and current_price <= target_price:
                    triggered = True

                if triggered:
                    triggered_alerts.append({
                        'code': code,
                        'alert_type': alert_type,
                        'target_price': target_price,
                        'current_price': current_price,
                        'trigger_time': self.get_current_time_str()
                    })

            except Exception:
                continue

        return triggered_alerts

    def monitor_position_alerts(self, account_id: str, alerts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """持仓监控报警"""
        triggered_alerts = []

        for alert in alerts:
            code = alert['code']
            alert_type = alert['type']  # 'profit_above', 'loss_below'
            threshold = alert['threshold']  # 盈亏阈值（百分比）

            try:
                position = self.get_position_by_code(account_id, code)
                if not position:
                    continue

                profit_loss_pct = (position['profit_loss'] / position['cost']) * 100 if position['cost'] > 0 else 0

                triggered = False
                if alert_type == 'profit_above' and profit_loss_pct >= threshold:
                    triggered = True
                elif alert_type == 'loss_below' and profit_loss_pct <= -threshold:
                    triggered = True

                if triggered:
                    triggered_alerts.append({
                        'code': code,
                        'alert_type': alert_type,
                        'threshold': threshold,
                        'current_pnl_pct': profit_loss_pct,
                        'trigger_time': self.get_current_time_str()
                    })

            except Exception:
                continue

        return triggered_alerts

    # ==================== 18. 高级分析 ====================

    def calculate_correlation_matrix(self, codes: list[str], period: str = '1d', count: int = 60) -> pd.DataFrame:
        """计算股票相关性矩阵"""
        price_data = {}

        for code in codes:
            data = self.data_api.get_price([code], period=period, count=count)
            if data is not None and not data.empty:
                price_data[code] = data['close']

        if not price_data:
            return pd.DataFrame()

        # 创建价格DataFrame
        df = pd.DataFrame(price_data)

        # 计算收益率
        returns = df.pct_change().dropna()

        # 计算相关性矩阵
        correlation_matrix = returns.corr()

        return correlation_matrix

    def calculate_beta(self, stock_code: str, market_code: str = '000300.SH',
                      period: str = '1d', count: int = 252) -> float:
        """计算股票Beta值"""
        # 获取股票和市场数据
        stock_data = self.data_api.get_price([stock_code], period=period, count=count)
        market_data = self.data_api.get_price([market_code], period=period, count=count)

        if stock_data is None or market_data is None or stock_data.empty or market_data.empty:
            return 1.0

        # 计算收益率
        stock_returns = stock_data['close'].pct_change().dropna()
        market_returns = market_data['close'].pct_change().dropna()

        # 确保数据长度一致
        min_length = min(len(stock_returns), len(market_returns))
        stock_returns = stock_returns.tail(min_length)
        market_returns = market_returns.tail(min_length)

        # 计算Beta
        covariance = stock_returns.cov(market_returns)
        market_variance = market_returns.var()

        if market_variance == 0:
            return 1.0

        covariance_value = pd.to_numeric(pd.Series([covariance]), errors='coerce').iloc[0]
        market_variance_value = pd.to_numeric(pd.Series([market_variance]), errors='coerce').iloc[0]
        if pd.isna(covariance_value) or pd.isna(market_variance_value) or market_variance_value == 0:
            return 1.0
        beta = float(covariance_value) / float(market_variance_value)
        return float(beta)

    def calculate_sharpe_ratio(self, returns: pd.Series, risk_free_rate: float = 0.03) -> float:
        """计算夏普比率"""
        if returns.empty:
            return 0.0

        excess_returns = returns.mean() * 252 - risk_free_rate  # 年化超额收益
        volatility = returns.std() * math.sqrt(252)  # 年化波动率

        if volatility == 0:
            return 0.0

        return excess_returns / volatility

    # ==================== 19. 个股权重检查 ====================

    def check_stock_weight_in_portfolio(self, account_id: str, code: str) -> dict[str, float]:
        """检查个股在投资组合中的权重"""
        total_asset = self.get_total_asset(account_id)
        if total_asset <= 0:
            return {}

        position = self.get_position_by_code(account_id, code)
        if not position:
            return {
                'weight': 0.0,
                'market_value': 0.0,
                'total_asset': total_asset
            }

        market_value = position.get('market_value', 0.0)
        weight = (market_value / total_asset) * 100

        return {
            'weight': weight,
            'market_value': market_value,
            'total_asset': total_asset,
            'volume': position.get('volume', 0),
            'avg_cost': position.get('open_price', 0.0)
        }

    def check_all_stock_weights(self, account_id: str) -> pd.DataFrame:
        """检查所有持仓股票的权重"""
        positions = self.trade_api.get_positions(account_id)
        if positions is None or positions.empty:
            return pd.DataFrame()

        total_asset = self.get_total_asset(account_id)
        if total_asset <= 0:
            return pd.DataFrame()

        # 计算权重
        positions['weight'] = (positions['market_value'] / total_asset) * 100

        # 按权重排序
        positions = positions.sort_values('weight', ascending=False)

        return positions[['code', 'volume', 'market_value', 'weight', 'profit_loss']]

    def check_weight_limits(self, account_id: str, max_single_weight: float = 20.0,
                           max_sector_weight: float = 40.0) -> dict[str, Any]:
        """检查权重限制"""
        weights_df = self.check_all_stock_weights(account_id)
        if weights_df.empty:
            return {}

        # 检查单只股票权重
        over_limit_stocks = weights_df[weights_df['weight'] > max_single_weight]

        # 这里简化处理，实际应该根据行业分类检查板块权重
        max_weight = weights_df['weight'].max()

        return {
            'max_single_weight_limit': max_single_weight,
            'current_max_weight': max_weight,
            'over_limit_count': len(over_limit_stocks),
            'over_limit_stocks': over_limit_stocks.to_dict('records') if not over_limit_stocks.empty else [],
            'is_compliant': len(over_limit_stocks) == 0 and max_weight <= max_single_weight
        }

    def suggest_rebalance_for_weights(self, account_id: str, target_max_weight: float = 15.0) -> dict[str, dict[str, float]]:
        """建议权重再平衡方案"""
        weights_df = self.check_all_stock_weights(account_id)
        if weights_df.empty:
            return {}

        suggestions = {}
        total_asset = self.get_total_asset(account_id)

        for _, row in weights_df.iterrows():
            code = row['code']
            current_weight = row['weight']
            current_value = row['market_value']

            if current_weight > target_max_weight:
                # 需要减仓
                target_value = total_asset * (target_max_weight / 100)
                reduce_value = current_value - target_value
                reduce_ratio = reduce_value / current_value

                suggestions[code] = {
                    'action': 'reduce',
                    'current_weight': current_weight,
                    'target_weight': target_max_weight,
                    'current_value': current_value,
                    'target_value': target_value,
                    'reduce_value': reduce_value,
                    'reduce_ratio': reduce_ratio
                }

        return suggestions

    # ==================== 20. 组合优化再平衡 ====================

    def optimize_and_rebalance(
        self,
        account_id: str,
        returns: "pd.DataFrame",
        optimizer_config: "Any | None" = None,
        max_single_weight: float = 0.3,
        max_hhi: float = 0.2,
        dry_run: bool = False,
    ) -> tuple["Any", "Any", dict[str, list[int]]]:
        """优化权重 → 风控校验 → 再平衡，三步原子调用。

        Args:
            account_id:        账户 ID。
            returns:           历史收益率 DataFrame（行=日期，列=股票代码）。
            optimizer_config:  :class:`core.portfolio_optimizer.PortfolioOptimizeConfig`，
                               None 时使用默认配置（risk_parity, max_weight=0.3）。
            max_single_weight: 单仓上限，用于风控校验（默认 0.3）。
            max_hhi:           HHI 集中度上限，用于风控校验（默认 0.2）。
            dry_run:           为 True 时只返回优化/风控结果，不实际下单。

        Returns:
            (OptimizeResult, OptimalWeightRiskCheck, order_results)
            当 dry_run=True 或风控不通过时，order_results 为空 dict。
        """
        from core.portfolio_optimizer import PortfolioOptimizer
        from core.portfolio_risk import PortfolioRiskAnalyzer

        optimizer = PortfolioOptimizer(optimizer_config)
        opt_result = optimizer.optimize_result(returns)

        analyzer = PortfolioRiskAnalyzer()
        risk_check = analyzer.check_optimal_weights(
            opt_result.weights,
            max_single_weight=max_single_weight,
            max_hhi=max_hhi,
        )

        order_results: dict[str, list[int]] = {}
        _log = logging.getLogger(__name__)

        if not opt_result.feasible:
            _log.warning(
                "[optimize_and_rebalance] 优化器不可行，禁止下单。"
                " account=%s method=%s",
                account_id,
                getattr(optimizer_config, "method", "default"),
            )
        elif not risk_check.feasible:
            _log.warning(
                "[optimize_and_rebalance] 风控校验未通过，禁止下单。"
                " account=%s warnings=%s",
                account_id,
                risk_check.warnings,
            )
        elif dry_run:
            _log.info(
                "[optimize_and_rebalance] dry_run=True，跳过下单。"
                " account=%s",
                account_id,
            )
        else:
            order_results = self.rebalance_portfolio(account_id, opt_result.weights)

        return opt_result, risk_check, order_results

    def execute_twap(
        self,
        account_id: str,
        code: str,
        side: str,
        total_volume: int,
        slices: int = 5,
        interval_sec: float = 1.0,
        price: float = 0.0,
        price_type: str = "market",
        min_lot: int = 100,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        from .execution_algorithms import TwapPlan

        result: dict[str, Any] = {
            "account_id": account_id,
            "code": code,
            "side": side,
            "total_volume": int(total_volume),
            "slices": int(slices),
            "planned_volumes": [],
            "order_ids": [],
            "submitted_volumes": [],
            "feasible": False,
            "message": "",
        }
        if not self._connected_trade:
            result["message"] = "交易服务未连接"
            return result
        side_norm = str(side or "").strip().lower()
        if side_norm not in ("buy", "sell"):
            result["message"] = "side 仅支持 buy/sell"
            return result
        plan = TwapPlan(total_volume=max(int(total_volume), 0), slices=max(int(slices), 1), min_lot=max(int(min_lot), 1))
        volumes = plan.build()
        result["planned_volumes"] = volumes
        if not volumes:
            result["message"] = "切片后无有效下单数量"
            return result
        result["feasible"] = True
        if dry_run:
            result["message"] = "dry_run=True"
            return result

        _log = logging.getLogger(__name__)
        for idx, vol in enumerate(volumes):
            if side_norm == "buy":
                order_id = self.trade_api.buy(
                    account_id=account_id,
                    code=code,
                    volume=int(vol),
                    price=float(price),
                    price_type=price_type,
                )
            else:
                order_id = self.trade_api.sell(
                    account_id=account_id,
                    code=code,
                    volume=int(vol),
                    price=float(price),
                    price_type=price_type,
                )
            if order_id:
                oid: Any = getattr(order_id, "order_id", order_id)
                result["order_ids"].append(oid)
                result["submitted_volumes"].append(int(vol))
            else:
                _log.warning(
                    "[execute_twap] 子单失败 account=%s code=%s side=%s slice=%s volume=%s",
                    account_id,
                    code,
                    side_norm,
                    idx,
                    vol,
                )
            if idx < len(volumes) - 1 and interval_sec > 0:
                time.sleep(float(interval_sec))
        result["message"] = "ok"
        return result

    def execute_vwap(
        self,
        account_id: str,
        code: str,
        side: str,
        total_volume: int,
        volume_profile: list[float] | None = None,
        interval_sec: float = 1.0,
        price: float = 0.0,
        price_type: str = "market",
        min_lot: int = 100,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        from .execution_algorithms import TwapPlan, VwapPlan

        result: dict[str, Any] = {
            "account_id": account_id,
            "code": code,
            "side": side,
            "total_volume": int(total_volume),
            "planned_volumes": [],
            "order_ids": [],
            "submitted_volumes": [],
            "feasible": False,
            "fallback_to_twap": False,
            "message": "",
        }
        if not self._connected_trade:
            result["message"] = "交易服务未连接"
            return result
        side_norm = str(side or "").strip().lower()
        if side_norm not in ("buy", "sell"):
            result["message"] = "side 仅支持 buy/sell"
            return result
        profile = volume_profile or []
        if profile:
            volumes = VwapPlan(
                total_volume=max(int(total_volume), 0),
                profile=profile,
                min_lot=max(int(min_lot), 1),
            ).build()
        else:
            volumes = []
        if not volumes:
            fallback_slices = max(len(profile), 5)
            volumes = TwapPlan(
                total_volume=max(int(total_volume), 0),
                slices=fallback_slices,
                min_lot=max(int(min_lot), 1),
            ).build()
            result["fallback_to_twap"] = True
        result["planned_volumes"] = volumes
        if not volumes:
            result["message"] = "切片后无有效下单数量"
            return result
        result["feasible"] = True
        if dry_run:
            result["message"] = "dry_run=True"
            return result

        _log = logging.getLogger(__name__)
        for idx, vol in enumerate(volumes):
            if side_norm == "buy":
                order_id = self.trade_api.buy(
                    account_id=account_id,
                    code=code,
                    volume=int(vol),
                    price=float(price),
                    price_type=price_type,
                )
            else:
                order_id = self.trade_api.sell(
                    account_id=account_id,
                    code=code,
                    volume=int(vol),
                    price=float(price),
                    price_type=price_type,
                )
            if order_id:
                oid: Any = getattr(order_id, "order_id", order_id)
                result["order_ids"].append(oid)
                result["submitted_volumes"].append(int(vol))
            else:
                _log.warning(
                    "[execute_vwap] 子单失败 account=%s code=%s side=%s slice=%s volume=%s",
                    account_id,
                    code,
                    side_norm,
                    idx,
                    vol,
                )
            if idx < len(volumes) - 1 and interval_sec > 0:
                time.sleep(float(interval_sec))
        result["message"] = "ok"
        return result

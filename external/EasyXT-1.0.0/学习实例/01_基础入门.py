
"""
EasyXT学习实例 01 - 基础入门
学习目标：掌握EasyXT的基本初始化和简单数据获取
"""

import sys
import os
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

import easy_xt

# 不再使用模拟数据
MOCK_MODE = False

def lesson_01_basic_setup():
    """第1课：基础设置和初始化"""
    print("=" * 60)
    print("第1课：EasyXT基础设置")
    print("=" * 60)
    
    # 1. 导入和创建API实例
    print("1. 创建API实例")
    api = easy_xt.get_api()
    print("✓ API实例创建成功")
    
    # 2. 初始化数据服务
    print("\n2. 初始化数据服务")
    try:
        success = api.init_data()
        if success:
            print("✓ 数据服务初始化成功")
        else:
            print("⚠️ 数据服务初始化失败，这是正常的")
            print("💡 原因：需要启动迅投客户端并登录")
            print("🔄 继续使用模拟模式进行学习")
            success = True  # 继续学习
    except Exception as e:
        print(f"⚠️ 数据服务初始化异常: {e}")
        print("🔄 继续使用模拟模式进行学习")
        success = True  # 继续学习
    
    return success

def lesson_02_get_stock_data():
    """第2课：获取股票数据"""
    print("\n" + "=" * 60)
    print("第2课：获取股票数据")
    print("=" * 60)
    
    api = easy_xt.get_api()
    
    # 1. 获取单只股票的历史数据
    print("1. 获取平安银行(000001.SZ)最近10天数据")
    try:
        data = api.get_price('000001.SZ', count=10)
        print("✓ 数据获取成功")
        print(f"数据形状: {data.shape}")
        print("最新5条数据:")
        print(data.tail().to_string())
    except Exception as e:
        print(f"✗ 获取数据失败: {e}")
    
    # 2. 获取多只股票数据
    print("\n2. 获取多只股票数据")
    try:
        codes = ['000001.SZ', '000002.SZ', '600000.SH']  # 平安银行、万科A、浦发银行
        data = api.get_price(codes, count=5)
        if data is None or data.empty:
            if MOCK_MODE:
                print("🔄 切换到模拟数据模式...")
                data = api.mock_get_price(codes, count=5)
            else:
                raise Exception("无法获取数据")
                
        
        if not data.empty:
            print("✓ 多股票数据获取成功")
            print(f"数据形状: {data.shape}")
            print("数据预览:")
            print(data.head(10).to_string())
        else:
            print("✗ 未获取到数据")
    except Exception as e:
        print(f"✗ 获取多股票数据失败: {e}")

def lesson_03_different_periods():
    """第3课：获取不同周期的数据"""
    print("\n" + "=" * 60)
    print("第3课：获取不同周期的数据")
    print("=" * 60)
    
    api = easy_xt.get_api()
    code = '000001.SZ'
    
    # 测试稳定支持的周期（基于QMT数据周期支持情况报告）
    stable_periods = ['1d', '1m', '5m']  # 稳定支持的周期
    problematic_periods = ['15m', '30m', '1h']  # 有问题的周期
    
    print("测试稳定支持的数据周期:")
    for period in stable_periods:
        print(f"\n获取 {code} 的 {period} 数据:")
        try:
            data = api.get_price(code, period=period, count=5)
            if not data.empty:
                print(f"✓ {period} 数据获取成功，共 {len(data)} 条")
                if 'time' in data.columns:
                    print(f"时间范围: {data['time'].min()} 到 {data['time'].max()}")
                else:
                    print(f"时间范围: {data.index[0]} 到 {data.index[-1]}")
                print(f"最新价格: {data['close'].iloc[-1]:.2f}")
            else:
                print(f"✗ {period} 数据为空")
        except Exception as e:
            print(f"✗ {period} 数据获取失败: {e}")
    
    print("\n⚠️  注意：以下周期可能导致程序挂起，已跳过测试:")
    for period in problematic_periods:
        print(f"   - {period}: 可能导致程序无响应")
    
    print("\n💡 建议：")
    print("   - 日线数据使用 '1d'")
    print("   - 分钟数据使用 '1m' 或 '5m'")
    print("   - 避免使用 '15m', '30m', '1h' 周期")

def lesson_04_date_range_data():
    """第4课：按日期范围获取数据"""
    print("\n" + "=" * 60)
    print("第4课：按日期范围获取数据")
    print("=" * 60)
    
    api = easy_xt.get_api()
    code = '000001.SZ'
    
    # 1. 按日期范围获取数据（使用近期日期）
    print("1. 获取最近一周的数据")
    try:
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        print(f"获取 {start_str} 到 {end_str} 的数据")
        
        data = api.get_price(
            codes=code,
            start=start_str,
            end=end_str,
            period='1d'
        )
        if not data.empty:
            print("✓ 日期范围数据获取成功")
            print(f"数据条数: {len(data)}")
            if 'time' in data.columns:
                print(f"日期范围: {data['time'].min()} 到 {data['time'].max()}")
            else:
                print(f"日期范围: {data.index[0]} 到 {data.index[-1]}")
            print("价格统计:")
            print(f"  最高价: {data['high'].max():.2f}")
            print(f"  最低价: {data['low'].min():.2f}")
            print(f"  平均价: {data['close'].mean():.2f}")
        else:
            print("✗ 未获取到数据")
    except Exception as e:
        print(f"✗ 获取日期范围数据失败: {e}")
    
    # 2. 不同的日期格式（使用近期日期）
    print("\n2. 测试不同的日期格式")
    try:
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=3)
        
        date_formats = [
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),  # 标准格式
            (start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')),      # 紧凑格式
            (start_date.strftime('%Y/%m/%d'), end_date.strftime('%Y/%m/%d'))   # 斜杠格式
        ]
        
        for start, end in date_formats:
            print(f"测试日期格式: {start} 到 {end}")
            try:
                data = api.get_price(code, start=start, end=end)
                if not data.empty:
                    print(f"✓ 格式 {start} 解析成功，获取 {len(data)} 条数据")
                else:
                    print(f"✗ 格式 {start} 未获取到数据")
            except Exception as e:
                print(f"✗ 格式 {start} 解析失败: {e}")
    except Exception as e:
        print(f"✗ 日期格式测试失败: {e}")
    
    # 3. 使用count参数获取数据（更稳定的方式）
    print("\n3. 使用count参数获取最近数据（推荐方式）")
    try:
        data = api.get_price(code, period='1d', count=10)
        if not data.empty:
            print("✓ count方式数据获取成功")
            print(f"数据条数: {len(data)}")
            print("最新5条数据:")
            print(data.tail()[['time', 'code', 'open', 'high', 'low', 'close']].to_string())
        else:
            print("✗ count方式未获取到数据")
    except Exception as e:
        print(f"✗ count方式获取失败: {e}")

def lesson_05_current_price():
    """第5课：获取实时价格"""
    print("\n" + "=" * 60)
    print("第5课：获取实时价格")
    print("=" * 60)
    
    api = easy_xt.get_api()
    
    # 1. 获取单只股票实时价格
    print("1. 获取平安银行实时价格")
    try:
        current = api.get_current_price('000001.SZ')
        if current is None or current.empty:
            if MOCK_MODE:
                print("🔄 切换到模拟数据模式...")
                current = api.mock_get_current_price('000001.SZ')
            else:
                raise Exception("无法获取实时价格")
        
        if not current.empty:
            print("✓ 实时价格获取成功")
            print(current.to_string())
        else:
            print("✗ 未获取到实时价格")
    except Exception as e:
        print(f"✗ 获取实时价格失败: {e}")
    
    # 2. 获取多只股票实时价格
    print("\n2. 获取多只股票实时价格")
    try:
        codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
        current = api.get_current_price(codes)
        if current is None or current.empty:
            if MOCK_MODE:
                print("🔄 切换到模拟数据模式...")
                current = api.mock_get_current_price(codes)
            else:
                raise Exception("无法获取实时价格")
        
        if not current.empty:
            print("✓ 多股票实时价格获取成功")
            print("实时价格数据:")
            # 显示实际可用的字段
            available_columns = ['code', 'price', 'open', 'high', 'low', 'pre_close']
            display_columns = [col for col in available_columns if col in current.columns]
            print(current[display_columns].to_string())
            
            # 计算涨跌幅
            if 'price' in current.columns and 'pre_close' in current.columns:
                print("\n涨跌幅计算:")
                for _, row in current.iterrows():
                    if row['pre_close'] > 0:
                        change = row['price'] - row['pre_close']
                        change_pct = (change / row['pre_close']) * 100
                        print(f"{row['code']}: {change:+.2f} ({change_pct:+.2f}%)")
        else:
            print("✗ 未获取到实时价格")
    except Exception as e:
        print(f"✗ 获取多股票实时价格失败: {e}")

def lesson_06_stock_list():
    """第6课：获取股票列表"""
    print("\n" + "=" * 60)
    print("第6课：获取股票列表")
    print("=" * 60)
    
    api = easy_xt.get_api()
    
    # 1. 获取所有A股列表
    print("1. 获取A股列表")
    try:
        stock_list = api.get_stock_list('A股')
        if stock_list:
            print(f"✓ A股列表获取成功，共 {len(stock_list)} 只股票")
            print("前10只股票:")
            for i, code in enumerate(stock_list[:10]):
                print(f"  {i+1}. {code}")
        else:
            print("✗ 未获取到股票列表")
    except Exception as e:
        
        print(f"✗ 获取股票列表失败: {e}")
    
    # 2. 获取沪深300列表
    print("\n2. 获取沪深300列表")
    try:
        hs300_list = api.get_stock_list('沪深300')
        if hs300_list:
            print(f"✓ 沪深300列表获取成功，共 {len(hs300_list)} 只股票")
            print("前10只股票:")
            for i, code in enumerate(hs300_list[:10]):
                print(f"  {i+1}. {code}")
        else:
            print("✗ 未获取到沪深300列表")
    except Exception as e:
        print(f"✗ 获取沪深300列表失败: {e}")

def lesson_07_trading_dates():
    """第7课：获取交易日历"""
    print("\n" + "=" * 60)
    print("第7课：获取交易日历")
    print("=" * 60)
    
    api = easy_xt.get_api()
    
    # 1. 获取最近的交易日
    print("1. 获取最近10个交易日")
    try:
        trading_dates = api.get_trading_dates(market='SH', count=10)
        if trading_dates:
            print("✓ 交易日获取成功")
            print("最近10个交易日:")
            for i, date in enumerate(trading_dates[-10:]):
                print(f"  {i+1}. {date}")
        else:
            print("✗ 未获取到交易日")
    except Exception as e:
        print(f"✗ 获取交易日失败: {e}")
    
    # 2. 获取指定时间段的交易日（使用近期日期）
    print("\n2. 获取本月的交易日")
    try:
        from datetime import datetime
        current_date = datetime.now()
        start_of_month = current_date.replace(day=1)
        
        start_str = start_of_month.strftime('%Y-%m-%d')
        end_str = current_date.strftime('%Y-%m-%d')
        
        print(f"获取 {start_str} 到 {end_str} 的交易日")
        
        trading_dates = api.get_trading_dates(
            market='SH',
            start=start_str,
            end=end_str
        )
        if trading_dates:
            print(f"✓ 本月交易日获取成功，共 {len(trading_dates)} 天")
            print("交易日列表:")
            for date in trading_dates:
                print(f"  {date}")
        else:
            print("✗ 未获取到交易日")
    except Exception as e:
        print(f"✗ 获取交易日失败: {e}")
    
    # 3. 获取最近30个交易日（更稳定的方式）
    print("\n3. 获取最近30个交易日（推荐方式）")
    try:
        trading_dates = api.get_trading_dates(market='SH', count=30)
        if trading_dates:
            print("✓ 最近30个交易日获取成功")
            print("最近10个交易日:")
            for i, date in enumerate(trading_dates[-10:]):
                print(f"  {i+1}. {date}")
            print(f"... 共 {len(trading_dates)} 个交易日")
        else:
            print("✗ 未获取到交易日")
    except Exception as e:
        print(f"✗ 获取交易日失败: {e}")

def main():
    """主函数：运行所有基础学习课程"""
    print("🎓 EasyXT基础入门学习课程")
    print("本课程将带您学习EasyXT的基本功能")
    print("请确保已正确安装xtquant并启动相关服务")
    
    # 运行所有课程
    lessons = [
        lesson_01_basic_setup,
        lesson_02_get_stock_data,
        lesson_03_different_periods,
        lesson_04_date_range_data,
        lesson_05_current_price,
        lesson_06_stock_list,
        lesson_07_trading_dates
    ]
    
    for lesson in lessons:
        try:
            lesson()
            if not (len(sys.argv) > 1 and '--auto' in sys.argv):
                input("\n按回车键继续下一课...")
            else:
                print(f"\n✓ 第{lessons.index(lesson)+1}课完成，自动继续...")
        except KeyboardInterrupt:
            print("\n\n学习已中断")
            break
        except Exception as e:
            print(f"\n课程执行出错: {e}")
            input("按回车键继续...")
    
    print("\n🎉 基础入门课程完成！")
    print("接下来可以学习：")
    print("- 02_交易基础.py - 学习基础交易功能")
    print("- 03_高级交易.py - 学习高级交易功能")
    print("- 04_策略开发.py - 学习策略开发")

if __name__ == "__main__":
    main()

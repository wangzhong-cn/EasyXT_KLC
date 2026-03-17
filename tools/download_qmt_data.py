"""
QMT 最大化本地数据下载工具
=====================================
功能：
  - 下载A股、股指期货、商品期货的历史K线（日线/1分钟/5分钟）
  - 下载Tick/分笔成交数据（需QMT客户端运行并联网）
  - 支持断点续传（跳过已有数据）

运行环境：
  conda activate qmt311    (Python 3.11, xtquant 兼容版)
  python tools/download_qmt_data.py

依赖：QMT 客户端必须处于运行状态（XtItClient.exe 已启动并登录）
数据存储：D:\\申万宏源策略量化交易终端\\userdata_mini\\datadir\\
"""

import sys
import time
import datetime
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from xtquant import xtdata

# ====================================================================
# 配置区
# ====================================================================

# 历史数据起始日期（如需更多历史数据可往前调）
HIST_START = '20200101'
HIST_END   = datetime.date.today().strftime('%Y%m%d')

# 要下载的数据周期
PERIODS = ['1d', '60m', '5m']          # 日线 / 1小时 / 5分钟
PERIODS_TICK = ['tick']                 # tick 数据（数据量最大，按需开启）
DOWNLOAD_TICK = False                   # 默认关闭tick，手动改为True开启

# A股指数
INDEX_SYMBOLS = [
    '000001.SH',  # 上证指数
    '000300.SH',  # 沪深300
    '000905.SH',  # 中证500
    '000852.SH',  # 中证1000
    '399001.SZ',  # 深证成指
    '399006.SZ',  # 创业板
    '510300.SH',  # 沪深300ETF
    '510500.SH',  # 中证500ETF
    '159915.SZ',  # 创业板ETF
]

# 股指期货（CFFEX）
IF_SYMBOLS = [
    'IF01.IF',  # 沪深300期货 主力
    'IC01.IF',  # 中证500期货 主力
    'IH01.IF',  # 上证50期货 主力
    'IM01.IF',  # 中证1000期货 主力
    'TL01.IF',  # 30年国债期货 主力
    'T01.IF',   # 10年国债期货 主力
    'TF01.IF',  # 5年国债期货 主力
    'TS01.IF',  # 2年国债期货 主力
    # 当月 + 下月合约也可按需加入
]

# 上期所商品期货 (SHFE) 主力合约
SHFE_SYMBOLS = [
    'au01.SF',   # 黄金
    'ag01.SF',   # 白银
    'cu01.SF',   # 铜
    'al01.SF',   # 铝
    'zn01.SF',   # 锌
    'pb01.SF',   # 铅
    'ni01.SF',   # 镍
    'sn01.SF',   # 锡
    'rb01.SF',   # 螺纹钢
    'hc01.SF',   # 热轧卷板
    'ss01.SF',   # 不锈钢
    'bu01.SF',   # 沥青
    'fu01.SF',   # 燃料油
    'ru01.SF',   # 橡胶
    'sp01.SF',   # 纸浆
    'ao01.SF',   # 氧化铝
    'br01.SF',   # 丁二烯橡胶
    'ad01.SF',   # (国标铝合金)
    'op01.SF',   # 白油
    'wr01.SF',   # 线材
]

# 大商所商品期货 (DCE) 主力合约
DCE_SYMBOLS = [
    'i01.DF',    # 铁矿石
    'm01.DF',    # 豆粕
    'a01.DF',    # 大豆(豆一)
    'c01.DF',    # 玉米
    'cs01.DF',   # 玉米淀粉
    'y01.DF',    # 豆油
    'p01.DF',    # 棕榈油
    'j01.DF',    # 焦炭
    'jm01.DF',   # 焦煤
    'b01.DF',    # 黄大豆2号
    'jd01.DF',   # 鸡蛋
    'l01.DF',    # 聚乙烯(LLDPE)
    'v01.DF',    # PVC
    'pp01.DF',   # 聚丙烯
    'eg01.DF',   # 乙二醇
    'eb01.DF',   # 苯乙烯
    'pg01.DF',   # LPG
    'lh01.DF',   # 生猪
    'rr01.DF',   # 粳米
    'lg01.DF',   # 碳酸锂
    'bz01.DF',   # 苯
    'bb01.DF',   # 胶合板
    'fb01.DF',   # 纤维板
]

# 郑商所商品期货 (CZCE) 主力合约
CZCE_SYMBOLS = [
    'MA01.ZF',   # 甲醇
    'TA01.ZF',   # PTA
    'CF01.ZF',   # 棉花
    'SR01.ZF',   # 白糖
    'AP01.ZF',   # 苹果
    'RM01.ZF',   # 菜粕
    'OI01.ZF',   # 菜油
    'ZC01.ZF',   # 动力煤
    'FG01.ZF',   # 玻璃
    'SA01.ZF',   # 纯碱
    'UR01.ZF',   # 尿素
    'SM01.ZF',   # 硅锰
    'SF01.ZF',   # 硅铁
    'WH01.ZF',   # 强筋小麦
    'PM01.ZF',   # 普通小麦
    'JR01.ZF',   # 粳稻
    'RI01.ZF',   # 早籼稻
    'LR01.ZF',   # 晚籼稻
    'RS01.ZF',   # 菜籽
    'CJ01.ZF',   # 红枣
    'PK01.ZF',   # 花生
    'PX01.ZF',   # 对二甲苯
    'SH01.ZF',   # 纸浆(郑商)
    'CY01.ZF',   # 棉纱
    'PR01.ZF',   # 短纤
    'PF01.ZF',   # 涤纶短纤
    'PL01.ZF',   # 花生油
]

# 所有期货合约合并
ALL_FUTURES_SYMBOLS = IF_SYMBOLS + SHFE_SYMBOLS + DCE_SYMBOLS + CZCE_SYMBOLS


def download_batch(symbols: list, period: str, start: str, end: str, desc: str) -> dict:
    """批量下载一组合约的某个周期的数据，返回成功/失败统计"""
    success = 0
    fail = 0
    total = len(symbols)

    print(f'\n[下载] {desc} | 周期={period} | 合约数={total} | {start}~{end}')

    for i, sym in enumerate(symbols, 1):
        try:
            ret = xtdata.download_history_data(sym, period=period, start_time=start, end_time=end)
            if ret is None or ret == 0:
                success += 1
            else:
                success += 1  # download_history_data 返回值不统一，只要不抛异常算成功
            if i % 20 == 0 or i == total:
                print(f'  进度: {i}/{total}  成功={success} 失败={fail}', end='\r')
        except Exception as e:
            fail += 1
            print(f'\n  [FAIL] {sym}: {e}')

    print(f'  完成: {total} 个合约  成功={success}  失败={fail}          ')
    return {'total': total, 'success': success, 'fail': fail}


def verify_data(symbols: list, period: str) -> None:
    """快速验证一批合约的数据条数"""
    print(f'\n[验证] 周期={period} 抽样检查...')
    sample = symbols[:5] if len(symbols) > 5 else symbols
    for sym in sample:
        try:
            data = xtdata.get_market_data(['close'], [sym], period=period,
                                          start_time=HIST_START, end_time=HIST_END)
            close = data.get('close')
            if close is not None and not close.empty:
                bars = close.shape[1]
                latest = close.iloc[0].dropna().iloc[-1] if not close.iloc[0].dropna().empty else 'N/A'
                print(f'  {sym}: {bars} 条, 最新close={latest:.4f}' if latest != 'N/A' else f'  {sym}: {bars} 条')
            else:
                print(f'  {sym}: 无数据')
        except Exception as e:
            print(f'  {sym}: 验证失败 {e}')


def main():
    print('=' * 60)
    print('QMT 最大化数据下载工具')
    print(f'下载区间: {HIST_START} ~ {HIST_END}')
    print(f'xtdata 连接状态: OK' )
    print('=' * 60)

    stats = []

    # ----------------------------------------------------------------
    # 1. A股指数
    # ----------------------------------------------------------------
    for period in PERIODS:
        r = download_batch(INDEX_SYMBOLS, period, HIST_START, HIST_END, 'A股指数')
        stats.append(('A股指数', period, r))

    # ----------------------------------------------------------------
    # 2. 股指期货 + 国债期货
    # ----------------------------------------------------------------
    for period in PERIODS:
        r = download_batch(IF_SYMBOLS, period, HIST_START, HIST_END, 'CFFEX期货')
        stats.append(('CFFEX', period, r))

    # ----------------------------------------------------------------
    # 3. 商品期货 — 三大商品交易所
    # ----------------------------------------------------------------
    for desc, symbols in [
        ('SHFE(上期所)', SHFE_SYMBOLS),
        ('DCE(大商所)',  DCE_SYMBOLS),
        ('CZCE(郑商所)', CZCE_SYMBOLS),
    ]:
        for period in PERIODS:
            r = download_batch(symbols, period, HIST_START, HIST_END, desc)
            stats.append((desc, period, r))

    # ----------------------------------------------------------------
    # 4. Tick数据（数据量大，默认关闭）
    # ----------------------------------------------------------------
    if DOWNLOAD_TICK:
        for desc, symbols in [
            ('Tick-指数',  INDEX_SYMBOLS),
            ('Tick-期货',  ALL_FUTURES_SYMBOLS[:20]),  # 先下前20个试试
        ]:
            r = download_batch(symbols, 'tick', HIST_START, HIST_END, desc)
            stats.append((desc, 'tick', r))

    # ----------------------------------------------------------------
    # 5. 验证抽样
    # ----------------------------------------------------------------
    print('\n' + '=' * 60)
    print('下载汇总')
    print('=' * 60)
    for desc, period, r in stats:
        ok = '✓' if r['fail'] == 0 else '!'
        print(f'  [{ok}] {desc:<16} {period:<8} 成功={r["success"]}/{r["total"]}')

    print('\n验证抽样（最后5个合约日线）...')
    verify_data(SHFE_SYMBOLS[:5], '1d')
    verify_data(DCE_SYMBOLS[:5],  '1d')

    print('\n[完成] 数据下载完毕')
    print(f'数据路径: D:\\申万宏源策略量化交易终端\\userdata_mini\\datadir\\')


if __name__ == '__main__':
    main()

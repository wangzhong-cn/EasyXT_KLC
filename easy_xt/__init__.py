"""
EasyXT - xtquant的简化API封装
让用户更方便快捷地调用xtquant功能
"""

import threading

__version__ = "1.0.0"
__author__ = "CodeBuddy"

# 启动横幅：仅在设置环境变量 EASYXT_SHOW_BANNER=1 时输出，避免生产日志污染
import os as _os
if _os.environ.get("EASYXT_SHOW_BANNER", "0") == "1":
    print("作者微信: www_ptqmt_com")
    print("欢迎关注微信公众号: 王者quant")
del _os

# ── 线程安全锁 ──
# xtquant C 扩展存在全局状态，多线程并发初始化 / 调用会导致 segfault。
# _xt_init_lock 保护所有 singleton 工厂和 xtquant 延迟导入路径。
# 使用 RLock (可重入锁) 以允许同一线程在持有锁时调用需要锁的函数。
_xt_init_lock = threading.RLock()

# 延迟导入避免循环依赖
def _get_api():
    from .api import EasyXT
    return EasyXT()

def _get_extended_api():
    from .extended_api import ExtendedAPI
    return ExtendedAPI()

def _get_advanced_api():
    from .advanced_trade_api import AdvancedTradeAPI
    return AdvancedTradeAPI()

def _get_xtquant_broker():
    from .xtquant_broker import get_xtquant_broker as _get
    return _get()

# 创建全局实例
api = None
extended_api = None
advanced_api = None
xtquant_broker = None

def get_api():
    """获取全局API实例（线程安全）"""
    global api
    if api is not None:
        return api
    with _xt_init_lock:
        if api is None:
            api = _get_api()
        return api

def get_extended_api():
    """获取扩展API实例（线程安全，包含完整的trader功能）"""
    global extended_api
    if extended_api is not None:
        return extended_api
    with _xt_init_lock:
        if extended_api is None:
            extended_api = _get_extended_api()
        return extended_api

def get_advanced_api():
    """获取高级交易API实例（线程安全）"""
    global advanced_api
    if advanced_api is not None:
        return advanced_api
    with _xt_init_lock:
        if advanced_api is None:
            advanced_api = _get_advanced_api()
        return advanced_api

def get_xtquant_broker():
    """获取xtquant串行访问代理（线程安全）"""
    global xtquant_broker
    if xtquant_broker is not None:
        return xtquant_broker
    with _xt_init_lock:
        if xtquant_broker is None:
            xtquant_broker = _get_xtquant_broker()
        return xtquant_broker

# 为了向后兼容，在模块级别提供类的导入
def __getattr__(name):
    if name == 'EasyXT':
        from .api import EasyXT
        return EasyXT
    elif name == 'ExtendedAPI':
        from .extended_api import ExtendedAPI
        return ExtendedAPI
    elif name == 'AdvancedTradeAPI':
        from .advanced_trade_api import AdvancedTradeAPI
        return AdvancedTradeAPI
    elif name == 'DataAPI':
        from .data_api import DataAPI
        return DataAPI
    elif name == 'TradeAPI':
        from .trade_api import TradeAPI
        return TradeAPI
    elif name == 'api':
        return get_api()
    elif name == 'extended_api':
        return get_extended_api()
    elif name == 'advanced_api':
        return get_advanced_api()
    elif name == 'xtquant_broker':
        return get_xtquant_broker()
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# 导出主要接口
__all__ = [
    'get_api',
    'get_extended_api',
    'get_advanced_api',
    'get_xtquant_broker',
    'api',
    'extended_api',
    'advanced_api',
    'xtquant_broker'
]

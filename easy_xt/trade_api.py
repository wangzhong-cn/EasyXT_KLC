"""
交易API封装模块
简化xtquant交易接口的调用
"""

import logging
import os
import sys
import time
from threading import Event
from typing import Any, Optional, Tuple, Dict, List
from dataclasses import dataclass

@dataclass
class OrderResponse:
    order_id: Optional[int]
    status: str  # "submitted", "rejected_risk", "rejected_broker", "error"
    msg: str     # Details

    def __bool__(self):
        return self.order_id is not None and self.order_id > 0

log = logging.getLogger(__name__)

import pandas as pd

from .config import config

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
xtquant_path = os.path.join(project_root, "xtquant")


def _find_qmt_python_root(root: str) -> Optional[str]:
    if not root or not os.path.isdir(root):
        return None
    if os.path.basename(root).lower() == "xtquant":
        if os.path.exists(os.path.join(root, "__init__.py")):
            return os.path.dirname(root)
    root_depth = root.rstrip(os.sep).count(os.sep)
    for dirpath, dirnames, filenames in os.walk(root):
        if "xtpythonclient.pyd" in filenames or "xtpythonclient.dll" in filenames:
            return dirpath
        if "xtquant" in dirnames:
            xtquant_dir = os.path.join(dirpath, "xtquant")
            if os.path.exists(os.path.join(xtquant_dir, "__init__.py")):
                return dirpath
        if dirpath.count(os.sep) - root_depth >= 6:
            dirnames[:] = []
    return None


def _ensure_xtquant_paths() -> Optional[str]:
    candidates = []
    for key in ("settings.account.qmt_path", "trade.userdata_path", "qmt.detected_path"):
        value = config.get(key)
        if value:
            base = value
            if str(value).lower().endswith(".exe"):
                base = os.path.dirname(os.path.dirname(value))
            if "userdata" in value.lower():
                base = os.path.dirname(value)
            candidates.extend(
                [
                    base,
                    os.path.dirname(base),
                    os.path.join(base, "bin"),
                    os.path.join(base, "bin.x64"),
                    os.path.join(base, "python"),
                    os.path.join(base, "python", "Lib", "site-packages"),
                    os.path.join(base, "Lib", "site-packages"),
                    os.path.join(base, "lib"),
                    os.path.join(base, "lib", "site-packages"),
                    os.path.join(base, "xtquant"),
                ]
            )
    found_root = None
    found_xtquant_dir = None
    for path in candidates:
        found = _find_qmt_python_root(path)
        if found:
            found_root = found
            candidate_xtquant_dir = os.path.join(found, "xtquant")
            if os.path.isdir(candidate_xtquant_dir):
                found_xtquant_dir = candidate_xtquant_dir
            break
    if xtquant_path not in sys.path:
        sys.path.insert(0, xtquant_path)
    if found_root and found_root not in sys.path:
        sys.path.insert(1, found_root)
    return found_xtquant_dir


_qmt_xtquant_dir = None
_xtquant_pkg = None
_initialized = False
_ensure_lock = __import__("threading").Lock()


def _ensure_xtquant():
    global _qmt_xtquant_dir, _xtquant_pkg, _initialized
    if _initialized:
        return
    with _ensure_lock:
        if _initialized:
            return
        _initialized = True

    from .config import config

    candidates = []
    for key in ("settings.account.qmt_path", "trade.userdata_path", "qmt.detected_path"):
        value = config.get(key)
        if value:
            base = value
            if str(value).lower().endswith(".exe"):
                base = os.path.dirname(os.path.dirname(value))
            if "userdata" in value.lower():
                base = os.path.dirname(value)
            candidates.extend(
                [
                    base,
                    os.path.dirname(base),
                    os.path.join(base, "bin"),
                    os.path.join(base, "bin.x64"),
                    os.path.join(base, "python"),
                    os.path.join(base, "python", "Lib", "site-packages"),
                    os.path.join(base, "Lib", "site-packages"),
                    os.path.join(base, "lib"),
                    os.path.join(base, "lib", "site-packages"),
                    os.path.join(base, "xtquant"),
                ]
            )
    found_root = None
    found_xtquant_dir = None
    for path in candidates:
        found = _find_qmt_python_root(path)
        if found:
            found_root = found
            candidate_xtquant_dir = os.path.join(found, "xtquant")
            if os.path.isdir(candidate_xtquant_dir):
                found_xtquant_dir = candidate_xtquant_dir
            break
    if xtquant_path not in sys.path:
        sys.path.insert(0, xtquant_path)
    if found_root and found_root not in sys.path:
        sys.path.insert(1, found_root)
    _qmt_xtquant_dir = found_xtquant_dir

    try:
        import xtquant as _xtquant_pkg

        if _qmt_xtquant_dir and _qmt_xtquant_dir not in _xtquant_pkg.__path__:
            _xtquant_pkg.__path__ = list(_xtquant_pkg.__path__) + [_qmt_xtquant_dir]
    except ImportError:
        pass


xt_trader: Any = None
xt_type: Any = None
xt_const: Any = None
xtdata: Any = None
_xt_import_lock = __import__("threading").Lock()


def _lazy_import_xtquant():
    global xt_trader, xt_type, xt_const, xtdata
    if xt_trader is not None:
        return
    with _xt_import_lock:
        if xt_trader is not None:
            return
        try:
            _ensure_xtquant()
            import xtquant.xtconstant as xt_const
            import xtquant.xttrader as xt_trader
            import xtquant.xttype as xt_type
            from xtquant import xtdata

            log.info("xtquant.xttrader 导入成功")
        except ImportError as e:
            log.warning("xtquant.xttrader 导入失败: %s", e)
            log.warning("交易服务未连接")


from .utils import ErrorHandler, StockCodeUtils


class SimpleCallback:
    """简化的交易回调类"""

    def __init__(self):
        self.connected = False
        self.orders = {}
        self.trades = {}
        self.positions = {}
        self.assets = {}
        self.errors = []

        # 事件通知
        self.order_event = Event()
        self.trade_event = Event()

    def on_connected(self):
        """连接成功"""
        self.connected = True
        log.info("交易连接成功")

    def on_disconnected(self):
        """连接断开"""
        self.connected = False
        log.warning("交易连接断开")

    def on_stock_order(self, order):
        """委托回调"""
        self.orders[order.order_id] = order
        self.order_event.set()

    def on_stock_trade(self, trade):
        """成交回调"""
        self.trades[trade.traded_id] = trade
        self.trade_event.set()

    def on_stock_position(self, position):
        """持仓回调"""
        key = f"{position.account_id}_{position.stock_code}"
        self.positions[key] = position

    def on_stock_asset(self, asset):
        """资产回调"""
        self.assets[asset.account_id] = asset

    def on_order_error(self, order_error):
        """委托错误回调"""
        self.errors.append(order_error)
        log.error("委托错误: %s", order_error.error_msg)


class TradeAPI:
    """交易API封装类"""

    def __init__(self):
        self.trader = None
        self.callback = None
        self.accounts = {}
        self._session_id = config.get("trade.session_id", "default")
        # 风控引擎（可选）：通过 attach_risk_engine() 注入
        self._risk_engine: Optional[Any] = None
        # 审计链路（可选）：通过 attach_audit_trail() 注入
        self._audit_trail: Optional[Any] = None

    def attach_risk_engine(self, risk_engine: Any) -> None:
        """
        注入风控引擎（RiskEngine 实例）。
        注入后，buy()/sell() 每次下单前都会执行预交易风控检查。
        若检查结果为 LIMIT 或 HALT，则拒绝下单并返回 None。

        示例::

            from core.risk_engine import RiskEngine
            risk = RiskEngine()
            api.trade.attach_risk_engine(risk)
        """
        self._risk_engine = risk_engine
        log.info("TradeAPI: 风控引擎已注入 [%s]", type(risk_engine).__name__)

    def attach_audit_trail(self, audit_trail: Any) -> None:
        """
        注入审计链路（AuditTrail 实例）。
        注入后，buy()/sell() 成功下单后会自动写入 audit_orders。
        """
        self._audit_trail = audit_trail
        log.info("TradeAPI: 审计链路已注入 [%s]", type(audit_trail).__name__)

    def _get_nav_and_positions(self, account_id: str) -> tuple[float, dict]:
        """获取当前账户净值和持仓市值（用于风控计算），失败时返回安全默认值。"""
        try:
            asset = self.get_account_asset(account_id)
            nav = float(asset.get("total_asset", 0)) if asset else 0.0
            pos_df = self.get_positions(account_id)
            positions: dict = {}
            if pos_df is not None and not pos_df.empty and "code" in pos_df.columns:
                for _, row in pos_df.iterrows():
                    code = str(row.get("code", ""))
                    mv = float(row.get("market_value", 0) or 0)
                    if code:
                        positions[code] = mv
            return nav, positions
        except Exception as e:
            log.warning("TradeAPI._get_nav_and_positions 获取失败，跳过风控: %s", e)
            return 0.0, {}

    def _run_risk_check(
        self,
        account_id: str,
        code: str,
        volume: int,
        price: float,
        direction: str,
    ) -> Tuple[bool, str]:
        """
        执行预交易风控检查。返回 (是否放行, 拦截/警告原因)。

        若未注入 RiskEngine 则直接放行（向后兼容）。
        若 nav <= 0（无法获取账户信息），以警告日志放行，避免误阻断。
        """
        if self._risk_engine is None:
            return True, ""  # 未配置风控，直接放行

        nav, positions = self._get_nav_and_positions(account_id)

        if nav <= 0:
            log.warning(
                "TradeAPI 风控: 账户净值为 0（获取失败），跳过风控直接放行 %s %s",
                direction, code,
            )
            return True, "WARN: 获取资产失败直接放行"

        try:
            result = self._risk_engine.check_pre_trade(
                account_id=account_id,
                code=code,
                volume=float(volume),
                price=float(price),
                direction=direction,
                positions=positions,
                nav=nav,
            )
        except Exception as e:
            log.error("TradeAPI 风控检查异常，放行下单: %s", e)
            return True, f"ERROR_PASS: {e}"  # 异常不应阻断交易

        if result.action.value == "halt":
            log.error(
                "TradeAPI 风控 HALT 拒单: %s %s x%d @ %.4f | 原因: %s | 指标: %s",
                direction, code, volume, price, result.reason, result.metrics,
            )
            return False, f"RISK_REJECT: {result.action.value} - {result.reason}"

        if result.action.value == "limit":
            if direction == "sell":
                log.warning(
                    "TradeAPI 风控: LIMIT 触发，但卖出视为减仓降险，放行: %s %s x%d @ %.4f | 原因: %s",
                    direction, code, volume, price, result.reason,
                )
                return True, f"RISK_WARN: LIMIT for sell allowed - {result.reason}"
            else:
                log.error(
                    "TradeAPI 风控 LIMIT 拒单: %s %s x%d @ %.4f | 原因: %s | 指标: %s",
                    direction, code, volume, price, result.reason, result.metrics,
                )
                return False, f"RISK_REJECT: {result.action.value} - {result.reason}"

        if result.action.value == "warn":
            log.warning(
                "TradeAPI 风控 WARN（放行）: %s %s x%d @ %.4f | 原因: %s | 指标: %s",
                direction, code, volume, price, result.reason, result.metrics,
            )
            return True, f"RISK_WARN: {result.reason}"

        return True, ""  # PASS

    def connect(self, userdata_path: str, session_id: Optional[str] = None) -> bool:
        """
        连接交易服务

        Args:
            userdata_path: 迅投客户端userdata路径
            session_id: 会话ID

        Returns:
            bool: 是否连接成功
        """
        try:
            _lazy_import_xtquant()
            if not xt_trader:
                ErrorHandler.log_error("xtquant交易模块未正确导入")
                return False

            if not userdata_path:
                ErrorHandler.log_error("userdata_path不能为空")
                return False

            if session_id:
                self._session_id = session_id

            # 处理路径编码问题
            try:
                # 确保路径是字符串格式，处理中文路径
                if isinstance(userdata_path, bytes):
                    userdata_path = userdata_path.decode("utf-8")

                # 规范化路径
                userdata_path = os.path.normpath(userdata_path)

                # 检查路径是否存在
                if not os.path.exists(userdata_path):
                    ErrorHandler.log_error(f"userdata路径不存在: {userdata_path}")
                    return False

            except Exception as path_error:
                ErrorHandler.log_error(f"路径处理失败: {str(path_error)}")
                return False

            # 创建回调对象
            self.callback = SimpleCallback()

            # 创建交易对象 - 修复session_id类型问题
            try:
                # 根据错误信息，XtQuantAsyncClient需要的第三个参数是int类型
                # 尝试将session_id转换为数字，如果失败则使用默认值
                try:
                    # 使用时间戳作为session_id以确保唯一性
                    session_int = int(time.time() * 1000) % 1000000
                except (ValueError, TypeError, OverflowError):
                    session_int = 123456  # 默认session ID

                log.debug("使用session_id: %d", session_int)

                # 创建交易对象，使用数字类型的session_id
                self.trader = xt_trader.XtQuantTrader(userdata_path, session_int)
                # 注册回调
                self.trader.register_callback(self.callback)
            except Exception as create_error:
                error_text = str(create_error)
                if "xtpythonclient" in error_text.lower():
                    ErrorHandler.log_error(
                        "未检测到xtpythonclient组件，请在QMT终端启用本地Python组件，并确认安装目录下存在xtpythonclient.pyd或xtpythonclient.dll"
                    )
                ErrorHandler.log_error(f"创建交易对象失败: {error_text}")
                return False

            # 启动交易
            log.info("启动交易服务...")
            self.trader.start()

            # 连接
            log.info("连接交易服务...")
            result = self.trader.connect()
            if result == 0:
                log.info("交易服务连接成功")
                return True
            else:
                ErrorHandler.log_error(f"交易服务连接失败，错误码: {result}")
                return False

        except Exception as e:
            ErrorHandler.log_error(f"连接交易服务失败: {str(e)}")
            return False

    def add_account(self, account_id: str, account_type: str = "STOCK") -> bool:
        """
        添加交易账户

        Args:
            account_id: 资金账号
            account_type: 账户类型，'STOCK'股票, 'CREDIT'信用

        Returns:
            bool: 是否成功
        """
        if not self.trader:
            ErrorHandler.log_error("交易服务未连接")
            return False

        try:

            def normalize_account_type(value):
                if value is None:
                    return None
                if isinstance(value, int):
                    if xt_const and hasattr(xt_const, "ACCOUNT_TYPE_DICT"):
                        mapped = xt_const.ACCOUNT_TYPE_DICT.get(value)
                        if mapped:
                            return mapped
                    return str(value)
                return str(value).upper()

            account_id_str = str(account_id) if account_id is not None else ""
            account_type_str = normalize_account_type(account_type)
            log.info("添加账户: %s", account_id_str)
            account = xt_type.StockAccount(account_id_str, account_type_str)
            if isinstance(account, str):  # 错误信息
                ErrorHandler.log_error(account)
                return False

            # 订阅账户
            log.info("订阅账户...")
            result = self.trader.subscribe(account)
            if result == 0:
                self.accounts[account_id_str] = account
                log.info("账户 %s 添加成功", account_id_str)
                return True
            else:
                try:
                    if hasattr(self.trader, "query_account_infos"):
                        infos = self.trader.query_account_infos()
                        if infos:
                            match_info = None
                            for info in infos:
                                info_id = getattr(info, "account_id", None)
                                if info_id is not None and str(info_id) == account_id_str:
                                    match_info = info
                                    break
                            if match_info is not None:
                                detected_type = normalize_account_type(
                                    getattr(match_info, "account_type", None)
                                )
                                if detected_type and detected_type != account_type_str:
                                    retry_account = xt_type.StockAccount(
                                        account_id_str, detected_type
                                    )
                                    retry_result = self.trader.subscribe(retry_account)
                                    if retry_result == 0:
                                        self.accounts[account_id_str] = retry_account
                                        log.info("账户 %s 添加成功", account_id_str)
                                        return True
                                ErrorHandler.log_error(
                                    f"订阅账户失败，账户类型可能不匹配: {detected_type}"
                                )
                            else:
                                available_ids = [
                                    str(getattr(info, "account_id", "")) for info in infos
                                ]
                                ErrorHandler.log_error(f"账户不存在于已登录列表: {available_ids}")
                except Exception as query_error:
                    ErrorHandler.log_error(f"查询账户列表失败: {str(query_error)}")
                ErrorHandler.log_error(f"订阅账户失败，错误码: {result}")
                return False

        except Exception as e:
            ErrorHandler.log_error(f"添加账户失败: {str(e)}")
            return False

    @ErrorHandler.handle_api_error
    def buy(
        self, account_id: str, code: str, volume: int, price: float = 0, price_type: str = "market", signal_id: str = ""
    ) -> OrderResponse:
        """
        买入股票（含风控前检和结构化拒单+审计写入）。
        """
        import uuid
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return OrderResponse(None, "error", "未连接或未添加账户")

        account = self.accounts[account_id]
        code = StockCodeUtils.normalize_code(code)

        # ── 预交易风控检查 ──────────────────────────────────────────────
        audit_trail = getattr(self, "_audit_trail", None)
        passed, msg = self._run_risk_check(account_id, code, volume, price, "buy")
        if not passed:
            if audit_trail is not None:
                audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "buy", volume, price, "rejected_risk")
            return OrderResponse(None, "rejected_risk", msg)

        # 价格类型映射
        price_type_map = {
            "market": xt_const.MARKET_PEER_PRICE_FIRST,  # 对手价
            "limit": xt_const.FIX_PRICE,  # 限价
            "市价": xt_const.MARKET_PEER_PRICE_FIRST,
            "限价": xt_const.FIX_PRICE,
        }

        xt_price_type = price_type_map.get(price_type, xt_const.MARKET_PEER_PRICE_FIRST)

        try:
            log.info("买入 %s, 数量: %d, 价格: %s, 类型: %s", code, volume, price, price_type)
            order_id = self.trader.order_stock(
                account=account,
                stock_code=code,
                order_type=xt_const.STOCK_BUY,
                order_volume=volume,
                price_type=xt_price_type,
                price=price,
                strategy_name="EasyXT",
                order_remark=f"买入{code}",
            )

            if order_id > 0:
                log.info("买入委托成功: %s, 数量: %d, 委托号: %d", code, volume, order_id)
                if audit_trail is not None:
                    audit_trail.record_order(str(order_id), signal_id, code, "buy", volume, price, "submitted")
                return OrderResponse(order_id, "submitted", "")
            else:
                ErrorHandler.log_error(f"买入委托失败，返回值: {order_id}")
                if audit_trail is not None:
                    audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "buy", volume, price, "rejected_broker")
                return OrderResponse(None, "rejected_broker", f"Broker Reject Code: {order_id}")

        except Exception as e:
            ErrorHandler.log_error(f"买入操作失败: {str(e)}")
            if audit_trail is not None:
                audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "buy", volume, price, "error")
            return OrderResponse(None, "error", str(e))

    @ErrorHandler.handle_api_error
    def sell(
        self, account_id: str, code: str, volume: int, price: float = 0, price_type: str = "market", signal_id: str = ""
    ) -> OrderResponse:
        """
        卖出股票（含风控前检和结构化拒单+审计写入）。
        """
        import uuid
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return OrderResponse(None, "error", "未连接或未添加账户")

        account = self.accounts[account_id]
        code = StockCodeUtils.normalize_code(code)

        # ── 预交易风控检查 ──────────────────────────────────────────────
        audit_trail = getattr(self, "_audit_trail", None)
        passed, msg = self._run_risk_check(account_id, code, volume, price, "sell")
        if not passed:
            if audit_trail is not None:
                audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "sell", volume, price, "rejected_risk")
            return OrderResponse(None, "rejected_risk", msg)

        # 价格类型映射
        price_type_map = {
            "market": xt_const.MARKET_PEER_PRICE_FIRST,
            "limit": xt_const.FIX_PRICE,
            "市价": xt_const.MARKET_PEER_PRICE_FIRST,
            "限价": xt_const.FIX_PRICE,
        }

        xt_price_type = price_type_map.get(price_type, xt_const.MARKET_PEER_PRICE_FIRST)

        try:
            log.info("卖出 %s, 数量: %d, 价格: %s, 类型: %s", code, volume, price, price_type)
            order_id = self.trader.order_stock(
                account=account,
                stock_code=code,
                order_type=xt_const.STOCK_SELL,
                order_volume=volume,
                price_type=xt_price_type,
                price=price,
                strategy_name="EasyXT",
                order_remark=f"卖出{code}",
            )

            if order_id > 0:
                log.info("卖出委托成功: %s, 数量: %d, 委托号: %d", code, volume, order_id)
                if audit_trail is not None:
                    audit_trail.record_order(str(order_id), signal_id, code, "sell", volume, price, "submitted")
                return OrderResponse(order_id, "submitted", "")
            else:
                ErrorHandler.log_error(f"卖出委托失败，返回值: {order_id}")
                if audit_trail is not None:
                    audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "sell", volume, price, "rejected_broker")
                return OrderResponse(None, "rejected_broker", f"Broker Reject Code: {order_id}")

        except Exception as e:
            ErrorHandler.log_error(f"卖出操作失败: {str(e)}")
            if audit_trail is not None:
                audit_trail.record_order(str(uuid.uuid4()), signal_id, code, "sell", volume, price, "error")
            return OrderResponse(None, "error", str(e))

    @ErrorHandler.handle_api_error
    def cancel_order(self, account_id: str, order_id: int) -> bool:
        """
        撤销委托

        Args:
            account_id: 资金账号
            order_id: 委托编号

        Returns:
            bool: 是否成功
        """
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return False

        account = self.accounts[account_id]

        try:
            result = self.trader.cancel_order_stock(account, order_id)
            if result == 0:
                log.info("撤单成功: %d", order_id)
                return True
            else:
                ErrorHandler.log_error(f"撤单失败，错误码: {result}")
                return False

        except Exception as e:
            ErrorHandler.log_error(f"撤单操作失败: {str(e)}")
            return False

    @ErrorHandler.handle_api_error
    def get_account_asset(self, account_id: str) -> Optional[dict[str, Any]]:
        """
        获取账户资产

        Args:
            account_id: 资金账号

        Returns:
            Optional[Dict]: 资产信息
        """
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return None

        account = self.accounts[account_id]

        try:
            asset = self.trader.query_stock_asset(account)
            if asset:
                return {
                    "account_id": asset.account_id,
                    "cash": asset.cash,  # 可用资金
                    "frozen_cash": asset.frozen_cash,  # 冻结资金
                    "market_value": asset.market_value,  # 持仓市值
                    "total_asset": asset.total_asset,  # 总资产
                }
            return None

        except Exception as e:
            ErrorHandler.log_error(f"获取账户资产失败: {str(e)}")
            return None

    @ErrorHandler.handle_api_error
    def get_positions(self, account_id: str, code: Optional[str] = None) -> pd.DataFrame:
        """
        获取持仓信息

        Args:
            account_id: 资金账号
            code: 股票代码，为空则获取所有持仓

        Returns:
            DataFrame: 持仓信息
        """
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return pd.DataFrame()

        account = self.accounts[account_id]

        try:
            if code:
                # 获取单只股票持仓
                code = StockCodeUtils.normalize_code(code)
                position = self.trader.query_stock_position(account, code)
                if position:
                    return pd.DataFrame(
                        [
                            {
                                "code": position.stock_code,
                                "volume": position.volume,
                                "can_use_volume": position.can_use_volume,
                                "open_price": position.open_price,
                                "market_value": position.market_value,
                                "frozen_volume": position.frozen_volume,
                            }
                        ]
                    )
                else:
                    return pd.DataFrame()
            else:
                # 获取所有持仓
                positions = self.trader.query_stock_positions(account)
                if positions:
                    data = []
                    for pos in positions:
                        data.append(
                            {
                                "code": pos.stock_code,
                                "volume": pos.volume,
                                "can_use_volume": pos.can_use_volume,
                                "open_price": pos.open_price,
                                "market_value": pos.market_value,
                                "frozen_volume": pos.frozen_volume,
                            }
                        )
                    return pd.DataFrame(data)
                else:
                    return pd.DataFrame()

        except Exception as e:
            ErrorHandler.log_error(f"获取持仓信息失败: {str(e)}")
            return pd.DataFrame()

    @ErrorHandler.handle_api_error
    def get_orders(self, account_id: str, cancelable_only: bool = False) -> pd.DataFrame:
        """
        获取委托信息

        Args:
            account_id: 资金账号
            cancelable_only: 是否只获取可撤销委托

        Returns:
            DataFrame: 委托信息
        """
        if not self.trader or account_id not in self.accounts:
            ErrorHandler.log_error("交易服务未连接或账户未添加")
            return pd.DataFrame()

        account = self.accounts[account_id]

        try:
            orders = self.trader.query_stock_orders(account, cancelable_only)
            if orders:
                data = []
                for order in orders:
                    # 委托类型转换
                    order_type_name = "买入" if order.order_type == xt_const.STOCK_BUY else "卖出"

                    # 委托状态转换
                    status_map = {
                        xt_const.ORDER_UNREPORTED: "未报",
                        xt_const.ORDER_WAIT_REPORTING: "待报",
                        xt_const.ORDER_REPORTED: "已报",
                        xt_const.ORDER_PART_SUCC: "部成",
                        xt_const.ORDER_SUCCEEDED: "已成",
                        xt_const.ORDER_PART_CANCEL: "部撤",
                        xt_const.ORDER_CANCELED: "已撤",
                        xt_const.ORDER_JUNK: "废单",
                    }
                    status_name = status_map.get(order.order_status, "未知")

                    data.append(
                        {
                            "order_id": order.order_id,
                            "code": order.stock_code,
                            "order_type": order_type_name,
                            "volume": order.order_volume,
                            "price": order.price,
                            "traded_volume": order.traded_volume,
                            "status": status_name,
                            "order_time": order.order_time,
                            "remark": order.order_remark,
                        }
                    )
                return pd.DataFrame(data)
            else:
                return pd.DataFrame()

        except Exception as e:
            ErrorHandler.log_error(f"获取委托信息失败: {str(e)}")
            return pd.DataFrame()

    def get_trades(self, account_id: str, timeout: int = 5) -> pd.DataFrame:
        """
        获取成交信息 - 修复版本，解决QMT API查询问题

        Args:
            account_id: 资金账号
            timeout: 超时时间（秒），默认5秒

        Returns:
            DataFrame: 成交信息
        """
        if not self.trader or account_id not in self.accounts:
            log.error("交易服务未连接或账户未添加")
            return pd.DataFrame()

        account = self.accounts[account_id]

        log.info("正在查询成交信息...")

        try:
            # 方法1：直接查询成交
            log.debug("尝试方法1：直接查询成交...")
            trades = self.trader.query_stock_trades(account)

            if trades and len(trades) > 0:
                log.info("直接查询成功，找到 %d 条成交记录", len(trades))
                return self._process_trades_data(trades)
            else:
                log.warning("直接查询无成交记录")

            # 方法2：从委托信息推断成交
            log.debug("尝试方法2：从委托信息推断成交...")
            trades_from_orders = self.get_trades_from_orders(account_id)
            if not trades_from_orders.empty:
                log.info("从委托推断成功，找到 %d 条成交记录", len(trades_from_orders))
                return trades_from_orders

            # 方法3：使用回调中的成交信息
            log.debug("尝试方法3：使用回调成交信息...")
            if self.callback and self.callback.trades:
                callback_trades = list(self.callback.trades.values())
                if callback_trades:
                    log.info("回调查询成功，找到 %d 条成交记录", len(callback_trades))
                    return self._process_trades_data(callback_trades)

            log.info("所有方法均未找到成交记录")
            return pd.DataFrame()

        except Exception as e:
            log.error("成交查询异常: %s", e)
            # 异常时也尝试从委托推断
            try:
                return self.get_trades_from_orders(account_id)
            except Exception as _inner_err:
                log.warning("成交查询二次推断也失败: %s", _inner_err)
                return pd.DataFrame()

    def _process_trades_data(self, trades) -> pd.DataFrame:
        """处理成交数据"""
        if not trades:
            return pd.DataFrame()

        log.debug("正在处理成交数据...")
        data = []

        for trade in trades:
            # 委托类型转换
            order_type_name = "买入" if trade.order_type == xt_const.STOCK_BUY else "卖出"

            data.append(
                {
                    "code": trade.stock_code,
                    "order_type": order_type_name,
                    "volume": trade.traded_volume,
                    "price": trade.traded_price,
                    "amount": trade.traded_amount,
                    "time": trade.traded_time,
                    "order_id": trade.order_id,
                    "trade_id": trade.traded_id,
                    "strategy_name": getattr(trade, "strategy_name", ""),
                    "remark": getattr(trade, "order_remark", ""),
                }
            )

        result_df = pd.DataFrame(data)
        log.info("成交数据处理完成，共 %d 条记录", len(result_df))
        return result_df

    def get_trades_from_orders(self, account_id: str) -> pd.DataFrame:
        """
        从委托信息推断成交情况（备用方案）

        Args:
            account_id: 资金账号

        Returns:
            DataFrame: 推断的成交信息
        """
        log.info("使用备用方案：从委托信息推断成交...")

        orders_df = self.get_orders(account_id)
        if orders_df.empty:
            log.info("无委托信息，无法推断成交")
            return pd.DataFrame()

        # 筛选已成交的委托
        filled_orders = orders_df[orders_df["status"].isin(["已成", "部成"])]

        if filled_orders.empty:
            log.info("无已成交委托")
            return pd.DataFrame()

        # 转换为成交格式
        trades_data = []
        for _, order in filled_orders.iterrows():
            if order["traded_volume"] > 0:
                trades_data.append(
                    {
                        "证券代码": order["code"],
                        "委托类型": order["order_type"],
                        "成交数量": order["traded_volume"],
                        "委托价格": order["price"],
                        "委托时间": order["order_time"],
                        "状态": order["status"],
                        "备注": "从委托推断",
                    }
                )

        if trades_data:
            result_df = pd.DataFrame(trades_data)
            log.info("从委托推断出 %d 条成交记录", len(result_df))
            return result_df
        else:
            log.info("无法从委托推断出成交信息")
            return pd.DataFrame()

    def disconnect(self):
        """断开连接"""
        if self.trader:
            try:
                self.trader.stop()
                log.info("交易服务已断开")
            except Exception as e:
                ErrorHandler.log_error(f"断开交易服务失败: {str(e)}")

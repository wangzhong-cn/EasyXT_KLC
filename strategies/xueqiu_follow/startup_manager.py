#!/usr/bin/env python3
"""
雪球跟单策略启动管理器
"""

import asyncio
import atexit
import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# 添加项目路径
sys.path.append(os.path.dirname(__file__))

from strategies.xueqiu_follow.core.config_manager import ConfigManager
from strategies.xueqiu_follow.core.risk_manager import RiskManager
from strategies.xueqiu_follow.core.strategy_engine import StrategyEngine
from strategies.xueqiu_follow.core.trade_executor import TradeExecutor
from strategies.xueqiu_follow.core.xueqiu_collector import XueqiuCollector


class StartupManager:
    """启动管理器"""

    def __init__(self):
        self.config_manager: Optional[ConfigManager] = None
        self.risk_manager: Optional[RiskManager] = None
        self.xueqiu_collector: Optional[XueqiuCollector] = None
        self.strategy_engine: Optional[StrategyEngine] = None
        self.trade_executor: Optional[TradeExecutor] = None

        self.startup_time = datetime.now()
        self.shutdown_requested = False
        self.components_status = {}

        # 注册信号处理器
        self._register_signal_handlers()

        # 注册退出处理器
        atexit.register(self._cleanup_on_exit)

    def _register_signal_handlers(self):
        """注册信号处理器"""
        if sys.platform != 'win32':
            # Unix系统信号处理
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGHUP, self._signal_handler)
        else:
            # Windows系统信号处理
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        signal_names = {
            signal.SIGINT: 'SIGINT',
            signal.SIGTERM: 'SIGTERM'
        }

        if sys.platform != 'win32' and hasattr(signal, 'SIGHUP'):
            signal_names[signal.SIGHUP] = 'SIGHUP'

        signal_name = signal_names.get(signum, f'Signal {signum}')
        print(f"\n收到信号 {signal_name}，正在优雅关闭系统...")

        self.shutdown_requested = True

        # 创建异步任务来处理关闭
        if hasattr(self, '_shutdown_event'):
            self._shutdown_event.set()

    def _cleanup_on_exit(self):
        """退出时清理"""
        if not self.shutdown_requested:
            print("程序退出，执行清理...")
            self.shutdown_requested = True

    async def startup(self):
        """启动系统"""
        print("🚀 雪球跟单策略启动中...")
        print("=" * 50)
        print(f"启动时间: {self.startup_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Python版本: {sys.version}")
        print(f"工作目录: {os.getcwd()}")
        print()

        try:
            # 创建关闭事件
            self._shutdown_event = asyncio.Event()

            # 1. 预启动检查
            await self._pre_startup_checks()

            # 2. 初始化组件
            await self._initialize_components()

            # 3. 启动组件
            await self._start_components()

            # 4. 启动完成
            await self._startup_complete()

            # 5. 运行主循环
            await self._run_main_loop()

        except KeyboardInterrupt:
            print("\n用户中断，正在关闭系统...")
        except Exception as e:
            print(f"\n启动失败: {e}")
            raise
        finally:
            await self._shutdown()

    async def _pre_startup_checks(self):
        """预启动检查"""
        print("🔍 执行预启动检查...")

        # 检查配置文件
        config_file = Path('config/unified_config.json')
        if not config_file.exists():
            raise Exception("配置文件不存在: config/unified_config.json")

        print("   ✅ 配置文件存在")

        # 检查日志目录
        logs_dir = Path('logs')
        if not logs_dir.exists():
            logs_dir.mkdir(exist_ok=True)
            print("   📁 创建日志目录")
        else:
            print("   ✅ 日志目录存在")

        # 检查核心模块
        core_modules = [
            'core.config_manager',
            'core.risk_manager',
            'core.xueqiu_collector',
            'core.strategy_engine',
            'core.trade_executor'
        ]

        for module in core_modules:
            try:
                __import__(module)
                print(f"   ✅ {module}")
            except ImportError as e:
                raise Exception(f"核心模块导入失败: {module} - {e}")

        print("   ✅ 预启动检查完成")

    async def _initialize_components(self):
        """初始化组件"""
        print("\n⚙️ 初始化系统组件...")

        try:
            # 1. 配置管理器
            print("   初始化配置管理器...")
            self.config_manager = ConfigManager()
            self.components_status['config_manager'] = 'initialized'
            print("   ✅ 配置管理器初始化完成")

            # 2. 风险管理器
            print("   初始化风险管理器...")
            self.risk_manager = RiskManager(self.config_manager)
            self.components_status['risk_manager'] = 'initialized'
            print("   ✅ 风险管理器初始化完成")

            # 3. 雪球采集器
            print("   初始化雪球采集器...")
            self.xueqiu_collector = XueqiuCollector()
            self.components_status['xueqiu_collector'] = 'initialized'
            print("   ✅ 雪球采集器初始化完成")

            # 4. 策略引擎
            print("   初始化策略引擎...")
            self.strategy_engine = StrategyEngine(
                self.config_manager
            )
            self.components_status['strategy_engine'] = 'initialized'
            print("   ✅ 策略引擎初始化完成")

            # 5. 交易执行器
            print("   初始化交易执行器...")
            self.trade_executor = TradeExecutor(self._build_qmt_config())
            self.components_status['trade_executor'] = 'initialized'
            print("   ✅ 交易执行器初始化完成")

            print("   ✅ 所有组件初始化完成")

        except Exception as e:
            print(f"   ❌ 组件初始化失败: {e}")
            raise

    def _build_qmt_config(self) -> dict[str, Any]:
        cfg = self.config_manager
        if cfg is None:
            return {}
        account_id = cfg.get_account_id()
        return {
            "userdata_path": cfg.get_qmt_path(),
            "account_id": account_id if account_id else None,
            "trade_mode": cfg.get_trade_mode(),
            "max_position_ratio": cfg.get_max_position_ratio(),
            "log_level": cfg.get_setting("system.log_level", "INFO"),
            "max_concurrent_orders": cfg.get_setting("trading.max_concurrent_orders", 10),
            "order_timeout": cfg.get_setting("trading.order_timeout", 30),
            "retry_times": cfg.get_setting("trading.retry_times", 3),
            "retry_delay": cfg.get_setting("trading.retry_delay", 1),
        }

    async def _start_components(self):
        """启动组件"""
        print("\n🔄 启动系统组件...")

        try:
            config_manager = self.config_manager
            xueqiu_collector = self.xueqiu_collector
            strategy_engine = self.strategy_engine
            trade_executor = self.trade_executor
            if config_manager is None or xueqiu_collector is None or strategy_engine is None or trade_executor is None:
                raise RuntimeError("组件未初始化")
            # 检查紧急停止状态
            emergency_stop = config_manager.get_global_setting('emergency_stop', False)
            if emergency_stop:
                print("   ⚠️ 系统处于紧急停止状态，跳过组件启动")
                return

            # 启动雪球采集器
            print("   启动雪球采集器...")
            await xueqiu_collector.initialize()
            self.components_status['xueqiu_collector'] = 'running'
            print("   ✅ 雪球采集器启动完成")

            # 启动策略引擎
            print("   启动策略引擎...")
            await strategy_engine.start()
            self.components_status['strategy_engine'] = 'running'
            print("   ✅ 策略引擎启动完成")

            # 启动交易执行器
            print("   启动交易执行器...")
            await trade_executor.start()
            self.components_status['trade_executor'] = 'running'
            print("   ✅ 交易执行器启动完成")

            print("   ✅ 所有组件启动完成")

        except Exception as e:
            print(f"   ❌ 组件启动失败: {e}")
            raise

    async def _startup_complete(self):
        """启动完成"""
        startup_duration = (datetime.now() - self.startup_time).total_seconds()

        print("\n" + "=" * 50)
        print("🎉 系统启动完成！")
        print("=" * 50)
        print(f"启动耗时: {startup_duration:.2f}秒")
        print("系统状态: 运行中")

        # 显示组件状态
        print("\n📊 组件状态:")
        for component, status in self.components_status.items():
            status_icon = "✅" if status == 'running' else "⚙️" if status == 'initialized' else "❌"
            print(f"   {status_icon} {component}: {status}")

        # 显示配置信息
        config_manager = self.config_manager
        if config_manager is None:
            raise RuntimeError("配置管理器未初始化")
        enabled_portfolios = config_manager.get_enabled_portfolios()
        print(f"\n📈 启用组合数: {len(enabled_portfolios)}")

        trade_mode = config_manager.get_setting('trading.trade_mode', 'simulation')
        print(f"🔧 交易模式: {trade_mode}")

        print("\n💡 提示:")
        print("   - 按 Ctrl+C 优雅关闭系统")
        print("   - 查看日志: logs/ 目录")
        print("   - 系统监控: python system_monitor.py")
        print("   - 健康检查: python health_check.py")
        print()

        # 记录启动日志
        self._log_startup_info()

    def _log_startup_info(self):
        """记录启动信息"""
        try:
            config_manager = self.config_manager
            if config_manager is None:
                return
            startup_info = {
                'timestamp': self.startup_time.isoformat(),
                'python_version': sys.version,
                'working_directory': os.getcwd(),
                'components_status': self.components_status,
                'enabled_portfolios': len(config_manager.get_enabled_portfolios()),
                'trade_mode': config_manager.get_setting('trading.trade_mode', 'simulation')
            }

            log_file = Path('logs') / f'startup_{self.startup_time.strftime("%Y%m%d")}.log'

            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%H:%M:%S')}] 系统启动\n")
                f.write(f"启动信息: {json.dumps(startup_info, ensure_ascii=False, indent=2)}\n")
                f.write("-" * 50 + "\n")

        except Exception as e:
            print(f"记录启动日志失败: {e}")

    async def _run_main_loop(self):
        """运行主循环"""
        print("🔄 进入主运行循环...")

        try:
            while not self.shutdown_requested:
                # 等待关闭信号或定期检查
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=30.0)
                    break  # 收到关闭信号
                except asyncio.TimeoutError:
                    # 定期检查系统状态
                    await self._periodic_health_check()

        except Exception as e:
            print(f"主循环异常: {e}")
            raise

    async def _periodic_health_check(self):
        """定期健康检查"""
        try:
            # 检查紧急停止状态
            config_manager = self.config_manager
            if config_manager is None:
                return
            emergency_stop = config_manager.get_global_setting('emergency_stop', False)
            if emergency_stop:
                print("⚠️ 检测到紧急停止信号，正在关闭系统...")
                self.shutdown_requested = True
                self._shutdown_event.set()
                return

            # 检查组件状态
            all_healthy = True

            for component_name, component in [
                ('xueqiu_collector', self.xueqiu_collector),
                ('strategy_engine', self.strategy_engine),
                ('trade_executor', self.trade_executor)
            ]:
                if component is not None and hasattr(component, 'is_healthy'):
                    if not await component.is_healthy():
                        print(f"⚠️ 组件 {component_name} 健康检查失败")
                        all_healthy = False

            if not all_healthy:
                print("⚠️ 系统健康检查发现问题，建议检查日志")

        except Exception as e:
            print(f"健康检查异常: {e}")

    async def _shutdown(self):
        """关闭系统"""
        print("\n🔄 正在关闭系统...")

        shutdown_start = datetime.now()

        try:
            # 按相反顺序关闭组件
            components_to_shutdown = [
                ('trade_executor', self.trade_executor),
                ('strategy_engine', self.strategy_engine),
                ('xueqiu_collector', self.xueqiu_collector)
            ]

            for component_name, component in components_to_shutdown:
                if component and hasattr(component, 'stop'):
                    try:
                        print(f"   关闭 {component_name}...")
                        await component.stop()
                        self.components_status[component_name] = 'stopped'
                        print(f"   ✅ {component_name} 已关闭")
                    except Exception as e:
                        print(f"   ❌ {component_name} 关闭失败: {e}")
                        self.components_status[component_name] = 'error'
                elif component_name == 'xueqiu_collector' and component and hasattr(component, 'close'):
                    try:
                        print(f"   关闭 {component_name}...")
                        await component.close()
                        self.components_status[component_name] = 'stopped'
                        print(f"   ✅ {component_name} 已关闭")
                    except Exception as e:
                        print(f"   ❌ {component_name} 关闭失败: {e}")
                        self.components_status[component_name] = 'error'

            # 保存最终状态
            if self.config_manager:
                try:
                    self.config_manager.save_all()
                    print("   ✅ 配置已保存")
                except Exception as e:
                    print(f"   ⚠️ 配置保存失败: {e}")

            shutdown_duration = (datetime.now() - shutdown_start).total_seconds()

            print(f"\n✅ 系统关闭完成 (耗时: {shutdown_duration:.2f}秒)")

            # 记录关闭日志
            self._log_shutdown_info(shutdown_duration)

        except Exception as e:
            print(f"关闭过程中发生错误: {e}")

    def _log_shutdown_info(self, shutdown_duration: float):
        """记录关闭信息"""
        try:
            shutdown_info = {
                'timestamp': datetime.now().isoformat(),
                'startup_time': self.startup_time.isoformat(),
                'running_duration': (datetime.now() - self.startup_time).total_seconds(),
                'shutdown_duration': shutdown_duration,
                'components_status': self.components_status
            }

            log_file = Path('logs') / f'startup_{self.startup_time.strftime("%Y%m%d")}.log'

            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{datetime.now().strftime('%H:%M:%S')}] 系统关闭\n")
                f.write(f"关闭信息: {json.dumps(shutdown_info, ensure_ascii=False, indent=2)}\n")
                f.write("=" * 50 + "\n")

        except Exception as e:
            print(f"记录关闭日志失败: {e}")

    def get_system_status(self) -> dict[str, Any]:
        """获取系统状态"""
        return {
            'startup_time': self.startup_time.isoformat(),
            'running_duration': (datetime.now() - self.startup_time).total_seconds(),
            'components_status': self.components_status,
            'shutdown_requested': self.shutdown_requested
        }


async def main():
    """主函数"""
    try:
        startup_manager = StartupManager()
        await startup_manager.startup()
        return 0

    except KeyboardInterrupt:
        print("\n用户中断")
        return 0
    except Exception as e:
        print(f"\n系统启动失败: {e}")
        return 1


if __name__ == "__main__":
    # 设置事件循环策略 (Windows)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    exit_code = asyncio.run(main())
    sys.exit(exit_code)

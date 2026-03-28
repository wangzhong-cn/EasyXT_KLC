"""
Windows服务管理器
统一管理HTTP API和WebSocket服务
"""

import asyncio
import atexit
import io
import json
import logging
import multiprocessing as mp
import os
import signal
import socket
import sys
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

_SH = ZoneInfo("Asia/Shanghai")
from typing import Optional

from .config.settings import RealtimeDataConfig
from .http_server import HTTPServerManager
from .websocket_server import WebSocketServer

# 配置日志
# Windows 默认 stdout/stderr 编码为 GBK，中文日志经 QProcess 按 UTF-8 解码会乱码。
# 强制 StreamHandler 使用 UTF-8 编码（不再用 open(fileno()) 高风险 FD 重绑）。
_stream_handler = logging.StreamHandler(
    io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True)
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/service_manager.log", encoding="utf-8"),
        _stream_handler,
    ],
)

logger = logging.getLogger(__name__)
_instance_lock_file = None


def _is_port_available(port: int, host: str = "127.0.0.1") -> bool:
    """检测端口是否可用（未被占用）"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


def _run_http_server():
    manager = HTTPServerManager()
    asyncio.run(manager.start_server())


def _run_websocket_server(ws_host: str, ws_port: int):
    server = WebSocketServer()
    server.config.update_config("websocket.host", ws_host)
    server.config.update_config("websocket.port", ws_port)
    asyncio.run(server.start())


def _run_core_api_server(host: str, port: int):
    """启动核心API服务器 (FastAPI + WebSocket)"""
    import uvicorn

    uvicorn.run("core.api_server:app", host=host, port=port, reload=False)


def _acquire_single_instance_lock():
    lock_path = os.path.abspath(os.path.join("logs", "service_manager.lock"))
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    lock_file = open(lock_path, "a+b")
    try:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            lock_file.write(b"0")
            lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return lock_file
    except Exception:
        try:
            lock_file.close()
        except Exception:
            pass
        return None


def _release_single_instance_lock(lock_file):
    if lock_file is None:
        return
    try:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        lock_file.close()
    except Exception:
        pass


class EasyXTServiceManager:
    """EasyXT服务管理器"""

    def __init__(self):
        self.config = RealtimeDataConfig()
        self.http_process: Optional[mp.Process] = None
        self.websocket_process: Optional[mp.Process] = None
        self.core_api_process: Optional[mp.Process] = None
        # --- 增强项A: 启动实例标识 (session_id / boot_id) ---
        # 每次进程启动产生唯一 ID，便于多次重启的链路追踪
        self.session_id: str = (
            datetime.now(tz=_SH).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
        )
        # --- 增强项B: 失败原因分级计数 ---
        # key: 失败原因分类 (gbk_crash / bind_conflict / unexpected_exit)
        self._fail_counter: dict[str, int] = {
            "gbk_crash": 0,
            "bind_conflict": 0,
            "unexpected_exit": 0,
        }
        # --- 增强项C: EASYXT_MANAGED_WEBSOCKET 开关说明 ---
        # 默认 "0" = 不再单独拉起 WebSocket 进程（push_service 已内置 WebSocket）
        # 设为 "1" 或 "true" 才启动独立 WebSocket 进程（仅在不使用 push_service 时开启，否则会导致 8765 端口双绑）
        # 示例：set EASYXT_MANAGED_WEBSOCKET=0   # 生产/本机默认，推荐
        #        set EASYXT_MANAGED_WEBSOCKET=1   # 纯独立模式，不含 push_service 时使用
        requested_standalone_ws = os.environ.get("EASYXT_MANAGED_WEBSOCKET", "0") in (
            "1",
            "true",
            "True",
        )
        allow_standalone_ws = os.environ.get("EASYXT_ALLOW_STANDALONE_WEBSOCKET", "0") in (
            "1",
            "true",
            "True",
        )
        self._manage_standalone_websocket = requested_standalone_ws and allow_standalone_ws
        if requested_standalone_ws and not allow_standalone_ws:
            logger.warning(
                "检测到 EASYXT_MANAGED_WEBSOCKET=1，但默认禁用独立WebSocket进程。"
                "如确需启用，请同时设置 EASYXT_ALLOW_STANDALONE_WEBSOCKET=1"
            )
        self.running = False
        logger.info(f"[SESSION:{self.session_id}] EasyXTServiceManager 初始化完成")

    def start_http_server_process(self):
        """启动HTTP服务器进程"""
        try:
            api_config = self.config.config.get("api", {})
            http_port = api_config.get("port", 8080)

            if not _is_port_available(http_port):
                logger.error(
                    f"端口 {http_port} 已被占用，跳过 HTTP 服务器启动。"
                    f"请检查是否有残留进程占用该端口。"
                )
                return

            logger.info(f"启动HTTP服务器进程 (端口: {http_port})")

            self.http_process = mp.Process(target=_run_http_server, name="EasyXT-HTTP")
            self.http_process.daemon = True
            self.http_process.start()
            logger.info(f"HTTP服务器进程已启动 (PID: {self.http_process.pid})")

        except Exception as e:
            logger.error(f"启动HTTP服务器失败: {e}")
            raise

    def start_websocket_server_process(self):
        """启动WebSocket服务器进程"""
        try:
            websocket_config = self.config.get_websocket_config()
            ws_host = websocket_config.get("host", "localhost")
            ws_port = websocket_config.get("port", 8765)

            if not _is_port_available(ws_port, ws_host if ws_host != "localhost" else "127.0.0.1"):
                logger.error(
                    f"端口 {ws_port} 已被占用，跳过 WebSocket 服务器启动。"
                    f"请检查是否有残留进程占用该端口。"
                )
                return

            logger.info(f"启动WebSocket服务器进程 (端口: {ws_port})")

            self.websocket_process = mp.Process(
                target=_run_websocket_server, args=(ws_host, ws_port), name="EasyXT-WebSocket"
            )
            self.websocket_process.daemon = True
            self.websocket_process.start()
            logger.info(f"WebSocket服务器进程已启动 (PID: {self.websocket_process.pid})")

        except Exception as e:
            logger.error(f"启动WebSocket服务器失败: {e}")
            raise

    def start_core_api_server_process(self):
        """启动核心API服务器进程 (FastAPI + WebSocket)"""
        try:
            core_host = os.environ.get("EASYXT_API_HOST", "127.0.0.1")
            core_port = int(os.environ.get("EASYXT_API_PORT", "8765"))

            if not _is_port_available(core_port, core_host):
                logger.error(
                    f"端口 {core_port} 已被占用，跳过核心API服务器启动。"
                    f"请检查是否有残留进程占用该端口。"
                )
                return

            logger.info(f"启动核心API服务器进程 (端口: {core_port})")

            self.core_api_process = mp.Process(
                target=_run_core_api_server, args=(core_host, core_port), name="EasyXT-CoreAPI"
            )
            self.core_api_process.daemon = True
            self.core_api_process.start()
            logger.info(f"核心API服务器进程已启动 (PID: {self.core_api_process.pid})")

        except Exception as e:
            logger.error(f"启动核心API服务器失败: {e}")
            raise

    def start_services(self):
        """启动所有服务"""
        logger.info(f"=== EasyXT 实时数据服务启动 [session={self.session_id}] ===")

        try:
            # 启动HTTP服务器
            self.start_http_server_process()
            time.sleep(2)  # 等待HTTP服务器启动

            # 可选启动独立WebSocket服务器（默认关闭，避免与push_service重复占用8765端口）
            if self._manage_standalone_websocket:
                self.start_websocket_server_process()
                time.sleep(2)  # 等待WebSocket服务器启动

            # 启动核心API服务器 (提供 /ws/market/{symbol} WebSocket路由)
            self.start_core_api_server_process()
            time.sleep(2)  # 等待核心API服务器启动

            self.running = True
            logger.info(f"[SESSION:{self.session_id}] 所有服务启动成功")
            api_config = self.config.config.get("api", {})
            http_host = api_config.get("host", "localhost")
            http_port = api_config.get("port", 8080)
            logger.info(f"HTTP API: http://{http_host}:{http_port}")
            if self._manage_standalone_websocket:
                websocket_config = self.config.get_websocket_config()
                ws_host = websocket_config.get("host", "localhost")
                ws_port = websocket_config.get("port", 8765)
                logger.info(f"WebSocket: ws://{ws_host}:{ws_port}")
            core_host = os.environ.get("EASYXT_API_HOST", "127.0.0.1")
            core_port = int(os.environ.get("EASYXT_API_PORT", "8765"))
            logger.info(f"Core API: http://{core_host}:{core_port} (WebSocket: /ws/market/)")

        except Exception as e:
            logger.error(f"服务启动失败: {e}")
            self.stop_services()
            raise

    def stop_services(self):
        """停止所有服务"""
        logger.info("正在停止所有服务...")
        self.running = False

        # 停止HTTP服务器
        if self.http_process and self.http_process.is_alive():
            logger.info("停止HTTP服务器进程")
            self.http_process.terminate()
            self.http_process.join(timeout=10)
            if self.http_process.is_alive():
                logger.warning("强制终止HTTP服务器进程")
                self.http_process.kill()

        # 停止WebSocket服务器
        if self.websocket_process and self.websocket_process.is_alive():
            logger.info("停止WebSocket服务器进程")
            self.websocket_process.terminate()
            self.websocket_process.join(timeout=10)
            if self.websocket_process.is_alive():
                logger.warning("强制终止WebSocket服务器进程")
                self.websocket_process.kill()

        # 停止核心API服务器
        if self.core_api_process and self.core_api_process.is_alive():
            logger.info("停止核心API服务器进程")
            self.core_api_process.terminate()
            self.core_api_process.join(timeout=10)
            if self.core_api_process.is_alive():
                logger.warning("强制终止核心API服务器进程")
                self.core_api_process.kill()

        logger.info("所有服务已停止")

    def _classify_and_record_failure(self, proc_name: str, exit_code: Optional[int]):
        """对子进程异常退出进行原因分类，更新计数器并追加写入 stability_diag.log"""
        reason = "unexpected_exit"
        # 读取最近日志行判断失败原因
        log_path = os.path.abspath(os.path.join("logs", "service_manager.log"))
        hint = ""
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    recent = f.readlines()[-50:]
                for line in reversed(recent):
                    low = line.lower()
                    if "gbk" in low and "codec" in low:
                        reason = "gbk_crash"
                        hint = "GBK编码异常"
                        break
                    if "10048" in low or "address already in use" in low:
                        reason = "bind_conflict"
                        hint = "端口冲突 (10048)"
                        break
            except Exception:
                pass
        self._fail_counter[reason] = self._fail_counter.get(reason, 0) + 1
        record = {
            "ts": datetime.now(tz=_SH).strftime("%Y-%m-%d %H:%M:%S"),
            "session_id": self.session_id,
            "phase": "subprocess_exit",
            "proc": proc_name,
            "exit_code": exit_code,
            "reason": reason,
            "hint": hint,
            "fail_counter": dict(self._fail_counter),
        }
        try:
            diag_dir = os.path.abspath("logs")
            os.makedirs(diag_dir, exist_ok=True)
            diag_path = os.path.join(diag_dir, "stability_diag.log")
            with open(diag_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"stability_diag.log 写入失败: {e}")
        logger.error(
            f"[SESSION:{self.session_id}] {proc_name} 异常退出 exit={exit_code} "
            f"reason={reason} hint={hint} "
            f"累计: {self._fail_counter}"
        )

    _CHILD_MAX_RESTARTS = 3
    _CHILD_RESTART_INTERVALS = [30, 60, 120]

    def monitor_services(self):
        """监控服务状态（子进程最多重启 _CHILD_MAX_RESTARTS 次，带退避）"""
        http_restarts = 0
        ws_restarts = 0
        while self.running:
            try:
                # 检查HTTP服务器
                if self.http_process and not self.http_process.is_alive():
                    exit_code = self.http_process.exitcode
                    self._classify_and_record_failure("HTTP", exit_code)
                    if http_restarts < self._CHILD_MAX_RESTARTS:
                        wait = self._CHILD_RESTART_INTERVALS[
                            min(http_restarts, len(self._CHILD_RESTART_INTERVALS) - 1)
                        ]
                        http_restarts += 1
                        logger.error(
                            f"HTTP服务器异常退出，{wait}s 后重启 "
                            f"({http_restarts}/{self._CHILD_MAX_RESTARTS})"
                        )
                        time.sleep(wait)
                        self.start_http_server_process()
                    else:
                        logger.error(
                            f"HTTP子进程已达最大重启次数 ({self._CHILD_MAX_RESTARTS})，停止自动重启"
                        )

                if self._manage_standalone_websocket:
                    if self.websocket_process and not self.websocket_process.is_alive():
                        exit_code = self.websocket_process.exitcode
                        self._classify_and_record_failure("WebSocket", exit_code)
                        if ws_restarts < self._CHILD_MAX_RESTARTS:
                            wait = self._CHILD_RESTART_INTERVALS[
                                min(ws_restarts, len(self._CHILD_RESTART_INTERVALS) - 1)
                            ]
                            ws_restarts += 1
                            logger.error(
                                f"WebSocket服务器异常退出，{wait}s 后重启 "
                                f"({ws_restarts}/{self._CHILD_MAX_RESTARTS})"
                            )
                            time.sleep(wait)
                            self.start_websocket_server_process()
                        else:
                            logger.error(
                                f"WebSocket子进程已达最大重启次数 ({self._CHILD_MAX_RESTARTS})，停止自动重启"
                            )

                time.sleep(30)

            except Exception as e:
                logger.error(f"服务监控异常: {e}")
                time.sleep(60)

    def run(self):
        """运行服务管理器"""

        # 注册信号处理器
        def signal_handler(signum, frame):
            logger.info(f"收到信号 {signum}，正在关闭服务...")
            self.stop_services()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            # 启动服务
            self.start_services()

            # 监控服务
            self.monitor_services()

        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭服务...")
        except Exception as e:
            logger.error(f"服务运行异常: {e}")
        finally:
            self.stop_services()


def main():
    """主函数"""
    os.makedirs("logs", exist_ok=True)
    global _instance_lock_file
    _instance_lock_file = _acquire_single_instance_lock()
    if _instance_lock_file is None:
        logger.warning("检测到服务管理器已在运行，跳过重复启动")
        return
    atexit.register(lambda: _release_single_instance_lock(_instance_lock_file))

    manager = EasyXTServiceManager()
    try:
        manager.run()
    finally:
        _release_single_instance_lock(_instance_lock_file)


if __name__ == "__main__":
    main()

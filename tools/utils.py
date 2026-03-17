"""
tools/utils.py — CI 工具链共用工具函数

提供标准化的日志记录器工厂，统一格式：
  [时间戳] [脚本名] [级别] 消息
"""
from __future__ import annotations

import logging
import sys


def init_script_logger(script_name: str, level: int = logging.INFO) -> logging.Logger:
    """
    返回一个已配置好 Handler 的 Logger。

    输出格式：[2026-03-07 23:41:37] [check_phase_exit] [INFO] 消息内容
    所有输出写入 stdout（便于 CI 捕获并保留颜色/顺序）。

    多次调用同一 script_name 不会重复添加 Handler（幂等）。
    """
    logger = logging.getLogger(f"ci_gate.{script_name}")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger

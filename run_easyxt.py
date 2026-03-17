#!/usr/bin/env python3
"""
EasyXT 启动脚本
解决 PyQt WebEngine 在某些环境下的 GIL 问题
"""

import os
import sys

# 在导入任何 Qt 相关模块之前设置环境变量
if os.name == "nt":
    os.environ.setdefault("QT_QPA_PLATFORM", "windows")
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")

# 关键：在创建 QApplication 之前导入 WebEngine
try:
    from PyQt5.QtWebEngineWidgets import QWebEngineView
except ImportError:
    pass

# 启动主程序
if __name__ == "__main__":
    sys.exit("请使用以下命令运行: python gui_app/main_window.py")

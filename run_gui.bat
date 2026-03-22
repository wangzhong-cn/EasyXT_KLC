@echo off
set QT_QPA_PLATFORM=windows
set QTWEBENGINE_CHROMIUM_FLAGS=--disable-gpu
echo Starting EasyXT GUI...
"C:\Users\wangzhong\miniconda3\envs\myenv\python.exe" "%~dp0gui_app\main_window.py"
pause

#!/usr/bin/env python3
"""
实时管道监控设置对话框
用于配置实时数据管道的各种参数
"""

import getpass
import os
from datetime import datetime
from typing import Any

from PyQt5.QtCore import QSettings
from PyQt5.QtWidgets import (
    QCheckBox,
    QDialog,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMessageBox,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
)


class RealtimeSettingsDialog(QDialog):
    """实时管道监控设置对话框"""

    def __init__(self, parent=None, pipeline_manager=None):
        super().__init__(parent)
        self.pipeline_manager = pipeline_manager
        self.setWindowTitle("实时管道监控设置")
        self.setModal(True)
        self.resize(400, 300)

        # 从环境变量或默认值获取当前配置
        self._load_current_settings()

        self._setup_ui()
        self._setup_connections()

    def _load_current_settings(self):
        """从GUI、settings、环境变量或默认值加载当前配置（优先级：GUI > settings > env > default）"""
        # 创建QSettings对象来从配置文件读取
        settings = QSettings("EasyXT", "KLineChartWorkspace")

        # 环境变量优先级低于配置文件
        self.current_drop_threshold = float(
            settings.value(
                "realtime/drop_threshold",
                float(os.environ.get("EASYXT_RT_DROP_THRESHOLD", 0.1)),
                type=float,
            )
        )
        self.current_window_seconds = float(
            settings.value(
                "realtime/window_seconds",
                float(os.environ.get("EASYXT_RT_WINDOW_SECONDS", 60.0)),
                type=float,
            )
        )
        self.current_alert_sustain_s = float(
            settings.value(
                "realtime/alert_sustain_s",
                float(os.environ.get("EASYXT_RT_ALERT_SUSTAIN_S", 5.0)),
                type=float,
            )
        )
        self.current_flush_interval_ms = float(
            settings.value(
                "realtime/flush_interval_ms",
                float(os.environ.get("EASYXT_RT_FLUSH_MS", 200.0)),
                type=float,
            )
        )
        self.current_max_queue = float(
            settings.value(
                "realtime/max_queue", int(os.environ.get("EASYXT_RT_MAX_QUEUE", 256)), type=int
            )
        )
        self.current_ws_reconnect_initial = float(
            settings.value(
                "realtime/ws_reconnect_initial",
                float(os.environ.get("EASYXT_WS_RECONNECT_INITIAL", 1.5)),
                type=float,
            )
        )
        self.current_ws_reconnect_max = float(
            settings.value(
                "realtime/ws_reconnect_max",
                float(os.environ.get("EASYXT_WS_RECONNECT_MAX", 15.0)),
                type=float,
            )
        )
        self.current_ws_reconnect_factor = float(
            settings.value(
                "realtime/ws_reconnect_factor",
                float(os.environ.get("EASYXT_WS_RECONNECT_FACTOR", 1.8)),
                type=float,
            )
        )
        self.current_tdx_error_log_cooldown = float(
            settings.value(
                "realtime/tdx_error_log_cooldown",
                float(os.environ.get("EASYXT_TDX_ERROR_LOG_COOLDOWN", 15.0)),
                type=float,
            )
        )
        self.current_only_non_default_source = bool(
            settings.value("realtime/ui/only_non_default_source", False, type=bool)
        )

        # 记录配置来源（GUI > settings > env > default）
        source_map = {
            "drop_threshold": settings.value("realtime/source/drop_threshold"),
            "window_seconds": settings.value("realtime/source/window_seconds"),
            "alert_sustain_s": settings.value("realtime/source/alert_sustain_s"),
            "flush_interval_ms": settings.value("realtime/source/flush_interval_ms"),
            "max_queue": settings.value("realtime/source/max_queue"),
            "ws_reconnect_initial": settings.value("realtime/source/ws_reconnect_initial"),
            "ws_reconnect_max": settings.value("realtime/source/ws_reconnect_max"),
            "ws_reconnect_factor": settings.value("realtime/source/ws_reconnect_factor"),
            "tdx_error_log_cooldown": settings.value("realtime/source/tdx_error_log_cooldown"),
        }
        self.last_updated_by = settings.value("realtime/meta/updated_by", "")
        self.last_updated_at = settings.value("realtime/meta/updated_at", "")
        self.config_sources = {
            "drop_threshold": "gui"
            if settings.contains("realtime/drop_threshold")
            and str(source_map["drop_threshold"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/drop_threshold")
            else "env"
            if os.environ.get("EASYXT_RT_DROP_THRESHOLD")
            else "default",
            "window_seconds": "gui"
            if settings.contains("realtime/window_seconds")
            and str(source_map["window_seconds"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/window_seconds")
            else "env"
            if os.environ.get("EASYXT_RT_WINDOW_SECONDS")
            else "default",
            "alert_sustain_s": "gui"
            if settings.contains("realtime/alert_sustain_s")
            and str(source_map["alert_sustain_s"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/alert_sustain_s")
            else "env"
            if os.environ.get("EASYXT_RT_ALERT_SUSTAIN_S")
            else "default",
            "flush_interval_ms": "gui"
            if settings.contains("realtime/flush_interval_ms")
            and str(source_map["flush_interval_ms"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/flush_interval_ms")
            else "env"
            if os.environ.get("EASYXT_RT_FLUSH_MS")
            else "default",
            "max_queue": "gui"
            if settings.contains("realtime/max_queue")
            and str(source_map["max_queue"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/max_queue")
            else "env"
            if os.environ.get("EASYXT_RT_MAX_QUEUE")
            else "default",
            "ws_reconnect_initial": "gui"
            if settings.contains("realtime/ws_reconnect_initial")
            and str(source_map["ws_reconnect_initial"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/ws_reconnect_initial")
            else "env"
            if os.environ.get("EASYXT_WS_RECONNECT_INITIAL")
            else "default",
            "ws_reconnect_max": "gui"
            if settings.contains("realtime/ws_reconnect_max")
            and str(source_map["ws_reconnect_max"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/ws_reconnect_max")
            else "env"
            if os.environ.get("EASYXT_WS_RECONNECT_MAX")
            else "default",
            "ws_reconnect_factor": "gui"
            if settings.contains("realtime/ws_reconnect_factor")
            and str(source_map["ws_reconnect_factor"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/ws_reconnect_factor")
            else "env"
            if os.environ.get("EASYXT_WS_RECONNECT_FACTOR")
            else "default",
            "tdx_error_log_cooldown": "gui"
            if settings.contains("realtime/tdx_error_log_cooldown")
            and str(source_map["tdx_error_log_cooldown"]).lower() == "gui"
            else "settings"
            if settings.contains("realtime/tdx_error_log_cooldown")
            else "env"
            if os.environ.get("EASYXT_TDX_ERROR_LOG_COOLDOWN")
            else "default",
        }

    def _setup_ui(self):
        """设置界面"""
        layout = QVBoxLayout(self)

        # 参数设置组
        params_group = QGroupBox("监控参数")
        params_layout = QFormLayout(params_group)

        # 丢包阈值
        self.drop_threshold_spin = QDoubleSpinBox()
        self.drop_threshold_spin.setRange(0.1, 50.0)
        self.drop_threshold_spin.setValue(self.current_drop_threshold * 100)  # 显示为百分比
        self.drop_threshold_spin.setSuffix("%")
        self.drop_threshold_spin.setSingleStep(0.5)
        params_layout.addRow("丢包阈值:", self.drop_threshold_spin)

        # 恢复阈值（只读，显示计算值）
        self.recovery_threshold_label = QLabel(f"{self.current_drop_threshold * 100 * 0.6:.2f}%")
        params_layout.addRow("恢复阈值:", self.recovery_threshold_label)

        # 窗口长度
        self.window_seconds_spin = QSpinBox()
        self.window_seconds_spin.setRange(1, 3600)
        self.window_seconds_spin.setValue(int(self.current_window_seconds))
        self.window_seconds_spin.setSuffix(" 秒")
        params_layout.addRow("窗口长度:", self.window_seconds_spin)

        # 持续告警时间
        self.alert_sustain_spin = QSpinBox()
        self.alert_sustain_spin.setRange(1, 600)
        self.alert_sustain_spin.setValue(int(self.current_alert_sustain_s))
        self.alert_sustain_spin.setSuffix(" 秒")
        params_layout.addRow("持续告警时间:", self.alert_sustain_spin)

        # 刷新间隔
        self.flush_interval_spin = QSpinBox()
        self.flush_interval_spin.setRange(50, 1000)
        self.flush_interval_spin.setValue(int(self.current_flush_interval_ms))
        self.flush_interval_spin.setSuffix(" 毫秒")
        params_layout.addRow("刷新间隔:", self.flush_interval_spin)

        # 最大队列长度
        self.max_queue_spin = QSpinBox()
        self.max_queue_spin.setRange(32, 10000)
        self.max_queue_spin.setValue(int(self.current_max_queue))
        params_layout.addRow("最大队列长度:", self.max_queue_spin)

        layout.addWidget(params_group)

        advanced_group = QGroupBox("高级参数")
        advanced_layout = QFormLayout(advanced_group)
        self.ws_reconnect_initial_spin = QDoubleSpinBox()
        self.ws_reconnect_initial_spin.setRange(0.1, 60.0)
        self.ws_reconnect_initial_spin.setDecimals(2)
        self.ws_reconnect_initial_spin.setSingleStep(0.1)
        self.ws_reconnect_initial_spin.setValue(float(self.current_ws_reconnect_initial))
        self.ws_reconnect_initial_spin.setSuffix(" 秒")
        advanced_layout.addRow("WS初始重连:", self.ws_reconnect_initial_spin)
        self.ws_reconnect_max_spin = QDoubleSpinBox()
        self.ws_reconnect_max_spin.setRange(0.5, 300.0)
        self.ws_reconnect_max_spin.setDecimals(2)
        self.ws_reconnect_max_spin.setSingleStep(0.5)
        self.ws_reconnect_max_spin.setValue(float(self.current_ws_reconnect_max))
        self.ws_reconnect_max_spin.setSuffix(" 秒")
        advanced_layout.addRow("WS最大重连:", self.ws_reconnect_max_spin)
        self.ws_reconnect_factor_spin = QDoubleSpinBox()
        self.ws_reconnect_factor_spin.setRange(1.0, 5.0)
        self.ws_reconnect_factor_spin.setDecimals(2)
        self.ws_reconnect_factor_spin.setSingleStep(0.1)
        self.ws_reconnect_factor_spin.setValue(float(self.current_ws_reconnect_factor))
        advanced_layout.addRow("WS退避倍数:", self.ws_reconnect_factor_spin)
        self.tdx_error_log_cooldown_spin = QDoubleSpinBox()
        self.tdx_error_log_cooldown_spin.setRange(0.0, 300.0)
        self.tdx_error_log_cooldown_spin.setDecimals(1)
        self.tdx_error_log_cooldown_spin.setSingleStep(1.0)
        self.tdx_error_log_cooldown_spin.setValue(float(self.current_tdx_error_log_cooldown))
        self.tdx_error_log_cooldown_spin.setSuffix(" 秒")
        advanced_layout.addRow("TDX日志冷却:", self.tdx_error_log_cooldown_spin)
        layout.addWidget(advanced_group)

        # 配置来源信息组
        source_group = QGroupBox("配置来源")
        source_layout = QFormLayout(source_group)
        self.only_non_default_check = QCheckBox("仅显示非默认项")
        self.only_non_default_check.setChecked(self.current_only_non_default_source)
        source_layout.addRow(self.only_non_default_check)
        self._source_rows: list[tuple[str, QLabel, QLabel]] = []

        self.drop_threshold_source = self._create_source_label(self.config_sources["drop_threshold"])
        self.window_seconds_source = self._create_source_label(self.config_sources["window_seconds"])
        self.alert_sustain_source = self._create_source_label(self.config_sources["alert_sustain_s"])
        self.flush_interval_source = self._create_source_label(self.config_sources["flush_interval_ms"])
        self.max_queue_source = self._create_source_label(self.config_sources["max_queue"])
        self.ws_reconnect_initial_source = self._create_source_label(
            self.config_sources["ws_reconnect_initial"]
        )
        self.ws_reconnect_max_source = self._create_source_label(
            self.config_sources["ws_reconnect_max"]
        )
        self.ws_reconnect_factor_source = self._create_source_label(
            self.config_sources["ws_reconnect_factor"]
        )
        self.tdx_error_log_cooldown_source = self._create_source_label(
            self.config_sources["tdx_error_log_cooldown"]
        )

        self._add_source_row(source_layout, "drop_threshold", "丢包阈值:", self.drop_threshold_source)
        self._add_source_row(source_layout, "window_seconds", "窗口长度:", self.window_seconds_source)
        self._add_source_row(source_layout, "alert_sustain_s", "持续告警:", self.alert_sustain_source)
        self._add_source_row(source_layout, "flush_interval_ms", "刷新间隔:", self.flush_interval_source)
        self._add_source_row(source_layout, "max_queue", "最大队列:", self.max_queue_source)
        self._add_source_row(
            source_layout,
            "ws_reconnect_initial",
            "WS初始重连:",
            self.ws_reconnect_initial_source,
        )
        self._add_source_row(
            source_layout,
            "ws_reconnect_max",
            "WS最大重连:",
            self.ws_reconnect_max_source,
        )
        self._add_source_row(
            source_layout,
            "ws_reconnect_factor",
            "WS退避倍数:",
            self.ws_reconnect_factor_source,
        )
        self._add_source_row(
            source_layout,
            "tdx_error_log_cooldown",
            "TDX日志冷却:",
            self.tdx_error_log_cooldown_source,
        )

        updated_text = "N/A"
        if self.last_updated_by and self.last_updated_at:
            updated_text = f"{self.last_updated_by} @ {self.last_updated_at}"
        self.last_updated_label = QLabel(updated_text)
        source_layout.addRow("最后修改:", self.last_updated_label)
        self._apply_source_filter()

        layout.addWidget(source_group)

        # 按钮布局
        button_layout = QHBoxLayout()

        self.reset_btn = QPushButton("恢复默认")
        self.ok_btn = QPushButton("确定")
        self.cancel_btn = QPushButton("取消")
        self.export_btn = QPushButton("导出降级日志")

        button_layout.addStretch()
        button_layout.addWidget(self.export_btn)
        button_layout.addWidget(self.reset_btn)
        button_layout.addWidget(self.ok_btn)
        button_layout.addWidget(self.cancel_btn)

        layout.addLayout(button_layout)

        # 更新恢复阈值显示
        self.drop_threshold_spin.valueChanged.connect(self._update_recovery_threshold)

    def _setup_connections(self):
        """设置信号连接"""
        self.ok_btn.clicked.connect(self.accept)
        self.cancel_btn.clicked.connect(self.reject)
        self.reset_btn.clicked.connect(self._reset_to_defaults)
        self.export_btn.clicked.connect(self._export_degrade_logs)
        self.only_non_default_check.toggled.connect(self._apply_source_filter)

    def _add_source_row(
        self,
        source_layout: QFormLayout,
        source_key: str,
        title: str,
        value_label: QLabel,
    ) -> None:
        title_label = QLabel(title)
        self._source_rows.append((source_key, title_label, value_label))
        source_layout.addRow(title_label, value_label)

    @staticmethod
    def _source_meta(source: str) -> tuple[str, str]:
        normalized = str(source or "").strip().lower()
        if normalized == "gui":
            return "🖥 GUI", "#2e7d32"
        if normalized == "settings":
            return "📁 配置文件", "#1565c0"
        if normalized == "env":
            return "🌐 环境变量", "#ef6c00"
        return "⚪ 默认值", "#6b7280"

    def _create_source_label(self, source: str) -> QLabel:
        text, color = self._source_meta(source)
        label = QLabel(text)
        label.setStyleSheet(f"color: {color};")
        return label

    def _apply_source_filter(self) -> None:
        only_non_default = bool(
            hasattr(self, "only_non_default_check") and self.only_non_default_check.isChecked()
        )
        for source_key, title_label, value_label in getattr(self, "_source_rows", []):
            source = str(self.config_sources.get(source_key, "default")).lower()
            visible = (source != "default") if only_non_default else True
            title_label.setVisible(visible)
            value_label.setVisible(visible)

    def _update_recovery_threshold(self, value):
        """更新恢复阈值显示"""
        recovery_value = value * 0.6
        self.recovery_threshold_label.setText(f"{recovery_value:.2f}%")

    def _reset_to_defaults(self):
        """重置为默认值"""
        self.drop_threshold_spin.setValue(10.0)
        self.window_seconds_spin.setValue(60)
        self.alert_sustain_spin.setValue(5)
        self.flush_interval_spin.setValue(200)
        self.max_queue_spin.setValue(256)
        self.ws_reconnect_initial_spin.setValue(1.5)
        self.ws_reconnect_max_spin.setValue(15.0)
        self.ws_reconnect_factor_spin.setValue(1.8)
        self.tdx_error_log_cooldown_spin.setValue(15.0)

    def _export_degrade_logs(self):
        log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        log_dir = os.path.abspath(log_dir)
        src_path = os.path.join(log_dir, "realtime_degrade.log")
        if not os.path.exists(src_path):
            QMessageBox.information(self, "提示", "暂无降级日志可导出")
            return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"realtime_degrade_{ts}.log"
        target_path, _ = QFileDialog.getSaveFileName(
            self, "导出降级日志", default_name, "Log文件 (*.log);;所有文件 (*)"
        )
        if not target_path:
            return
        try:
            with open(src_path, encoding="utf-8") as src:
                content = src.read()
            with open(target_path, "w", encoding="utf-8") as dst:
                dst.write(content)
            QMessageBox.information(self, "导出完成", f"已导出至: {target_path}")
        except Exception as e:
            QMessageBox.warning(self, "导出失败", str(e))

    def get_settings(self) -> dict[str, Any]:
        """获取设置值"""
        return {
            "drop_threshold": self.drop_threshold_spin.value() / 100,  # 转换回小数
            "window_seconds": float(self.window_seconds_spin.value()),
            "alert_sustain_s": float(self.alert_sustain_spin.value()),
            "flush_interval_ms": float(self.flush_interval_spin.value()),
            "max_queue": float(self.max_queue_spin.value()),
            "ws_reconnect_initial": float(self.ws_reconnect_initial_spin.value()),
            "ws_reconnect_max": float(self.ws_reconnect_max_spin.value()),
            "ws_reconnect_factor": float(self.ws_reconnect_factor_spin.value()),
            "tdx_error_log_cooldown": float(self.tdx_error_log_cooldown_spin.value()),
        }

    def accept(self):
        """接受更改并保存设置"""
        # 获取新设置
        new_settings = self.get_settings()

        # 验证参数边界
        errors = []
        if not (0.001 <= new_settings["drop_threshold"] <= 0.999):
            errors.append("丢包阈值必须在 0.1% 到 99.9% 之间")
        if not (1.0 <= new_settings["window_seconds"] <= 3600.0):
            errors.append("窗口长度必须在 1 到 3600 秒之间")
        if not (0.1 <= new_settings["alert_sustain_s"] <= 600.0):
            errors.append("持续告警时间必须在 0.1 到 600 秒之间")
        if not (0.1 <= new_settings["ws_reconnect_initial"] <= 60.0):
            errors.append("WS初始重连必须在 0.1 到 60 秒之间")
        if not (0.5 <= new_settings["ws_reconnect_max"] <= 300.0):
            errors.append("WS最大重连必须在 0.5 到 300 秒之间")
        if new_settings["ws_reconnect_max"] < new_settings["ws_reconnect_initial"]:
            errors.append("WS最大重连不能小于WS初始重连")
        if not (1.0 <= new_settings["ws_reconnect_factor"] <= 5.0):
            errors.append("WS退避倍数必须在 1.0 到 5.0 之间")
        if not (0.0 <= new_settings["tdx_error_log_cooldown"] <= 300.0):
            errors.append("TDX日志冷却必须在 0 到 300 秒之间")

        if errors:
            QMessageBox.warning(self, "参数错误", "\n".join(errors))
            return

        # 保存到配置文件
        settings = QSettings("EasyXT", "KLineChartWorkspace")
        settings.setValue("realtime/drop_threshold", new_settings["drop_threshold"])
        settings.setValue("realtime/window_seconds", new_settings["window_seconds"])
        settings.setValue("realtime/alert_sustain_s", new_settings["alert_sustain_s"])
        settings.setValue("realtime/flush_interval_ms", new_settings["flush_interval_ms"])
        settings.setValue("realtime/max_queue", new_settings["max_queue"])
        settings.setValue("realtime/ws_reconnect_initial", new_settings["ws_reconnect_initial"])
        settings.setValue("realtime/ws_reconnect_max", new_settings["ws_reconnect_max"])
        settings.setValue("realtime/ws_reconnect_factor", new_settings["ws_reconnect_factor"])
        settings.setValue(
            "realtime/tdx_error_log_cooldown", new_settings["tdx_error_log_cooldown"]
        )
        settings.setValue("realtime/source/drop_threshold", "gui")
        settings.setValue("realtime/source/window_seconds", "gui")
        settings.setValue("realtime/source/alert_sustain_s", "gui")
        settings.setValue("realtime/source/flush_interval_ms", "gui")
        settings.setValue("realtime/source/max_queue", "gui")
        settings.setValue("realtime/source/ws_reconnect_initial", "gui")
        settings.setValue("realtime/source/ws_reconnect_max", "gui")
        settings.setValue("realtime/source/ws_reconnect_factor", "gui")
        settings.setValue("realtime/source/tdx_error_log_cooldown", "gui")
        settings.setValue("realtime/meta/updated_by", getpass.getuser())
        settings.setValue("realtime/meta/updated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        settings.setValue(
            "realtime/ui/only_non_default_source", bool(self.only_non_default_check.isChecked())
        )

        os.environ["EASYXT_WS_RECONNECT_INITIAL"] = str(new_settings["ws_reconnect_initial"])
        os.environ["EASYXT_WS_RECONNECT_MAX"] = str(new_settings["ws_reconnect_max"])
        os.environ["EASYXT_WS_RECONNECT_FACTOR"] = str(new_settings["ws_reconnect_factor"])
        os.environ["EASYXT_TDX_ERROR_LOG_COOLDOWN"] = str(
            new_settings["tdx_error_log_cooldown"]
        )

        # 如果有pipeline manager，应用热更新
        if self.pipeline_manager:
            self.pipeline_manager.update_config(
                drop_rate_threshold=new_settings["drop_threshold"],
                window_seconds=new_settings["window_seconds"],
                alert_sustain_s=new_settings["alert_sustain_s"],
                flush_interval_ms=new_settings["flush_interval_ms"],
                max_queue=new_settings["max_queue"],
            )

        super().accept()

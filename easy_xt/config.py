"""
EasyXT配置管理
"""

import logging
import os
from typing import Any, Optional

log = logging.getLogger(__name__)

from .load_config import update_config_with_unified_settings
from .qmt_paths import QMT_POSSIBLE_PATHS, QMT_SIMULATED_KEYWORDS, QMT_USERDATA_SUBPATH


def deep_update(base_dict: dict[str, Any], update_dict: dict[str, Any]) -> None:
    """深度更新字典"""
    for key, value in update_dict.items():
        if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
            deep_update(base_dict[key], value)
        else:
            base_dict[key] = value


class Config:
    """配置管理类"""

    def __init__(self):
        # 默认配置
        self.settings = {
            'data': {
                'duckdb_path': '',
                'fill_data': True,
                'auto_retry': True,
                'retry_count': 3,
                'timeout': 30
            },
            'trade': {
                'session_id': 'default',
                'userdata_path': '',
                'callback_timeout': 10
            },
            'qmt': {
                # QMT可能的安装路径列表（从qmt_paths.py导入）
                'possible_paths': QMT_POSSIBLE_PATHS,
                'userdata_subpath': QMT_USERDATA_SUBPATH,
                'detected_path': None
            }
        }

    def _detect_qmt_path(self) -> Optional[str]:
        """自动检测QMT安装路径（仅模拟盘）- 仅在配置路径无效时调用"""
        # 优先检测包含"模拟"或"mini"关键词的路径（模拟盘）
        for path in self.settings['qmt']['possible_paths']:
            if os.path.exists(path):
                userdata_path = os.path.join(path, self.settings['qmt']['userdata_subpath'])
                if os.path.exists(userdata_path):
                    # 检查是否为模拟盘路径（包含模拟盘关键词）
                    if any(keyword in path for keyword in QMT_SIMULATED_KEYWORDS):
                        self.settings['qmt']['detected_path'] = path
                        self.settings['trade']['userdata_path'] = userdata_path
                        log.info("自动检测到模拟盘QMT路径: %s", path)
                        return path

        # 如果没有找到模拟盘路径，显示提示信息
        log.error("未能自动检测到模拟盘QMT路径")
        log.info("当前只检测模拟盘路径，如需使用实盘路径请手动设置")
        return None

    def get_qmt_path(self) -> Optional[str]:
        """获取QMT安装路径"""
        return self.settings['qmt']['detected_path']

    def get_userdata_path(self) -> Optional[str]:
        """获取用户数据路径"""
        return self.settings['trade']['userdata_path']

    def set_qmt_path(self, path: str) -> bool:
        """手动设置QMT路径"""
        if not os.path.exists(path):
            log.error("QMT路径不存在: %s", path)
            return False

        userdata_path = os.path.join(path, self.settings['qmt']['userdata_subpath'])
        if not os.path.exists(userdata_path):
            log.error("未找到userdata_mini目录: %s", userdata_path)
            return False

        self.settings['qmt']['detected_path'] = path
        self.settings['trade']['userdata_path'] = userdata_path

        # 将新路径添加到可能路径列表的开头
        possible_paths: list[str] = self.settings['qmt']['possible_paths']
        if path not in possible_paths:
            possible_paths.insert(0, path)

        log.info("QMT路径设置成功: %s", path)
        return True

    def validate_qmt_setup(self) -> tuple[bool, str]:
        """验证QMT配置"""
        qmt_path = self.get_qmt_path()
        if not qmt_path:
            return False, "未配置QMT路径"

        if not os.path.exists(qmt_path):
            return False, f"QMT路径不存在: {qmt_path}"

        userdata_path = self.get_userdata_path()
        if not userdata_path or not os.path.exists(userdata_path):
            return False, f"userdata路径不存在: {userdata_path}"

        return True, "QMT配置正常"

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值"""
        keys = key.split('.')
        value: Any = self.settings

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def set(self, key: str, value: Any) -> None:
        """设置配置值"""
        keys = key.split('.')
        target = self.settings

        for k in keys[:-1]:
            if k not in target:
                target[k] = {}
            target = target[k]

        target[keys[-1]] = value

    def update(self, config_dict: dict[str, Any]) -> None:
        """批量更新配置"""
        deep_update(self.settings, config_dict)

    def print_qmt_status(self):
        """打印QMT配置状态"""
        log.info("=" * 50)
        log.info("QMT配置状态")
        log.info("=" * 50)

        qmt_path = self.get_qmt_path()
        userdata_path = self.get_userdata_path()

        if qmt_path:
            log.info("QMT安装路径: %s", qmt_path)
            log.info("用户数据路径: %s", userdata_path)

            is_valid, msg = self.validate_qmt_setup()
            if is_valid:
                log.info("配置状态: %s", msg)
            else:
                log.error("配置状态: %s", msg)
        else:
            log.error("未检测到QMT安装路径")
            log.info("可能的解决方案: 1. 确保QMT已正确安装; 2. 手动设置QMT路径: config.set_qmt_path('...')")
            for path in self.settings['qmt']['possible_paths']:
                log.info("常见QMT安装路径: %s", path)

        log.info("=" * 50)


# 全局配置实例
config = Config()

# 尝试从统一配置文件加载配置
update_config_with_unified_settings(config)

# 尝试从统一配置文件加载配置
update_config_with_unified_settings(config)

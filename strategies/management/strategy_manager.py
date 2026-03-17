#!/usr/bin/env python3
"""
策略管理器 - 阶段5核心组件
负责策略的创建、校验、存储、版本管理
"""

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, field_validator


class StrategyConfig(BaseModel):
    """策略配置参数模型"""
    strategy_id: str                    # 策略唯一标识
    strategy_name: str                  # 策略名称
    strategy_type: str                  # 策略类型
    base_strategy: str                  # 基础策略类名
    parameters: dict[str, Any]          # 策略参数
    risk_controls: dict[str, float]     # 风控参数
    symbols: list[str]                  # 标的列表
    period: str = "1d"                  # 数据周期
    backtest_range: dict[str, str]      # 回测区间
    trading_cost: dict[str, float]      # 交易成本
    version: int = 1
    created_at: str = datetime.now().isoformat()
    updated_at: str = datetime.now().isoformat()

    @field_validator('strategy_type')
    @classmethod
    def validate_strategy_type(cls, v):
        valid_types = ["trend", "reversion", "factor", "grid", "conditional", "hedge"]
        if v not in valid_types:
            raise ValueError(f"策略类型必须是: {valid_types}")
        return v
    @field_validator('period')
    @classmethod
    def validate_period(cls, v):
        valid_periods = ["1d", "1m", "5m", "15m", "30m", "1h"]
        if v not in valid_periods:
            raise ValueError(f"周期必须是: {valid_periods}")
        return v

    @field_validator('risk_controls')
    @classmethod
    def validate_risk_controls(cls, v):
        if 'max_position' not in v or 'daily_stop_loss' not in v or 'max_drawdown' not in v:
            raise ValueError("必须设置最大仓位、单日止损、最大回撤")
        if v['max_position'] > 0.2:
            raise ValueError("单策略最大仓位不能超过20%")
        if v['daily_stop_loss'] > 0.03:
            raise ValueError("单日止损阈值不能超过3%")
        if v['max_drawdown'] > 0.15:
            raise ValueError("最大回撤不能超过15%")
        return v


class StrategyManager:
    """策略管理器"""

    def __init__(self, config_dir: str = "strategies/configs"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.strategies: dict[str, StrategyConfig] = {}
        self.logger = logging.getLogger(__name__)
        self._load_existing_strategies()

    def _config_to_dict(self, config: StrategyConfig) -> dict[str, Any]:
        return config.model_dump()

    def _config_to_json(self, config: StrategyConfig) -> str:
        return json.dumps(self._config_to_dict(config), ensure_ascii=False, indent=2)

    def _load_existing_strategies(self):
        """加载现有策略配置"""
        for config_file in self.config_dir.glob("*.json"):
            try:
                with open(config_file, encoding='utf-8') as f:
                    config_data = json.load(f)
                config = StrategyConfig(**config_data)
                self.strategies[config.strategy_id] = config
            except Exception as e:
                self.logger.warning("加载策略配置失败 %s: %s", config_file, e)

    def create_strategy(self, config_data: dict[str, Any]) -> str:
        """创建新策略"""
        try:
            # 生成唯一ID
            strategy_id = str(uuid.uuid4())
            config_data['strategy_id'] = strategy_id
            config_data.setdefault('version', 1)

            # 验证配置
            config = StrategyConfig(**config_data)

            # 保存配置
            config_file = self.config_dir / f"{strategy_id}.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                f.write(self._config_to_json(config))

            self.strategies[strategy_id] = config
            return strategy_id

        except Exception as e:
            self.logger.error("策略创建失败: %s", e)
            raise ValueError(f"策略创建失败: {e}")

    def get_strategy(self, strategy_id: str) -> Optional[StrategyConfig]:
        """获取策略配置"""
        return self.strategies.get(strategy_id)

    def list_strategies(self) -> list[dict[str, Any]]:
        """列出所有策略"""
        return [
            {
                'strategy_id': config.strategy_id,
                'strategy_name': config.strategy_name,
                'strategy_type': config.strategy_type,
                'created_at': config.created_at,
                'version': config.version,
                'symbols_count': len(config.symbols)
            }
            for config in self.strategies.values()
        ]

    def update_strategy(self, strategy_id: str, updates: dict[str, Any]) -> bool:
        """更新策略配置"""
        if strategy_id not in self.strategies:
            return False

        try:
            current_config = self._config_to_dict(self.strategies[strategy_id])
            current_config.update(updates)
            current_config['updated_at'] = datetime.now().isoformat()
            current_config['version'] = int(current_config.get('version', 1)) + 1

            # 重新验证配置
            config = StrategyConfig(**current_config)

            # 保存更新
            config_file = self.config_dir / f"{strategy_id}.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                f.write(self._config_to_json(config))

            self.strategies[strategy_id] = config
            return True

        except Exception as e:
            self.logger.error("策略更新失败: %s", e)
            return False

    def delete_strategy(self, strategy_id: str) -> bool:
        """删除策略"""
        if strategy_id not in self.strategies:
            return False

        try:
            config_file = self.config_dir / f"{strategy_id}.json"
            config_file.unlink()  # 删除配置文件
            del self.strategies[strategy_id]
            return True
        except Exception as e:
            self.logger.error("策略删除失败: %s", e)
            return False


# 策略管理器实例
strategy_manager = StrategyManager()


if __name__ == "__main__":
    # 测试策略管理器
    manager = StrategyManager()

    # 创建示例策略
    sample_config = {
        "strategy_name": "双均线趋势策略",
        "strategy_type": "trend",
        "base_strategy": "MovingAverageCrossover",
        "parameters": {
            "fast_period": 5,
            "slow_period": 20,
            "adjust": "front"
        },
        "risk_controls": {
            "max_position": 0.2,
            "daily_stop_loss": 0.03,
            "max_drawdown": 0.15
        },
        "symbols": ["000001.SZ", "000002.SZ"],
        "backtest_range": {
            "start": "2023-01-01",
            "end": "2023-12-31"
        },
        "trading_cost": {
            "commission": 0.0003,
            "tax": 0.001
        }
    }

    try:
        strategy_id = manager.create_strategy(sample_config)
        print(f"策略创建成功: {strategy_id}")

        # 列出策略
        strategies = manager.list_strategies()
        print("策略列表:", strategies)

    except Exception as e:
        print(f"测试失败: {e}")

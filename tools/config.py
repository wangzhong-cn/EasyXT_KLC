"""
tools/config.py — CI 工具链统一配置常量

集中管理各 gate 脚本的可调参数，避免硬编码散落各处。
若需覆盖，直接修改此文件或通过子类 / 环境变量扩展。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class WaiverConfig:
    """覆盖率豁免相关配置。"""
    # 签名密钥的环境变量名
    signing_key_env: str = "WAIVER_SIGNING_KEY"
    # 区分生产/开发环境的环境变量名（值 == "prod" 时视为生产环境）
    env_var: str = "ENVIRONMENT"
    # ≤ alert_days 天时输出 WAIVER_EXPIRING 告警
    expiry_alert_days: int = 7
    # ≤ critical_days 天时输出紧急告警
    expiry_critical_days: int = 3
    # 豁免文件必填字段
    required_fields: tuple[str, ...] = (
        "enabled", "phase", "effective_target",
        "expires", "reason", "approver", "approval_id",
    )
    # 双人审批字段（四眼原则）；至少其中一个非空则通过
    dual_approver_fields: tuple[str, ...] = ("approver", "approver_2")
    # 豁免文件签名规范串字段顺序（固定，避免序列化差异导致验签失败）
    canonical_fields: tuple[str, ...] = (
        "approval_id", "phase", "effective_target", "expires", "reason",
    )


@dataclass(frozen=True)
class ThreadConfig:
    """线程违规检查相关配置。"""
    # 历史状态文件名（相对项目根的 logs/）
    state_filename: str = "thread_lifecycle_state.json"
    # new_violations_this_build 超过此阈值时 CI 以 FAIL 阻断（0 = 任何新增均阻断）
    delta_max_new: int = 0
    # 历史条目保留天数（超过此天数的记录将被清除）
    history_retention_days: int = 30


@dataclass(frozen=True)
class CIGateConfig:
    """CI gate 汇总相关配置。"""
    total_gates: int = 7
    # Pipeline ID 的环境变量（按优先级排列）
    pipeline_env_vars: tuple[str, ...] = ("CI_PIPELINE_ID", "GITHUB_RUN_ID")
    # Commit SHA 的环境变量（按优先级排列，fallback 用 git）
    sha_env_vars: tuple[str, ...] = ("CI_COMMIT_SHA", "GITHUB_SHA")
    # Gate 版本号（修改逻辑时手动递增）
    gate_version: str = "1.3.0"


@dataclass(frozen=True)
class LedgerConfig:
    """
    台账治理配置（集中管理合规规则常量，修改合规要求只需改此处）。

    变更日志：
    - v1.0 (2026-03-08)：初始版本，包含 max_waiver_days / retention_days /
                        reason_max_len / archive_index_filename
    - v1.1 (2026-03-08)：新增 archive_index_schema_version，支持索引结构演进兼容解析
    - v1.2 (2026-03-08)：新增 unindexed_block_threshold，漏记告警从"一律阻断"升级为分级响应
    """
    # 豁免有效期上限（金融合规短期豁免约束）
    max_waiver_days: int = 90
    # 主台账保留天数（超过则归档）
    retention_days: int = 180
    # reason 摘录最大长度（截断保护，防止滔出内部信息）
    reason_max_len: int = 100
    # 归档索引文件名（相对 logs/）
    archive_index_filename: str = "governance_ledger_archive_index.json"
    # 归档索引结构版本（升级时递增，便于审计脚本做兼容解析）
    archive_index_schema_version: str = "1.1"
    # 反向漏记检查：漏记文件数 >= 此阈值时阻断 CI，< 阈值时仅输出 WARNING
    # 设为 1 可恢复严格模式；调高可容忍偶发性漏记
    unindexed_block_threshold: int = 3


# 模块级单例（其他脚本直接 import 这些实例）
WAIVER = WaiverConfig()
THREAD = ThreadConfig()
CI_GATE = CIGateConfig()
LEDGER = LedgerConfig()

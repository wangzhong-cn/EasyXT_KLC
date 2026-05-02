"""四点三线 N 字原子结构识别引擎 (StructureEngine)

完全基于「四点三线 N 字原子结构」最终公理化体系实现，
严格遵循三层分离架构，零主观规则。

# ==================================================
# AXIOM CORE LAYER 公理核心层（零参数、纯序比较、无依赖）
# ==================================================
#
# 定义 1.1  演化相空间 Ω = (T, S, γ)  双全序不可逆演化空间
# 定义 1.2  枢轴序列 Π  公理性质: 时间全序 + 峰谷交替 + 局部极值
# 定义 1.3  N字结构 N_k = (π_k, π_{k+1}, π_{k+2}, π_{k+3})  四点三线
# 定义 1.4  延续闭合: σ_k · (s_{k+3} - s_{k+1}) > 0  纯序零参数
# 定义 1.4b 反转闭合: σ_{k-1} · (s_{k+2} - s_k) > 0  = N_{k-1} 延续闭合
# 定义 1.4c 折返域分叉:
#           反转域: σ · (A0 - point) > 0 → 突破 A0 → 反转
#           延续域: σ · (point - A0) ≥ 0 且 σ · (A1 - point) > 0
#           无效域: σ · (point - A1) ≥ 0 → 非有效折返（趋势延伸）
# 定义 1.5  锚点迭代:
#           延续 → (A0', A1') = (P2, P3)
#           反转 → (A0', A1') = (old_A1, reversal_point), 方向翻转
#
# 公理 0   有限序列边界公理（首尾处理规则）
# 公理 1   结构本体公理（最小性 + 跨尺度定义不变性）
# 公理 2   结构自生长公理（M₀ 步内必然闭合）
# 公理 3   走势二元性公理（延续/反转 时序互斥完备）
#
# 定理 1   对偶定理: 反转(k) ≡ 延续(k-1)  代数恒等式
# 定理 2   互斥完备性: 同一锚点对生命周期内 延续/反转 严格互斥
#
# ==================================================
# INSTANTIATION LAYER 实例化层（尺度绑定、参数校准）
# ==================================================
# 枢轴序列提取: 邻域宽度 m(τ)=2, 时间确认规则, 平极值取最早
# M₀ 取值: 由历史数据校准 max(有效样本闭合步数)
#
# ==================================================
# ENGINE LAYER 工程层（实盘逻辑、事件回调）
# ==================================================
# 时序决策状态机 + 活跃锚点生命周期 + 闭合事件触发

用法：
    from data_manager.structure_engine import StructureEngine, NStructure

    engine = StructureEngine()
    structures = engine.scan(bars_df)      # 历史回溯扫描
    engine.push(price_point)               # 实时推送单个价格点
    current = engine.current_structure     # 当前活跃结构
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional

if TYPE_CHECKING:
    from data_manager.duckdb_connection_pool import DuckDBConnectionManager

log = logging.getLogger(__name__)


# ── 基础数据结构 ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PricePoint:
    """带时间戳的不可变价格点（严格对应公理中的 π_n）。

    ``timestamp`` 为毫秒级 UTC 整数，严格递增，禁止回溯修改（frozen=True）。
    ``price`` 使用 ``close_mapped``（局部除权映射后价格），禁止使用 raw 价。
    """
    timestamp: int    # UTC 毫秒，位移不可逆
    price: float      # close_mapped（绝不使用 close_raw）

    def __post_init__(self) -> None:
        if self.timestamp <= 0:
            raise ValueError(f"PricePoint.timestamp 必须为正整数，得到 {self.timestamp}")
        if self.price <= 0:
            raise ValueError(f"PricePoint.price 必须为正数，得到 {self.price}")


@dataclass
class NStructure:
    """有效 N 字结构（四点三线），初始化时自动验证公理约束。

    仅在延续闭合确认时创建。反转闭合不创建 NStructure，仅触发回调。
    若违反公理约束，``__post_init__`` 直接抛出 ``ValueError``。
    """
    p0: PricePoint
    p1: PricePoint
    p2: PricePoint
    p3: PricePoint
    direction: str          # "down" | "up"
    struct_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "active"  # "active" | "closed" | "reversed"

    # 可选的衍生指标（首期默认 None，由 DrawdownTracker/贝叶斯层填充）
    retrace_ratio: Optional[float] = None   # (P2-P1)/(P0-P1)
    attractor_mean: Optional[float] = None  # DPMM 吸引子均值（预留）
    attractor_std: Optional[float] = None   # DPMM 吸引子标准差（预留）
    bayes_lower: Optional[float] = None     # 贝叶斯下边界（预留）
    bayes_upper: Optional[float] = None     # 贝叶斯上边界（预留）

    def __post_init__(self) -> None:
        self._validate_time_order()
        self._validate_price_constraints()
        # 计算折返深度
        denom = abs(self.p0.price - self.p1.price)
        if denom > 0:
            self.retrace_ratio = abs(self.p2.price - self.p1.price) / denom

    # ── 内部验证 ─────────────────────────────────────────────────────────────

    def _validate_time_order(self) -> None:
        """公理: 时间序严格单调递增 (t0 < t1 < t2 < t3)。"""
        if not (self.p0.timestamp < self.p1.timestamp
                < self.p2.timestamp < self.p3.timestamp):
            raise ValueError(
                f"NStructure 时间序违反不可逆约束: "
                f"{self.p0.timestamp} < {self.p1.timestamp} < "
                f"{self.p2.timestamp} < {self.p3.timestamp} 不成立"
            )

    def _validate_price_constraints(self) -> None:
        """定义 1.4c 折返域(延续域) + 定义 1.4 延续闭合条件。"""
        if self.direction == "down":
            # 折返延续域: P1 < P2 ≤ P0  (a1 < fold ≤ a0)
            if not (self.p2.price > self.p1.price and self.p2.price <= self.p0.price):
                raise ValueError(
                    f"下跌 N 字折返约束不满足: "
                    f"P2({self.p2.price:.4f}) > P1({self.p1.price:.4f}) "
                    f"且 P2 ≤ P0({self.p0.price:.4f})"
                )
            # 延续闭合: σ*(P3-P1) > 0 → (-1)*(P3-P1) > 0 → P3 < P1
            if not (self.p3.price < self.p1.price):
                raise ValueError(
                    f"下跌 N 字延续闭合不满足: "
                    f"P3({self.p3.price:.4f}) < P1({self.p1.price:.4f})"
                )
        elif self.direction == "up":
            # 折返延续域: P0 ≤ P2 < P1  (a0 ≤ fold < a1)
            if not (self.p2.price < self.p1.price and self.p2.price >= self.p0.price):
                raise ValueError(
                    f"上涨 N 字折返约束不满足: "
                    f"P2({self.p2.price:.4f}) < P1({self.p1.price:.4f}) "
                    f"且 P2 ≥ P0({self.p0.price:.4f})"
                )
            # 延续闭合: σ*(P3-P1) > 0 → (+1)*(P3-P1) > 0 → P3 > P1
            if not (self.p3.price > self.p1.price):
                raise ValueError(
                    f"上涨 N 字延续闭合不满足: "
                    f"P3({self.p3.price:.4f}) > P1({self.p1.price:.4f})"
                )
        else:
            raise ValueError(f"direction 必须为 'up' 或 'down'，得到 '{self.direction}'")

    # ── 公开属性 ─────────────────────────────────────────────────────────────

    @property
    def stop_loss_price(self) -> float:
        """止损价 = P2.price（公理定义的唯一边界锚点，禁止主观修改）。"""
        return self.p2.price

    @property
    def stop_loss_distance(self) -> float:
        """止损距离 = |P3.price − P2.price|（绝对值，正数）。"""
        return abs(self.p3.price - self.p2.price)

    def to_dict(self) -> dict:
        """序列化为可 JSON 存储的字典（用于 structure_audit.snapshot_json）。"""
        return {
            "struct_id": self.struct_id,
            "direction": self.direction,
            "status": self.status,
            "p0": {"ts": self.p0.timestamp, "price": self.p0.price},
            "p1": {"ts": self.p1.timestamp, "price": self.p1.price},
            "p2": {"ts": self.p2.timestamp, "price": self.p2.price},
            "p3": {"ts": self.p3.timestamp, "price": self.p3.price},
            "retrace_ratio": self.retrace_ratio,
            "attractor_mean": self.attractor_mean,
            "attractor_std": self.attractor_std,
            "bayes_lower": self.bayes_lower,
            "bayes_upper": self.bayes_upper,
        }


# ── StructureEngine ───────────────────────────────────────────────────────────

class StructureEngine:
    """实时/回测 N 字结构识别引擎。

    严格遵循最终公理化体系：
    - 闭合条件: 纯序比较 σ*(s_{k+3} − s_{k+1}) > 0，零参数、零阈值
    - 反转处理: 不创建 NStructure，仅标记旧结构为 reversed 并触发回调
    - 折返域三分区: 反转域 / 延续域 / 无效域，互斥且完备（含 P2=P0 边界）

    参数：
        on_structure_created:
            延续闭合首次触发（新方向链起点），签名 ``(struct: NStructure) → None``。
        on_structure_extended:
            同方向延续链后续闭合触发，签名同上。
        on_structure_reversed:
            反转触发的回调，传入被反转的旧结构（status='reversed'），签名同上。
    """

    def __init__(
        self,
        on_structure_created: Optional[Callable[[NStructure], Any]] = None,
        on_structure_extended: Optional[Callable[[NStructure], Any]] = None,
        on_structure_reversed: Optional[Callable[[NStructure], Any]] = None,
        db_manager: Optional["DuckDBConnectionManager"] = None,
        code: str = "",
        interval: str = "",
    ) -> None:
        self._on_created = on_structure_created
        self._on_extended = on_structure_extended
        self._on_reversed = on_structure_reversed
        self._db_manager = db_manager
        self._code = code
        self._interval = interval

        # 内部状态
        self._history: list[NStructure] = []
        self._current: Optional[NStructure] = None

        # 活跃锚点对 (A0, A1) + 折返极值 + 方向
        self._a0: Optional[PricePoint] = None       # 锚点 A0（参考起点）
        self._a1: Optional[PricePoint] = None       # 锚点 A1（趋势极值）
        self._fold: Optional[PricePoint] = None     # 折返极值 P2 候选
        self._sigma: int = 0                         # 方向: +1=上涨, -1=下跌
        self._state: str = "init"                    # "init" | "tracking"

    # ── 公开属性 ─────────────────────────────────────────────────────────────

    @property
    def current_structure(self) -> Optional[NStructure]:
        """当前活跃结构 (status='active')，未识别到任何结构时返回 None。"""
        return self._current

    @property
    def all_structures(self) -> list[NStructure]:
        """已识别的所有结构（含历史，按时间升序）。"""
        return list(self._history)

    # ── 核心接口 ─────────────────────────────────────────────────────────────

    def scan(self, bars_df: Any) -> list[NStructure]:
        """对历史 K 线 DataFrame 执行全量回溯扫描。

        参数：
            bars_df: 必须含 ``trade_date``（毫秒）和 ``close_mapped`` 列，按时间升序。

        返回：
            识别到的所有有效 N 字结构列表（按识别时间升序）。
        """
        self.reset()
        for _, row in bars_df.iterrows():
            pt = PricePoint(int(row["trade_date"]), float(row["close_mapped"]))
            self.push(pt)
        return list(self._history)

    def push(self, point: PricePoint) -> Optional[NStructure]:
        """推送单个价格点（实时模式）。

        参数：
            point: 当前 K 线的 ``PricePoint``（使用 close_mapped）。

        返回：
            若本次推送触发了延续闭合，返回新建 NStructure；否则 None。
            反转闭合不返回结构（通过 on_structure_reversed 回调通知）。
        """
        if self._state == "init":
            return self._handle_init(point)
        return self._handle_tracking(point)

    def reset(self) -> None:
        """重置引擎内部状态（用于重新扫描或单元测试）。"""
        self._history.clear()
        self._current = None
        self._a0 = None
        self._a1 = None
        self._fold = None
        self._sigma = 0
        self._state = "init"

    def iter_structures(self) -> Iterator[NStructure]:
        """逐个迭代已识别结构（含当前活跃结构）。"""
        yield from self._history

    # ── 内部状态机 ───────────────────────────────────────────────────────────

    def _handle_init(self, point: PricePoint) -> Optional[NStructure]:
        """公理 0: 初始锚点对构建。

        有限序列起点处理：前两个价格不同的点构成初始锚点对 (A0, A1)。
        平极值取最早点（不更新 A0）。
        """
        if self._a0 is None:
            self._a0 = point
            return None

        # 等待方向确认 (A1 ≠ A0)
        if point.price > self._a0.price:
            self._a1 = point
            self._sigma = 1    # 上涨
            self._state = "tracking"
        elif point.price < self._a0.price:
            self._a1 = point
            self._sigma = -1   # 下跌
            self._state = "tracking"
        # price == a0.price: 平极值取最早，不更新 a0
        return None

    def _handle_tracking(self, point: PricePoint) -> Optional[NStructure]:
        """定义 1.4 / 1.4b / 1.4c 时序决策状态机。

        统一方向化公式（σ 为方向符号 ±1）：
          趋势延伸 / 闭合:  σ * (point − A1) > 0
          折返域(延续域):    σ * (point − A0) ≥ 0  且  σ * (A1 − point) > 0
          反转域:            σ * (A0 − point) > 0
          折返极值更新:      σ * (fold − point) > 0
        """
        σ = self._sigma
        a0, a1, fold = self._a0, self._a1, self._fold

        # ── 统一方向化比较 ──
        trend_extends = σ * (point.price - a1.price) > 0
        reversal_hit = σ * (a0.price - point.price) > 0
        in_fold_zone = (σ * (point.price - a0.price) >= 0
                        and σ * (a1.price - point.price) > 0)

        if fold is None:
            # ── 尚未出现折返 ──
            if trend_extends:
                # 无效域: 趋势延伸，更新 A1（P3 极值跟踪）
                self._a1 = point
            elif reversal_hit:
                # 反转域: 未经折返直接突破 A0
                return self._fire_reversal(point)
            elif in_fold_zone:
                # 延续域: 折返开始，记录极值
                self._fold = point
        else:
            # ── 折返已出现，寻找延续闭合（定理 2: 时序互斥） ──
            if trend_extends:
                # 定义 1.4 延续闭合: σ*(P3 − A1) > 0
                return self._fire_continuation(point)
            # 折返是否加深?
            fold_deeper = σ * (fold.price - point.price) > 0
            if fold_deeper:
                if reversal_hit:
                    # 定义 1.4b 反转闭合: 折返突破 A0
                    return self._fire_reversal(point)
                # 折返极值更新（仍在延续域内）
                self._fold = point

        return None

    # ── 审计日志写入（可选，失败静默） ─────────────────────────────────────────

    def _write_audit(
        self,
        struct: NStructure,
        event_type: str,
        event_ts: int,
    ) -> None:
        """向 structure_audit 表追加一条不可变审计记录（失败静默）。"""
        if self._db_manager is None:
            return
        try:
            self._db_manager.execute_write_query(
                "INSERT INTO structure_audit "
                "(id, structure_id, code, interval, event_type, event_ts, snapshot_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    uuid.uuid4().hex,
                    struct.struct_id,
                    self._code,
                    self._interval,
                    event_type,
                    event_ts,
                    json.dumps(struct.to_dict()),
                ),
            )
        except Exception:
            log.exception(
                "StructureEngine._write_audit 写入失败 (struct_id=%s, event=%s)",
                struct.struct_id, event_type,
            )

    def _persist_structure(
        self,
        struct: NStructure,
        closed_at: int | None = None,
    ) -> None:
        """将结构持久化到 structure_analyze 表（失败静默）。"""
        if self._db_manager is None:
            return
        try:
            self._db_manager.execute_write_query(
                """
                INSERT OR REPLACE INTO structure_analyze
                  (id, code, interval, created_at, direction,
                   p0_ts, p0_price, p1_ts, p1_price,
                   p2_ts, p2_price, p3_ts, p3_price,
                   attractor_mean, attractor_std, bayes_lower, bayes_upper,
                   retrace_ratio, status, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    struct.struct_id,
                    self._code,
                    self._interval,
                    struct.p3.timestamp,
                    struct.direction,
                    struct.p0.timestamp,
                    struct.p0.price,
                    struct.p1.timestamp,
                    struct.p1.price,
                    struct.p2.timestamp,
                    struct.p2.price,
                    struct.p3.timestamp,
                    struct.p3.price,
                    struct.attractor_mean,
                    struct.attractor_std,
                    struct.bayes_lower,
                    struct.bayes_upper,
                    struct.retrace_ratio,
                    struct.status,
                    closed_at,
                ),
            )
        except Exception:
            log.exception(
                "StructureEngine._persist_structure 写入失败 (struct_id=%s, status=%s)",
                struct.struct_id,
                struct.status,
            )

    def _update_structure_status(
        self,
        struct: NStructure,
        closed_at: int,
    ) -> None:
        """更新已存在 structure_analyze 行的生命周期状态（失败静默）。"""
        if self._db_manager is None:
            return
        try:
            self._db_manager.execute_write_query(
                "UPDATE structure_analyze SET status = ?, closed_at = ? WHERE id = ?",
                (struct.status, closed_at, struct.struct_id),
            )
        except Exception:
            log.exception(
                "StructureEngine._update_structure_status 失败 (struct_id=%s, status=%s)",
                struct.struct_id,
                struct.status,
            )

    # ── 闭合事件处理 ─────────────────────────────────────────────────────────

    def _fire_continuation(self, point: PricePoint) -> Optional[NStructure]:
        """定义 1.4 延续闭合 + 定义 1.5 锚点迭代。

        前置条件: σ * (point.price − A1.price) > 0 已由调用方保证。
        创建 NStructure(A0, A1, fold, point)，锚点迭代为 (fold, point)。
        """
        direction = "up" if self._sigma == 1 else "down"

        try:
            struct = NStructure(
                p0=self._a0,
                p1=self._a1,
                p2=self._fold,
                p3=point,
                direction=direction,
            )
        except ValueError as e:
            log.debug("延续闭合结构验证失败（忽略）: %s", e)
            return None

        # 判定 create vs extend（同方向链延续 = extend）
        previous_current = self._current
        is_extend = previous_current is not None and previous_current.direction == direction
        if is_extend:
            previous_current.status = "closed"
            self._update_structure_status(previous_current, closed_at=point.timestamp)
        event_type = "extend" if is_extend else "create"

        # 注册新结构
        self._current = struct
        self._history.append(struct)

        # 定义 1.5 锚点迭代（延续）: (A0', A1') = (P2, P3)
        self._a0 = self._fold
        self._a1 = point
        self._fold = None

        log.debug(
            "StructureEngine [%s] %s N字: P0=%.4f P1=%.4f P2=%.4f P3=%.4f",
            event_type, direction,
            struct.p0.price, struct.p1.price,
            struct.p2.price, struct.p3.price,
        )

        self._persist_structure(struct)
        self._write_audit(struct, event_type, point.timestamp)

        cb = self._on_extended if is_extend else self._on_created
        if cb:
            try:
                cb(struct)
            except Exception:
                log.exception("StructureEngine 回调异常（%s）", event_type)

        return struct

    def _fire_reversal(self, point: PricePoint) -> None:
        """定义 1.4b 反转闭合 + 定义 1.5 锚点迭代。

        前置条件: σ * (A0.price − point.price) > 0 已由调用方保证。
        不创建 NStructure（对偶定理: 反转(k) ≡ 延续(k−1)）。
        标记当前结构为 reversed 并触发回调，锚点迭代为 (old_A1, point) + 方向翻转。
        """
        if self._current is not None and self._current.status == "active":
            self._current.status = "reversed"
            log.debug(
                "StructureEngine [reverse] %s N字被反转 (struct_id=%s)",
                self._current.direction, self._current.struct_id,
            )
            self._update_structure_status(self._current, closed_at=point.timestamp)
            self._write_audit(self._current, "reverse", point.timestamp)
            if self._on_reversed:
                try:
                    self._on_reversed(self._current)
                except Exception:
                    log.exception("StructureEngine 反转回调异常")

        # 定义 1.5 锚点迭代（反转）: (A0', A1') = (old_A1, point), σ 翻转
        old_a1 = self._a1
        self._a0 = old_a1
        self._a1 = point
        self._sigma = -self._sigma
        self._fold = None

        return None

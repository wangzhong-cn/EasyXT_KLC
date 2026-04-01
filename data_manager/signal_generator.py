"""
信号生成器 (SignalGenerator) + 回撤追踪器 (DrawdownTracker)

信号生成规则（100% 对应公理，无主观添加）：
  - LONG  : 上涨 N 字结构公理 1/2 确认（P3 > P1）
  - SHORT : 下跌 N 字结构公理 1/2 确认（P3 < P1）
  - EXIT  : 公理 3 反转确认（或价格触及止损位 P2）
  - HOLD  : 结构有效、尚未触达任何边界（可选心跳信号）

止损位规则（不可修改）：
  stop_loss_price = P2.price（公理定义的唯一边界锚点）
  stop_loss_distance = |trigger_price − stop_loss_price|

回撤规则（DrawdownTracker）：
  - 峰值净值 = 信号序列中历史最高净值
  - drawdown_pct = (peak - current) / peak × 100
  - max_drawdown = 历史最大 drawdown_pct
  - calmar = CAGR / max_drawdown（CAGR 需外部传入或按时间窗口近似）

用法：
    from data_manager.signal_generator import SignalGenerator, DrawdownTracker

    tracker = DrawdownTracker()
    gen = SignalGenerator(drawdown_tracker=tracker)

    engine = StructureEngine(
        on_structure_created=gen.on_structure_event,
        on_structure_extended=gen.on_structure_event,
        on_structure_reversed=gen.on_structure_event,
    )
    bars_df = ...          # close_mapped 列
    engine.scan(bars_df)

    for sig in gen.signals:
        print(sig)
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from data_manager.structure_engine import NStructure

log = logging.getLogger(__name__)


# ── 信号数据结构 ─────────────────────────────────────────────────────────────

@dataclass
class StructuredSignal:
    """单条结构化信号，完整对应 signal_structured 表的一行。"""
    signal_id: str
    structure_id: str           # FK → NStructure.struct_id → structure_analyze.id
    code: str                   # 股票代码（由外部在 SignalGenerator 初始化时传入）
    interval: str               # 周期（如 "1d"）
    signal_ts: int              # UTC 毫秒时间戳（P3 触发时刻）
    signal_type: str            # "LONG" | "SHORT" | "EXIT" | "HOLD"
    trigger_price: float        # 信号触发价（close_mapped）
    stop_loss_price: float      # = P2.price（公理锚定）
    stop_loss_distance: float   # |trigger_price − stop_loss_price|
    drawdown_pct: Optional[float] = None    # 当前回撤 % （由 DrawdownTracker 填入）
    calmar_snapshot: Optional[float] = None # Calmar 快照（由 DrawdownTracker 填入）
    remarks: str = ""

    def to_dict(self) -> dict:
        return {
            "signal_id": self.signal_id,
            "structure_id": self.structure_id,
            "code": self.code,
            "interval": self.interval,
            "signal_ts": self.signal_ts,
            "signal_type": self.signal_type,
            "trigger_price": self.trigger_price,
            "stop_loss_price": self.stop_loss_price,
            "stop_loss_distance": self.stop_loss_distance,
            "drawdown_pct": self.drawdown_pct,
            "calmar_snapshot": self.calmar_snapshot,
            "remarks": self.remarks,
        }


# ── DrawdownTracker ───────────────────────────────────────────────────────────

class DrawdownTracker:
    """回撤追踪器。

    以信号触发价格序列为净值基础，实时维护：
    - 当前回撤（drawdown_pct）
    - 历史最大回撤（max_drawdown_pct）
    - Calmar 比率快照（= CAGR / max_drawdown，CAGR 由外部传入）

    注意：此处使用信号触发价格作为净值代理，而非账户净值。
    精确的账户净值回撤需接入实盘资金曲线。当前实现为结构层回撤估算。
    """

    def __init__(self) -> None:
        self._peak: float = 0.0
        self._trough: float = float("inf")
        self._max_drawdown_pct: float = 0.0
        self._first_price: float = 0.0
        self._first_ts: int = 0        # UTC 毫秒
        self._last_ts: int = 0         # UTC 毫秒
        self._last_price: float = 0.0

    @property
    def peak(self) -> float:
        return self._peak

    @property
    def max_drawdown_pct(self) -> float:
        """历史最大回撤百分比（正数，如 15.3 表示 15.3%）。"""
        return self._max_drawdown_pct

    def update(self, price: float, ts: int) -> float:
        """推入新价格，返回当前回撤百分比（正数）。"""
        if price <= 0:
            return self._max_drawdown_pct

        if self._peak == 0.0:
            self._peak = price
            self._first_price = price
            self._first_ts = ts
            self._last_price = price
            self._last_ts = ts
            return 0.0

        self._last_price = price
        self._last_ts = ts

        if price > self._peak:
            self._peak = price
            self._trough = price  # 重置谷底（开始新峰谷周期）

        current_dd = (self._peak - price) / self._peak * 100.0 if self._peak > 0 else 0.0
        if current_dd > self._max_drawdown_pct:
            self._max_drawdown_pct = current_dd

        return current_dd

    def calmar(
        self,
        cagr: Optional[float] = None,
        annual_return_pct: Optional[float] = None,
    ) -> Optional[float]:
        """Calmar 比率 = CAGR / max_drawdown。

        参数：
            cagr: 年化收益率（小数，如 0.25 表示 25%）。
            annual_return_pct: 年化收益率（百分比形式，如 25.0 表示 25%）。
            两者传其一即可；均为 None 时，从 first_price/last_price/时间跨度自动估算。

        返回：
            Calmar 比率（如 2.5）；max_drawdown = 0 或数据不足时返回 None。
        """
        if self._max_drawdown_pct <= 0:
            return None

        if cagr is not None:
            return (cagr * 100.0) / self._max_drawdown_pct
        if annual_return_pct is not None:
            return annual_return_pct / self._max_drawdown_pct

        # 自动估算 CAGR（需要足够长的时间跨度）
        if self._first_ts <= 0 or self._last_ts <= self._first_ts or self._first_price <= 0:
            return None
        elapsed_years = (self._last_ts - self._first_ts) / (1000 * 86400 * 365.25)
        if elapsed_years < 0.01:
            return None
        total_return = self._last_price / self._first_price
        estimated_cagr_pct = (math.pow(total_return, 1.0 / elapsed_years) - 1.0) * 100.0
        return estimated_cagr_pct / self._max_drawdown_pct

    def reset(self) -> None:
        """重置所有统计（用于重新扫描或单元测试）。"""
        self.__init__()


# ── SignalGenerator ───────────────────────────────────────────────────────────

class SignalGenerator:
    """结构化信号生成器。

    与 StructureEngine 解耦：通过回调或直接调用 ``on_structure_event()`` 接收
    结构事件，输出 StructuredSignal 列表，并写入 ``signal_structured`` DuckDB 表。

    参数：
        code: 股票代码（如 "000001.SZ"）。
        interval: 周期（如 "1d"、"5m"）。
        drawdown_tracker: DrawdownTracker 实例。若为 None 则自动创建。
        db_manager: DuckDB 连接管理器。若为 None，信号只存内存，不写库。
    """

    def __init__(
        self,
        code: str = "",
        interval: str = "1d",
        drawdown_tracker: Optional[DrawdownTracker] = None,
        db_manager=None,  # DuckDBConnectionManager | None
    ) -> None:
        self.code = code
        self.interval = interval
        self._tracker = drawdown_tracker or DrawdownTracker()
        self._db = db_manager
        self._signals: list[StructuredSignal] = []

    @property
    def signals(self) -> list[StructuredSignal]:
        """已生成的信号列表（按时间升序）。"""
        return list(self._signals)

    @property
    def drawdown_tracker(self) -> DrawdownTracker:
        return self._tracker

    # ── 主要接口 ─────────────────────────────────────────────────────────────

    def on_structure_event(self, struct: "NStructure") -> StructuredSignal:
        """接收 StructureEngine 回调，生成并记录信号。

        可直接传给 StructureEngine 的 on_structure_created/extended/reversed。
        """
        return self._generate(struct)

    def generate_from_structure(self, struct: "NStructure") -> StructuredSignal:
        """手动从结构生成信号（不依赖回调）。"""
        return self._generate(struct)

    def reset(self) -> None:
        """清空信号列表和回撤统计（用于重新运行或测试）。"""
        self._signals.clear()
        self._tracker.reset()

    # ── 内部逻辑 ─────────────────────────────────────────────────────────────

    def _generate(self, struct: "NStructure") -> StructuredSignal:
        """根据结构状态生成对应信号类型。"""
        # 信号类型映射
        if struct.status == "reversed":
            # 公理 3 触发：EXIT（结束当前方向持仓）
            signal_type = "EXIT"
        elif struct.direction == "up":
            signal_type = "LONG"
        elif struct.direction == "down":
            signal_type = "SHORT"
        else:
            signal_type = "HOLD"

        trigger_price = struct.p3.price
        signal_ts = struct.p3.timestamp

        # 更新回撤追踪
        dd_pct = self._tracker.update(trigger_price, signal_ts)
        calmar = self._tracker.calmar()

        sig = StructuredSignal(
            signal_id=uuid.uuid4().hex,
            structure_id=struct.struct_id,
            code=self.code,
            interval=self.interval,
            signal_ts=signal_ts,
            signal_type=signal_type,
            trigger_price=trigger_price,
            stop_loss_price=struct.stop_loss_price,
            stop_loss_distance=struct.stop_loss_distance,
            drawdown_pct=round(dd_pct, 4),
            calmar_snapshot=round(calmar, 4) if calmar is not None else None,
        )

        self._signals.append(sig)
        log.info(
            "SignalGenerator [%s] %s @ %.4f  止损=%.4f  回撤=%.2f%%  Calmar=%.2f",
            self.code, signal_type, trigger_price, sig.stop_loss_price,
            dd_pct, calmar or 0.0,
        )

        if self._db is not None:
            self._persist(sig)

        return sig

    def _persist(self, sig: StructuredSignal) -> None:
        """将信号持久化到 signal_structured 表。"""
        try:
            self._db.execute_write_query(
                """
                INSERT OR REPLACE INTO signal_structured
                  (id, structure_id, code, interval, signal_ts, signal_type,
                   trigger_price, stop_loss_price, stop_loss_distance,
                   drawdown_pct, calmar_snapshot, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sig.signal_id,
                    sig.structure_id,
                    sig.code,
                    sig.interval,
                    sig.signal_ts,
                    sig.signal_type,
                    sig.trigger_price,
                    sig.stop_loss_price,
                    sig.stop_loss_distance,
                    sig.drawdown_pct,
                    sig.calmar_snapshot,
                    sig.remarks,
                ),
            )
        except Exception:
            log.exception("SignalGenerator._persist 写库失败 (signal_id=%s)", sig.signal_id)

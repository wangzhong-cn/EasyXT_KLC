from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TwapPlan:
    total_volume: int
    slices: int
    min_lot: int = 100

    def build(self) -> list[int]:
        if self.total_volume <= 0 or self.slices <= 0:
            return []
        base = self.total_volume // self.slices
        vols = [base for _ in range(self.slices)]
        remainder = self.total_volume - base * self.slices
        for i in range(remainder):
            vols[i % self.slices] += 1
        if self.min_lot > 1:
            rounded: list[int] = []
            carry = 0
            for i, v in enumerate(vols):
                v += carry
                if i == len(vols) - 1:
                    q = (v // self.min_lot) * self.min_lot
                    rounded.append(q)
                    carry = v - q
                else:
                    q = (v // self.min_lot) * self.min_lot
                    rounded.append(q)
                    carry = v - q
            if carry > 0 and rounded:
                rounded[-1] += carry
            vols = rounded
        return [v for v in vols if v > 0]


@dataclass(frozen=True)
class VwapPlan:
    total_volume: int
    profile: list[float]
    min_lot: int = 100

    def build(self) -> list[int]:
        if self.total_volume <= 0:
            return []
        profile = [max(float(x), 0.0) for x in self.profile]
        if not profile:
            return []
        total = sum(profile)
        if total <= 0:
            return []
        raw = [self.total_volume * (p / total) for p in profile]
        vols = [int(v) for v in raw]
        remain = self.total_volume - sum(vols)
        if remain > 0:
            order = sorted(range(len(raw)), key=lambda i: (raw[i] - vols[i]), reverse=True)
            for i in range(remain):
                vols[order[i % len(order)]] += 1
        if self.min_lot > 1:
            rounded: list[int] = []
            carry = 0
            for i, v in enumerate(vols):
                v += carry
                q = (v // self.min_lot) * self.min_lot if i < len(vols) - 1 else v
                rounded.append(q)
                carry = v - q
            vols = rounded
        return [v for v in vols if v > 0]

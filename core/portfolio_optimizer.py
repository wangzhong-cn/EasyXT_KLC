"""
投资组合优化模块

提供多种组合权重优化算法：
  - equal_weight: 等权重分配
  - risk_parity: 风险平价（等风险贡献）
  - min_variance: 最小方差
  - mean_variance: 均值方差（最大夏普比率近似）

用法示例::

    from core.portfolio_optimizer import PortfolioOptimizeConfig, PortfolioOptimizer

    opt = PortfolioOptimizer(PortfolioOptimizeConfig(method="risk_parity", max_weight=0.4))
    weights = opt.optimize(returns_df)          # dict[str, float]
    result  = opt.optimize_result(returns_df)   # OptimizeResult
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 数据容器
# ---------------------------------------------------------------------------


@dataclass
class PortfolioOptimizeConfig:
    """优化配置参数。"""

    method: str = "equal_weight"
    """优化方法：equal_weight | risk_parity | min_variance | mean_variance"""

    max_weight: float = 1.0
    """单资产最大权重上限（0, 1]"""

    min_weight: float = 0.0
    """单资产最小权重下限 [0, max_weight)"""

    risk_free_rate: float = 0.0
    """无风险收益率（用于夏普比率计算）"""


@dataclass
class OptimizeResult:
    """组合优化结果。"""

    weights: Dict[str, float]
    """各资产权重，合计应为 1.0（不可行时为空字典）"""

    portfolio_vol: float
    """组合波动率（年化后的日波动率）"""

    risk_contributions: Dict[str, float]
    """各资产风险贡献（比例，合计 1.0）"""

    expected_return: float
    """预期收益（样本均值加权）"""

    method: str
    """使用的优化方法"""

    feasible: bool
    """是否求解成功"""

    solver: str = "internal"
    """求解器标识"""

    status: str = "optimal"
    """求解状态描述"""


# ---------------------------------------------------------------------------
# 优化器
# ---------------------------------------------------------------------------


class PortfolioOptimizer:
    """投资组合权重优化器。"""

    def __init__(self, config: Optional[PortfolioOptimizeConfig] = None) -> None:
        self.config = config or PortfolioOptimizeConfig()

    # ------------------------------------------------------------------
    # 公共接口
    # ------------------------------------------------------------------

    def optimize(self, returns: pd.DataFrame) -> Dict[str, float]:
        """
        计算最优权重字典。

        Args:
            returns: 形状 (T, N) 的日收益率 DataFrame，列名为资产代码。

        Returns:
            {code: weight} 字典，权重合计为 1.0；不可行时返回等权结果。
        """
        result = self.optimize_result(returns)
        if not result.feasible:
            # 降级为等权
            n = len(returns.columns)
            w = 1.0 / n if n > 0 else 0.0
            return {c: w for c in returns.columns}
        return result.weights

    def optimize_result(self, returns: pd.DataFrame) -> OptimizeResult:
        """
        进行完整优化并返回 OptimizeResult。

        Args:
            returns: 形状 (T, N) 的日收益率 DataFrame。

        Returns:
            OptimizeResult 实例。
        """
        # 清洗数据
        clean = returns.replace([np.inf, -np.inf], np.nan).dropna(how="any")
        codes = list(returns.columns)
        n = len(codes)

        if n == 0 or len(clean) < 5:
            return OptimizeResult(
                weights={},
                portfolio_vol=0.0,
                risk_contributions={},
                expected_return=0.0,
                method=self.config.method,
                feasible=False,
                solver="none",
                status="empty_returns",
            )

        cfg = self.config
        method = cfg.method
        bounds_tuple = [(cfg.min_weight, cfg.max_weight)] * n

        if method == "equal_weight":
            raw_weights = self._equal_weight(n)
            solver = "equal_weight"
        elif method == "risk_parity":
            raw_weights, solver = self._risk_parity(clean.values, n, bounds_tuple)
        elif method == "min_variance":
            raw_weights, solver = self._min_variance(clean.values, n, bounds_tuple)
        elif method == "mean_variance":
            raw_weights, solver = self._mean_variance(clean.values, n, bounds_tuple)
        else:
            log.warning("未知优化方法 '%s'，降级为 equal_weight", method)
            raw_weights = self._equal_weight(n)
            solver = "equal_weight"

        # equal_weight 仍需裁剪；scipy 方法已经满足 bounds，但做最终精确裁剪无害
        clipped = self._clip_and_normalize(raw_weights, cfg.min_weight, cfg.max_weight, n)
        weights = dict(zip(codes, clipped))

        # 计算组合指标
        port_vol, risk_contribs = self._portfolio_metrics(clean.values, clipped, codes)
        exp_ret = float(np.dot(clipped, clean.mean().values))

        return OptimizeResult(
            weights=weights,
            portfolio_vol=port_vol,
            risk_contributions=dict(zip(codes, risk_contribs)),
            expected_return=exp_ret,
            method=method,
            feasible=True,
            solver=solver,
            status="optimal",
        )

    # ------------------------------------------------------------------
    # 算法实现
    # ------------------------------------------------------------------

    @staticmethod
    def _equal_weight(n: int) -> np.ndarray:
        return np.full(n, 1.0 / n)

    @staticmethod
    def _risk_parity(
        returns_arr: np.ndarray,
        n: int,
        bounds: list | None = None,
    ) -> tuple[np.ndarray, str]:
        """风险平价：使各资产风险贡献相等。使用 scipy.minimize 支持 bounds。"""
        cov = np.cov(returns_arr.T)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        minimize = PortfolioOptimizer._try_get_scipy_minimize()
        if minimize is None:
            return PortfolioOptimizer._fallback_risk_parity(cov, n), "internal_fallback"
        w0 = np.full(n, 1.0 / n)

        def objective(w: np.ndarray) -> float:
            sigma_p = math.sqrt(max(float(w @ cov @ w), 1e-20))
            rc = w * (cov @ w) / sigma_p
            target = sigma_p / n
            return float(np.sum((rc - target) ** 2))

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bnds = bounds if bounds is not None else [(0.0, 1.0)] * n
        res = minimize(objective, w0, method="SLSQP", bounds=bnds, constraints=constraints,
                       options={"ftol": 1e-12, "maxiter": 500})
        if getattr(res, "success", False) and getattr(res, "x", None) is not None:
            w = np.array(res.x, dtype=float)
            if np.all(np.isfinite(w)):
                w = np.maximum(w, 0.0)
                return w, "scipy_slsqp"
        return PortfolioOptimizer._fallback_risk_parity(cov, n), "internal_fallback"

    @staticmethod
    def _min_variance(
        returns_arr: np.ndarray,
        n: int,
        bounds: list | None = None,
    ) -> tuple[np.ndarray, str]:
        """最小方差：使用 scipy.minimize 支持 bounds。"""
        cov = np.cov(returns_arr.T)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        minimize = PortfolioOptimizer._try_get_scipy_minimize()
        if minimize is None:
            return PortfolioOptimizer._fallback_min_variance(cov, n), "internal_fallback"
        w0 = np.full(n, 1.0 / n)

        def objective(w: np.ndarray) -> float:
            return float(w @ cov @ w)

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bnds = bounds if bounds is not None else [(0.0, 1.0)] * n
        res = minimize(objective, w0, method="SLSQP", bounds=bnds, constraints=constraints,
                       options={"ftol": 1e-12, "maxiter": 500})
        if getattr(res, "success", False) and getattr(res, "x", None) is not None:
            w = np.array(res.x, dtype=float)
            if np.all(np.isfinite(w)):
                w = np.maximum(w, 0.0)
                return w, "scipy_slsqp"
        return PortfolioOptimizer._fallback_min_variance(cov, n), "internal_fallback"

    @staticmethod
    def _mean_variance(
        returns_arr: np.ndarray,
        n: int,
        bounds: list | None = None,
    ) -> tuple[np.ndarray, str]:
        """均值方差（最大化夏普比率近似）：使用 scipy.minimize 支持 bounds。"""
        cov = np.cov(returns_arr.T)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        mu = returns_arr.mean(axis=0)
        minimize = PortfolioOptimizer._try_get_scipy_minimize()
        if minimize is None:
            return PortfolioOptimizer._fallback_mean_variance(mu, cov, n), "internal_fallback"
        w0 = np.full(n, 1.0 / n)

        def neg_sharpe(w: np.ndarray) -> float:
            port_ret = float(np.dot(w, mu))
            port_var = float(w @ cov @ w)
            port_vol = math.sqrt(max(port_var, 1e-20))
            return -port_ret / port_vol

        constraints = [{"type": "eq", "fun": lambda w: np.sum(w) - 1.0}]
        bnds = bounds if bounds is not None else [(0.0, 1.0)] * n
        res = minimize(neg_sharpe, w0, method="SLSQP", bounds=bnds, constraints=constraints,
                       options={"ftol": 1e-12, "maxiter": 500})
        if getattr(res, "success", False) and getattr(res, "x", None) is not None:
            w = np.array(res.x, dtype=float)
            if np.all(np.isfinite(w)):
                w = np.maximum(w, 0.0)
                return w, "scipy_slsqp"
        return PortfolioOptimizer._fallback_mean_variance(mu, cov, n), "internal_fallback"

    @staticmethod
    def _try_get_scipy_minimize():
        try:
            from scipy.optimize import minimize

            return minimize
        except Exception as exc:
            log.warning("SciPy.optimize.minimize 不可用，降级到内部启发式优化: %s", exc)
            return None

    @staticmethod
    def _fallback_risk_parity(cov: np.ndarray, n: int) -> np.ndarray:
        diag = np.diag(cov) if cov.ndim == 2 else np.array([float(cov)])
        vol = np.sqrt(np.maximum(diag, 1e-12))
        inv_vol = 1.0 / np.maximum(vol, 1e-12)
        if not np.all(np.isfinite(inv_vol)) or float(inv_vol.sum()) <= 0:
            return np.full(n, 1.0 / n)
        return inv_vol / inv_vol.sum()

    @staticmethod
    def _fallback_min_variance(cov: np.ndarray, n: int) -> np.ndarray:
        diag = np.diag(cov) if cov.ndim == 2 else np.array([float(cov)])
        inv_var = 1.0 / np.maximum(diag, 1e-12)
        if not np.all(np.isfinite(inv_var)) or float(inv_var.sum()) <= 0:
            return np.full(n, 1.0 / n)
        return inv_var / inv_var.sum()

    @staticmethod
    def _fallback_mean_variance(mu: np.ndarray, cov: np.ndarray, n: int) -> np.ndarray:
        diag = np.diag(cov) if cov.ndim == 2 else np.array([float(cov)])
        score = np.maximum(mu, 0.0) / np.maximum(diag, 1e-12)
        if not np.all(np.isfinite(score)) or float(score.sum()) <= 0:
            score = 1.0 / np.maximum(diag, 1e-12)
        if not np.all(np.isfinite(score)) or float(score.sum()) <= 0:
            return np.full(n, 1.0 / n)
        return score / score.sum()

    @staticmethod
    def _clip_and_normalize(
        w: np.ndarray,
        min_w: float,
        max_w: float,
        n: int,
        max_iter: int = 100,
    ) -> np.ndarray:
        """投影到 box + simplex：min_w <= w_i <= max_w 且 sum(w)=1。"""
        if n <= 0:
            return np.array([], dtype=float)
        if min_w * n > 1.0 + 1e-12 or max_w * n < 1.0 - 1e-12:
            return np.full(n, 1.0 / n)

        w = np.asarray(w, dtype=float).copy()
        if w.shape[0] != n or not np.all(np.isfinite(w)):
            w = np.full(n, 1.0 / n)

        base = np.full(n, min_w, dtype=float)
        caps = np.full(n, max_w - min_w, dtype=float)
        remaining = 1.0 - float(base.sum())
        if remaining <= 1e-15:
            return base

        score = np.maximum(w - min_w, 0.0)
        if float(score.sum()) <= 1e-15:
            score = np.ones(n, dtype=float)

        alloc = np.zeros(n, dtype=float)
        active = np.ones(n, dtype=bool)

        for _ in range(max_iter):
            if remaining <= 1e-12 or not active.any():
                break
            active_score = score * active
            if float(active_score.sum()) <= 1e-15:
                active_score = active.astype(float)
            share = remaining * active_score / float(active_score.sum())
            headroom = np.maximum(caps - alloc, 0.0)
            clipped_share = np.minimum(share, headroom)
            alloc += clipped_share
            remaining = 1.0 - float((base + alloc).sum())
            active = headroom - clipped_share > 1e-12

        w = base + alloc
        if remaining > 1e-12:
            active = w < max_w - 1e-12
            if active.any():
                w[active] += remaining / int(active.sum())

        w = np.clip(w, min_w, max_w)
        delta = 1.0 - float(w.sum())
        if abs(delta) > 1e-12:
            if delta > 0:
                candidates = np.where(w < max_w - 1e-12)[0]
            else:
                candidates = np.where(w > min_w + 1e-12)[0]
            if len(candidates) == 0:
                return np.full(n, 1.0 / n)
            idx = int(candidates[np.argmax(w[candidates])])
            w[idx] = np.clip(w[idx] + delta, min_w, max_w)
        final_delta = 1.0 - float(w.sum())
        if abs(final_delta) > 1e-9:
            candidates = np.where((w > min_w + 1e-12) & (w < max_w - 1e-12))[0]
            if len(candidates) > 0:
                idx = int(candidates[np.argmax(w[candidates])])
                w[idx] += final_delta
        return w

    @staticmethod
    def _portfolio_metrics(
        returns_arr: np.ndarray,
        w: np.ndarray,
        codes: list,
    ) -> tuple[float, np.ndarray]:
        """计算组合波动率及风险贡献。"""
        n = len(codes)
        cov = np.cov(returns_arr.T)
        if cov.ndim == 0:
            cov = np.array([[float(cov)]])
        port_var = float(w @ cov @ w)
        port_vol = math.sqrt(max(port_var, 0.0))
        if port_vol < 1e-12:
            rc = np.full(n, 1.0 / n)
        else:
            marginal = cov @ w
            abs_rc = w * marginal
            total_rc = abs_rc.sum()
            rc = abs_rc / total_rc if total_rc > 0 else np.full(n, 1.0 / n)
        return port_vol, rc

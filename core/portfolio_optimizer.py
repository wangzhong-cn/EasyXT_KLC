from __future__ import annotations

import importlib
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy.optimize import minimize


@dataclass(frozen=True)
class PortfolioOptimizeConfig:
    method: str = "risk_parity"
    max_weight: float = 0.3
    min_weight: float = 0.0
    annualize_factor: int = 252
    risk_aversion: float = 3.0
    solver: str = "auto"


@dataclass(frozen=True)
class OptimizeResult:
    weights: dict[str, float]
    portfolio_vol: float
    risk_contributions: dict[str, float]
    expected_return: float
    method: str
    feasible: bool
    solver: str
    status: str = ""
    warnings: list[str] = field(default_factory=list)


class PortfolioOptimizer:
    def __init__(self, config: PortfolioOptimizeConfig | None = None) -> None:
        self.config = config or PortfolioOptimizeConfig()

    def optimize(self, returns: pd.DataFrame) -> dict[str, float]:
        return self.optimize_result(returns).weights

    def optimize_result(self, returns: pd.DataFrame) -> OptimizeResult:
        clean = self._sanitize_returns(returns)
        if clean.empty:
            return OptimizeResult(
                weights={},
                portfolio_vol=0.0,
                risk_contributions={},
                expected_return=0.0,
                method=(self.config.method or "risk_parity").lower().strip(),
                feasible=False,
                solver="none",
                status="empty_returns",
                warnings=["returns 输入为空或全部无效"],
            )
        cov_daily = self._cov(clean)
        mu_daily = clean.mean(axis=0).to_numpy(dtype=float)
        mu_annual = mu_daily * self.config.annualize_factor
        cov_annual = cov_daily * self.config.annualize_factor
        method = (self.config.method or "risk_parity").lower().strip()
        warnings: list[str] = []
        status = "ok"
        feasible = True
        solver = "analytic"
        solver_feasible = False  # 求解器是否给出了可行解
        if method == "equal_weight":
            raw = np.ones(clean.shape[1], dtype=float)
        elif method == "mean_variance":
            raw, feasible, solver, status = self._mean_variance_qp(mu_annual, cov_annual)
            solver_feasible = feasible
            if not feasible:
                warnings.append("mean_variance 求解器未找到可行解，已回退等权重")
                raw = np.ones(clean.shape[1], dtype=float)
        elif method == "min_variance":
            raw, feasible, solver, status = self._min_variance_qp(cov_annual)
            solver_feasible = feasible
            if not feasible:
                warnings.append("min_variance 求解器未找到可行解，已回退等权重")
                raw = np.ones(clean.shape[1], dtype=float)
        else:
            raw, feasible, solver, status = self._risk_parity_solver(cov_annual)
            solver_feasible = feasible
            if not feasible:
                warnings.append("risk_parity 求解失败，已回退逆波动近似")
                raw = self._risk_parity_inverse_vol(clean)
        # 求解器已在约束内求解时直接归一化；仅对等权/回退路径做完整重投影
        if solver_feasible:
            w_arr = np.asarray(raw, dtype=float)
            w_arr = np.clip(w_arr, 0.0, None)
            total = float(w_arr.sum())
            weights = w_arr / total if total > 0 else np.ones(len(w_arr)) / len(w_arr)
        else:
            weights = self._project_weights(raw)
        weight_map = {col: float(weights[i]) for i, col in enumerate(clean.columns)}
        portfolio_vol, rc_pct, expected_return = self._compute_metrics(weights, mu_annual, cov_annual, clean.columns.tolist())
        return OptimizeResult(
            weights=weight_map,
            portfolio_vol=portfolio_vol,
            risk_contributions=rc_pct,
            expected_return=expected_return,
            method=method,
            feasible=feasible,
            solver=solver,
            status=status,
            warnings=warnings,
        )

    def _sanitize_returns(self, returns: pd.DataFrame) -> pd.DataFrame:
        if returns is None or returns.empty:
            return pd.DataFrame()
        clean = returns.copy()
        clean = clean.replace([np.inf, -np.inf], np.nan).dropna(how="all")
        clean = clean.fillna(0.0)
        keep = clean.std(axis=0, ddof=0) > 0
        clean = clean.loc[:, keep]
        return clean

    def _cov(self, returns: pd.DataFrame) -> np.ndarray:
        cov = returns.cov().values.astype(float)
        if cov.size == 0:
            return cov
        cov = cov + np.eye(cov.shape[0]) * 1e-8
        return cov

    def _risk_parity_inverse_vol(self, returns: pd.DataFrame) -> np.ndarray:
        vol = returns.std(axis=0, ddof=0).to_numpy(dtype=float)
        vol = np.where(vol <= 1e-12, 1e-12, vol)
        return 1.0 / vol

    def _mean_variance_qp(self, mu_annual: np.ndarray, cov_annual: np.ndarray) -> tuple[np.ndarray, bool, str, str]:
        return self._solve_qp(cov_annual=cov_annual, mu_annual=mu_annual, include_return=True)

    def _min_variance_qp(self, cov_annual: np.ndarray) -> tuple[np.ndarray, bool, str, str]:
        return self._solve_qp(cov_annual=cov_annual, mu_annual=np.zeros(cov_annual.shape[0]), include_return=False)

    def _solve_qp(
        self, cov_annual: np.ndarray, mu_annual: np.ndarray, include_return: bool
    ) -> tuple[np.ndarray, bool, str, str]:
        n = cov_annual.shape[0]
        lb = self.config.min_weight
        ub = max(self.config.max_weight, lb)
        if self.config.solver in ("auto", "cvxpy"):
            try:
                cp = importlib.import_module("cvxpy")

                w = cp.Variable(n)
                risk = cp.quad_form(w, cov_annual)
                if include_return:
                    obj = cp.Minimize(risk - self.config.risk_aversion * (mu_annual @ w))
                else:
                    obj = cp.Minimize(risk)
                constraints = [cp.sum(w) == 1, w >= lb, w <= ub]
                prob = cp.Problem(obj, constraints)
                prob.solve(solver=cp.OSQP)
                if w.value is not None and prob.status in ("optimal", "optimal_inaccurate"):
                    return np.asarray(w.value, dtype=float), True, "cvxpy_osqp", str(prob.status)
            except Exception:
                pass
        x0 = np.ones(n, dtype=float) / n
        bounds = [(lb, ub) for _ in range(n)]
        constraints = ({"type": "eq", "fun": lambda x: np.sum(x) - 1.0},)

        def objective(x: np.ndarray) -> float:
            risk = float(x @ cov_annual @ x)
            if include_return:
                return risk - self.config.risk_aversion * float(mu_annual @ x)
            return risk

        res = minimize(objective, x0=x0, method="SLSQP", bounds=bounds, constraints=constraints)
        if res.success and res.x is not None:
            return np.asarray(res.x, dtype=float), True, "scipy_slsqp", str(res.status)
        return x0, False, "scipy_slsqp", str(getattr(res, "message", "failed"))

    def _risk_parity_solver(self, cov_annual: np.ndarray) -> tuple[np.ndarray, bool, str, str]:
        n = cov_annual.shape[0]
        lb = self.config.min_weight
        ub = max(self.config.max_weight, lb)
        x0 = np.ones(n, dtype=float) / n
        bounds = [(lb, ub) for _ in range(n)]
        constraints = ({"type": "eq", "fun": lambda x: np.sum(x) - 1.0},)

        def objective(x: np.ndarray) -> float:
            port_var = float(x @ cov_annual @ x)
            if port_var <= 1e-18:
                return 1e6
            mrc = cov_annual @ x
            rc = x * mrc
            target = port_var / n
            return float(np.sum((rc - target) ** 2))

        res = minimize(objective, x0=x0, method="SLSQP", bounds=bounds, constraints=constraints)
        if res.success and res.x is not None:
            return np.asarray(res.x, dtype=float), True, "scipy_slsqp", str(res.status)
        return self._risk_parity_inverse_vol(pd.DataFrame(np.eye(n))), False, "scipy_slsqp", str(getattr(res, "message", "failed"))

    def _compute_metrics(
        self, weights: np.ndarray, mu_annual: np.ndarray, cov_annual: np.ndarray, symbols: list[str]
    ) -> tuple[float, dict[str, float], float]:
        port_var = float(weights @ cov_annual @ weights)
        port_var = max(port_var, 0.0)
        port_vol = float(np.sqrt(port_var))
        expected_return = float(mu_annual @ weights)
        if port_var <= 1e-18:
            rc_pct = {s: 0.0 for s in symbols}
            return port_vol, rc_pct, expected_return
        mrc = cov_annual @ weights
        rc_abs = weights * mrc
        rc_pct = {symbols[i]: float(rc_abs[i] / port_var) for i in range(len(symbols))}
        return port_vol, rc_pct, expected_return

    def _project_weights(self, raw: np.ndarray) -> np.ndarray:
        n = len(raw)
        if n == 0:
            return raw
        w = np.array(raw, dtype=float)
        w[np.isnan(w)] = 0.0
        if np.allclose(w.sum(), 0.0):
            w = np.ones(n, dtype=float)
        w = np.maximum(w, self.config.min_weight)
        upper = max(self.config.max_weight, self.config.min_weight)
        w = np.minimum(w, upper)
        total = float(w.sum())
        if total <= 0:
            return np.ones(n, dtype=float) / n
        w = w / total
        for _ in range(5):
            over = w > upper
            if not np.any(over):
                break
            excess = float((w[over] - upper).sum())
            w[over] = upper
            under = ~over
            if np.any(under):
                under_sum = float(w[under].sum())
                if under_sum > 0:
                    w[under] += excess * (w[under] / under_sum)
                else:
                    w[under] += excess / max(int(np.sum(under)), 1)
            w = np.maximum(w, self.config.min_weight)
            w = np.minimum(w, upper)
            norm = float(w.sum())
            if norm > 0:
                w = w / norm
        return w

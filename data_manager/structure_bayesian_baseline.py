"""Layer 4 MVP：结构级 Bayesian baseline。

设计目标：
  - 基于 ``StructureDatasetBuilder`` 产出的离线结构语料，给每个结构分桶估计
    "延续成功概率" 的后验均值与可信区间
  - 将最小可证伪的贝叶斯结果写回 ``structure_analyze.bayes_lower/bayes_upper``
  - 保持离线、可重算、无 GUI / 无实时依赖

当前 MVP 定义：
  - 观测值 success = 1：结构当前状态为 active/closed，且最新信号属于 LONG/SHORT
  - 观测值 success = 0：结构当前状态为 reversed，或最新信号为 EXIT
  - 分桶默认键：("interval", "direction")
  - 先验：Beta(alpha_prior=1, beta_prior=1)
  - 区间：使用正态近似的后验 95% 区间（deterministic, dependency-light）

注意：
  - 这是 Layer 4 的 baseline，不是最终的非参数贝叶斯模型
  - 其价值在于：为 bayes_lower/bayes_upper 提供一个可验证、可回归、可批量重算的起点
"""

from __future__ import annotations

import json
import logging
import math
from statistics import NormalDist
from typing import TYPE_CHECKING, Any, Iterable, Sequence

import pandas as pd

from data_manager.structure_dataset_builder import StructureDatasetBuilder

if TYPE_CHECKING:
    from data_manager.duckdb_connection_pool import DuckDBConnectionManager

log = logging.getLogger(__name__)


class StructureBayesianBaseline:
    """基于结构语料的最小贝叶斯基线。"""

    def __init__(
        self,
        db_manager: "DuckDBConnectionManager | None" = None,
        duckdb_path: str | None = None,
        dataset_builder: StructureDatasetBuilder | None = None,
    ) -> None:
        self._builder = dataset_builder
        self._db_manager = db_manager
        if self._builder is None and (db_manager is not None or duckdb_path is not None):
            self._builder = StructureDatasetBuilder(
                db_manager=db_manager,
                duckdb_path=duckdb_path,
            )
        if self._builder is not None:
            self._db_manager = self._builder._db_manager

    def fit(
        self,
        dataset: pd.DataFrame | None = None,
        *,
        group_by: Sequence[str] = ("interval", "direction"),
        group_strategy: str = "fixed",
        min_observations: int = 3,
        fallback_group_by_levels: Sequence[Sequence[str]] | None = None,
        alpha_prior: float = 1.0,
        beta_prior: float = 1.0,
        credible_level: float = 0.95,
    ) -> pd.DataFrame:
        """拟合分桶后验，返回每个桶的 posterior 摘要。"""
        if alpha_prior <= 0 or beta_prior <= 0:
            raise ValueError("alpha_prior 和 beta_prior 必须为正数")
        if not 0 < credible_level < 1:
            raise ValueError("credible_level 必须在 (0, 1) 区间内")
        if group_strategy not in {"fixed", "adaptive"}:
            raise ValueError("group_strategy 只能是 'fixed' 或 'adaptive'")
        if min_observations <= 0:
            raise ValueError("min_observations 必须为正整数")

        dataset = self._coerce_dataset(dataset)
        empty = self._empty_posterior(list(group_by))
        if dataset.empty:
            return empty if group_strategy == "fixed" else self._empty_adaptive_summary()

        missing = [col for col in group_by if col not in dataset.columns]
        if missing:
            raise ValueError(f"dataset 缺少 group_by 列: {missing}")

        work = dataset.copy()
        work["observation"] = work.apply(self._infer_observation, axis=1)
        observed = work.dropna(subset=["observation"])
        if observed.empty:
            return empty if group_strategy == "fixed" else self._empty_adaptive_summary()

        if group_strategy == "fixed":
            return self._fit_grouped(
                observed,
                group_by=group_by,
                alpha_prior=alpha_prior,
                beta_prior=beta_prior,
                credible_level=credible_level,
            )

        adaptive = self._annotate_dataset_adaptive(
            dataset=dataset,
            observed=observed,
            primary_group_by=group_by,
            fallback_group_by_levels=fallback_group_by_levels,
            min_observations=min_observations,
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
        if adaptive.empty:
            return self._empty_adaptive_summary()
        summary = (
            adaptive.groupby(["bayes_group_level", "bayes_group_key"], dropna=False)
            .agg(
                selected_row_count=("structure_id", "count"),
                observation_count=("observation_count", "first"),
                continuation_count=("continuation_count", "first"),
                reversal_count=("reversal_count", "first"),
                posterior_alpha=("posterior_alpha", "first"),
                posterior_beta=("posterior_beta", "first"),
                posterior_mean=("posterior_mean", "first"),
                bayes_lower=("bayes_lower", "first"),
                bayes_upper=("bayes_upper", "first"),
            )
            .reset_index()
        )
        return summary.sort_values(["bayes_group_level", "bayes_group_key"]).reset_index(drop=True)

    def annotate_dataset(
        self,
        dataset: pd.DataFrame | None = None,
        *,
        posterior: pd.DataFrame | None = None,
        group_by: Sequence[str] = ("interval", "direction"),
        group_strategy: str = "fixed",
        min_observations: int = 3,
        fallback_group_by_levels: Sequence[Sequence[str]] | None = None,
        **fit_kwargs: Any,
    ) -> pd.DataFrame:
        """把 posterior 区间回填到结构语料 DataFrame。"""
        dataset = self._coerce_dataset(dataset)
        if dataset.empty:
            return dataset.copy()
        if group_strategy == "adaptive":
            work = dataset.copy()
            work["observation"] = work.apply(self._infer_observation, axis=1)
            observed = work.dropna(subset=["observation"])
            if observed.empty:
                return dataset.copy()
            return self._annotate_dataset_adaptive(
                dataset=dataset,
                observed=observed,
                primary_group_by=group_by,
                fallback_group_by_levels=fallback_group_by_levels,
                min_observations=min_observations,
                alpha_prior=float(fit_kwargs.get("alpha_prior", 1.0)),
                beta_prior=float(fit_kwargs.get("beta_prior", 1.0)),
                credible_level=float(fit_kwargs.get("credible_level", 0.95)),
            )

        if posterior is None:
            posterior = self.fit(dataset, group_by=group_by, group_strategy="fixed", **fit_kwargs)
        if posterior.empty:
            return dataset.copy()
        merge_cols = list(group_by)
        work = dataset.drop(
            columns=[
                col
                for col in (
                    "posterior_mean",
                    "bayes_lower",
                    "bayes_upper",
                    "observation_count",
                    "continuation_count",
                    "reversal_count",
                    "posterior_alpha",
                    "posterior_beta",
                    "bayes_group_level",
                    "bayes_group_key",
                )
                if col in dataset.columns
            ],
            errors="ignore",
        )
        return work.merge(
            posterior[
                merge_cols
                + [
                    "posterior_mean",
                    "bayes_lower",
                    "bayes_upper",
                    "observation_count",
                    "continuation_count",
                    "reversal_count",
                    "posterior_alpha",
                    "posterior_beta",
                ]
            ],
            on=merge_cols,
            how="left",
        )

    def writeback_structure_bounds(
        self,
        dataset: pd.DataFrame | None = None,
        *,
        posterior: pd.DataFrame | None = None,
        group_by: Sequence[str] = ("interval", "direction"),
        group_strategy: str = "fixed",
        min_observations: int = 3,
        fallback_group_by_levels: Sequence[Sequence[str]] | None = None,
        **fit_kwargs: Any,
    ) -> int:
        """将 bayes_lower / bayes_upper 写回 structure_analyze。返回更新行数。"""
        if self._db_manager is None:
            raise ValueError("writeback_structure_bounds 需要可写 db_manager")
        annotated = self.annotate_dataset(
            dataset,
            posterior=posterior,
            group_by=group_by,
            group_strategy=group_strategy,
            min_observations=min_observations,
            fallback_group_by_levels=fallback_group_by_levels,
            **fit_kwargs,
        )
        if annotated.empty:
            return 0

        updated = 0
        for row in annotated.itertuples(index=False):
            lower = getattr(row, "bayes_lower", None)
            upper = getattr(row, "bayes_upper", None)
            if lower is None or upper is None:
                continue
            self._db_manager.execute_write_query(
                "UPDATE structure_analyze SET bayes_lower = ?, bayes_upper = ? WHERE id = ?",
                (float(lower), float(upper), getattr(row, "structure_id")),
            )
            updated += 1
        log.info("StructureBayesianBaseline 写回完成: updated=%d", updated)
        return updated

    def _coerce_dataset(self, dataset: pd.DataFrame | None) -> pd.DataFrame:
        if dataset is not None:
            return dataset.copy()
        if self._builder is None:
            raise ValueError("dataset 为空时需要提供 dataset_builder 或 db_manager")
        return self._builder.build_dataset()

    @staticmethod
    def build_runtime_dataset(
        structures: Sequence[Any],
        signals: Sequence[Any],
        *,
        code: str = "",
        interval: str = "",
    ) -> pd.DataFrame:
        """从内存中的结构/信号对象构建轻量语料，用于 GUI/交互式实验。"""
        latest_signal_by_structure: dict[str, Any] = {}
        for sig in signals:
            structure_id = getattr(sig, "structure_id", "")
            if not structure_id:
                continue
            current = latest_signal_by_structure.get(structure_id)
            current_key = (
                getattr(current, "signal_ts", -1),
                1 if getattr(current, "signal_type", "") == "EXIT" else 0,
                getattr(current, "signal_id", ""),
            ) if current is not None else (-1, -1, "")
            candidate_key = (
                getattr(sig, "signal_ts", -1),
                1 if getattr(sig, "signal_type", "") == "EXIT" else 0,
                getattr(sig, "signal_id", ""),
            )
            if candidate_key >= current_key:
                latest_signal_by_structure[structure_id] = sig

        rows: list[dict[str, Any]] = []
        for struct in structures:
            latest = latest_signal_by_structure.get(getattr(struct, "struct_id", ""))
            rows.append(
                {
                    "structure_id": getattr(struct, "struct_id", None),
                    "code": code,
                    "interval": interval,
                    "direction": getattr(struct, "direction", None),
                    "status": getattr(struct, "status", None),
                    "latest_signal_type": getattr(latest, "signal_type", None),
                    "latest_signal_ts": getattr(latest, "signal_ts", None),
                    "bayes_lower": getattr(struct, "bayes_lower", None),
                    "bayes_upper": getattr(struct, "bayes_upper", None),
                }
            )
        return pd.DataFrame(rows)

    def annotate_structure_objects(
        self,
        structures: Sequence[Any],
        signals: Sequence[Any],
        *,
        code: str = "",
        interval: str = "",
        group_by: Sequence[str] = ("interval", "direction"),
        group_strategy: str = "fixed",
        min_observations: int = 3,
        fallback_group_by_levels: Sequence[Sequence[str]] | None = None,
        **fit_kwargs: Any,
    ) -> list[Any]:
        """将 Bayesian 区间回填到内存结构对象，适用于 GUI 展示。"""
        dataset = self.build_runtime_dataset(structures, signals, code=code, interval=interval)
        annotated = self.annotate_dataset(
            dataset=dataset,
            group_by=group_by,
            group_strategy=group_strategy,
            min_observations=min_observations,
            fallback_group_by_levels=fallback_group_by_levels,
            **fit_kwargs,
        )
        annotated_by_id = {
            row["structure_id"]: row for row in annotated.to_dict(orient="records")
        }
        for struct in structures:
            row = annotated_by_id.get(getattr(struct, "struct_id", None))
            if row is None:
                continue
            setattr(struct, "bayes_lower", row.get("bayes_lower"))
            setattr(struct, "bayes_upper", row.get("bayes_upper"))
            setattr(struct, "posterior_mean", row.get("posterior_mean"))
            setattr(struct, "observation_count", row.get("observation_count"))
            setattr(struct, "continuation_count", row.get("continuation_count"))
            setattr(struct, "reversal_count", row.get("reversal_count"))
            setattr(struct, "bayes_group_level", row.get("bayes_group_level"))
            setattr(struct, "bayes_group_key", row.get("bayes_group_key"))
        return list(structures)

    @staticmethod
    def summarize_annotated_dataset(annotated: pd.DataFrame | None) -> pd.DataFrame:
        """对已注解语料做 Layer 4 摘要，便于 API / GUI 展示。"""
        if annotated is None or annotated.empty:
            return pd.DataFrame(
                columns=[
                    "bayes_group_level",
                    "bucket_count",
                    "structure_count",
                    "mean_posterior_mean",
                    "mean_observation_count",
                    "mean_retrace_ratio",
                    "reversed_ratio",
                    "mean_audit_event_count",
                    "mean_extend_event_count",
                    "mean_reverse_event_count",
                ]
            )

        work = annotated.copy()
        if "bayes_group_level" not in work.columns:
            work["bayes_group_level"] = "fixed"
        if "bayes_group_key" not in work.columns:
            work["bayes_group_key"] = "{}"
        work["is_reversed"] = (work.get("status") == "reversed").astype(float)
        grouped = (
            work.groupby("bayes_group_level", dropna=False)
            .agg(
                bucket_count=("bayes_group_key", "nunique"),
                structure_count=("structure_id", "count"),
                mean_posterior_mean=("posterior_mean", "mean"),
                mean_observation_count=("observation_count", "mean"),
                mean_retrace_ratio=("retrace_ratio", "mean"),
                reversed_ratio=("is_reversed", "mean"),
                mean_audit_event_count=("audit_event_count", "mean"),
                mean_extend_event_count=("extend_event_count", "mean"),
                mean_reverse_event_count=("reverse_event_count", "mean"),
            )
            .reset_index()
        )
        return grouped.sort_values("bayes_group_level").reset_index(drop=True)

    @staticmethod
    def _infer_observation(row: pd.Series) -> float | None:
        signal_type = row.get("latest_signal_type")
        status = row.get("status")
        if signal_type == "EXIT" or status == "reversed":
            return 0.0
        if signal_type in {"LONG", "SHORT"} and status in {"active", "closed"}:
            return 1.0
        return None

    @staticmethod
    def iter_group_keys(posterior: pd.DataFrame, group_by: Iterable[str]) -> list[tuple[Any, ...]]:
        """便于调用方查看已拟合桶键。"""
        return [tuple(row[col] for col in group_by) for _, row in posterior.iterrows()]

    @staticmethod
    def _empty_posterior(group_by: list[str]) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                *group_by,
                "observation_count",
                "continuation_count",
                "reversal_count",
                "posterior_alpha",
                "posterior_beta",
                "posterior_mean",
                "bayes_lower",
                "bayes_upper",
            ]
        )

    @staticmethod
    def _empty_adaptive_summary() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "bayes_group_level",
                "bayes_group_key",
                "selected_row_count",
                "observation_count",
                "continuation_count",
                "reversal_count",
                "posterior_alpha",
                "posterior_beta",
                "posterior_mean",
                "bayes_lower",
                "bayes_upper",
            ]
        )

    @staticmethod
    def _fit_grouped(
        observed: pd.DataFrame,
        *,
        group_by: Sequence[str],
        alpha_prior: float,
        beta_prior: float,
        credible_level: float,
    ) -> pd.DataFrame:
        grouped = (
            observed.groupby(list(group_by), dropna=False)["observation"]
            .agg([("observation_count", "count"), ("continuation_count", "sum")])
            .reset_index()
        )
        grouped["reversal_count"] = grouped["observation_count"] - grouped["continuation_count"]
        grouped["posterior_alpha"] = grouped["continuation_count"] + alpha_prior
        grouped["posterior_beta"] = grouped["reversal_count"] + beta_prior
        grouped["posterior_mean"] = (
            grouped["posterior_alpha"] / (grouped["posterior_alpha"] + grouped["posterior_beta"])
        )

        z = NormalDist().inv_cdf((1.0 + credible_level) / 2.0)

        def _bounds(row: pd.Series) -> pd.Series:
            alpha = float(row["posterior_alpha"])
            beta = float(row["posterior_beta"])
            mean = float(row["posterior_mean"])
            variance = (alpha * beta) / (((alpha + beta) ** 2) * (alpha + beta + 1.0))
            std = math.sqrt(max(variance, 0.0))
            lower = max(0.0, mean - z * std)
            upper = min(1.0, mean + z * std)
            return pd.Series({"bayes_lower": lower, "bayes_upper": upper})

        grouped[["bayes_lower", "bayes_upper"]] = grouped.apply(_bounds, axis=1)
        return grouped.sort_values(list(group_by)).reset_index(drop=True)

    @staticmethod
    def _resolve_fallback_levels(
        primary_group_by: Sequence[str],
        fallback_group_by_levels: Sequence[Sequence[str]] | None,
        dataset_columns: Sequence[str],
    ) -> list[tuple[str, ...]]:
        allowed = set(dataset_columns)
        candidates = list(fallback_group_by_levels or [])
        if not candidates:
            candidates = [
                tuple(primary_group_by),
                tuple(col for col in ("interval", "direction") if col in allowed),
                tuple(col for col in ("direction",) if col in allowed),
                tuple(),
            ]
        levels: list[tuple[str, ...]] = []
        for level in candidates:
            normalized = tuple(level)
            if any(col not in allowed for col in normalized):
                continue
            if normalized not in levels:
                levels.append(normalized)
        if tuple() not in levels:
            levels.append(tuple())
        return levels

    def _annotate_dataset_adaptive(
        self,
        *,
        dataset: pd.DataFrame,
        observed: pd.DataFrame,
        primary_group_by: Sequence[str],
        fallback_group_by_levels: Sequence[Sequence[str]] | None,
        min_observations: int,
        alpha_prior: float,
        beta_prior: float,
        credible_level: float,
    ) -> pd.DataFrame:
        levels = self._resolve_fallback_levels(primary_group_by, fallback_group_by_levels, dataset.columns)
        posterior_lookups: list[tuple[tuple[str, ...], dict[tuple[Any, ...], dict[str, Any]]]] = []
        for level in levels:
            posterior = self._fit_grouped(
                observed,
                group_by=level or ("interval",),
                alpha_prior=alpha_prior,
                beta_prior=beta_prior,
                credible_level=credible_level,
            ) if level else self._fit_global(
                observed,
                alpha_prior=alpha_prior,
                beta_prior=beta_prior,
                credible_level=credible_level,
            )
            lookup: dict[tuple[Any, ...], dict[str, Any]] = {}
            for row in posterior.to_dict(orient="records"):
                key = tuple(row.get(col) for col in level) if level else tuple()
                lookup[key] = row
            posterior_lookups.append((level, lookup))

        rows: list[dict[str, Any]] = []
        for row in dataset.to_dict(orient="records"):
            merged = dict(row)
            for level, lookup in posterior_lookups:
                key = tuple(row.get(col) for col in level) if level else tuple()
                posterior = lookup.get(key)
                if posterior is None:
                    continue
                if int(posterior.get("observation_count") or 0) >= min_observations or level == levels[-1]:
                    merged.update(
                        {
                            "observation_count": posterior.get("observation_count"),
                            "continuation_count": posterior.get("continuation_count"),
                            "reversal_count": posterior.get("reversal_count"),
                            "posterior_alpha": posterior.get("posterior_alpha"),
                            "posterior_beta": posterior.get("posterior_beta"),
                            "posterior_mean": posterior.get("posterior_mean"),
                            "bayes_lower": posterior.get("bayes_lower"),
                            "bayes_upper": posterior.get("bayes_upper"),
                            "bayes_group_level": "/".join(level) if level else "global",
                            "bayes_group_key": json.dumps(
                                {col: row.get(col) for col in level},
                                ensure_ascii=False,
                                sort_keys=True,
                            ) if level else "{}",
                        }
                    )
                    break
            rows.append(merged)
        return pd.DataFrame(rows)

    @staticmethod
    def _fit_global(
        observed: pd.DataFrame,
        *,
        alpha_prior: float,
        beta_prior: float,
        credible_level: float,
    ) -> pd.DataFrame:
        work = observed.copy()
        work["__global__"] = "global"
        grouped = StructureBayesianBaseline._fit_grouped(
            work,
            group_by=("__global__",),
            alpha_prior=alpha_prior,
            beta_prior=beta_prior,
            credible_level=credible_level,
        )
        return grouped.drop(columns=["__global__"], errors="ignore")
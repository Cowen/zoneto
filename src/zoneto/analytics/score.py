"""Batch and single-application scoring from trained joblib models."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import pandas as pd
import polars as pl

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
)

# Model registry: source → list of (model_name, pred_col, is_regressor)
# dev_applications_approved retired: dataset is frozen (no new records), class
# imbalance is 97.3% approved, and CV variance (±0.178 AUC) is too high for
# reliable predictions. Re-enable when a live replacement dataset is found.
_DEV_MODELS: list[tuple[str, str, bool]] = [
    ("dev_applications_appealed", "pred_dev_appealed", False),
]
_COA_MODELS: list[tuple[str, str, bool]] = [
    ("coa_approved", "pred_coa_approved", False),
    ("coa_days_to_approval", "pred_coa_days_to_approval", True),
]
_PERMIT_MODELS: list[tuple[str, str, bool]] = [
    ("permit_issuance_days", "pred_permit_issuance_days", True),
]


def _load(model_dir: Path, model_name: str) -> Any:
    return joblib.load(model_dir / f"{model_name}.joblib")


def _predict_classifier(
    pipe: Any, X: pd.DataFrame, pred_col: str, prob_col: str
) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    probs = pipe.predict_proba(X)[:, 1].tolist()
    return {pred_col: preds, prob_col: probs}


def _predict_regressor(pipe: Any, X: pd.DataFrame, pred_col: str) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    return {pred_col: preds}


def _predict_survival_median(
    pipe: Any, X: pd.DataFrame, pred_col: str
) -> dict[str, list]:
    """Extract median survival time (time at which S(t) crosses 0.5).

    Falls back to max observed time if the survival function never crosses 0.5
    within the fitted range (right-censored upper tail).

    IMPORTANT: sklearn Pipeline does not proxy predict_survival_function().
    We preprocess X through all pipeline steps except the last (estimator),
    then call predict_survival_function() on the estimator directly.
    """
    # Preprocess X through all pipeline steps except the final estimator
    X_transformed = pipe[:-1].transform(X)
    # Call predict_survival_function on the estimator (last pipeline step)
    survival_fns = pipe[-1].predict_survival_function(X_transformed)
    medians: list[float] = []
    for fn in survival_fns:
        times = fn.x
        probs = fn.y
        crossing = times[probs <= 0.5]
        if len(crossing) > 0:
            medians.append(float(crossing[0]))
        else:
            medians.append(float(times[-1]))
    return {pred_col: medians}


def score_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> None:
    """Run batch inference on enriched parquet files; write data/scores/*.parquet."""
    scores_dir = data_dir / "scores"
    scores_dir.mkdir(parents=True, exist_ok=True)

    # --- dev_applications ---
    dev_enriched = data_dir / "enriched" / "dev_applications.parquet"
    df_dev = pl.read_parquet(dev_enriched)
    all_dev_cols = DEV_CAT_COLS + DEV_NUM_COLS
    X_dev = df_dev.select(all_dev_cols).to_pandas()

    extra: dict[str, list] = {}
    for model_name, pred_col, is_reg in _DEV_MODELS:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            extra.update(_predict_regressor(pipe, X_dev, pred_col))
        else:
            extra.update(_predict_classifier(pipe, X_dev, pred_col, prob_col))

    # Survival model (optional — skip if .joblib absent)
    _surv_model_path = model_dir / "dev_days_to_decision.joblib"
    if _surv_model_path.exists():
        _surv_pipe = _load(model_dir, "dev_days_to_decision")
        extra.update(
            _predict_survival_median(_surv_pipe, X_dev, "pred_dev_days_to_decision")
        )

    df_dev_scored = df_dev.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )
    df_dev_scored.write_parquet(scores_dir / "dev_applications.parquet")

    # --- coa ---
    coa_enriched = data_dir / "enriched" / "coa.parquet"
    df_coa = pl.read_parquet(coa_enriched)
    all_coa_cols = COA_CAT_COLS + COA_NUM_COLS
    X_coa = df_coa.select(all_coa_cols).to_pandas()

    extra_coa: dict[str, list] = {}
    for model_name, pred_col, is_reg in _COA_MODELS:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            extra_coa.update(_predict_regressor(pipe, X_coa, pred_col))
        else:
            extra_coa.update(_predict_classifier(pipe, X_coa, pred_col, prob_col))

    df_coa_scored = df_coa.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra_coa.items()]
    )
    df_coa_scored.write_parquet(scores_dir / "coa.parquet")

    # --- permits_cleared (optional: skip if enriched file absent) ---
    permits_enriched = data_dir / "enriched" / "permits_cleared.parquet"
    if permits_enriched.exists():
        df_permits = pl.read_parquet(permits_enriched)
        all_permit_cols = PERMIT_CAT_COLS + PERMIT_NUM_COLS
        X_permits = df_permits.select(all_permit_cols).to_pandas()

        extra_permits: dict[str, list] = {}
        for model_name, pred_col, is_reg in _PERMIT_MODELS:
            pipe = _load(model_dir, model_name)
            prob_col = pred_col.replace("pred_", "prob_")
            if is_reg:
                extra_permits.update(_predict_regressor(pipe, X_permits, pred_col))
            else:
                extra_permits.update(
                    _predict_classifier(pipe, X_permits, pred_col, prob_col)
                )

        df_permits_scored = df_permits.with_columns(
            [pl.Series(name=k, values=v) for k, v in extra_permits.items()]
        )
        df_permits_scored.write_parquet(scores_dir / "permits_cleared.parquet")


def score_one(
    source: str,
    features: dict[str, Any],
    model_dir: Path = Path("models"),
) -> dict[str, Any]:
    """Score a single application dict. Returns prediction dict.

    source must be 'dev_applications', 'coa', or 'permits_cleared'.
    """
    if source == "dev_applications":
        models = _DEV_MODELS
        all_cols = DEV_CAT_COLS + DEV_NUM_COLS
    elif source == "coa":
        models = _COA_MODELS
        all_cols = COA_CAT_COLS + COA_NUM_COLS
    elif source == "permits_cleared":
        models = _PERMIT_MODELS
        all_cols = PERMIT_CAT_COLS + PERMIT_NUM_COLS
    else:
        raise ValueError(
            f"Unknown source: {source!r}. Must be 'dev_applications', 'coa',"
            " or 'permits_cleared'."
        )

    X = pd.DataFrame([{col: features.get(col) for col in all_cols}])

    result: dict[str, Any] = {}
    for model_name, pred_col, is_reg in models:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            result[pred_col] = float(pipe.predict(X)[0])
        else:
            result[pred_col] = int(pipe.predict(X)[0])
            result[prob_col] = float(pipe.predict_proba(X)[0, 1])

    # Survival model for dev_applications (optional — skip if .joblib absent)
    if source == "dev_applications":
        _surv_path = model_dir / "dev_days_to_decision.joblib"
        if _surv_path.exists():
            _surv_pipe = _load(model_dir, "dev_days_to_decision")
            _surv_preds = _predict_survival_median(
                _surv_pipe, X, "pred_dev_days_to_decision"
            )
            result["pred_dev_days_to_decision"] = float(
                _surv_preds["pred_dev_days_to_decision"][0]
            )

    return result

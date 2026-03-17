"""Batch and single-application scoring from trained joblib models."""

from __future__ import annotations

import json
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

# Model registry: dev_applications models only.
# coa_approved: AUC 0.535 at 94% base rate — retired.
# coa_days_to_approval: R² < 0 — tracking only, not served.
# permit_issuance_days: R² 0.039 — retired (queue depth signal absent).
_DEV_MODELS: list[tuple[str, str, bool]] = [
    ("dev_applications_appealed", "pred_dev_appealed", False),
]
_COA_MODELS: list[tuple[str, str, bool]] = []
_PERMIT_MODELS: list[tuple[str, str, bool]] = []


_SURVIVAL_TYPES: frozenset[str] = frozenset({"OZ", "SA"})


def _load(model_dir: Path, model_name: str) -> Any:
    return joblib.load(model_dir / f"{model_name}.joblib")


def _load_production_ready(model_dir: Path) -> dict[str, bool]:
    """Load production_ready flags from metrics.json. Returns {} if absent."""
    metrics_path = model_dir / "metrics.json"
    if not metrics_path.exists():
        return {}
    with open(metrics_path) as f:
        metrics: dict[str, Any] = json.load(f)
    return {name: bool(m.get("production_ready", True)) for name, m in metrics.items()}


def _predict_classifier(
    pipe: Any, X: pd.DataFrame, pred_col: str, prob_col: str
) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    probs = pipe.predict_proba(X)[:, 1].tolist()
    return {pred_col: preds, prob_col: probs}


def _predict_regressor(pipe: Any, X: pd.DataFrame, pred_col: str) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    return {pred_col: preds}


def _survival_percentile(times: Any, probs: Any, quantile: float) -> float:
    """Extract the time at which S(t) crosses the given quantile threshold.

    For p25 (quantile=0.25): find where S(t) <= 0.75 (75% still surviving).
    For p50 (quantile=0.50): find where S(t) <= 0.50 (median).
    For p75 (quantile=0.75): find where S(t) <= 0.25 (25% still surviving).

    Falls back to max observed time if the curve never crosses.
    """
    threshold = 1.0 - quantile
    crossing = times[probs <= threshold]
    if len(crossing) > 0:
        return float(crossing[0])
    return float(times[-1])


def _predict_survival_percentiles(pipe: Any, X: pd.DataFrame) -> dict[str, list[float]]:
    """Extract p25/p50/p75 survival times from fitted survival pipeline.

    IMPORTANT: sklearn Pipeline does not proxy predict_survival_function().
    We preprocess X through all pipeline steps except the last (estimator),
    then call predict_survival_function() on the estimator directly.
    """
    X_transformed = pipe[:-1].transform(X)
    survival_fns = pipe[-1].predict_survival_function(X_transformed)
    p25s: list[float] = []
    p50s: list[float] = []
    p75s: list[float] = []
    for fn in survival_fns:
        times = fn.x
        probs = fn.y
        p25s.append(_survival_percentile(times, probs, 0.25))
        p50s.append(_survival_percentile(times, probs, 0.50))
        p75s.append(_survival_percentile(times, probs, 0.75))
    return {
        "pred_dev_days_p25": p25s,
        "pred_dev_days_p50": p50s,
        "pred_dev_days_p75": p75s,
    }


def score_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> None:
    """Run batch inference on enriched parquet files; write data/scores/*.parquet.

    Models marked production_ready=False in metrics.json are skipped — their
    prediction columns are not written to output.
    """
    scores_dir = data_dir / "scores"
    scores_dir.mkdir(parents=True, exist_ok=True)
    production_ready = _load_production_ready(model_dir)

    # --- dev_applications ---
    dev_enriched = data_dir / "enriched" / "dev_applications.parquet"
    df_dev = pl.read_parquet(dev_enriched)

    # Apply NLP vectorizer if available (adds desc_svd_0..19 columns)
    # Skip if enriched parquet already has SVD columns (idempotency check)
    _tfidf_path = model_dir / "desc_tfidf.joblib"
    if (
        _tfidf_path.exists()
        and "description" in df_dev.columns
        and "desc_svd_0" not in df_dev.columns
    ):
        _tfidf_pipe = joblib.load(_tfidf_path)
        _texts = df_dev["description"].fill_null("").cast(pl.String).to_list()
        _vectors = _tfidf_pipe.transform(_texts)
        _svd_cols = [
            pl.Series(f"desc_svd_{i}", _vectors[:, i].tolist(), dtype=pl.Float64)
            for i in range(_vectors.shape[1])
        ]
        df_dev = df_dev.with_columns(_svd_cols)

    all_dev_cols = DEV_CAT_COLS + DEV_NUM_COLS
    X_dev = df_dev.select(all_dev_cols).to_pandas()

    extra: dict[str, list] = {}
    for model_name, pred_col, is_reg in _DEV_MODELS:
        if not production_ready.get(model_name, True):
            continue
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            extra.update(_predict_regressor(pipe, X_dev, pred_col))
        else:
            extra.update(_predict_classifier(pipe, X_dev, pred_col, prob_col))

    # Survival model (optional — skip if .joblib absent or not production_ready).
    # Only scores OZ+SA applications — model trained exclusively on OZ+SA; CD/SB/PL
    # have fundamentally different timelines and were never in training data.
    # Outputs p25/p50/p75 percentiles instead of single median for better UX.
    _surv_model_path = model_dir / "dev_days_to_decision.joblib"
    if _surv_model_path.exists() and production_ready.get("dev_days_to_decision", True):
        _surv_pipe = _load(model_dir, "dev_days_to_decision")
        oz_sa_mask = df_dev["application_type"].is_in(list(_SURVIVAL_TYPES))
        oz_sa_indices = oz_sa_mask.arg_true().to_list()
        _pct_cols = ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]
        pct_lists: dict[str, list[float | None]] = {
            c: [None] * len(df_dev) for c in _pct_cols
        }
        if oz_sa_indices:
            X_surv = df_dev.filter(oz_sa_mask).select(all_dev_cols).to_pandas()
            surv_result = _predict_survival_percentiles(_surv_pipe, X_surv)
            for col in _pct_cols:
                for i, val in zip(oz_sa_indices, surv_result[col]):
                    pct_lists[col][i] = val
        extra.update(pct_lists)

    df_dev_scored = df_dev.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )
    df_dev_scored.write_parquet(scores_dir / "dev_applications.parquet")

    # Write active applications separately for product visibility.
    # Active apps are where commercial value lives — developers want to know
    # about *their* pending application, not historical data.
    if "is_active" in df_dev_scored.columns:
        df_active = df_dev_scored.filter(pl.col("is_active") == 1)
        if len(df_active) > 0:
            df_active.write_parquet(scores_dir / "dev_applications_active.parquet")


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
        # Apply NLP vectorizer if available
        _tfidf_path = model_dir / "desc_tfidf.joblib"
        if _tfidf_path.exists() and "description" in features:
            _tfidf_pipe = joblib.load(_tfidf_path)
            _text = str(features.get("description") or "")
            _vec = _tfidf_pipe.transform([_text])
            for i in range(_vec.shape[1]):
                features = {**features, f"desc_svd_{i}": float(_vec[0, i])}
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

    # Survival model for dev_applications (OZ+SA only — skip non-OZ/SA types and
    # skip if .joblib absent; CD/SB/PL have different timelines and were never trained)
    # Returns p25/p50/p75 percentiles for project finance timeline planning.
    if source == "dev_applications":
        _surv_path = model_dir / "dev_days_to_decision.joblib"
        if _surv_path.exists() and features.get("application_type") in _SURVIVAL_TYPES:
            _surv_pipe = _load(model_dir, "dev_days_to_decision")
            _surv_preds = _predict_survival_percentiles(_surv_pipe, X)
            for key in ("pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"):
                result[key] = float(_surv_preds[key][0])

    return result

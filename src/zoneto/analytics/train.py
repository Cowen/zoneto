"""Training pipeline: build sklearn models from enriched parquet."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import joblib
import numpy as np
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import KFold, TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sksurv.ensemble import GradientBoostingSurvivalAnalysis
from sksurv.metrics import concordance_index_censored

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS

logger = logging.getLogger(__name__)


def _fill_missing_cols(
    df: pl.DataFrame,
    cat_cols: list[str],
    num_cols: list[str],
) -> pl.DataFrame:
    """Add null columns for any cat/num features absent from df."""
    for col in cat_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    for col in num_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    return df


# Natural primary keys per enriched dataset, used as a tie-breaker after the
# year-based sort so TimeSeriesSplit folds are identical across enrich runs
# (parquet row order is non-deterministic).
_STABLE_ID_CANDIDATES: tuple[str, ...] = (
    "folderrsn",
    "reference_file",
    "permit_number",
    "id",
)


def _sort_temporal(df: pl.DataFrame, year_col: str) -> pl.DataFrame:
    """Stable temporal sort: by year_col then a deterministic secondary key.

    Polars stable sort preserves the input row order within a year. The parquet
    row order coming out of enrich is itself non-deterministic, so a single
    sort on year_col leaves within-year ordering different across runs. That
    makes TimeSeriesSplit's index-based fold cuts hit different rows each run
    and the CV metric swings by ±0.04 AUC on identical code.

    Resolution: sort first by year_col, then by the first natural ID column
    available (folderrsn / reference_file / permit_number / id). When none of
    those exist (small synthetic test fixtures), fall back to a content hash so
    the sort still has a deterministic secondary key.
    """
    secondary = next((c for c in _STABLE_ID_CANDIDATES if c in df.columns), None)
    if secondary is not None:
        return df.sort([year_col, secondary])
    # Fixture-only fallback — hash the input row content. Deterministic across
    # processes per polars docs; cheap on small synthetic data.
    return (
        df.with_columns(df.hash_rows().alias("__row_hash"))
        .sort([year_col, "__row_hash"])
        .drop("__row_hash")
    )


def _build_survival_pipeline(
    cat_cols: list[str],
    num_cols: list[str],
) -> Pipeline:
    """Pipeline for GradientBoostingSurvivalAnalysis with median imputation.

    Adds SimpleImputer(strategy='median') for numeric features because
    GradientBoostingSurvivalAnalysis does not handle NaN natively.
    """
    _cat_pipe = Pipeline(
        [
            ("impute", SimpleImputer(strategy="constant", fill_value="__missing__")),
            (
                "encode",
                OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1),
            ),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", _cat_pipe, cat_cols),
            ("num", SimpleImputer(strategy="median"), num_cols),
        ]
    )
    return Pipeline(
        [
            ("preprocessor", preprocessor),
            ("estimator", GradientBoostingSurvivalAnalysis(random_state=42)),
        ]
    )


def train_survival(
    enriched_path: Path,
    time_col: str,
    event_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    model_name: str,
    model_dir: Path,
) -> int:
    """Train a GradientBoostingSurvivalAnalysis model. Returns row count used.

    Filters to rows where event_col is not null (OZ+SA only).
    Labels are structured numpy array [(event: bool, time: int)].
    No CalibratedClassifierCV wrapper — survival models are not calibrated.
    Serializes to model_dir/<model_name>.joblib.
    """
    df = pl.read_parquet(enriched_path)
    df = df.filter(pl.col(event_col).is_not_null() & pl.col(time_col).is_not_null())

    df = _fill_missing_cols(df, cat_cols, num_cols)
    all_cols = cat_cols + num_cols
    X = df.select(all_cols).to_pandas()

    events = df[event_col].cast(pl.Boolean).to_numpy()
    times = df[time_col].cast(pl.Int32).to_numpy()
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    pipe = _build_survival_pipeline(cat_cols, num_cols)
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")
    return len(df)


def evaluate_survival(
    enriched_path: Path,
    time_col: str,
    event_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    *,
    cv: int = 5,
    year_col: str | None = "year_submitted",
) -> dict[str, float | int]:
    """Cross-validate survival model. Returns concordance_index_mean/std and n.

    Uses TimeSeriesSplit when year_col is present (temporal CV to avoid leakage).
    Uses sksurv.metrics.concordance_index_censored for scoring each fold.
    """
    df = pl.read_parquet(enriched_path).filter(
        pl.col(event_col).is_not_null() & pl.col(time_col).is_not_null()
    )
    df = _fill_missing_cols(df, cat_cols, num_cols)

    if year_col is not None and year_col in df.columns:
        df = _sort_temporal(df, year_col)

    effective_cv = min(cv, len(df) - 1)
    cv_obj: TimeSeriesSplit | KFold = (
        TimeSeriesSplit(n_splits=effective_cv)
        if year_col is not None and year_col in df.columns
        else KFold(effective_cv)
    )

    all_cols = cat_cols + num_cols
    X = df.select(all_cols).to_pandas()
    events = df[event_col].cast(pl.Boolean).to_numpy()
    times = df[time_col].cast(pl.Int32).to_numpy()
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    pipeline = _build_survival_pipeline(cat_cols, num_cols)

    ci_scores: list[float] = []
    for train_idx, test_idx in cv_obj.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]
        try:
            pipeline.fit(X_train, y_train)
            risk_scores = pipeline.predict(X_test)
            ci, *_ = concordance_index_censored(
                y_test["event"], y_test["time"], risk_scores
            )
            ci_scores.append(ci)
        except Exception as exc:
            logger.warning("Survival CV fold failed: %s", exc)
            ci_scores.append(float("nan"))

    non_nan = [s for s in ci_scores if not np.isnan(s)]
    return {
        "concordance_index_mean": float(np.mean(non_nan)) if non_nan else float("nan"),
        "concordance_index_std": float(np.std(non_nan)) if non_nan else float("nan"),
        "n": len(df),
    }


def train_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> tuple[dict[str, int], dict[str, dict[str, float | int]]]:
    """Train all production models. Returns (row_counts, metrics).

    The only predictive model is the survival model dev_days_to_decision, trained
    when the enriched dev parquet carries the dev_days_observed column (AIC scraped).
    The five structured classifier/regressor models that never cleared the quality
    bar (dev_applications_appealed, coa_approved, dev_applications_approved,
    coa_days_to_approval, permit_issuance_days) were deleted — each failed because
    of underlying training-data limitations, not tunable modelling choices.

    First element: {model_name: row_count}
    Second element: {model_name: {metric_mean: float, metric_std: float, ..., n: int}}
    """
    dev_path = data_dir / "enriched" / "dev_applications.parquet"

    counts: dict[str, int] = {}
    metrics: dict[str, dict[str, float | int]] = {}

    # --- Survival model for dev_days_to_decision ---
    # Only train if enriched dev parquet has dev_days_observed column (AIC scraped)
    if dev_path.exists():
        _dev_df_cols = pl.read_parquet(dev_path).columns
        if "dev_days_observed" in _dev_df_cols:
            surv_count = train_survival(
                enriched_path=dev_path,
                time_col="dev_days_observed",
                event_col="dev_decision_event",
                cat_cols=DEV_CAT_COLS,
                num_cols=DEV_NUM_COLS,
                model_name="dev_days_to_decision",
                model_dir=model_dir,
            )
            counts["dev_days_to_decision"] = surv_count
            surv_eval = evaluate_survival(
                enriched_path=dev_path,
                time_col="dev_days_observed",
                event_col="dev_decision_event",
                cat_cols=DEV_CAT_COLS,
                num_cols=DEV_NUM_COLS,
            )
            # Survival model: gate on Harrell's c-index >= 0.65.
            # NaN comparison is always False in Python, so NaN → not production_ready.
            surv_eval["production_ready"] = bool(
                surv_eval.get("concordance_index_mean", float("nan")) >= 0.65
            )
            metrics["dev_days_to_decision"] = surv_eval

    # Save metrics.json (model_dir created by train_survival)
    model_dir.mkdir(parents=True, exist_ok=True)
    metrics_file = model_dir / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return counts, metrics

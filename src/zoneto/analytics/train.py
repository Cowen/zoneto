"""Training pipeline: build sklearn models from enriched parquet."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.model_selection import (
    KFold,
    StratifiedKFold,
    TimeSeriesSplit,
    cross_validate,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder
from sksurv.ensemble import GradientBoostingSurvivalAnalysis
from sksurv.metrics import concordance_index_censored

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)

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

    Unlike build_pipeline (which passes numeric NaN through for HistGradientBoosting),
    this adds SimpleImputer(strategy='median') for numeric features because
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


def build_pipeline(
    cat_cols: list[str],
    num_cols: list[str],
    estimator: Any,
) -> Pipeline:
    """Return an unfitted sklearn Pipeline with OrdinalEncoder + estimator."""
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline(
                    [
                        (
                            "impute",
                            SimpleImputer(
                                strategy="constant", fill_value="__missing__"
                            ),
                        ),
                        (
                            "encode",
                            OrdinalEncoder(
                                handle_unknown="use_encoded_value",
                                unknown_value=-1,
                            ),
                        ),
                    ]
                ),
                cat_cols,
            ),
            ("num", "passthrough", num_cols),
        ]
    )
    return Pipeline(
        [
            ("preprocessor", preprocessor),
            ("estimator", estimator),
        ]
    )


_MIN_CALIBRATION_ROWS = 20


def train_source(
    enriched_path: Path,
    label_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    model_name: str,
    model_dir: Path,
    *,
    regressor: bool = False,
    calibrate: bool = True,
) -> int:
    """Train one model, serialize to *model_dir*/<model_name>.joblib.

    For classifiers with calibrate=True and sufficient data (>= 20 rows), wraps
    the fitted pipeline in CalibratedClassifierCV so predict_proba returns
    calibrated probabilities. Regressors are never calibrated.

    Returns number of training rows used.
    """
    df = pl.read_parquet(enriched_path)

    # Drop rows with null labels
    df = df.filter(pl.col(label_col).is_not_null())

    all_cols = cat_cols + num_cols
    df = _fill_missing_cols(df, cat_cols, num_cols)
    X = df.select(all_cols).to_pandas()
    y = df[label_col].to_numpy()

    estimator = (
        HistGradientBoostingRegressor(random_state=42)
        if regressor
        else HistGradientBoostingClassifier(random_state=42)
    )
    pipe = build_pipeline(cat_cols=cat_cols, num_cols=num_cols, estimator=estimator)

    if not regressor and calibrate and len(y) >= _MIN_CALIBRATION_ROWS:
        # Wrap the unfitted pipeline in CalibratedClassifierCV (5-fold internal CV)
        # so that predict_proba returns calibrated probabilities.
        model_to_save: Pipeline | CalibratedClassifierCV = CalibratedClassifierCV(
            pipe, cv=5, method="isotonic"
        )
        model_to_save.fit(X, y)
    else:
        pipe.fit(X, y)
        model_to_save = pipe

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(model_to_save, model_dir / f"{model_name}.joblib")
    return len(df)


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


def evaluate_source(
    enriched_path: Path,
    label_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    *,
    regressor: bool = False,
    cv: int = 5,
    year_col: str | None = "year_submitted",
) -> dict[str, float | int]:
    """Cross-validate a model pipeline. Returns a metrics dict with mean/std per metric.

    Classifiers: roc_auc_mean/std, brier_score_mean/std, avg_precision_mean/std, n
    Regressors: r2_mean/std, mae_mean/std, rmse_mean/std, n

    Sklearn's negated loss metrics (neg_brier_score, neg_mae, neg_rmse) are
    sign-flipped before returning so callers always get positive values.

    If year_col is set and present in the data, uses TimeSeriesSplit (temporal CV)
    to avoid leaking future data into past folds. Falls back to shuffled CV otherwise.
    """
    df = pl.read_parquet(enriched_path).filter(pl.col(label_col).is_not_null())
    all_cols = cat_cols + num_cols
    df = _fill_missing_cols(df, cat_cols, num_cols)

    # Cap cv at n_samples - 1 to avoid splits > samples errors on small datasets
    effective_cv = min(cv, len(df) - 1)
    if year_col is not None and year_col in df.columns:
        df = _sort_temporal(df, year_col)
        cv_obj: KFold | StratifiedKFold | TimeSeriesSplit = TimeSeriesSplit(
            n_splits=effective_cv
        )
    elif regressor:
        cv_obj = KFold(effective_cv)
    else:
        # Also cap by minority class count so StratifiedKFold never warns
        label_series = df[label_col]
        min_class = label_series.drop_nulls().value_counts()["count"].min()
        assert isinstance(min_class, int)
        effective_cv = min(effective_cv, min_class)
        cv_obj = StratifiedKFold(effective_cv)

    X = df.select(all_cols).to_pandas()
    y = df[label_col].to_pandas()

    estimator = (
        HistGradientBoostingRegressor(random_state=42)
        if regressor
        else HistGradientBoostingClassifier(random_state=42)
    )
    pipeline = build_pipeline(cat_cols, num_cols, estimator)

    if regressor:
        scoring = {
            "r2": "r2",
            "neg_mae": "neg_mean_absolute_error",
            "neg_rmse": "neg_root_mean_squared_error",
        }
    else:
        scoring = {
            "roc_auc": "roc_auc",
            "neg_brier_score": "neg_brier_score",
            "avg_precision": "average_precision",
        }

    cv_results = cross_validate(
        pipeline, X, y, cv=cv_obj, scoring=scoring, error_score=np.nan
    )

    result: dict[str, float | int] = {"n": len(y)}
    for key in scoring:
        vals = cv_results[f"test_{key}"]
        # Strip neg_ prefix and flip sign so returned values are always positive
        is_neg = key.startswith("neg_")
        out_key = key[4:] if is_neg else key
        sign = -1.0 if is_neg else 1.0
        non_nan = vals[~np.isnan(vals)]
        result[f"{out_key}_mean"] = (
            sign * float(np.mean(non_nan)) if len(non_nan) else float("nan")
        )
        result[f"{out_key}_std"] = (
            float(np.std(non_nan)) if len(non_nan) else float("nan")
        )
    return result


def train_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> tuple[dict[str, int], dict[str, dict[str, float | int]]]:
    """Train all models. Returns (row_counts, metrics).

    Core models (always trained): dev_applications_appealed, coa_approved,
    coa_days_to_approval. dev_applications_approved is retired (dataset frozen,
    97.3% class imbalance, ±0.267 AUC variance).
    Optional model (trained if enriched file exists): permit_issuance_days.

    First element: {model_name: row_count}
    Second element: {model_name: {metric_mean: float, metric_std: float, ..., n: int}}
    """
    dev_path = data_dir / "enriched" / "dev_applications.parquet"
    coa_path = data_dir / "enriched" / "coa.parquet"

    jobs: list[tuple[Path, str, list[str], list[str], str, bool]] = [
        # dev_applications_approved retired: dataset frozen (no new records since city
        # retired the dataset), class imbalance is 97.3% approved, and CV variance
        # (±0.267 AUC) is too high for reliable predictions.
        (
            dev_path,
            "dev_appealed",
            DEV_CAT_COLS,
            DEV_NUM_COLS,
            "dev_applications_appealed",
            False,
        ),
        # coa_approved retired: AUC 0.535 with 94% base rate. Worse than majority class.
        # Cannot be improved with available structured features. COA approval is nearly
        # certain; the meaningful question is "under what conditions"
        # (requires text analysis, not classification).
        #
        # coa_days_to_approval trains for metric tracking only.
        # production_ready is forced False below regardless of R².
        (
            coa_path,
            "coa_days_to_approval",
            COA_CAT_COLS,
            COA_NUM_COLS,
            "coa_days_to_approval",
            True,
        ),
    ]

    # permit_issuance_days retired: R² 0.039 on 133K rows is conclusive.
    # Queue depth (the primary driver) is not in open data.
    # Not trained even when enriched file is present.

    counts: dict[str, int] = {}
    metrics: dict[str, dict[str, float | int]] = {}
    for path, label, cat, num, name, is_reg in jobs:
        # Skip if enriched file doesn't exist (e.g., COA not enriched)
        if not path.exists():
            continue
        count = train_source(
            enriched_path=path,
            label_col=label,
            cat_cols=cat,
            num_cols=num,
            model_name=name,
            model_dir=model_dir,
            regressor=is_reg,
        )
        counts[name] = count
        # COA models use KFold (year_col=None); dev_applications uses TimeSeriesSplit
        year_col_for_eval = None if name.startswith("coa_") else "year_submitted"
        eval_result = evaluate_source(
            enriched_path=path,
            label_col=label,
            cat_cols=cat,
            num_cols=num,
            regressor=is_reg,
            year_col=year_col_for_eval,
        )
        metrics[name] = eval_result

    # --- Optional: survival model for dev_days_to_decision ---
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
            metrics["dev_days_to_decision"] = surv_eval

    # Gate each model: mark production_ready based on metric thresholds.
    # Classifiers: roc_auc_mean >= 0.65. Regressors: r2_mean >= 0.10.
    # Survival: concordance_index_mean >= 0.65.
    # NaN comparison is always False in Python, so NaN → not production_ready.
    # Regressor threshold raised to 0.10: a model explaining <10% of variance
    # is not useful for production decisions.
    reg_model_names = {job[4] for job in jobs if job[5]}
    for name, m in metrics.items():
        if name == "dev_days_to_decision":
            # Survival model: gate on Harrell's c-index >= 0.65
            m["production_ready"] = bool(
                m.get("concordance_index_mean", float("nan")) >= 0.65
            )
        elif name in reg_model_names:
            m["production_ready"] = bool(m.get("r2_mean", float("nan")) >= 0.10)
        else:
            m["production_ready"] = bool(m.get("roc_auc_mean", 0.0) >= 0.65)

    # Force coa_days_to_approval to production_ready=False regardless of metric score.
    # It trains for tracking only — to monitor whether R² improves as data grows.
    # Serving predictions from a model with R² < 0 is worse than presenting the mean.
    if "coa_days_to_approval" in metrics:
        metrics["coa_days_to_approval"]["production_ready"] = False

    # Save metrics.json (model_dir already created by train_source)
    metrics_file = model_dir / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return counts, metrics

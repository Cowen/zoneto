"""Training pipeline: build sklearn models from enriched parquet."""

from __future__ import annotations

import json
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

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
)


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
        df = df.sort(year_col)
        cv_obj: KFold | StratifiedKFold | TimeSeriesSplit = TimeSeriesSplit(
            n_splits=effective_cv
        )
    else:
        cv_obj = KFold(effective_cv) if regressor else StratifiedKFold(effective_cv)

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

    Core models (always trained): dev_applications_approved,
    dev_applications_appealed, coa_approved, coa_days_to_approval.
    Optional model (trained if enriched file exists): permit_issuance_days.

    First element: {model_name: row_count}
    Second element: {model_name: {metric_mean: float, metric_std: float, ..., n: int}}
    """
    dev_path = data_dir / "enriched" / "dev_applications.parquet"
    coa_path = data_dir / "enriched" / "coa.parquet"
    permits_path = data_dir / "enriched" / "permits_cleared.parquet"

    jobs: list[tuple[Path, str, list[str], list[str], str, bool]] = [
        (
            dev_path,
            "dev_approved",
            DEV_CAT_COLS,
            DEV_NUM_COLS,
            "dev_applications_approved",
            False,
        ),
        (
            dev_path,
            "dev_appealed",
            DEV_CAT_COLS,
            DEV_NUM_COLS,
            "dev_applications_appealed",
            False,
        ),
        (coa_path, "coa_approved", COA_CAT_COLS, COA_NUM_COLS, "coa_approved", False),
        (
            coa_path,
            "coa_days_to_approval",
            COA_CAT_COLS,
            COA_NUM_COLS,
            "coa_days_to_approval",
            True,
        ),
    ]

    # Permit issuance model is optional — skip if enriched file absent
    if permits_path.exists():
        jobs.append(
            (
                permits_path,
                "permit_issuance_days",
                PERMIT_CAT_COLS,
                PERMIT_NUM_COLS,
                "permit_issuance_days",
                True,
            )
        )

    counts: dict[str, int] = {}
    metrics: dict[str, dict[str, float | int]] = {}
    for path, label, cat, num, name, is_reg in jobs:
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

    # Save metrics.json (model_dir already created by train_source)
    metrics_file = model_dir / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return counts, metrics

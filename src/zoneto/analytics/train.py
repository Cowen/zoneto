"""Training pipeline: build sklearn models from enriched parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import polars as pl
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


def train_source(
    enriched_path: Path,
    label_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    model_name: str,
    model_dir: Path,
    *,
    regressor: bool = False,
) -> int:
    """Train one model, serialize to *model_dir*/<model_name>.joblib.

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
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")
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

    if year_col is not None and year_col in df.columns:
        df = df.sort(year_col)
        # TimeSeriesSplit requires n_samples >= n_splits + 1
        effective_cv = min(cv, len(df) - 1)
        cv_obj: KFold | StratifiedKFold | TimeSeriesSplit = TimeSeriesSplit(
            n_splits=effective_cv
        )
    else:
        cv_obj = KFold(cv) if regressor else StratifiedKFold(cv)

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
        result[f"{out_key}_mean"] = sign * float(np.nanmean(vals))
        result[f"{out_key}_std"] = float(np.nanstd(vals))
    return result


def train_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> tuple[dict[str, int], dict[str, dict[str, float | int]]]:
    """Train all 4 models. Returns (row_counts, metrics).

    First element: {model_name: row_count}
    Second element: {model_name: {metric_mean: float, metric_std: float, ..., n: int}}
    """
    dev_path = data_dir / "enriched" / "dev_applications.parquet"
    coa_path = data_dir / "enriched" / "coa.parquet"

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
        eval_result = evaluate_source(
            enriched_path=path,
            label_col=label,
            cat_cols=cat,
            num_cols=num,
            regressor=is_reg,
            year_col="year_submitted",
        )
        metrics[name] = eval_result

    # Save metrics.json (model_dir already created by train_source)
    metrics_file = model_dir / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return counts, metrics

"""Training pipeline: build sklearn models from enriched parquet."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.model_selection import KFold, StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
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
    # Fill missing cat columns with None (polars), num columns left as-is
    for col in cat_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    for col in num_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

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
    cv: int = 3,
) -> dict[str, float | int]:
    """Cross-validate a model pipeline. Returns {mean, std, n}."""
    df = pl.read_parquet(enriched_path).filter(pl.col(label_col).is_not_null())
    all_cols = cat_cols + num_cols
    for col in cat_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    for col in num_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))
    X = df.select(all_cols).to_pandas()
    y = df[label_col].to_pandas()
    estimator = (
        HistGradientBoostingRegressor(random_state=42)
        if regressor
        else HistGradientBoostingClassifier(random_state=42)
    )
    pipeline = build_pipeline(cat_cols, num_cols, estimator)
    scoring = "r2" if regressor else "roc_auc"
    cv_obj: KFold | StratifiedKFold = KFold(cv) if regressor else StratifiedKFold(cv)
    scores = cross_val_score(pipeline, X, y, cv=cv_obj, scoring=scoring)
    return {"mean": float(scores.mean()), "std": float(scores.std()), "n": len(y)}


def train_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> tuple[dict[str, int], dict[str, dict[str, float | int]]]:
    """Train all 4 models. Returns (row_counts, metrics).

    First element: {model_name: row_count}
    Second element: {model_name: {"mean": float, "std": float, "n": int}}
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
            "dev_no_appeal",
            DEV_CAT_COLS,
            DEV_NUM_COLS,
            "dev_applications_no_appeal",
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
        )
        metrics[name] = eval_result

    # Save metrics.json
    model_dir.mkdir(parents=True, exist_ok=True)
    metrics_file = model_dir / "metrics.json"
    with open(metrics_file, "w") as f:
        json.dump(metrics, f, indent=2)

    return counts, metrics

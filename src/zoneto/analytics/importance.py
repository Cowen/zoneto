"""Feature importance for trained outcome-prediction models."""

from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS

# Model registry: model_name → (enriched_parquet, label_col, cat_cols, num_cols).
# dev_days_to_decision (survival) is the only served model; the structured
# classifier/regressor models were deleted for failing the quality bar.
_MODEL_META: dict[str, tuple[str, str, list[str], list[str]]] = {
    "dev_days_to_decision": (
        "dev_applications",
        "dev_days_observed",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
    ),
}


def feature_importance(
    model_name: str,
    model_dir: Path = Path("models"),
) -> pl.DataFrame:
    """Return a DataFrame of features ranked by importance (descending).

    Columns: feature, importance_mean, importance_std.

    Uses gain-based importance from the survival model's public
    feature_importances_. The survival model (GradientBoostingSurvivalAnalysis)
    does not support permutation importance with standard sklearn scorers, so this
    is the only supported mode.
    """
    if model_name not in _MODEL_META:
        known = ", ".join(_MODEL_META)
        raise ValueError(f"Unknown model: {model_name!r}. Must be one of: {known}.")

    _, _, cat_cols, num_cols = _MODEL_META[model_name]
    all_cols = cat_cols + num_cols

    pipe = joblib.load(model_dir / f"{model_name}.joblib")
    estimator = pipe.named_steps["estimator"]
    # GradientBoostingSurvivalAnalysis exposes public feature_importances_
    importances = estimator.feature_importances_
    result = pl.DataFrame(
        {
            "feature": all_cols,
            "importance_mean": importances.tolist(),
            "importance_std": [0.0] * len(all_cols),
        }
    )

    return result.sort("importance_mean", descending=True)

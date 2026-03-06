"""Feature importance for trained outcome-prediction models."""
from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import polars as pl

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)

# Model registry: model_name → (enriched_parquet, label_col, cat_cols, num_cols)
_MODEL_META: dict[str, tuple[str, str, list[str], list[str]]] = {
    "dev_applications_approved": (
        "dev_applications", "dev_approved", DEV_CAT_COLS, DEV_NUM_COLS,
    ),
    "dev_applications_no_appeal": (
        "dev_applications", "dev_no_appeal", DEV_CAT_COLS, DEV_NUM_COLS,
    ),
    "coa_approved": (
        "coa", "coa_approved", COA_CAT_COLS, COA_NUM_COLS,
    ),
    "coa_days_to_approval": (
        "coa", "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS,
    ),
}


def _gain_importances(estimator: object, n_features: int) -> np.ndarray:
    """Compute gain-based feature importances from HistGradientBoosting internals.

    NOTE: This relies on `_predictors`, a private sklearn attribute. It is the
    only way to get no-data importance for HistGradientBoosting (which does not
    expose `feature_importances_`). Test against sklearn upgrades.
    """
    if not hasattr(estimator, "_predictors"):
        raise AttributeError(
            f"{type(estimator).__name__} has no '_predictors' attribute. "
            "Built-in importance requires a fitted HistGradientBoosting estimator."
        )
    gains = np.zeros(n_features)
    for iteration in estimator._predictors:  # type: ignore[attr-defined]
        for tree in iteration:
            nodes = tree.nodes
            split_nodes = nodes[nodes["is_leaf"] == 0]
            for node in split_nodes:
                idx = int(node["feature_idx"])
                if idx < n_features:
                    gains[idx] += float(node["gain"])
    # Gains from HistGradientBoosting should be non-negative, but clip defensively
    # before normalising so a degenerate model doesn't produce negative fractions.
    gains = np.maximum(gains, 0.0)
    total = gains.sum()
    return gains / total if total > 0 else gains


def feature_importance(
    model_name: str,
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
    *,
    builtin: bool = False,
    n_repeats: int = 10,
    random_state: int = 42,
) -> pl.DataFrame:
    """Return a DataFrame of features ranked by importance (descending).

    Columns: feature, importance_mean, importance_std.

    If builtin=True, uses gain-based importance derived from the model's internal
    tree structure (fast, no data required, but biased toward high-cardinality
    categoricals and relies on a private sklearn API).
    Otherwise uses permutation importance on the enriched parquet for the given
    model (slower but more reliable).
    """
    if model_name not in _MODEL_META:
        known = ", ".join(_MODEL_META)
        raise ValueError(f"Unknown model: {model_name!r}. Must be one of: {known}.")

    source_name, label_col, cat_cols, num_cols = _MODEL_META[model_name]
    all_cols = cat_cols + num_cols

    pipe = joblib.load(model_dir / f"{model_name}.joblib")

    if builtin:
        importances = _gain_importances(pipe.named_steps["estimator"], len(all_cols))
        result = pl.DataFrame({
            "feature": all_cols,
            "importance_mean": importances.tolist(),
            "importance_std": [0.0] * len(all_cols),
        })
    else:
        import pandas as pd
        from sklearn.inspection import permutation_importance

        enriched = data_dir / "enriched" / f"{source_name}.parquet"
        df = pl.read_parquet(enriched).filter(pl.col(label_col).is_not_null())
        X: pd.DataFrame = df.select(all_cols).to_pandas()
        y: np.ndarray = df[label_col].to_numpy()

        scoring = "r2" if "days" in label_col else "roc_auc"
        perm = permutation_importance(
            pipe, X, y,
            n_repeats=n_repeats,
            scoring=scoring,
            random_state=random_state,
            n_jobs=-1,
        )
        result = pl.DataFrame({
            "feature": all_cols,
            "importance_mean": perm.importances_mean.tolist(),
            "importance_std": perm.importances_std.tolist(),
        })

    return result.sort("importance_mean", descending=True)

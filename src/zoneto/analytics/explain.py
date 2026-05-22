"""SHAP-based per-application explanation for trained classifiers."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from zoneto.analytics.features import (
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)

logger = logging.getLogger(__name__)


def _unwrap_pipeline(pipe: Any) -> Any:
    """Unwrap CalibratedClassifierCV to get the base Pipeline.

    Follows the same unwrapping pattern as analytics/importance.py.
    """
    from sklearn.calibration import CalibratedClassifierCV  # noqa: PLC0415

    if isinstance(pipe, CalibratedClassifierCV):
        return pipe.calibrated_classifiers_[0].estimator
    return pipe


def explain_one(
    source: str,
    features: dict[str, Any],
    model_dir: Path,
    model_name: str,
    *,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Return top-N SHAP feature contributions for a single application.

    Uses shap.TreeExplainer on the base HistGradientBoosting estimator
    (after unwrapping CalibratedClassifierCV).

    Returns a list of dicts with keys:
        feature (str): feature name
        shap_value (float): SHAP value for this prediction
        direction (str): "increases_risk" if shap_value > 0, else "decreases_risk"

    Returns [] when the model file is absent or SHAP computation fails.
    """
    import shap  # noqa: PLC0415

    model_path = model_dir / f"{model_name}.joblib"
    if not model_path.exists():
        return []

    try:
        pipe = joblib.load(model_path)
        base_pipe = _unwrap_pipeline(pipe)
        estimator = base_pipe.named_steps["estimator"]
        preprocessor = base_pipe.named_steps["preprocessor"]

        if source == "dev_applications":
            all_cols = DEV_CAT_COLS + DEV_NUM_COLS
        else:
            logger.warning("explain_one: source %r not supported for SHAP", source)
            return []

        # pandas required: sklearn's ColumnTransformer/predict APIs don't accept polars
        X_raw = pd.DataFrame([{col: features.get(col) for col in all_cols}])
        X_transformed = preprocessor.transform(X_raw)

        explainer = shap.TreeExplainer(estimator)
        shap_values = explainer.shap_values(X_transformed, check_additivity=False)

        # For binary classifier: shap_values may be list[array] or array
        # Take class-1 SHAP values (risk class)
        if isinstance(shap_values, list):
            values = shap_values[1][0] if len(shap_values) > 1 else shap_values[0][0]
        else:
            values = shap_values[0]

        # Get feature names from the preprocessor's output
        try:
            feature_names: list[str] = list(
                base_pipe.named_steps["preprocessor"].get_feature_names_out()
            )
        except AttributeError as exc:
            logger.warning("explain_one: could not get feature names: %s", exc)
            feature_names = [f"feature_{i}" for i in range(len(values))]

        # Sort by absolute SHAP value, take top_n
        indexed = sorted(enumerate(values), key=lambda x: abs(x[1]), reverse=True)[
            :top_n
        ]

        return [
            {
                "feature": feature_names[i]
                if i < len(feature_names)
                else f"feature_{i}",
                "shap_value": round(float(v), 4),
                "direction": "increases_risk" if v > 0 else "decreases_risk",
            }
            for i, v in indexed
        ]

    except Exception as exc:
        logger.warning("explain_one: SHAP computation failed: %s", exc)
        return []

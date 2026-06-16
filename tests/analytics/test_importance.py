"""Tests for importance.py — survival model only.

feature_importance() serves only the dev_days_to_decision survival model
(gain-based importance from its public feature_importances_); the structured
classifier/regressor models were deleted.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl
import pytest

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.importance import feature_importance
from zoneto.analytics.train import _build_survival_pipeline


@pytest.fixture()
def model_dir(tmp_path: Path) -> Path:
    d = tmp_path / "models"
    d.mkdir()

    n = 20
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in DEV_CAT_COLS})
    X[DEV_NUM_COLS] = pd.DataFrame(
        np.random.default_rng(1)
        .integers(0, 5, size=(n, len(DEV_NUM_COLS)))
        .astype(float),
        columns=DEV_NUM_COLS,
    )
    events = np.array([True, False] * 10)
    times = np.array([365 + i * 20 for i in range(n)], dtype=np.int32)
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )
    pipe = _build_survival_pipeline(DEV_CAT_COLS, DEV_NUM_COLS)
    pipe.fit(X, y)
    joblib.dump(pipe, d / "dev_days_to_decision.joblib")
    return d


def test_feature_importance_columns(model_dir: Path) -> None:
    result = feature_importance("dev_days_to_decision", model_dir=model_dir)
    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["feature", "importance_mean", "importance_std"]


def test_feature_importance_row_count(model_dir: Path) -> None:
    result = feature_importance("dev_days_to_decision", model_dir=model_dir)
    assert len(result) == len(DEV_CAT_COLS) + len(DEV_NUM_COLS)


def test_feature_importance_sorted_descending(model_dir: Path) -> None:
    result = feature_importance("dev_days_to_decision", model_dir=model_dir)
    means = result["importance_mean"].to_list()
    assert means == sorted(means, reverse=True)


def test_feature_importance_std_is_zero(model_dir: Path) -> None:
    result = feature_importance("dev_days_to_decision", model_dir=model_dir)
    assert result["importance_std"].to_list() == [0.0] * len(result)


def test_feature_importance_unknown_model(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown model"):
        feature_importance("nonexistent_model", model_dir=tmp_path)

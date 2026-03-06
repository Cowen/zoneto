"""Tests for importance.py."""
from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

from zoneto.analytics.features import COA_CAT_COLS, COA_NUM_COLS, DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.importance import feature_importance
from zoneto.analytics.train import build_pipeline


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def model_dir(tmp_path: Path) -> Path:
    d = tmp_path / "models"
    d.mkdir()

    n = 10
    rng = np.random.default_rng(0)
    y_binary = np.array([0, 1] * 5)

    # DEV models (classifier)
    X_dev = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in DEV_CAT_COLS})
    X_dev[DEV_NUM_COLS] = pd.DataFrame(
        rng.integers(0, 5, size=(n, len(DEV_NUM_COLS))).astype(float),
        columns=DEV_NUM_COLS,
    )
    dev_pipe = build_pipeline(
        DEV_CAT_COLS, DEV_NUM_COLS, HistGradientBoostingClassifier(random_state=0)
    )
    dev_pipe.fit(X_dev, y_binary)
    joblib.dump(dev_pipe, d / "dev_applications_approved.joblib")
    joblib.dump(dev_pipe, d / "dev_applications_no_appeal.joblib")

    # COA classifier
    X_coa = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in COA_CAT_COLS})
    X_coa[COA_NUM_COLS] = pd.DataFrame(
        rng.integers(2015, 2023, size=(n, len(COA_NUM_COLS))).astype(float),
        columns=COA_NUM_COLS,
    )
    coa_clf_pipe = build_pipeline(
        COA_CAT_COLS, COA_NUM_COLS, HistGradientBoostingClassifier(random_state=0)
    )
    coa_clf_pipe.fit(X_coa, y_binary)
    joblib.dump(coa_clf_pipe, d / "coa_approved.joblib")

    # COA regressor
    y_days = rng.integers(10, 500, size=n).astype(float)
    coa_reg_pipe = build_pipeline(
        COA_CAT_COLS, COA_NUM_COLS, HistGradientBoostingRegressor(random_state=0)
    )
    coa_reg_pipe.fit(X_coa, y_days)
    joblib.dump(coa_reg_pipe, d / "coa_days_to_approval.joblib")

    return d


@pytest.fixture()
def enriched_dir(tmp_path: Path) -> Path:
    n = 10
    rng = np.random.default_rng(1)

    out = tmp_path / "enriched"
    out.mkdir()

    # DEV enriched
    pl.DataFrame({
        "application_type": ["Rezoning", "Site Plan"] * 5,
        "ward_number": ["Ward 1", "Ward 2"] * 5,
        "zoning_class": ["RS", None] * 5,
        "secondary_plan_name": [None, "Midtown"] * 5,
        "year_submitted": [2021, 2022] * 5,
        "in_heritage_register": [0, 1] * 5,
        "in_heritage_district": [0, 0] * 5,
        "in_secondary_plan": [0, 1] * 5,
        "has_community_meeting": [1, 0] * 5,
        "dev_approved": [1, 0] * 5,
        "dev_no_appeal": [0, 1] * 5,
    }).write_parquet(out / "dev_applications.parquet")

    # COA enriched
    pl.DataFrame({
        "application_type": ["Minor Variance", "Consent"] * 5,
        "sub_type": ["A", "B"] * 5,
        "ward_number": ["Ward 3", "Ward 4"] * 5,
        "zoning_designation": ["RS", None] * 5,
        "year_submitted": [2021, 2022] * 5,
        "coa_approved": [1, 0] * 5,
        "coa_days_to_approval": rng.integers(30, 400, size=n).astype(float).tolist(),
    }).write_parquet(out / "coa.parquet")

    return tmp_path  # return data_dir (parent of enriched/)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_feature_importance_columns(model_dir: Path, enriched_dir: Path) -> None:
    result = feature_importance(
        "dev_applications_approved",
        data_dir=enriched_dir,
        model_dir=model_dir,
    )
    assert isinstance(result, pl.DataFrame)
    assert result.columns == ["feature", "importance_mean", "importance_std"]


def test_feature_importance_row_count(model_dir: Path, enriched_dir: Path) -> None:
    """One row per feature."""
    result = feature_importance(
        "dev_applications_approved",
        data_dir=enriched_dir,
        model_dir=model_dir,
    )
    assert len(result) == len(DEV_CAT_COLS) + len(DEV_NUM_COLS)


def test_feature_importance_sorted_descending(model_dir: Path, enriched_dir: Path) -> None:
    result = feature_importance(
        "dev_applications_approved",
        data_dir=enriched_dir,
        model_dir=model_dir,
    )
    means = result["importance_mean"].to_list()
    assert means == sorted(means, reverse=True)


def test_feature_importance_builtin(model_dir: Path, tmp_path: Path) -> None:
    """Builtin flag works without enriched data."""
    result = feature_importance(
        "dev_applications_no_appeal",
        data_dir=tmp_path,
        model_dir=model_dir,
        builtin=True,
    )
    assert len(result) == len(DEV_CAT_COLS) + len(DEV_NUM_COLS)
    assert result.columns == ["feature", "importance_mean", "importance_std"]
    # importance_std is always 0.0 for builtin
    assert result["importance_std"].to_list() == [0.0] * len(result)


def test_feature_importance_coa_classifier(model_dir: Path, enriched_dir: Path) -> None:
    """COA classifier model produces correct output."""
    result = feature_importance(
        "coa_approved",
        data_dir=enriched_dir,
        model_dir=model_dir,
    )
    assert len(result) == len(COA_CAT_COLS) + len(COA_NUM_COLS)
    means = result["importance_mean"].to_list()
    assert means == sorted(means, reverse=True)


def test_feature_importance_coa_regressor(model_dir: Path, enriched_dir: Path) -> None:
    """COA regressor model uses r2 scoring and produces correct output."""
    result = feature_importance(
        "coa_days_to_approval",
        data_dir=enriched_dir,
        model_dir=model_dir,
    )
    assert len(result) == len(COA_CAT_COLS) + len(COA_NUM_COLS)
    means = result["importance_mean"].to_list()
    assert means == sorted(means, reverse=True)


def test_feature_importance_unknown_model(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown model"):
        feature_importance("nonexistent_model", data_dir=tmp_path, model_dir=tmp_path)

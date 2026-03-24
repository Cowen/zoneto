"""Tests for SHAP explanation generation."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.explain import explain_one
from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import train_source


def _make_dev_parquet(tmp_path: Path) -> Path:
    """30-row dev_applications parquet for training a test model."""
    rng = np.random.default_rng(42)
    n = 30
    dev_appealed = (rng.uniform(size=n) < 0.4).astype(float).tolist()
    for i in range(2):
        dev_appealed[i] = None  # type: ignore[call-overload]
    df = pl.DataFrame(
        {
            "application_type": rng.choice(["OZ", "SA", "OPA"], size=n).tolist(),
            "ward_number": [str(rng.integers(1, 26)) for _ in range(n)],
            "zoning_class": rng.choice(["RS", "RM", None], size=n).tolist(),
            "secondary_plan_name": [None] * n,
            "year_submitted": rng.integers(2018, 2024, size=n).tolist(),
            "in_heritage_register": rng.integers(0, 2, size=n).tolist(),
            "in_heritage_district": rng.integers(0, 2, size=n).tolist(),
            "in_secondary_plan": rng.integers(0, 2, size=n).tolist(),
            "has_community_meeting": rng.integers(0, 2, size=n).tolist(),
            "ward_pct_renters": rng.uniform(0.2, 0.7, size=n).tolist(),
            "ward_median_income": rng.uniform(40_000, 120_000, size=n).tolist(),
            "ward_pop_density": rng.uniform(1000, 8000, size=n).tolist(),
            "ward_pct_detached": rng.uniform(0.1, 0.6, size=n).tolist(),
            "has_parent_application": rng.integers(0, 2, size=n).tolist(),
            "is_combined_application": rng.integers(0, 2, size=n).tolist(),
            "proposed_storeys": pl.Series(
                rng.integers(1, 40, size=n).tolist(), dtype=pl.Int32
            ),
            "proposed_units": pl.Series(
                rng.integers(1, 500, size=n).tolist(), dtype=pl.Int32
            ),
            "ward_appeal_rate_3y": rng.uniform(0.05, 0.25, size=n).tolist(),
            "in_mtsa": rng.integers(0, 2, size=n).tolist(),
            **{f"desc_svd_{i}": rng.uniform(-1, 1, size=n).tolist() for i in range(20)},
            "dev_appealed": pl.Series(dev_appealed, dtype=pl.Float64),
        }
    )
    dest = tmp_path / "dev_applications.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest)
    return dest


@pytest.fixture
def trained_model(tmp_path: Path) -> Path:
    """Train a minimal appeal model and return model_dir."""
    path = _make_dev_parquet(tmp_path)
    train_source(
        enriched_path=path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=tmp_path / "models",
        regressor=False,
        calibrate=False,
    )
    return tmp_path / "models"


@pytest.fixture
def trained_calibrated_model(tmp_path: Path) -> Path:
    """Train a calibrated appeal model (production default) and return model_dir."""
    path = _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models_calibrated"
    train_source(
        enriched_path=path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=model_dir,
        regressor=False,
        calibrate=True,
    )
    return model_dir


def test_explain_one_returns_list(trained_model: Path) -> None:
    """explain_one() returns a list of explanation dicts."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert isinstance(result, list)
    assert len(result) > 0


def test_explain_one_top_n_limit(trained_model: Path) -> None:
    """Returns at most top_n contributions."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=3,
    )
    assert len(result) <= 3


def test_explain_one_result_shape(trained_model: Path) -> None:
    """Each explanation dict has feature, value, and direction keys."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert len(result) > 0
    for item in result:
        assert "feature" in item
        assert "shap_value" in item
        assert "direction" in item
        assert item["direction"] in ("increases_risk", "decreases_risk")


def test_explain_one_missing_model_returns_empty(tmp_path: Path) -> None:
    """Returns empty list when model file does not exist."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ"},
        model_dir=tmp_path,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert result == []


def test_explain_one_works_with_calibrated_model(
    trained_calibrated_model: Path,
) -> None:
    """explain_one() works through the CalibratedClassifierCV unwrapping path."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_calibrated_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert isinstance(result, list)
    assert len(result) > 0

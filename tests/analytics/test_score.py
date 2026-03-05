"""Tests for score.py — uses synthetic trained models from tmp_path."""
from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl
import pytest
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)
from zoneto.analytics.score import score_all, score_one
from zoneto.analytics.train import build_pipeline

# ---------------------------------------------------------------------------
# Fixtures: synthetic trained models + enriched parquet
# ---------------------------------------------------------------------------


def _train_dummy_model(
    model_dir: Path,
    model_name: str,
    cat_cols: list[str],
    num_cols: list[str],
    *,
    regressor: bool = False,
) -> None:
    """Train a minimal sklearn Pipeline on 3 synthetic rows and save it."""
    import numpy as np
    import pandas as pd

    n = 6
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in cat_cols})
    X[num_cols] = pd.DataFrame(
        np.random.default_rng(0)
        .integers(0, 5, size=(n, len(num_cols)))
        .astype(float),
        columns=num_cols,
    )
    y = np.array([0, 1, 0, 1, 0, 1], dtype=float if regressor else int)

    est = (
        HistGradientBoostingRegressor(random_state=0)
        if regressor
        else HistGradientBoostingClassifier(random_state=0)
    )
    pipe = build_pipeline(cat_cols=cat_cols, num_cols=num_cols, estimator=est)
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")


def _setup_models(tmp_path: Path) -> Path:
    model_dir = tmp_path / "models"
    _train_dummy_model(
        model_dir, "dev_applications_approved", DEV_CAT_COLS, DEV_NUM_COLS
    )
    _train_dummy_model(
        model_dir, "dev_applications_no_appeal", DEV_CAT_COLS, DEV_NUM_COLS
    )
    _train_dummy_model(model_dir, "coa_approved", COA_CAT_COLS, COA_NUM_COLS)
    _train_dummy_model(
        model_dir,
        "coa_days_to_approval",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=True,
    )
    return model_dir


def _make_dev_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "application_type": ["Rezoning", "Site Plan"],
            "ward_number": ["Ward 1", "Ward 2"],
            "zoning_class": ["RS", None],
            "secondary_plan_name": [None, "Midtown"],
            "year_submitted": [2021, 2022],
            "in_heritage_register": [0, 1],
            "in_heritage_district": [0, 0],
            "in_secondary_plan": [0, 1],
            "has_community_meeting": [1, 0],
            "dev_approved": [1, 0],
            "dev_no_appeal": [0, 1],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")


def _make_coa_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "application_type": ["Minor Variance", "Consent"],
            "sub_type": ["A", "B"],
            "ward_number": ["1", "2"],
            "zoning_designation": ["RS", None],
            "year_submitted": [2021, 2022],
            "coa_approved": [1, 0],
            "coa_days_to_approval": [90, None],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    df.write_parquet(out / "coa.parquet")


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------


def test_score_all_creates_parquet(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    assert (tmp_path / "scores" / "dev_applications.parquet").exists()
    assert (tmp_path / "scores" / "coa.parquet").exists()


def test_score_all_dev_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_approved" in df.columns
    assert "pred_dev_no_appeal" in df.columns
    assert "prob_dev_approved" in df.columns
    assert "prob_dev_no_appeal" in df.columns


def test_score_all_coa_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "coa.parquet")
    assert "pred_coa_approved" in df.columns
    assert "prob_coa_approved" in df.columns
    assert "pred_coa_days_to_approval" in df.columns


def test_score_all_prob_range(tmp_path: Path) -> None:
    """Probability predictions should be in [0, 1]."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert df["prob_dev_approved"].min() >= 0.0
    assert df["prob_dev_approved"].max() <= 1.0


# ---------------------------------------------------------------------------
# score_one
# ---------------------------------------------------------------------------


def test_score_one_returns_dict(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    result = score_one(
        source="dev_applications",
        features={
            "application_type": "Rezoning",
            "ward_number": "Ward 5",
            "zoning_class": "RS",
            "secondary_plan_name": None,
            "year_submitted": 2022,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_secondary_plan": 0,
            "has_community_meeting": 0,
        },
        model_dir=model_dir,
    )
    assert "pred_dev_approved" in result
    assert "prob_dev_approved" in result
    assert "pred_dev_no_appeal" in result


def test_score_one_coa(tmp_path: Path) -> None:
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    result = score_one(
        source="coa",
        features={
            "application_type": "Minor Variance",
            "sub_type": "A",
            "ward_number": "3",
            "zoning_designation": "RM",
            "year_submitted": 2021,
        },
        model_dir=model_dir,
    )
    assert "pred_coa_approved" in result
    assert "pred_coa_days_to_approval" in result


def test_score_one_unknown_source(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    with pytest.raises(ValueError, match="Unknown source"):
        score_one(source="invalid", features={}, model_dir=model_dir)

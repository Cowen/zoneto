"""Canonical guard that the five quality-bar failures stay deleted.

dev_applications_appealed, dev_applications_approved, coa_approved,
coa_days_to_approval, and permit_issuance_days were deleted — none ever cleared the
production quality bar (training-data limitations). The only served predictive model
is the dev_days_to_decision survival model. These tests ensure the deleted models do
not creep back into training, scoring, metrics, or feature-importance.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.importance import feature_importance
from zoneto.analytics.score import score_all, score_one
from zoneto.analytics.train import train_all

DELETED_MODELS = [
    "dev_applications_appealed",
    "dev_applications_approved",
    "coa_approved",
    "coa_days_to_approval",
    "permit_issuance_days",
]


def _make_dev_parquet(tmp_path: Path) -> Path:
    """Minimal dev_applications parquet with survival labels (dev_days_to_decision)."""
    rng = np.random.default_rng(0)
    n = 120
    df = pl.DataFrame(
        {
            "application_type": rng.choice(["OZ", "SA"], size=n).tolist(),
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
            "unit_excess_ratio": rng.uniform(0.1, 5.0, size=n).tolist(),
            "storey_excess_ratio": rng.uniform(0.1, 5.0, size=n).tolist(),
            "ward_appeal_rate_3y": rng.uniform(0.05, 0.25, size=n).tolist(),
            "in_mtsa": rng.integers(0, 2, size=n).tolist(),
            "in_trca_regulated_area": rng.integers(0, 2, size=n).tolist(),
            "in_greenbelt": rng.integers(0, 2, size=n).tolist(),
            **{f"desc_svd_{i}": rng.uniform(-1, 1, size=n).tolist() for i in range(20)},
            "dev_days_observed": pl.Series(
                rng.integers(30, 1200, size=n).tolist(), dtype=pl.Int32
            ),
            "dev_decision_event": pl.Series(
                (rng.uniform(size=n) < 0.8).astype(int).tolist(), dtype=pl.Int8
            ),
            "is_active": [0] * n,
        }
    )
    dest = tmp_path / "enriched" / "dev_applications.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest)
    return dest


def test_deleted_models_not_trained(tmp_path: Path) -> None:
    """train_all() trains only the survival model; none of the deleted models."""
    _make_dev_parquet(tmp_path)
    # Stub COA + permits parquet to confirm they no longer trigger any model.
    for name in ("coa", "permits_cleared"):
        stub = tmp_path / "enriched" / f"{name}.parquet"
        pl.DataFrame({"placeholder": [1]}).write_parquet(stub)

    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    for name in DELETED_MODELS:
        assert name not in counts, f"{name} must not be trained"
        assert name not in metrics, f"{name} must not appear in metrics"
    assert "dev_days_to_decision" in counts, "survival model must still train"


def test_deleted_models_not_scored(tmp_path: Path) -> None:
    """score_all() writes only dev_applications.parquet, with no appeal columns."""
    _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models"
    train_all(data_dir=tmp_path, model_dir=model_dir)

    # Stub COA + permits enriched parquet to verify they are not scored.
    for name in ("coa", "permits_cleared"):
        stub = tmp_path / "enriched" / f"{name}.parquet"
        pl.DataFrame({"placeholder": [1]}).write_parquet(stub)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    scores_dir = tmp_path / "scores"
    assert (scores_dir / "dev_applications.parquet").exists()
    assert not (scores_dir / "coa.parquet").exists()
    assert not (scores_dir / "permits_cleared.parquet").exists()

    scored_cols = pl.read_parquet(scores_dir / "dev_applications.parquet").columns
    assert "pred_dev_appealed" not in scored_cols
    assert "prob_dev_appealed" not in scored_cols


def test_score_one_no_appeal_predictions(tmp_path: Path) -> None:
    """score_one() never returns appeal predictions; coa/permits return empty."""
    _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models"
    train_all(data_dir=tmp_path, model_dir=model_dir)

    result = score_one(
        "dev_applications", {"application_type": "OZ"}, model_dir=model_dir
    )
    assert "pred_dev_appealed" not in result
    assert "prob_dev_appealed" not in result

    assert score_one("coa", {}, model_dir=model_dir) == {}
    assert score_one("permits_cleared", {}, model_dir=model_dir) == {}


@pytest.mark.parametrize("model_name", DELETED_MODELS)
def test_feature_importance_rejects_deleted_models(model_name: str) -> None:
    """feature_importance() only knows the survival model."""
    with pytest.raises(ValueError):
        feature_importance(model_name)

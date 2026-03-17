"""Tests verifying model retirement gates are enforced."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.score import score_all
from zoneto.analytics.train import train_all, train_source


def _make_dev_parquet(tmp_path: Path) -> Path:
    """Minimal 50-row dev_applications parquet with dev_appealed labels."""
    rng = np.random.default_rng(0)
    n = 50
    dev_appealed = (rng.uniform(size=n) < 0.15).astype(float).tolist()
    # make exactly 5 None to test null handling
    for i in range(5):
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
    dest = tmp_path / "enriched" / "dev_applications.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest)
    return dest


def test_retired_models_not_in_train_all_output(tmp_path: Path) -> None:
    """coa_approved and permit_issuance_days must not appear in train_all() results."""
    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "coa_approved" not in counts, "coa_approved must be retired from train_all()"
    assert "permit_issuance_days" not in counts, (
        "permit_issuance_days must be retired from train_all()"
    )


def test_coa_days_to_approval_is_tracking_only(tmp_path: Path) -> None:
    """coa_days_to_approval must train but production_ready must be False."""
    # Create minimal COA parquet so the model can train
    rng = np.random.default_rng(1)
    n = 30
    coa_approved = (rng.uniform(size=n) < 0.94).astype(int)
    coa_days: list[float | None] = [
        float(rng.uniform(30, 400)) if coa_approved[i] == 1 else None for i in range(n)
    ]
    coa_df = pl.DataFrame(
        {
            "application_type": rng.choice(
                ["Minor Variance", "Consent"], size=n
            ).tolist(),
            "sub_type": rng.choice(["A", "B"], size=n).tolist(),
            "ward_number": [str(rng.integers(1, 26)) for _ in range(n)],
            "zoning_designation": rng.choice(["RS", "RM", None], size=n).tolist(),
            "planning_district": ["Toronto & East York"] * n,
            "work_type": rng.choice(["Variance", "Consent"], size=n).tolist(),
            "year_submitted": rng.integers(2018, 2024, size=n).tolist(),
            "coa_approved": pl.Series(coa_approved.tolist(), dtype=pl.Int8),
            "coa_days_to_approval": pl.Series(coa_days, dtype=pl.Float64),
        }
    )
    coa_path = tmp_path / "enriched" / "coa.parquet"
    coa_path.parent.mkdir(parents=True, exist_ok=True)
    coa_df.write_parquet(coa_path)

    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "coa_days_to_approval" in counts, (
        "coa_days_to_approval must still train for metric tracking"
    )
    assert metrics["coa_days_to_approval"]["production_ready"] is False, (
        "coa_days_to_approval must be production_ready=False regardless of R²"
    )


def test_permits_parquet_does_not_trigger_permit_training(tmp_path: Path) -> None:
    """Even when permits_cleared.parquet exists, permit_issuance_days must not train."""
    rng = np.random.default_rng(2)
    n = 30
    permits_df = pl.DataFrame(
        {
            "permit_type": rng.choice(["New Houses", "Commercial"], size=n).tolist(),
            "structure_type": rng.choice(["Detached House", "Office"], size=n).tolist(),
            "ward_grid": [f"W{rng.integers(1, 30):02d}" for _ in range(n)],
            "est_const_cost": rng.uniform(50_000, 5_000_000, size=n).tolist(),
            "dwelling_units_created": rng.integers(0, 5, size=n).tolist(),
            "dwelling_units_lost": rng.integers(0, 2, size=n).tolist(),
            "residential": rng.integers(0, 2, size=n).tolist(),
            "mercantile": rng.integers(0, 2, size=n).tolist(),
            "industrial": rng.integers(0, 2, size=n).tolist(),
            "institutional": rng.integers(0, 2, size=n).tolist(),
            "permit_issuance_days": rng.integers(10, 300, size=n).tolist(),
        }
    )
    permits_path = tmp_path / "enriched" / "permits_cleared.parquet"
    permits_path.parent.mkdir(parents=True, exist_ok=True)
    permits_df.write_parquet(permits_path)

    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "permit_issuance_days" not in counts, (
        "permit_issuance_days must not train even when enriched parquet exists"
    )


def test_score_all_only_produces_dev_applications_output(tmp_path: Path) -> None:
    """score_all() only writes dev_applications.parquet — no coa or permits output."""
    dev_path = _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    # Train only the appeal model
    train_source(
        enriched_path=dev_path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=model_dir,
        regressor=False,
    )
    # Write minimal metrics.json with production_ready=True for the appeal model
    (model_dir / "metrics.json").write_text(
        json.dumps({"dev_applications_appealed": {"production_ready": True}})
    )

    # Create stub COA and permits enriched parquet to verify they are NOT scored
    coa_stub = tmp_path / "enriched" / "coa.parquet"
    permits_stub = tmp_path / "enriched" / "permits_cleared.parquet"
    pl.DataFrame({"placeholder": [1]}).write_parquet(coa_stub)
    pl.DataFrame({"placeholder": [1]}).write_parquet(permits_stub)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    scores_dir = tmp_path / "scores"
    assert (scores_dir / "dev_applications.parquet").exists(), (
        "dev_applications.parquet must be written"
    )
    assert not (scores_dir / "coa.parquet").exists(), (
        "coa.parquet must NOT be written — model retired"
    )
    assert not (scores_dir / "permits_cleared.parquet").exists(), (
        "permits_cleared.parquet must NOT be written — model retired"
    )

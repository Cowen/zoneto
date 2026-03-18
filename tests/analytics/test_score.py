"""Tests for score.py — uses synthetic trained models from tmp_path."""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import polars as pl
import pytest
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sksurv.ensemble import GradientBoostingSurvivalAnalysis

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
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
        np.random.default_rng(0).integers(0, 5, size=(n, len(num_cols))).astype(float),
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


def _train_dummy_survival_model(
    model_dir: Path,
    model_name: str,
    cat_cols: list[str],
    num_cols: list[str],
) -> None:
    """Train a minimal GradientBoostingSurvivalAnalysis and save as .joblib."""
    import pandas as pd

    n = 20
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in cat_cols})
    X[num_cols] = pd.DataFrame(
        np.random.default_rng(1).integers(0, 5, size=(n, len(num_cols))).astype(float),
        columns=num_cols,
    )
    events = np.array([True, False] * 10)
    times = np.array([365 + i * 20 for i in range(n)], dtype=np.int32)
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    pipe = build_pipeline(
        cat_cols=cat_cols,
        num_cols=num_cols,
        estimator=GradientBoostingSurvivalAnalysis(random_state=0),
    )
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")


def _setup_models(tmp_path: Path) -> Path:
    model_dir = tmp_path / "models"
    _train_dummy_model(
        model_dir, "dev_applications_approved", DEV_CAT_COLS, DEV_NUM_COLS
    )
    _train_dummy_model(
        model_dir, "dev_applications_appealed", DEV_CAT_COLS, DEV_NUM_COLS
    )
    _train_dummy_model(model_dir, "coa_approved", COA_CAT_COLS, COA_NUM_COLS)
    _train_dummy_model(
        model_dir,
        "coa_days_to_approval",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=True,
    )
    _train_dummy_model(
        model_dir,
        "permit_issuance_days",
        PERMIT_CAT_COLS,
        PERMIT_NUM_COLS,
        regressor=True,
    )
    _train_dummy_survival_model(
        model_dir, "dev_days_to_decision", DEV_CAT_COLS, DEV_NUM_COLS
    )
    return model_dir


def _make_dev_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "application_type": ["OZ", "SA"],
            "ward_number": ["Ward 1", "Ward 2"],
            "zoning_class": ["RS", None],
            "secondary_plan_name": [None, "Midtown"],
            "postal_fsa": ["M5V", "M4K"],
            "year_submitted": [2021, 2022],
            "in_heritage_register": [0, 1],
            "in_heritage_district": [0, 0],
            "in_secondary_plan": [0, 1],
            "has_community_meeting": [1, 0],
            "has_parent_application": [0, 1],
            "is_combined_application": [1, 0],
            "proposed_storeys": [12, 0],
            "proposed_units": [200, 0],
            "ward_appeal_rate_3y": [0.15, 0.0],
            "in_mtsa": [1, 0],
            **{f"desc_svd_{i}": [float(i % 3), float((i + 1) % 3)] for i in range(20)},
            "ward_pct_renters": [45.5, 50.2],
            "ward_median_income": [75000.0, 80000.0],
            "ward_pop_density": [3500.0, 4200.0],
            "ward_pct_detached": [25.5, 20.0],
            "dev_approved": [1, 0],
            "dev_appealed": [0, 1],
            "dev_decision_event": [1, 0],
            "dev_days_observed": [525, 800],
            "dev_days_to_decision": [525, None],
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
            "planning_district": ["Toronto & East York", "North York"],
            "work_type": ["Construction", "Change of Use"],
            "year_submitted": [2021, 2022],
            "ward_pct_renters": [45.5, 50.2],
            "ward_median_income": [75000.0, 80000.0],
            "ward_pop_density": [3500.0, 4200.0],
            "ward_pct_detached": [25.5, 20.0],
            "hearing_month": [3, 9],
            "coa_approved": [1, 0],
            "coa_days_to_approval": [90, None],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    df.write_parquet(out / "coa.parquet")


def _make_permits_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "permit_type": ["New Houses", "Small Residential Projects"],
            "structure_type": ["Detached House", "Semi-Detached"],
            "ward_grid": ["W01", "W02"],
            "est_const_cost": [500000.0, 150000.0],
            "dwelling_units_created": [1, 0],
            "dwelling_units_lost": [0, 0],
            "residential": [1, 1],
            "mercantile": [0, 0],
            "industrial": [0, 0],
            "institutional": [0, 0],
            "application_year": [2022, 2023],
            "permit_issuance_days": [100, 45],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    df.write_parquet(out / "permits_cleared.parquet")


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------


def test_score_all_creates_parquet(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    assert (tmp_path / "scores" / "dev_applications.parquet").exists()
    # COA scoring is retired — no coa.parquet output
    assert not (tmp_path / "scores" / "coa.parquet").exists()


def test_score_all_dev_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    # dev_approved is retired (dataset retired, class imbalance 97.3%)
    assert "pred_dev_approved" not in df.columns
    assert "prob_dev_approved" not in df.columns
    # dev_appealed remains the primary product model
    assert "pred_dev_appealed" in df.columns
    assert "prob_dev_appealed" in df.columns


def test_score_all_coa_columns(tmp_path: Path) -> None:
    """COA scoring is retired — this test is removed."""
    # Models retired: coa_approved (AUC 0.535 at 94% base rate),
    # coa_days_to_approval (tracking-only, not served)


def test_score_all_prob_range(tmp_path: Path) -> None:
    """Probability predictions should be in [0, 1]."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert df["prob_dev_appealed"].min() >= 0.0
    assert df["prob_dev_appealed"].max() <= 1.0


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
            "has_parent_application": 0,
            "is_combined_application": 0,
            "proposed_storeys": 10,
            "proposed_units": 50,
            "ward_appeal_rate_3y": 0.15,
            "in_mtsa": 1,
            **{f"desc_svd_{i}": float(i % 3) for i in range(20)},
            "ward_pct_renters": 45.5,
            "ward_median_income": 75000.0,
            "ward_pop_density": 3500.0,
            "ward_pct_detached": 25.5,
        },
        model_dir=model_dir,
    )
    # dev_approved retired — only dev_appealed is scored
    assert "pred_dev_approved" not in result
    assert "prob_dev_approved" not in result
    assert "pred_dev_appealed" in result
    assert "prob_dev_appealed" in result


def test_score_one_coa(tmp_path: Path) -> None:
    """COA scoring is retired — models retired."""
    # coa_approved: AUC 0.535 at 94% base rate
    # coa_days_to_approval: tracking-only, not served


def test_score_one_unknown_source(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    with pytest.raises(ValueError, match="Unknown source"):
        score_one(source="invalid", features={}, model_dir=model_dir)


def test_score_all_permits_parquet(tmp_path: Path) -> None:
    """permit_issuance_days is retired — no permits_cleared.parquet output."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    _make_permits_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    # Permits scoring is retired (R² 0.039, no queue depth signal in open data)
    assert not (tmp_path / "scores" / "permits_cleared.parquet").exists()


def test_score_all_permits_column(tmp_path: Path) -> None:
    """permit_issuance_days is retired — test removed."""
    # R² 0.039 on 133K rows is conclusive; queue depth not in open data


def test_score_all_no_permits_enriched(tmp_path: Path) -> None:
    """score_all succeeds without permit model when enriched file is absent."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    # No permits_cleared.parquet written since no enriched file
    assert not (tmp_path / "scores" / "permits_cleared.parquet").exists()


def test_score_one_permits(tmp_path: Path) -> None:
    """permit_issuance_days is retired — test removed."""
    # score_one() still supports permits_cleared source for API compatibility
    # but returns empty dict (no production-ready models)


def test_score_all_writes_survival_percentiles(
    tmp_path: Path,
) -> None:
    """score_all() appends pred_dev_days_p25/p50/p75 when model exists."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col in df.columns
        assert df[col].dtype in (pl.Float32, pl.Float64)


def test_score_all_skips_survival_model_when_absent(
    tmp_path: Path,
) -> None:
    """score_all() runs without survival percentiles when model .joblib absent."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    # Only non-survival models
    _train_dummy_model(
        model_dir, "dev_applications_appealed", DEV_CAT_COLS, DEV_NUM_COLS
    )
    _train_dummy_model(model_dir, "coa_approved", COA_CAT_COLS, COA_NUM_COLS)
    _train_dummy_model(
        model_dir, "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS, regressor=True
    )

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col not in df.columns


def test_score_one_returns_survival_percentiles(
    tmp_path: Path,
) -> None:
    """score_one() returns pred_dev_days_p25/p50/p75 for OZ dev_applications."""
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "OZ"

    result = score_one("dev_applications", features, model_dir=model_dir)

    for key in ("pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"):
        assert key in result
        assert isinstance(result[key], float)


# ---------------------------------------------------------------------------
# P0: Survival model scored on OZ+SA only
# ---------------------------------------------------------------------------


def _make_dev_enriched_mixed_types(tmp_path: Path) -> None:
    """Enriched dev parquet with OZ, SA, and CD rows."""
    df = pl.DataFrame(
        {
            "application_type": ["OZ", "SA", "CD"],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3"],
            "zoning_class": ["RS", None, "CR"],
            "secondary_plan_name": [None, "Midtown", None],
            "postal_fsa": ["M5V", "M4K", "M6K"],
            "year_submitted": [2021, 2022, 2020],
            "in_heritage_register": [0, 1, 0],
            "in_heritage_district": [0, 0, 0],
            "in_secondary_plan": [0, 1, 0],
            "has_community_meeting": [1, 0, 0],
            "has_parent_application": [0, 1, 0],
            "is_combined_application": [1, 0, 0],
            "proposed_storeys": [12, 0, 5],
            "proposed_units": [200, 0, 10],
            "ward_appeal_rate_3y": [0.15, 0.0, 0.0],
            "in_mtsa": [1, 0, 0],
            **{
                f"desc_svd_{i}": [float(i % 3), float((i + 1) % 3), float((i + 2) % 3)]
                for i in range(20)
            },
            "ward_pct_renters": [45.5, 50.2, 48.0],
            "ward_median_income": [75000.0, 80000.0, 78000.0],
            "ward_pop_density": [3500.0, 4200.0, 3800.0],
            "ward_pct_detached": [25.5, 20.0, 30.0],
            "dev_approved": [1, 0, 1],
            "dev_appealed": [0, 1, None],
            "dev_decision_event": [1, 0, None],
            "dev_days_observed": [525, 800, None],
            "dev_days_to_decision": [525, None, None],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")


def test_score_all_survival_null_for_non_oz_sa(tmp_path: Path) -> None:
    """Non-OZ/SA rows should have null survival percentiles."""
    _make_dev_enriched_mixed_types(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col in df.columns
        # CD row should be null
        cd_row = df.filter(pl.col("application_type") == "CD")
        assert cd_row[col][0] is None
    # OZ rows should have predictions
    oz_row = df.filter(pl.col("application_type") == "OZ")
    assert oz_row["pred_dev_days_p50"][0] is not None


def test_score_one_survival_skipped_for_non_oz_sa(tmp_path: Path) -> None:
    """score_one omits survival percentiles for non-OZ/SA application types."""
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "CD"

    result = score_one("dev_applications", features, model_dir=model_dir)

    for key in ("pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"):
        assert key not in result


# ---------------------------------------------------------------------------
# P1: Separate active-application scoring output
# ---------------------------------------------------------------------------


def _make_dev_enriched_with_active(tmp_path: Path) -> None:
    """Enriched dev parquet with active and closed rows."""
    df = pl.DataFrame(
        {
            "application_type": ["OZ", "SA", "OZ"],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3"],
            "zoning_class": ["RS", None, "CR"],
            "secondary_plan_name": [None, "Midtown", None],
            "postal_fsa": ["M5V", "M4K", "M6K"],
            "year_submitted": [2021, 2022, 2023],
            "in_heritage_register": [0, 1, 0],
            "in_heritage_district": [0, 0, 0],
            "in_secondary_plan": [0, 1, 0],
            "has_community_meeting": [1, 0, 0],
            "has_parent_application": [0, 1, 0],
            "is_combined_application": [1, 0, 0],
            "proposed_storeys": [12, 0, 5],
            "proposed_units": [200, 0, 10],
            "ward_appeal_rate_3y": [0.15, 0.0, 0.0],
            "in_mtsa": [1, 0, 0],
            **{
                f"desc_svd_{i}": [float(i % 3), float((i + 1) % 3), float((i + 2) % 3)]
                for i in range(20)
            },
            "ward_pct_renters": [45.5, 50.2, 48.0],
            "ward_median_income": [75000.0, 80000.0, 78000.0],
            "ward_pop_density": [3500.0, 4200.0, 3800.0],
            "ward_pct_detached": [25.5, 20.0, 30.0],
            "dev_approved": [1, 0, None],
            "dev_appealed": [0, 1, None],
            "dev_decision_event": [1, 0, None],
            "dev_days_observed": [525, 800, None],
            "dev_days_to_decision": [525, None, None],
            "is_active": [0, 0, 1],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")


def test_score_all_writes_active_parquet(tmp_path: Path) -> None:
    """score_all writes a separate dev_applications_active.parquet for active rows."""
    _make_dev_enriched_with_active(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    active_path = tmp_path / "scores" / "dev_applications_active.parquet"
    assert active_path.exists()
    df_active = pl.read_parquet(active_path)
    # Only the active row (is_active=1)
    assert len(df_active) == 1
    assert df_active["is_active"][0] == 1
    assert df_active["year_submitted"][0] == 2023
    # Should have prediction columns
    assert "pred_dev_appealed" in df_active.columns


def test_score_all_active_parquet_absent_when_no_active(tmp_path: Path) -> None:
    """score_all writes active parquet even if 0 active rows (empty file)."""
    _make_dev_enriched(tmp_path)  # has no is_active column by default
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    # Should still produce the main file
    assert (tmp_path / "scores" / "dev_applications.parquet").exists()


# ---------------------------------------------------------------------------
# P1: Skip scoring for non-production-ready models
# ---------------------------------------------------------------------------


def test_score_all_skips_model_when_not_production_ready(tmp_path: Path) -> None:
    """score_all skips models marked production_ready=False in metrics.json."""
    import json

    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    # Mark dev_appealed as not production_ready
    (model_dir / "metrics.json").write_text(
        json.dumps(
            {
                "dev_applications_appealed": {
                    "production_ready": False,
                    "roc_auc_mean": 0.75,
                    "n": 1000,
                }
            }
        )
    )

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_appealed" not in df.columns
    assert "prob_dev_appealed" not in df.columns


def test_score_all_scores_when_metrics_absent(tmp_path: Path) -> None:
    """score_all scores available models when metrics.json does not exist."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    # No metrics.json — default to scoring available models (dev_applications only)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_appealed" in df.columns


# ---------------------------------------------------------------------------
# P0: Survival model percentile output (p25/p50/p75)
# ---------------------------------------------------------------------------


def test_score_all_survival_percentile_columns(tmp_path: Path) -> None:
    """score_all outputs pred_dev_days_p25, p50, p75 columns for survival model."""
    _make_dev_enriched_mixed_types(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col in df.columns, f"Missing column {col}"
    # OZ/SA rows should have values
    oz_row = df.filter(pl.col("application_type") == "OZ")
    assert oz_row["pred_dev_days_p25"][0] is not None
    assert oz_row["pred_dev_days_p50"][0] is not None
    assert oz_row["pred_dev_days_p75"][0] is not None
    # p25 <= p50 <= p75
    assert oz_row["pred_dev_days_p25"][0] <= oz_row["pred_dev_days_p50"][0]
    assert oz_row["pred_dev_days_p50"][0] <= oz_row["pred_dev_days_p75"][0]
    # CD rows should be null
    cd_row = df.filter(pl.col("application_type") == "CD")
    assert cd_row["pred_dev_days_p25"][0] is None
    # Old single median column should be removed
    assert "pred_dev_days_to_decision" not in df.columns


def test_score_one_survival_percentiles(tmp_path: Path) -> None:
    """score_one returns percentile keys for OZ applications."""
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "OZ"

    result = score_one("dev_applications", features, model_dir=model_dir)

    assert "pred_dev_days_p25" in result
    assert "pred_dev_days_p50" in result
    assert "pred_dev_days_p75" in result
    assert result["pred_dev_days_p25"] <= result["pred_dev_days_p50"]
    assert result["pred_dev_days_p50"] <= result["pred_dev_days_p75"]
    # Old key should not be present
    assert "pred_dev_days_to_decision" not in result

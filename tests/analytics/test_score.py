"""Tests for score.py — survival model only (synthetic trained model in tmp_path).

The structured classifier/regressor models were deleted; score_all/score_one serve
only the dev_days_to_decision survival model (OZ+SA) plus the deterministic
statutory_min_decision_days column. See tests/analytics/test_retirement.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import polars as pl
import pytest

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.score import score_all, score_one
from zoneto.analytics.train import _build_survival_pipeline

# ---------------------------------------------------------------------------
# Fixtures: synthetic trained survival model + enriched parquet
# ---------------------------------------------------------------------------


def _train_dummy_survival_model(model_dir: Path) -> None:
    """Fit a minimal GradientBoostingSurvivalAnalysis and save as .joblib."""
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

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / "dev_days_to_decision.joblib")


def _setup_models(tmp_path: Path) -> Path:
    model_dir = tmp_path / "models"
    _train_dummy_survival_model(model_dir)
    (model_dir / "metrics.json").write_text(
        json.dumps({"dev_days_to_decision": {"production_ready": True}})
    )
    return model_dir


def _dev_row(app_type: str, idx: int) -> dict[str, object]:
    row: dict[str, object] = {
        "application_type": app_type,
        "ward_number": f"Ward {idx + 1}",
        "zoning_class": "RS",
        "secondary_plan_name": None,
        "year_submitted": 2021 + idx,
        "in_heritage_register": 0,
        "in_heritage_district": 0,
        "in_secondary_plan": 0,
        "has_community_meeting": 1,
        "has_parent_application": 0,
        "is_combined_application": 1,
        "proposed_storeys": 12,
        "proposed_units": 200,
        "unit_excess_ratio": 2.0,
        "storey_excess_ratio": 1.2,
        "ward_appeal_rate_3y": 0.15,
        "in_mtsa": 1,
        "in_trca_regulated_area": 0,
        "in_greenbelt": 0,
        "ward_pct_renters": 45.5,
        "ward_median_income": 75000.0,
        "ward_pop_density": 3500.0,
        "ward_pct_detached": 25.5,
    }
    for i in range(20):
        row[f"desc_svd_{i}"] = float((i + idx) % 3)
    return row


def _make_dev_enriched(tmp_path: Path, app_types: list[str]) -> None:
    rows = [_dev_row(t, i) for i, t in enumerate(app_types)]
    df = pl.DataFrame(rows)
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------


def test_score_all_creates_only_dev_parquet(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    scores = tmp_path / "scores"
    assert (scores / "dev_applications.parquet").exists()
    assert not (scores / "coa.parquet").exists()
    assert not (scores / "permits_cleared.parquet").exists()


def test_score_all_has_no_appeal_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_appealed" not in df.columns
    assert "prob_dev_appealed" not in df.columns
    assert "pred_dev_approved" not in df.columns


def test_score_all_writes_survival_percentiles(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col in df.columns
        assert df[col].dtype in (pl.Float32, pl.Float64)


def test_score_all_writes_statutory_column(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "statutory_min_decision_days" in df.columns


def test_score_all_survival_null_for_non_oz_sa(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA", "CD"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    cd_row = df.filter(pl.col("application_type") == "CD")
    assert cd_row["pred_dev_days_p50"][0] is None
    oz_row = df.filter(pl.col("application_type") == "OZ")
    assert oz_row["pred_dev_days_p50"][0] is not None


def test_score_all_percentile_ordering(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    oz_row = df.filter(pl.col("application_type") == "OZ")
    assert oz_row["pred_dev_days_p25"][0] <= oz_row["pred_dev_days_p50"][0]
    assert oz_row["pred_dev_days_p50"][0] <= oz_row["pred_dev_days_p75"][0]


def test_score_all_skips_survival_when_joblib_absent(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)
    # No survival joblib written.
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    for col in ["pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"]:
        assert col not in df.columns
    # Statutory column is deterministic and still written.
    assert "statutory_min_decision_days" in df.columns


def test_score_all_skips_survival_when_not_production_ready(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = _setup_models(tmp_path)
    (model_dir / "metrics.json").write_text(
        json.dumps({"dev_days_to_decision": {"production_ready": False}})
    )
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_days_p50" not in df.columns


def test_score_all_scores_when_metrics_absent(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path, ["OZ", "SA"])
    model_dir = tmp_path / "models"
    _train_dummy_survival_model(model_dir)  # no metrics.json written
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_days_p50" in df.columns


def test_score_all_writes_active_parquet(tmp_path: Path) -> None:
    rows = [_dev_row("OZ", 0), _dev_row("SA", 1), _dev_row("OZ", 2)]
    df = pl.DataFrame(rows).with_columns(pl.Series("is_active", [0, 0, 1]))
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    active_path = tmp_path / "scores" / "dev_applications_active.parquet"
    assert active_path.exists()
    df_active = pl.read_parquet(active_path)
    assert len(df_active) == 1
    assert df_active["is_active"][0] == 1
    assert "pred_dev_days_p50" in df_active.columns


# ---------------------------------------------------------------------------
# score_one
# ---------------------------------------------------------------------------


def test_score_one_returns_survival_percentiles_for_oz(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "OZ"

    result = score_one("dev_applications", features, model_dir=model_dir)

    assert "pred_dev_appealed" not in result
    for key in ("pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"):
        assert key in result
        assert isinstance(result[key], float)
    assert result["pred_dev_days_p25"] <= result["pred_dev_days_p50"]
    assert result["pred_dev_days_p50"] <= result["pred_dev_days_p75"]


def test_score_one_survival_skipped_for_non_oz_sa(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "CD"

    result = score_one("dev_applications", features, model_dir=model_dir)

    for key in ("pred_dev_days_p25", "pred_dev_days_p50", "pred_dev_days_p75"):
        assert key not in result


def test_score_one_coa_and_permits_return_empty(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    assert score_one("coa", {}, model_dir=model_dir) == {}
    assert score_one("permits_cleared", {}, model_dir=model_dir) == {}


def test_score_one_unknown_source(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    with pytest.raises(ValueError, match="Unknown source"):
        score_one(source="invalid", features={}, model_dir=model_dir)

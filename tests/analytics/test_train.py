"""Tests for train.py — synthetic data, no actual parquet required."""
from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl
from sklearn.pipeline import Pipeline

from zoneto.analytics.train import build_pipeline, train_all, train_source


def _make_dev_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched dev_applications.parquet."""
    df = pl.DataFrame(
        {
            "application_type": [
                "Rezoning", "Site Plan", "Rezoning", "OPA", "Site Plan"
            ],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3", "Ward 1", "Ward 4"],
            "zoning_class": ["RS", "RM", None, "CR", "RS"],
            "secondary_plan_name": [None, "Midtown", None, "Midtown", None],
            "year_submitted": [2018, 2019, 2020, 2021, 2022],
            "in_heritage_register": [0, 1, 0, 0, 1],
            "in_heritage_district": [0, 0, 0, 1, 0],
            "in_secondary_plan": [0, 1, 0, 1, 0],
            "has_community_meeting": [1, 0, 0, 1, 0],
            "dev_approved": [1, 0, 1, 1, 0],
            "dev_no_appeal": [0, None, 0, 1, 0],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_coa_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched coa.parquet."""
    df = pl.DataFrame(
        {
            "application_type": [
                "Minor Variance",
                "Consent",
                "Minor Variance",
                "Consent",
                "Minor Variance",
            ],
            "sub_type": ["A", "B", "A", "C", "B"],
            "ward_number": ["1", "2", "3", "4", "5"],
            "zoning_designation": ["RS", "RM", None, "CR", "RS"],
            "year_submitted": [2019, 2020, 2021, 2022, 2022],
            "coa_approved": [1, 0, 1, 0, 1],
            "coa_days_to_approval": [95, None, 120, None, 60],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    dest = out / "coa.parquet"
    df.write_parquet(dest)
    return dest


# ---------------------------------------------------------------------------
# build_pipeline
# ---------------------------------------------------------------------------


def test_build_pipeline_returns_sklearn_pipeline() -> None:
    """build_pipeline returns a Pipeline instance."""
    from sklearn.ensemble import HistGradientBoostingClassifier

    pipe = build_pipeline(
        cat_cols=["application_type", "ward_number"],
        num_cols=["year_submitted"],
        estimator=HistGradientBoostingClassifier(),
    )
    assert isinstance(pipe, Pipeline)


def test_build_pipeline_steps() -> None:
    """build_pipeline creates preprocessor and estimator steps."""
    from sklearn.ensemble import HistGradientBoostingClassifier

    pipe = build_pipeline(
        cat_cols=["application_type"],
        num_cols=["year_submitted"],
        estimator=HistGradientBoostingClassifier(),
    )
    step_names = [name for name, _ in pipe.steps]
    assert "preprocessor" in step_names
    assert "estimator" in step_names


# ---------------------------------------------------------------------------
# train_source
# ---------------------------------------------------------------------------


def test_train_source_classifier(tmp_path: Path) -> None:
    """train_source creates a joblib file for classifier."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_approved",
        cat_cols=[
            "application_type",
            "ward_number",
            "zoning_class",
            "secondary_plan_name",
        ],
        num_cols=[
            "year_submitted",
            "in_heritage_register",
            "in_heritage_district",
            "in_secondary_plan",
            "has_community_meeting",
        ],
        model_name="dev_applications_approved",
        model_dir=model_dir,
        regressor=False,
    )
    assert (model_dir / "dev_applications_approved.joblib").exists()


def test_train_source_loads_valid_pipeline(tmp_path: Path) -> None:
    """train_source creates a loadable sklearn Pipeline."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_approved",
        cat_cols=[
            "application_type",
            "ward_number",
            "zoning_class",
            "secondary_plan_name",
        ],
        num_cols=[
            "year_submitted",
            "in_heritage_register",
            "in_heritage_district",
            "in_secondary_plan",
            "has_community_meeting",
        ],
        model_name="dev_applications_approved",
        model_dir=model_dir,
        regressor=False,
    )
    pipe = joblib.load(model_dir / "dev_applications_approved.joblib")
    assert isinstance(pipe, Pipeline)


def test_train_source_regressor(tmp_path: Path) -> None:
    """train_source creates a joblib file for regressor."""
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "coa.parquet",
        label_col="coa_days_to_approval",
        cat_cols=[
            "application_type",
            "sub_type",
            "ward_number",
            "zoning_designation",
        ],
        num_cols=["year_submitted"],
        model_name="coa_days_to_approval",
        model_dir=model_dir,
        regressor=True,
    )
    assert (model_dir / "coa_days_to_approval.joblib").exists()


def test_train_source_drops_null_labels(tmp_path: Path) -> None:
    """train_source must not fail when some label rows are null."""
    _make_dev_enriched(tmp_path)
    model_dir = tmp_path / "models"
    # dev_no_appeal has one null row — should still train on non-null rows
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_no_appeal",
        cat_cols=[
            "application_type",
            "ward_number",
            "zoning_class",
            "secondary_plan_name",
        ],
        num_cols=[
            "year_submitted",
            "in_heritage_register",
            "in_heritage_district",
            "in_secondary_plan",
            "has_community_meeting",
        ],
        model_name="dev_applications_no_appeal",
        model_dir=model_dir,
        regressor=False,
    )
    assert (model_dir / "dev_applications_no_appeal.joblib").exists()


# ---------------------------------------------------------------------------
# train_all
# ---------------------------------------------------------------------------


def test_train_all_creates_four_models(tmp_path: Path) -> None:
    """train_all creates all four model files."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_all(data_dir=tmp_path, model_dir=model_dir)
    expected = [
        "dev_applications_approved.joblib",
        "dev_applications_no_appeal.joblib",
        "coa_approved.joblib",
        "coa_days_to_approval.joblib",
    ]
    for name in expected:
        assert (model_dir / name).exists(), f"Missing {name}"

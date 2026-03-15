"""Tests for train.py — synthetic data, no actual parquet required."""

from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl
import pytest
from sklearn.calibration import CalibratedClassifierCV
from sklearn.pipeline import Pipeline

from zoneto.analytics.train import build_pipeline, train_all, train_source


def _make_dev_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched dev_applications.parquet."""
    df = pl.DataFrame(
        {
            "application_type": [
                "Rezoning",
                "Site Plan",
                "Rezoning",
                "OPA",
                "Site Plan",
            ],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3", "Ward 1", "Ward 4"],
            "zoning_class": ["RS", "RM", None, "CR", "RS"],
            "secondary_plan_name": [None, "Midtown", None, "Midtown", None],
            "year_submitted": [2018, 2019, 2020, 2021, 2022],
            "in_heritage_register": [0, 1, 0, 0, 1],
            "in_heritage_district": [0, 0, 0, 1, 0],
            "in_secondary_plan": [0, 1, 0, 1, 0],
            "has_community_meeting": [1, 0, 0, 1, 0],
            "ward_pct_renters": [45.5, 50.2, 48.0, 45.5, 55.0],
            "ward_median_income": [75000.0, 80000.0, 78000.0, 75000.0, 70000.0],
            "ward_pop_density": [3500.0, 4200.0, 3800.0, 3500.0, 4500.0],
            "ward_pct_detached": [25.5, 20.0, 30.0, 25.5, 18.0],
            "dev_approved": [1, 0, 1, 1, 0],
            "dev_appealed": [0, None, 0, 1, 0],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_dev_enriched_large(tmp_path: Path) -> Path:
    """
    Write a larger enriched dev_applications.parquet for calibration tests (30 rows)
    """
    n = 30
    df = pl.DataFrame(
        {
            "application_type": (["Rezoning", "Site Plan"] * 15),
            "ward_number": ([f"Ward {i % 5 + 1}" for i in range(n)]),
            "zoning_class": (["RS", "RM", None, "CR", "RS"] * 6),
            "secondary_plan_name": ([None, "Midtown"] * 15),
            "year_submitted": ([2015 + i % 8 for i in range(n)]),
            "in_heritage_register": ([0, 1] * 15),
            "in_heritage_district": ([0, 0, 1, 0, 0] * 6),
            "in_secondary_plan": ([0, 1] * 15),
            "has_community_meeting": ([1, 0] * 15),
            "ward_pct_renters": ([45.5 + i * 0.5 for i in range(n)]),
            "ward_median_income": ([75000.0 + i * 100 for i in range(n)]),
            "ward_pop_density": ([3500.0 + i * 50 for i in range(n)]),
            "ward_pct_detached": ([25.5 + i * 0.3 for i in range(n)]),
            "dev_approved": ([1, 0] * 15),
            "dev_appealed": ([0, 1] * 15),
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_permits_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched permits_cleared.parquet."""
    df = pl.DataFrame(
        {
            "permit_type": [
                "New Houses",
                "Small Residential Projects",
                "Commercial",
                "New Houses",
            ],
            "structure_type": [
                "Detached House",
                "Semi-Detached",
                "Office",
                "Row House",
            ],
            "ward_grid": ["W01", "W02", "W03", "W04"],
            "est_const_cost": [500000.0, 150000.0, 2000000.0, 800000.0],
            "dwelling_units_created": [1, 0, 0, 2],
            "dwelling_units_lost": [0, 0, 0, 0],
            "residential": [1, 1, 0, 1],
            "mercantile": [0, 0, 1, 0],
            "industrial": [0, 0, 0, 0],
            "institutional": [0, 0, 0, 0],
            "permit_issuance_days": [100, 45, 200, 80],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "permits_cleared.parquet"
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
                "Consent",
                "Minor Variance",
                "Consent",
                "Minor Variance",
                "Consent",
            ],
            "sub_type": ["A", "B", "A", "C", "B", "A", "B", "C", "A", "B"],
            "ward_number": ["1", "2", "3", "4", "5", "1", "2", "3", "4", "5"],
            "zoning_designation": [
                "RS",
                "RM",
                None,
                "CR",
                "RS",
                "RS",
                "RM",
                None,
                "CR",
                "RS",
            ],
            "planning_district": [
                "Toronto & East York",
                "North York",
                "Etobicoke York",
                "Scarborough",
                "North York",
                "Toronto & East York",
                "North York",
                "Etobicoke York",
                "Scarborough",
                "North York",
            ],
            "work_type": [
                "Construction",
                "Change of Use",
                "Construction",
                "Change of Use",
                "Construction",
                "Change of Use",
                "Construction",
                "Change of Use",
                "Construction",
                "Change of Use",
            ],
            "year_submitted": [
                2019,
                2020,
                2021,
                2022,
                2022,
                2019,
                2020,
                2021,
                2022,
                2022,
            ],
            "ward_pct_renters": [
                45.5,
                50.2,
                48.0,
                52.0,
                55.0,
                45.5,
                50.2,
                48.0,
                52.0,
                55.0,
            ],
            "ward_median_income": [
                75000.0,
                80000.0,
                78000.0,
                70000.0,
                68000.0,
                75000.0,
                80000.0,
                78000.0,
                70000.0,
                68000.0,
            ],
            "ward_pop_density": [
                3500.0,
                4200.0,
                3800.0,
                4500.0,
                4800.0,
                3500.0,
                4200.0,
                3800.0,
                4500.0,
                4800.0,
            ],
            "ward_pct_detached": [
                25.5,
                20.0,
                30.0,
                18.5,
                15.0,
                25.5,
                20.0,
                30.0,
                18.5,
                15.0,
            ],
            "hearing_month": [3, 6, 9, 11, 4, 3, 6, 9, 11, 4],
            "coa_approved": [1, 0, 1, 0, 1, 1, 0, 1, 0, 1],
            "coa_days_to_approval": [95, None, 120, None, 60, 100, 50, 110, 70, 80],
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
    # dev_appealed has one null row — should still train on non-null rows
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_appealed",
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
        model_name="dev_applications_appealed",
        model_dir=model_dir,
        regressor=False,
    )
    assert (model_dir / "dev_applications_appealed.joblib").exists()


# ---------------------------------------------------------------------------
# train_all
# ---------------------------------------------------------------------------


def test_train_all_coa_uses_kfold_cv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """COA models use KFold (year_col=None), dev_appealed uses TimeSeriesSplit."""
    from zoneto.analytics.train import evaluate_source

    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    _make_permits_enriched(tmp_path)
    model_dir = tmp_path / "models"

    # Mock evaluate_source to capture the year_col parameter
    call_args = []
    original_evaluate = evaluate_source

    def mock_evaluate(
        enriched_path,
        label_col,
        cat_cols,
        num_cols,
        *,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    ):
        call_args.append(
            {
                "label_col": label_col,
                "year_col": year_col,
                "regressor": regressor,
            }
        )
        # Call original to get real results
        return original_evaluate(
            enriched_path=enriched_path,
            label_col=label_col,
            cat_cols=cat_cols,
            num_cols=num_cols,
            regressor=regressor,
            cv=cv,
            year_col=year_col,
        )

    monkeypatch.setattr("zoneto.analytics.train.evaluate_source", mock_evaluate)

    train_all(data_dir=tmp_path, model_dir=model_dir)

    # Find the calls for COA and dev_appealed models
    coa_approved_call = next(c for c in call_args if c["label_col"] == "coa_approved")
    coa_days_call = next(
        c for c in call_args if c["label_col"] == "coa_days_to_approval"
    )
    dev_appealed_call = next(c for c in call_args if c["label_col"] == "dev_appealed")

    # COA models must use year_col=None (KFold)
    assert coa_approved_call["year_col"] is None, (
        "coa_approved should use KFold (year_col=None)"
    )
    assert coa_days_call["year_col"] is None, (
        "coa_days_to_approval should use KFold (year_col=None)"
    )

    # dev_appealed must use year_col="year_submitted" (TimeSeriesSplit)
    assert dev_appealed_call["year_col"] == "year_submitted", (
        "dev_appealed should use TimeSeriesSplit"
    )


# ---------------------------------------------------------------------------
# evaluate_source
# ---------------------------------------------------------------------------


def test_evaluate_source_classifier(tmp_path: Path) -> None:
    """evaluate_source returns dict with per-metric mean/std/n for classifier."""
    from zoneto.analytics.train import evaluate_source

    _make_dev_enriched(tmp_path)
    result = evaluate_source(
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
        regressor=False,
        cv=2,
        year_col=None,  # shuffled CV for small test dataset
    )
    assert "n" in result
    assert result["n"] == 5
    assert "roc_auc_mean" in result
    assert "roc_auc_std" in result
    assert "brier_score_mean" in result
    assert "avg_precision_mean" in result


def test_evaluate_source_regressor(tmp_path: Path) -> None:
    """evaluate_source returns dict with per-metric mean/std/n for regressor."""
    from zoneto.analytics.train import evaluate_source

    _make_coa_enriched(tmp_path)
    result = evaluate_source(
        enriched_path=tmp_path / "enriched" / "coa.parquet",
        label_col="coa_days_to_approval",
        cat_cols=["application_type", "sub_type", "ward_number", "zoning_designation"],
        num_cols=["year_submitted"],
        regressor=True,
        cv=2,
        year_col=None,  # shuffled CV for small test dataset
    )
    assert "n" in result
    assert result["n"] == 8  # 8 non-null coa_days_to_approval rows
    assert "r2_mean" in result
    assert "mae_mean" in result
    assert "rmse_mean" in result


# ---------------------------------------------------------------------------
# train_all
# ---------------------------------------------------------------------------


def test_train_all_creates_core_models(tmp_path: Path) -> None:
    """train_all creates the three core model files (permits optional)."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    counts, metrics = train_all(data_dir=tmp_path, model_dir=model_dir)
    expected = [
        "dev_applications_appealed.joblib",
        "coa_approved.joblib",
        "coa_days_to_approval.joblib",
    ]
    for name in expected:
        assert (model_dir / name).exists(), f"Missing {name}"
    # dev_applications_approved is retired: dataset frozen, 97.3% class imbalance
    assert not (model_dir / "dev_applications_approved.joblib").exists()


def test_train_all_creates_permit_model(tmp_path: Path) -> None:
    """train_all creates permit_issuance_days.joblib when enriched permits exist."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    _make_permits_enriched(tmp_path)
    model_dir = tmp_path / "models"
    counts, metrics = train_all(data_dir=tmp_path, model_dir=model_dir)
    assert (model_dir / "permit_issuance_days.joblib").exists()
    assert "permit_issuance_days" in metrics
    assert "r2_mean" in metrics["permit_issuance_days"]


def test_train_all_skips_permit_model_if_no_data(tmp_path: Path) -> None:
    """train_all succeeds without permit model when enriched file is absent."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    # No permits_cleared.parquet
    model_dir = tmp_path / "models"
    counts, metrics = train_all(data_dir=tmp_path, model_dir=model_dir)
    assert "permit_issuance_days" not in metrics


def test_train_all_returns_metrics(tmp_path: Path) -> None:
    """train_all returns a tuple of (row_counts, metrics) with per-metric keys."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    counts, metrics = train_all(data_dir=tmp_path, model_dir=model_dir)
    assert isinstance(counts, dict)
    assert isinstance(metrics, dict)
    expected_models = [
        "dev_applications_appealed",
        "coa_approved",
        "coa_days_to_approval",
    ]
    for name in expected_models:
        assert name in metrics
        assert "n" in metrics[name]
        # Classifiers have roc_auc; regressors have r2
        assert "roc_auc_mean" in metrics[name] or "r2_mean" in metrics[name]
    assert (model_dir / "metrics.json").exists()


# ---------------------------------------------------------------------------
# calibration
# ---------------------------------------------------------------------------


def test_train_source_calibration_produces_calibrated_classifier(
    tmp_path: Path,
) -> None:
    """train_source with calibrate=True wraps classifiers in CalibratedClassifierCV."""
    _make_dev_enriched_large(tmp_path)
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
        calibrate=True,
    )
    model = joblib.load(model_dir / "dev_applications_approved.joblib")
    assert isinstance(model, CalibratedClassifierCV)


def test_train_source_calibration_false_produces_pipeline(tmp_path: Path) -> None:
    """train_source with calibrate=False saves a plain Pipeline."""
    _make_dev_enriched(tmp_path)
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
        calibrate=False,
    )
    model = joblib.load(model_dir / "dev_applications_approved.joblib")
    assert isinstance(model, Pipeline)


def test_train_source_calibration_regressor_is_pipeline(tmp_path: Path) -> None:
    """Regressors are never wrapped in CalibratedClassifierCV."""
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
            "planning_district",
        ],
        num_cols=["year_submitted", "hearing_month"],
        model_name="coa_days_to_approval",
        model_dir=model_dir,
        regressor=True,
        calibrate=True,  # ignored for regressors
    )
    model = joblib.load(model_dir / "coa_days_to_approval.joblib")
    assert isinstance(model, Pipeline)

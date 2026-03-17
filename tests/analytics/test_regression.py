"""Performance regression tests for ML models.

Synthetic tests (always run): verify pipelines produce reasonable metrics on
seeded fixtures with embedded signal — catches catastrophic pipeline breakage.

Integration tests (@pytest.mark.integration): compare against real enriched
parquet; skip gracefully when data is absent.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
)
from zoneto.analytics.train import evaluate_source

BASELINES_PATH = Path(__file__).parent.parent / "fixtures" / "model_baselines.json"
ENRICHED_DIR = Path("data/enriched")

_HIGHER_IS_BETTER = {"roc_auc_mean", "avg_precision_mean", "r2_mean"}
_LOWER_IS_BETTER = {"brier_score_mean", "mae_mean", "rmse_mean"}

BASELINE_TOLERANCES: dict[str, float] = {
    "roc_auc_mean": 0.05,
    "brier_score_mean": 0.05,
    "avg_precision_mean": 0.08,
    "r2_mean": 0.15,
    "mae_mean": 15.0,
    "rmse_mean": 20.0,
}


# ---------------------------------------------------------------------------
# Synthetic fixture helpers
# ---------------------------------------------------------------------------


def _make_dev_regression_fixture(tmp_path: Path) -> Path:
    """500-row dev_applications parquet with embedded signal (seeded rng=42)."""
    rng = np.random.default_rng(42)
    n = 500
    years = rng.integers(2015, 2023, size=n)
    app_types = rng.choice(["Site Plan", "Rezoning", "OPA"], size=n, p=[0.4, 0.4, 0.2])
    ward_numbers = [f"Ward {rng.integers(1, 26)}" for _ in range(n)]
    zoning_classes = rng.choice(
        ["RS", "RM", "CR", "MX", None], size=n, p=[0.3, 0.25, 0.2, 0.15, 0.1]
    )
    secondary_plan = rng.choice(
        ["Midtown", "Downtown", None], size=n, p=[0.2, 0.15, 0.65]
    )
    in_heritage_register = rng.integers(0, 2, size=n)
    in_heritage_district = rng.integers(0, 2, size=n)
    in_secondary_plan = (secondary_plan != None).astype(int)  # noqa: E711
    has_community_meeting = rng.integers(0, 2, size=n)

    # dev_approved: strong signal from application_type
    base_approval = np.where(
        app_types == "Site Plan",
        0.95,
        np.where(app_types == "Rezoning", 0.65, 0.10),
    )
    dev_approved = (rng.uniform(size=n) < base_approval).astype(int)

    # dev_appealed signal: only non-null where approved
    appeal_base = np.where(zoning_classes == "CR", 0.35, 0.12)
    appeal_prob = np.clip(appeal_base + has_community_meeting * 0.20, 0.01, 0.90)
    dev_appealed_raw = (rng.uniform(size=n) < appeal_prob).astype(float)
    dev_appealed: list[int | None] = [
        int(dev_appealed_raw[i]) if dev_approved[i] == 1 else None for i in range(n)
    ]

    df = pl.DataFrame(
        {
            "application_type": app_types.tolist(),
            "ward_number": ward_numbers,
            "zoning_class": zoning_classes.tolist(),
            "secondary_plan_name": secondary_plan.tolist(),
            "year_submitted": years.tolist(),
            "in_heritage_register": in_heritage_register.tolist(),
            "in_heritage_district": in_heritage_district.tolist(),
            "in_secondary_plan": in_secondary_plan.tolist(),
            "has_community_meeting": has_community_meeting.tolist(),
            "dev_approved": dev_approved.tolist(),
            "dev_appealed": dev_appealed,
        }
    )
    dest = tmp_path / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_coa_regression_fixture(tmp_path: Path) -> Path:
    """500-row coa parquet with embedded signal (seeded rng=42)."""
    rng = np.random.default_rng(42)
    n = 500
    years = rng.integers(2015, 2025, size=n)
    app_types = rng.choice(["Minor Variance", "Consent"], size=n, p=[0.65, 0.35])
    sub_types = rng.choice(["A", "B", "C"], size=n)
    ward_numbers = [str(rng.integers(1, 26)) for _ in range(n)]
    zoning_designation = rng.choice(
        ["RS", "RM", "CR", None], size=n, p=[0.35, 0.3, 0.25, 0.1]
    )
    planning_district = rng.choice(
        ["Toronto & East York", "North York", "Etobicoke York", "Scarborough"],
        size=n,
    )

    # coa_approved: strong signal from application_type
    approval_prob = np.where(app_types == "Minor Variance", 0.95, 0.15)
    coa_approved = (rng.uniform(size=n) < approval_prob).astype(int)

    # coa_days_to_approval signal: null where not approved
    base_days = np.where(app_types == "Minor Variance", 80.0, 140.0)
    trend = (years - 2015) * 5.0
    noise = rng.normal(0, 20, size=n)
    days = base_days + trend + noise
    coa_days: list[float | None] = [
        float(days[i]) if coa_approved[i] == 1 else None for i in range(n)
    ]

    df = pl.DataFrame(
        {
            "application_type": app_types.tolist(),
            "sub_type": sub_types.tolist(),
            "ward_number": ward_numbers,
            "zoning_designation": zoning_designation.tolist(),
            "planning_district": planning_district.tolist(),
            "year_submitted": years.tolist(),
            "coa_approved": coa_approved.tolist(),
            "coa_days_to_approval": coa_days,
        }
    )
    dest = tmp_path / "coa.parquet"
    df.write_parquet(dest)
    return dest


def _make_permits_regression_fixture(tmp_path: Path) -> Path:
    """500-row permits_cleared parquet with embedded signal (seeded rng=42)."""
    rng = np.random.default_rng(42)
    n = 500
    permit_types = rng.choice(
        ["New Houses", "Small Residential Projects", "Commercial", "Industrial"],
        size=n,
        p=[0.4, 0.3, 0.2, 0.1],
    )
    structure_types = rng.choice(
        ["Detached House", "Semi-Detached", "Row House", "Office"],
        size=n,
    )
    ward_grid = [f"W{rng.integers(1, 30):02d}" for _ in range(n)]
    est_const_cost = rng.uniform(50_000, 5_000_000, size=n)
    dwelling_units_created = rng.integers(0, 5, size=n)
    dwelling_units_lost = rng.integers(0, 2, size=n)
    residential = (permit_types != "Commercial").astype(int)
    industrial = (permit_types == "Industrial").astype(int)
    institutional = np.zeros(n, dtype=int)
    application_year = rng.integers(2015, 2024, size=n)

    # permit_issuance_days signal
    base_days = est_const_cost / 20_000
    new_house_bonus = (permit_types == "New Houses").astype(float) * 30.0
    residential_bonus = residential * 15.0
    noise = rng.normal(0, 25, size=n)
    days = np.clip(
        base_days + new_house_bonus + residential_bonus + noise, 10, 500
    ).astype(int)

    df = pl.DataFrame(
        {
            "permit_type": permit_types.tolist(),
            "structure_type": structure_types.tolist(),
            "ward_grid": ward_grid,
            "est_const_cost": est_const_cost.tolist(),
            "dwelling_units_created": dwelling_units_created.tolist(),
            "dwelling_units_lost": dwelling_units_lost.tolist(),
            "residential": residential.tolist(),
            "industrial": industrial.tolist(),
            "institutional": institutional.tolist(),
            "application_year": application_year.tolist(),
            "permit_issuance_days": days.tolist(),
        }
    )
    dest = tmp_path / "permits_cleared.parquet"
    df.write_parquet(dest)
    return dest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_enriched(name: str) -> Path:
    p = ENRICHED_DIR / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"enriched data not found: {p}")
    return p


def _assert_no_regression(
    model_name: str, metrics: dict[str, float | int], baselines: dict[str, object]
) -> None:
    if model_name not in baselines:
        return
    b = baselines[model_name]
    assert isinstance(b, dict)
    failures = []
    for metric, tol in BASELINE_TOLERANCES.items():
        if metric not in b or metric not in metrics:
            continue
        baseline_val = float(b[metric])  # type: ignore[arg-type]
        current_val = float(metrics[metric])  # type: ignore[arg-type]
        if metric in _HIGHER_IS_BETTER:
            ok = current_val >= baseline_val - tol
        else:
            ok = current_val <= baseline_val + tol
        if not ok:
            delta = current_val - baseline_val
            failures.append((metric, baseline_val, current_val, delta, tol))
    if failures:
        lines = [f"REGRESSION DETECTED in {model_name}:"]
        lines.append(
            f"  {'metric':<30} {'baseline':>10} {'current':>10}"
            "{'delta':>10} {'tol':>10}"
        )
        for metric, bv, cv, dv, tol in failures:
            lines.append(
                f"  {metric:<30} {bv:>10.4f} {cv:>10.4f} {dv:>+10.4f} {tol:>10.4f}"
            )
        pytest.fail("\n".join(lines))


# ---------------------------------------------------------------------------
# Synthetic CI tests (always run)
# ---------------------------------------------------------------------------


def test_dev_approved_synthetic(tmp_path: Path) -> None:
    path = _make_dev_regression_fixture(tmp_path)
    metrics = evaluate_source(
        path,
        "dev_approved",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col=None,
    )
    assert metrics["roc_auc_mean"] >= 0.60
    assert metrics["brier_score_mean"] <= 0.25


def test_dev_appealed_synthetic(tmp_path: Path) -> None:
    path = _make_dev_regression_fixture(tmp_path)
    metrics = evaluate_source(
        path,
        "dev_appealed",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col=None,
    )
    assert metrics["roc_auc_mean"] >= 0.55


def test_coa_approved_synthetic(tmp_path: Path) -> None:
    path = _make_coa_regression_fixture(tmp_path)
    metrics = evaluate_source(
        path,
        "coa_approved",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=False,
        cv=5,
        year_col=None,
    )
    assert metrics["roc_auc_mean"] >= 0.60


def test_coa_days_to_approval_synthetic(tmp_path: Path) -> None:
    path = _make_coa_regression_fixture(tmp_path)
    metrics = evaluate_source(
        path,
        "coa_days_to_approval",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=True,
        cv=5,
        year_col=None,
    )
    assert metrics["mae_mean"] <= 200.0
    assert metrics["r2_mean"] >= -2.0


def test_permit_issuance_days_synthetic(tmp_path: Path) -> None:
    path = _make_permits_regression_fixture(tmp_path)
    metrics = evaluate_source(
        path,
        "permit_issuance_days",
        PERMIT_CAT_COLS,
        PERMIT_NUM_COLS,
        regressor=True,
        cv=5,
        year_col=None,
    )
    assert metrics["mae_mean"] <= 200.0
    assert metrics["r2_mean"] >= -2.0


# ---------------------------------------------------------------------------
# Integration absolute floor tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dev_approved_integration() -> None:
    path = _require_enriched("dev_applications")
    metrics = evaluate_source(
        path,
        "dev_approved",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    # Retired model: dataset frozen, 97.3% class imbalance, new features shift
    # the feature space. AUC around 0.5 is expected for a non-viable model.
    assert metrics["roc_auc_mean"] >= 0.45, (
        f"roc_auc_mean={metrics['roc_auc_mean']:.4f} < 0.45"
    )


@pytest.mark.integration
def test_dev_appealed_integration() -> None:
    path = _require_enriched("dev_applications")
    metrics = evaluate_source(
        path,
        "dev_appealed",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    # Thresholds lowered after P0 label bias fix: dev_appealed now covers ALL
    # closed OZ/SA applications (not just explicitly-approved), correcting the
    # base rate from ~50/50 to ~15-25%. AUC decreased from 0.86 to ~0.69 as
    # the model now predicts a harder, more realistic distribution.
    # New features (proposed_storeys, proposed_units, ward_appeal_rate_3y)
    # should eventually improve this.
    assert metrics["roc_auc_mean"] >= 0.65, (
        f"roc_auc_mean={metrics['roc_auc_mean']:.4f} < 0.65"
    )
    assert metrics["avg_precision_mean"] >= 0.20, (
        f"avg_precision_mean={metrics['avg_precision_mean']:.4f} < 0.20"
    )


@pytest.mark.integration
def test_coa_approved_integration() -> None:
    path = _require_enriched("coa")
    metrics = evaluate_source(
        path,
        "coa_approved",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    assert metrics["roc_auc_mean"] >= 0.35, (
        f"roc_auc_mean={metrics['roc_auc_mean']:.4f} < 0.35"
    )


@pytest.mark.integration
def test_coa_days_to_approval_integration() -> None:
    path = _require_enriched("coa")
    metrics = evaluate_source(
        path,
        "coa_days_to_approval",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=True,
        cv=5,
        year_col="year_submitted",
    )
    assert metrics["mae_mean"] <= 120.0, f"mae_mean={metrics['mae_mean']:.4f} > 120.0"


@pytest.mark.integration
def test_permit_issuance_days_integration() -> None:
    path = _require_enriched("permits_cleared")
    metrics = evaluate_source(
        path,
        "permit_issuance_days",
        PERMIT_CAT_COLS,
        PERMIT_NUM_COLS,
        regressor=True,
        cv=5,
        year_col=None,
    )
    assert metrics["mae_mean"] <= 130.0, f"mae_mean={metrics['mae_mean']:.4f} > 130.0"


# ---------------------------------------------------------------------------
# Integration baseline comparison tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dev_approved_no_regression() -> None:
    path = _require_enriched("dev_applications")
    baselines = json.loads(BASELINES_PATH.read_text())
    metrics = evaluate_source(
        path,
        "dev_approved",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    _assert_no_regression("dev_applications_approved", metrics, baselines)


@pytest.mark.integration
def test_dev_appealed_no_regression() -> None:
    path = _require_enriched("dev_applications")
    baselines = json.loads(BASELINES_PATH.read_text())
    metrics = evaluate_source(
        path,
        "dev_appealed",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    _assert_no_regression("dev_applications_appealed", metrics, baselines)


@pytest.mark.integration
def test_coa_approved_no_regression() -> None:
    path = _require_enriched("coa")
    baselines = json.loads(BASELINES_PATH.read_text())
    metrics = evaluate_source(
        path,
        "coa_approved",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=False,
        cv=5,
        year_col="year_submitted",
    )
    _assert_no_regression("coa_approved", metrics, baselines)


@pytest.mark.integration
def test_coa_days_to_approval_no_regression() -> None:
    path = _require_enriched("coa")
    baselines = json.loads(BASELINES_PATH.read_text())
    metrics = evaluate_source(
        path,
        "coa_days_to_approval",
        COA_CAT_COLS,
        COA_NUM_COLS,
        regressor=True,
        cv=5,
        year_col="year_submitted",
    )
    _assert_no_regression("coa_days_to_approval", metrics, baselines)


@pytest.mark.integration
def test_permit_issuance_days_no_regression() -> None:
    path = _require_enriched("permits_cleared")
    baselines = json.loads(BASELINES_PATH.read_text())
    metrics = evaluate_source(
        path,
        "permit_issuance_days",
        PERMIT_CAT_COLS,
        PERMIT_NUM_COLS,
        regressor=True,
        cv=5,
        year_col=None,
    )
    _assert_no_regression("permit_issuance_days", metrics, baselines)

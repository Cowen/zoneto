"""Performance regression tests for the survival model (dev_days_to_decision).

dev_days_to_decision is the only served predictive model — the structured
classifier/regressor models were deleted for failing the quality bar.

Synthetic test (always run): verifies the survival pipeline recovers embedded
signal on a seeded fixture — catches catastrophic pipeline breakage.

Integration tests (@pytest.mark.integration): evaluate against the real enriched
parquet; skip gracefully when data is absent.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import evaluate_survival

BASELINES_PATH = Path(__file__).parent.parent / "fixtures" / "model_baselines.json"
ENRICHED_DIR = Path("data/enriched")

# Production gate for the survival model (train_all gates on this same floor).
CONCORDANCE_FLOOR = 0.65
# How far below the stored baseline the C-index may drift before we flag it.
CONCORDANCE_TOLERANCE = 0.05


# ---------------------------------------------------------------------------
# Synthetic fixture helpers
# ---------------------------------------------------------------------------


def _make_dev_survival_fixture(tmp_path: Path) -> Path:
    """700-row dev_applications parquet with embedded survival signal (rng=42).

    Only carries the columns that hold signal; evaluate_survival fills the
    remaining DEV feature columns with nulls (median-imputed in the pipeline).
    """
    rng = np.random.default_rng(42)
    n = 700
    app_types = rng.choice(["OZ", "SA"], size=n, p=[0.6, 0.4])
    years = rng.integers(2015, 2023, size=n)
    proposed_units = rng.integers(1, 400, size=n).astype(float)
    proposed_storeys = rng.integers(1, 60, size=n).astype(float)

    # Larger projects take longer to decide (monotonic signal the model can learn).
    base = 200.0 + proposed_units * 0.8 + proposed_storeys * 4.0
    noise = rng.normal(0, 40, size=n)
    days = np.clip(base + noise, 10, 3650).astype(int)
    # ~80% of rows are closed (event observed); the rest are censored.
    event = (rng.uniform(size=n) < 0.8).astype(int)

    df = pl.DataFrame(
        {
            "application_type": app_types.tolist(),
            "year_submitted": years.tolist(),
            "proposed_units": proposed_units.tolist(),
            "proposed_storeys": proposed_storeys.tolist(),
            "dev_days_observed": days.tolist(),
            "dev_decision_event": event.tolist(),
        }
    )
    dest = tmp_path / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _require_enriched(name: str) -> Path:
    p = ENRICHED_DIR / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"enriched data not found: {p}")
    return p


# ---------------------------------------------------------------------------
# Synthetic CI test (always runs)
# ---------------------------------------------------------------------------


def test_dev_survival_synthetic(tmp_path: Path) -> None:
    path = _make_dev_survival_fixture(tmp_path)
    metrics = evaluate_survival(
        path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        cv=5,
        year_col=None,
    )
    # Embedded monotonic signal → C-index comfortably above chance (0.5).
    assert metrics["concordance_index_mean"] >= 0.60, (
        f"concordance_index_mean={metrics['concordance_index_mean']:.4f} < 0.60"
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dev_survival_integration_floor() -> None:
    path = _require_enriched("dev_applications")
    metrics = evaluate_survival(
        path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
    )
    assert metrics["concordance_index_mean"] >= CONCORDANCE_FLOOR, (
        f"concordance_index_mean={metrics['concordance_index_mean']:.4f}"
        f" < {CONCORDANCE_FLOOR} (production gate)"
    )


@pytest.mark.integration
def test_dev_survival_no_regression() -> None:
    path = _require_enriched("dev_applications")
    baselines = json.loads(BASELINES_PATH.read_text())
    baseline = baselines.get("dev_days_to_decision")
    if not baseline or "concordance_index_mean" not in baseline:
        pytest.skip("no survival baseline recorded")
    metrics = evaluate_survival(
        path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
    )
    baseline_val = float(baseline["concordance_index_mean"])
    current_val = float(metrics["concordance_index_mean"])
    assert current_val >= baseline_val - CONCORDANCE_TOLERANCE, (
        f"REGRESSION: concordance_index_mean={current_val:.4f} <"
        f" baseline {baseline_val:.4f} - {CONCORDANCE_TOLERANCE}"
    )

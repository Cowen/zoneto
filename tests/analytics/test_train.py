"""Tests for train.py — survival model only (synthetic data, no real parquet).

The structured classifier/regressor training machinery was deleted along with the
five models that never cleared the quality bar; dev_days_to_decision (survival) is
the only remaining model. See tests/analytics/test_retirement.py for the guard.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import evaluate_survival, train_all, train_survival


def _make_dev_enriched_survival(tmp_path: Path) -> Path:
    """Write minimal enriched dev_applications.parquet with survival columns.

    Only OZ+SA rows with non-null dev_decision_event are usable for the
    survival model. Active rows (event=0) are right-censored observations.
    """
    n = 20
    df = pl.DataFrame(
        {
            "application_type": (["OZ", "SA"] * 10),
            "ward_number": ([f"Ward {i % 4 + 1}" for i in range(n)]),
            "zoning_class": (["RS", "RM", None, "CR", "RS"] * 4),
            "secondary_plan_name": ([None, "Midtown"] * 10),
            "year_submitted": ([2016 + i % 6 for i in range(n)]),
            "in_heritage_register": ([0, 1] * 10),
            "in_heritage_district": ([0, 0, 1, 0, 0] * 4),
            "in_secondary_plan": ([0, 1] * 10),
            "has_community_meeting": ([1, 0] * 10),
            "ward_pct_renters": ([45.0 + i for i in range(n)]),
            "ward_median_income": ([70000.0 + i * 500 for i in range(n)]),
            "ward_pop_density": ([3000.0 + i * 100 for i in range(n)]),
            "ward_pct_detached": ([20.0 + i * 0.5 for i in range(n)]),
            "has_parent_application": ([0, 1] * 10),
            "is_combined_application": ([1, 0] * 10),
            "proposed_storeys": ([12, None] * 10),
            "proposed_units": ([200, None] * 10),
            "ward_appeal_rate_3y": ([0.15, None] * 10),
            # Survival columns:
            "dev_decision_event": ([1, 1, 0, 1, 0] * 4),  # 1=closed, 0=active
            "dev_days_observed": ([365, 500, 800, 200, 1000] * 4),
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_dev_enriched_no_survival(tmp_path: Path) -> Path:
    """Write a dev_applications.parquet without the dev_days_observed column."""
    df = pl.DataFrame(
        {
            "application_type": ["OZ", "SA", "OZ"],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3"],
            "zoning_class": ["RS", "RM", None],
            "secondary_plan_name": [None, "Midtown", None],
            "year_submitted": [2018, 2019, 2020],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


# ---------------------------------------------------------------------------
# Survival model
# ---------------------------------------------------------------------------


def test_train_survival_creates_joblib(tmp_path: Path) -> None:
    """train_survival() serializes dev_days_to_decision.joblib."""
    path = _make_dev_enriched_survival(tmp_path)
    model_dir = tmp_path / "models"
    count = train_survival(
        enriched_path=path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_days_to_decision",
        model_dir=model_dir,
    )
    assert (model_dir / "dev_days_to_decision.joblib").exists()
    assert count > 0


def test_evaluate_survival_returns_concordance_index(tmp_path: Path) -> None:
    """evaluate_survival() returns dict with concordance_index_mean key."""
    path = _make_dev_enriched_survival(tmp_path)
    result = evaluate_survival(
        enriched_path=path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
    )
    assert "concordance_index_mean" in result
    assert "concordance_index_std" in result
    assert "n" in result
    assert isinstance(result["concordance_index_mean"], float)


# ---------------------------------------------------------------------------
# train_all
# ---------------------------------------------------------------------------


def test_train_all_includes_survival_when_label_present(tmp_path: Path) -> None:
    """train_all() trains dev_days_to_decision when survival columns exist."""
    _make_dev_enriched_survival(tmp_path)

    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "dev_days_to_decision" in counts
    assert "dev_days_to_decision" in metrics
    assert "concordance_index_mean" in metrics["dev_days_to_decision"]
    assert "production_ready" in metrics["dev_days_to_decision"]
    assert isinstance(metrics["dev_days_to_decision"]["production_ready"], bool)

    metrics_file = tmp_path / "models" / "metrics.json"
    assert metrics_file.exists()
    saved = json.loads(metrics_file.read_text())
    assert "dev_days_to_decision" in saved


def test_train_all_skips_survival_when_label_absent(tmp_path: Path) -> None:
    """train_all() skips survival model gracefully when dev_days_observed absent."""
    _make_dev_enriched_no_survival(tmp_path)

    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "dev_days_to_decision" not in counts
    assert "dev_days_to_decision" not in metrics
    # metrics.json still written (empty), so downstream loaders don't crash.
    assert (tmp_path / "models" / "metrics.json").exists()


def test_train_all_survival_gated_on_concordance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A survival model with c-index < 0.65 must be production_ready=False."""
    _make_dev_enriched_survival(tmp_path)

    def low_cindex(*args: object, **kwargs: object) -> dict[str, float | int]:
        return {"concordance_index_mean": 0.55, "concordance_index_std": 0.05, "n": 20}

    monkeypatch.setattr("zoneto.analytics.train.evaluate_survival", low_cindex)

    _, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")
    assert metrics["dev_days_to_decision"]["production_ready"] is False

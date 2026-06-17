"""Integration guard for the comparable-retrieval concordance harness.

Pins that description-text retrieval beats a random-comp baseline on the robust
axis (application_type) and is no worse than random on zone, against the real
enriched corpus — so a regression that severs retrieval from structured
comparability fails loudly. Skipped when enriched data or the TF-IDF model is
absent (CI-safe); the harness itself is in scripts/comps_eval.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from scripts.comps_eval import run_eval

_ENRICHED = Path("data/enriched/dev_applications.parquet")
_TFIDF = Path("models/desc_tfidf.joblib")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def results() -> dict:
    if not _ENRICHED.exists() or not _TFIDF.exists():
        pytest.skip("enriched data or desc_tfidf.joblib not found")
    return run_eval(str(_ENRICHED), "models", sample=100, k=10, seed=0, verbose=False)


def test_eval_runs_over_corpus(results: dict) -> None:
    """The harness must produce a non-empty query set from the real corpus."""
    assert results["n_queries"] > 0


def test_type_concordance_beats_random(results: dict) -> None:
    """Application_type is encoded in description boilerplate, so retrieval should
    clearly beat a random-comp baseline on it — the robust, well-covered axis."""
    conc = results["concordance"]["type"]
    base = results["baseline"]["type"]
    assert conc is not None and base is not None
    assert conc > base, (
        f"type concordance ({conc:.1%}) did not beat random ({base:.1%})"
    )


def test_zone_not_worse_than_random(results: dict) -> None:
    """Zone concordance is weakly captured by text (documented gap), but retrieval
    must never rank *worse* than chance on it."""
    conc = results["concordance"]["zone"]
    base = results["baseline"]["zone"]
    assert conc is not None and base is not None
    assert conc >= base, f"zone concordance ({conc:.1%}) fell below random ({base:.1%})"


def test_scale_magnitude_is_broadly_measurable(results: dict) -> None:
    """The absolute-magnitude scale axis must be measurable for a large share of
    queries (~60% coverage), unlike the sparse excess-ratio axis. Guards the
    column-selection regression where proposed_storeys/units were not loaded."""
    n_mag = results["measurable"]["scale_mag"]
    assert n_mag > 0.3 * results["n_queries"], (
        f"scale_mag measurable for only {n_mag}/{results['n_queries']} queries — "
        "expected broad coverage from absolute magnitude"
    )

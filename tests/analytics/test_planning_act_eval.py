"""Integration guard for the Planning Act process-classifier harness.

Pins the clean-signal OZ rezoning recall and rezoning precision against the real
enriched corpus so a regression that craters the process classifier fails loudly.
Skipped when enriched data is absent (CI-safe); the harness itself is in
scripts/planning_act_eval.py.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from scripts.planning_act_eval import run_eval

_ENRICHED = Path("data/enriched/dev_applications.parquet")

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def results() -> dict:
    if not _ENRICHED.exists():
        pytest.skip(f"enriched data not found: {_ENRICHED}")
    return run_eval(str(_ENRICHED), verbose=False)


def test_clean_recall_above_floor(results: dict) -> None:
    # Current ~10.4%; a loose floor catches a crater without being brittle to
    # the lower-bound nature of the metric (no height_m / use in the batch set).
    recall = results.get("OZ_recall_clean")
    assert recall is not None
    assert recall > 0.05, f"OZ clean rezoning recall cratered: {recall:.1%}"


def test_rezoning_precision_above_floor(results: dict) -> None:
    # Current ~65%; most rows we call 'rezoning' should actually be OZ filings.
    prec = results.get("rezoning_precision")
    assert prec is not None
    assert prec > 0.40, f"rezoning precision cratered: {prec:.1%}"

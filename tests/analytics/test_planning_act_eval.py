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
# The Official Plan land-use layer is optional/interim (CLAUDE.md): fetched
# separately by `just op`, graceful when absent. When it has not been fetched,
# zero OP coverage is the expected state, not a join regression — so the OP guard
# skips rather than false-failing.
_OP_SOURCE = Path("data/reference/op_land_use.geojson")

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


def test_op_layer_present_and_non_regressive(results: dict) -> None:
    """The OP layer must be joined (non-zero coverage) and the OP-conformity signal
    can only add OZ detections, never remove them (with_op >= path_only).

    Skipped when the optional op_land_use.geojson layer has not been fetched —
    zero coverage is then expected, not a regression. When the source IS present
    but coverage is still zero, that is a real join/staleness failure and the
    assertion below fires (re-run `just enrich` after `just op`).
    """
    if not _OP_SOURCE.exists():
        pytest.skip(
            "op_land_use.geojson not fetched (optional/interim layer) — run "
            "`just op` then `just enrich` to provision and exercise this guard"
        )
    coverage = results.get("op_coverage")
    assert coverage is not None
    assert coverage > 0.0, (
        "OP designation coverage is 0 — run `just op` then `just enrich` so the "
        "op_land_use.geojson layer is joined into the enriched corpus"
    )
    path_only = results.get("OZ_recall_path_only")
    with_op = results.get("OZ_recall_with_op")
    assert path_only is not None and with_op is not None
    assert with_op >= path_only, (
        f"OP-conformity signal reduced OZ detection: {with_op:.1%} < {path_only:.1%}"
    )

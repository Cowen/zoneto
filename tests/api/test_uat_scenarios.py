"""UAT failure-pattern scenarios as integration regression tests.

Wraps scripts/uat_scenarios.py so its HARD auto-checks (confidence band,
red-flag phrases, well-formed output) run under pytest in the integration
tier — i.e. `just regression-integration`. This is what makes the UAT a real
regression gate rather than a manual `just uat` spot-check.

ADVISORY signals (expected-phrase presence, the finch resubmission-pair
ordering) are intentionally NOT asserted here: they flake run-to-run on LLM
paraphrase and the documented pair-compression limitation (see the
finch-57-original golden case), so gating CI on them would make the suite
nondeterministic. `just uat` still surfaces them as NOTE lines for a human.

Requires ANTHROPIC_API_KEY + data/reference + data/enriched + models, like
the narrator integration tier. One LLM call per scenario (six total: four
singles plus the two finch halves), memoized per session. The scenario
definitions live in scripts/uat_scenarios.py and are imported here so the
two never drift.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from scripts.uat_scenarios import (
    _FINCH_ORIGINAL,
    _FINCH_REVISED,
    _SCENARIOS,
    _auto_checks,
    _run_single,
)

if TYPE_CHECKING:
    from zoneto.llm.agents import NarratorAgents

_REF_DIR = Path("data/reference")
_DATA_DIR = Path("data")
_MODEL_DIR = Path("models")
_ENRICHED_PATH = _DATA_DIR / "enriched" / "dev_applications.parquet"

_SCEN_BY_ID = {s.id: s for s in _SCENARIOS}
_SINGLE_IDS = [s.id for s in _SCENARIOS if s.id != "finch-57-pair"]

_RESULT_CACHE: dict[str, dict] = {}
_AGENTS: NarratorAgents | None = None


def _require() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        pytest.skip("ANTHROPIC_API_KEY not set")
    if not _REF_DIR.exists():
        pytest.skip(f"GIS reference data not found: {_REF_DIR}")
    if not _ENRICHED_PATH.exists():
        pytest.skip(f"enriched data not found: {_ENRICHED_PATH}")


def _agents() -> NarratorAgents:
    global _AGENTS
    if _AGENTS is None:
        from zoneto.llm.agents import make_narrator_agents

        _AGENTS = make_narrator_agents()
    return _AGENTS


def _result(scenario_id: str, lat: float, lon: float, description: str) -> dict:
    """Run one scenario through the pipeline, memoized per session."""
    if scenario_id not in _RESULT_CACHE:
        _RESULT_CACHE[scenario_id] = _run_single(
            scenario_id,
            lat,
            lon,
            description,
            _REF_DIR,
            _DATA_DIR,
            _MODEL_DIR,
            _agents(),
        )
    return _RESULT_CACHE[scenario_id]


@pytest.mark.integration
@pytest.mark.parametrize("scenario_id", _SINGLE_IDS)
def test_uat_scenario_hard_checks(scenario_id: str) -> None:
    """Given a UAT failure-pattern scenario, when narrated by the real LLM,
    then every HARD auto-check passes (confidence band, no red-flag phrase,
    well-formed output). Advisory phrase misses are ignored here by design.
    """
    _require()
    scenario = _SCEN_BY_ID[scenario_id]
    result = _result(scenario.id, scenario.lat, scenario.lon, scenario.description)
    hard, _advisory = _auto_checks(result, scenario)
    assert not hard, (
        f"{scenario.label}: hard UAT checks failed: {hard} — "
        f"run `just uat --case {scenario.id}` for the full output"
    )


@pytest.mark.integration
def test_uat_finch_pair_well_formed() -> None:
    """Given the finch resubmission pair, when both halves are narrated, then
    each returns a confidence score and a well-formed summary. The revised >
    original ordering is advisory only (the narrator cannot reliably separate
    the pair — see finch-57-original), so it is surfaced by `just uat`, not
    asserted here.
    """
    _require()
    pair = _SCEN_BY_ID["finch-57-pair"]
    r_orig = _result("finch-57-original", pair.lat, pair.lon, _FINCH_ORIGINAL)
    r_rev = _result("finch-57-revised", pair.lat, pair.lon, _FINCH_REVISED)
    for tag, r in (("original", r_orig), ("revised", r_rev)):
        assert r["score"] is not None, f"finch {tag}: narrator returned no confidence"
        assert r["summary"].strip(), f"finch {tag}: empty summary"
        assert "CONFIDENCE:" not in r["summary"], f"finch {tag}: CONFIDENCE line leaked"

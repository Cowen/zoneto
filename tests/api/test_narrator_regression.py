"""Narrator confidence calibration regression tests.

Driven by the golden-case fixture tests/fixtures/narrator_eval_cases.json:
real Toronto planning applications with known outcomes (verified against the
city's AIC ArcGIS FeatureServer), each exercising a distinct confidence
mechanism (floor-70 as-of-right, precedent floor-55, cap-30 extreme,
LLM-dominated mid band). `just narrator-eval` runs the same cases with
per-case diagnostics.

Two tiers:

CI-safe (no marker, runs in `just test` / pre-commit): replays each case's
snapshotted site context (ci.site) and hand-built similarity stub through
extract -> check_compliance -> the deterministic confidence overrides, with a
fake LLM. No network, no API key, no data/ dependency.

Integration (-m integration) requires:
  - ANTHROPIC_API_KEY environment variable set
  - GIS reference data at data/reference/
  - Enriched dev_applications.parquet at data/enriched/ (for similarity)
Each case is narrated by the real LLM exactly once per session (memoized),
so the whole tier costs len(cases) LLM calls. Note the cache assumes a
single-process pytest run (no xdist), which is how the justfile invokes it.

The pipeline helper `_narration_for` mirrors scripts/narrator_eval.py::
_narrate_case; keep them in sync.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from zoneto.analytics.compliance import check_compliance
from zoneto.analytics.extract import extract_project_features
from zoneto.api.llm_client import FakeLLMClient
from zoneto.api.narrator import _apply_confidence_overrides, narrate_evaluation

_FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "narrator_eval_cases.json"
_FIXTURE = json.loads(_FIXTURE_PATH.read_text())
_CASES = {c["id"]: c for c in _FIXTURE["cases"]}
_CASE_IDS = list(_CASES)
_ORDERINGS = _FIXTURE["orderings"]

_REF_DIR = Path("data/reference")
_DATA_DIR = Path("data")
_MODEL_DIR = Path("models")
_ENRICHED_PATH = _DATA_DIR / "enriched" / "dev_applications.parquet"


# ---------------------------------------------------------------------------
# CI-safe tier — deterministic overrides against snapshotted site contexts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case_id", _CASE_IDS)
def test_deterministic_overrides(case_id: str) -> None:
    """Given a golden case's snapshotted site context and similarity stub,
    when the confidence overrides are probed from both extremes (5 and 95),
    then exactly the floors/caps recorded in expected_overrides bind.
    """
    case = _CASES[case_id]
    ci = case["ci"]
    expected = ci["expected_overrides"]
    extracted = extract_project_features(case["description"])
    violations = check_compliance(extracted, ci["site"])

    low = _apply_confidence_overrides(
        5, violations, ci["site"], extracted, ci["sim_stub"]
    )
    high = _apply_confidence_overrides(
        95, violations, ci["site"], extracted, ci["sim_stub"]
    )

    label = case["label"]
    if expected["cap_30"]:
        assert high <= 30, f"{label}: cap-30 did not bind (95 -> {high})"
    elif expected["floor_70"]:
        assert low == 70, f"{label}: floor-70 did not bind (5 -> {low})"
        assert high == 95, f"{label}: unexpected cap (95 -> {high})"
    elif expected["precedent_floor_55"]:
        assert low == 55, f"{label}: precedent floor-55 did not bind (5 -> {low})"
        # high == 95 also proves the cap-30 was exempted by the precedent
        assert high == 95, f"{label}: cap not exempted by precedent (95 -> {high})"
    else:
        assert (low, high) == (5, 95), (
            f"{label}: expected passthrough, got (5 -> {low}, 95 -> {high})"
        )


@pytest.mark.parametrize("case_id", _CASE_IDS)
def test_narrate_end_to_end_mocked(case_id: str) -> None:
    """Given a golden case and a fake LLM answering CONFIDENCE: 50,
    when narrate_evaluation runs (prompt assembly, parse, overrides),
    then the returned score reflects the case's expected floors/caps.
    """
    case = _CASES[case_id]
    ci = case["ci"]
    expected = ci["expected_overrides"]
    extracted = extract_project_features(case["description"])
    violations = check_compliance(extracted, ci["site"])

    client = FakeLLMClient("Summary.\n\nCONFIDENCE: 50")
    summary, score = narrate_evaluation(
        ci["site"],
        extracted,
        violations,
        [],
        client,
        description=case["description"],
        description_similarity=ci["sim_stub"],
    )

    assert summary, "narrator returned an empty summary"
    assert score is not None
    if expected["cap_30"]:
        assert score == 30
    elif expected["floor_70"]:
        assert score == 70
    elif expected["precedent_floor_55"]:
        assert score == 55
    else:
        assert score == 50


# ---------------------------------------------------------------------------
# Integration tier — real LLM, real GIS reference data, real similarity corpus
# ---------------------------------------------------------------------------

_NARRATION_CACHE: dict[str, tuple[str, int | None]] = {}


def _require_api_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    return key


def _require_data() -> None:
    if not _REF_DIR.exists():
        pytest.skip(f"GIS reference data not found: {_REF_DIR}")
    if not _ENRICHED_PATH.exists():
        pytest.skip(f"enriched data not found: {_ENRICHED_PATH}")


def _narration_for(case_id: str, api_key: str) -> tuple[str, int | None]:
    """Run the full evaluate pipeline for a golden case, memoized per session.

    Mirrors scripts/narrator_eval.py::_narrate_case.
    """
    if case_id not in _NARRATION_CACHE:
        from zoneto.api.desc_similarity import score_description_similarity
        from zoneto.api.llm_client import AnthropicClient
        from zoneto.api.site_context import lookup_site_context

        case = _CASES[case_id]
        site = lookup_site_context(case["lat"], case["lon"], _REF_DIR)
        extracted = extract_project_features(case["description"])
        violations = check_compliance(extracted, site)
        sim = score_description_similarity(
            case["description"],
            data_dir=_DATA_DIR,
            model_dir=_MODEL_DIR,
            zoning_class=site.get("zoning_class"),
            lat=case["lat"],
            lon=case["lon"],
            radius_m=2000.0,
        )
        llm = AnthropicClient(api_key=api_key)
        _NARRATION_CACHE[case_id] = narrate_evaluation(
            site,
            extracted,
            violations,
            chunks=[],
            llm_client=llm,
            description=case["description"],
            description_similarity=sim,
        )
    return _NARRATION_CACHE[case_id]


@pytest.mark.integration
@pytest.mark.parametrize("case_id", _CASE_IDS)
def test_confidence_in_band(case_id: str) -> None:
    """Given a golden case with a verified real-world outcome, when narrated
    by the real LLM, then confidence lands in the case's expected band.
    Advisory cases pin documented behavior (see the fixture's notes field).
    """
    _require_data()
    api_key = _require_api_key()
    case = _CASES[case_id]
    _, score = _narration_for(case_id, api_key)
    lo = case["expected_confidence"]["min"]
    hi = case["expected_confidence"]["max"]
    assert score is not None, f"{case['label']}: narrator returned no confidence"
    assert lo <= score <= hi, (
        f"{case['label']}: confidence {score} outside [{lo}, {hi}] "
        f"({case['mechanism']}) — run `just narrator-eval` for details"
    )


@pytest.mark.integration
@pytest.mark.parametrize("case_id", _CASE_IDS)
def test_summary_well_formed(case_id: str) -> None:
    """Given any golden case, when narrated, then the summary is non-empty,
    bounded in length, and the CONFIDENCE line was consumed by parsing.
    """
    _require_data()
    api_key = _require_api_key()
    summary, _ = _narration_for(case_id, api_key)
    assert summary.strip(), "empty summary"
    assert len(summary) < 4000, f"summary unexpectedly long ({len(summary)} chars)"
    assert "CONFIDENCE:" not in summary, "CONFIDENCE line leaked into the summary"


@pytest.mark.integration
@pytest.mark.parametrize(
    "ordering",
    _ORDERINGS,
    ids=[f"{o['higher']}>{o['lower']}" for o in _ORDERINGS],
)
def test_orderings(ordering: dict) -> None:
    """Given two golden cases with a known relative ranking, when both are
    narrated, then the higher case strictly outscores the lower one.
    Uses the per-session cache — no extra LLM calls.
    """
    _require_data()
    api_key = _require_api_key()
    _, hi_score = _narration_for(ordering["higher"], api_key)
    _, lo_score = _narration_for(ordering["lower"], api_key)
    assert hi_score is not None and lo_score is not None
    assert hi_score > lo_score, (
        f"{ordering['higher']} ({hi_score}) should outscore "
        f"{ordering['lower']} ({lo_score}) — {ordering['note']}"
    )

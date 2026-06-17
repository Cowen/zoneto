"""UAT scenario runner for failure-pattern cases.

Runs five curated narrator scenarios (covering employment use-mismatch,
extreme scale in a contested ward, the employment-OPA blind spot, a
resubmission scope reduction, and an as-of-right success) through the full
evaluate pipeline and prints annotated output for a development professional
to review.

Each scenario carries two layers of checks:
  - AUTO-CHECKS (machine-decidable): confidence band, required/forbidden
    phrases in the summary, non-empty well-formed output, and — for the
    resubmission pair — that the revised proposal outscores the original.
    These gate the exit code.
  - HUMAN RUBRIC (judgement): yes/no questions printed for a planner to
    answer. Never gates the exit code; this is the user-acceptance surface.

Usage:
    uv run python scripts/uat_scenarios.py [--case mendota-2] [--quiet]

Requires ANTHROPIC_API_KEY, data/reference/, and data/enriched/ + models/.
Skips gracefully (exit 0) when prerequisites are missing.

Exit codes: 0 = all auto-checks passed (or skipped for missing prereqs),
1 = some auto-checks failed, 2 = bad arguments.

The pipeline helper is scripts/narrator_eval.py::_narrate_case; this runner
reuses it so the two stay in lockstep.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from zoneto.analytics.compliance import Severity

if TYPE_CHECKING:
    from zoneto.llm.agents import NarratorAgents


@dataclass
class UATScenario:
    id: str
    label: str
    lat: float
    lon: float
    description: str
    confidence_min: int
    confidence_max: int
    expected_phrases: list[str]  # appear somewhere in the summary (case-insensitive)
    red_flag_phrases: list[str]  # must NOT appear in the summary
    human_rubric: list[str]  # yes/no questions for a planner


# Five scenarios derived from the 2026-06-17 failure-pattern analysis.
# lat/lon/description are lifted from tests/fixtures/narrator_eval_cases.json
# for the four real golden cases; finch-57-pair runs both halves of the
# resubmission (_FINCH_ORIGINAL / _FINCH_REVISED) for an ordering check.
_SCENARIOS: list[UATScenario] = [
    UATScenario(
        id="mendota-2",
        label="2-4 Mendota Rd — self-storage in residential zone (refused 2026)",
        lat=43.62275534592343,
        lon=-79.49530611075724,
        description=(
            "Proposal to amend the Official Plan and Zoning By-law to permit a "
            "4-storey (15.2 metre) tall self-storage facility with a gross floor "
            "area of 11,168.26 square metres, 31 at-grade parking spaces and "
            "3 truck loading bays."
        ),
        confidence_min=10,
        confidence_max=55,
        expected_phrases=["rezoning"],
        red_flag_phrases=["as of right", "as-of-right"],
        human_rubric=[
            "Does the summary identify self-storage as the use and explain why it "
            "conflicts with the residential zone?",
            "Does it mention the rezoning (s.34) requirement?",
            "Is confidence <= 55 appropriate for a just-refused use mismatch?",
            "Is there a comps section, and are the comparables sensible?",
        ],
    ),
    UATScenario(
        id="sheppard-4155",
        label="4155 Sheppard Ave E — 46-storey OZ in RD zone, refused (Scarborough)",
        lat=43.782625464310485,
        lon=-79.28115891671413,
        description=(
            "Combined Official Plan and Zoning By-law Amendment application to "
            "permit a 46-storey mixed-use building containing 726 residential "
            "units, 1,226.63 sq.m. of indoor amenity space, 951.48 sq.m. of "
            "outdoor amenity space, and 520.66 sq.m. of commercial space "
            "resulting in a total gross floor area of 44,467.05 sq.m., or a FSI "
            "of 11.72. A total 232 vehicular parking spaces, and 550 bicycle "
            "parking spaces are proposed. Appealed to OLT"
        ),
        confidence_min=10,
        confidence_max=30,
        expected_phrases=["storey"],
        red_flag_phrases=["as of right", "likely approved"],
        human_rubric=[
            "Does the summary quantify the height excess (46 vs 2-storey limit)?",
            "Does it surface the secondary plan or the elevated ward appeal rate?",
            "Is the OLT/appeal process described?",
            "Would confidence <= 30 feel right to a Scarborough development broker?",
        ],
    ),
    UATScenario(
        id="dupont-328-opa",
        label="328 Dupont St — standalone employment OPA, refused 2010 [KNOWN GAP]",
        lat=43.67277991586383,
        lon=-79.40924372090099,
        description=(
            "Standard OPA application.  to change Official Plan designation from "
            "employment area to mixed use area.  No accompanying rezoning "
            "application.  "
        ),
        confidence_min=10,
        confidence_max=88,  # wide: advisory blind-spot case, floor pins >= 55
        expected_phrases=[],
        red_flag_phrases=[],  # fully human-judged — no content auto-check
        human_rubric=[
            "KNOWN GAP: refused because a standalone OPA without a companion ZBA "
            "is not actionable. The engine cannot detect this and floors at 55.",
            "What confidence did the system return? If > 60, flag as a false positive.",
            "Does the summary mention employment area, OPA, or land-use designation?",
            "Does it note an OPA alone is insufficient without a companion rezoning?",
            "If the summary reads generic or over-confident, record it: a future fix "
            "should move confidence into the [10, 40] range.",
        ],
    ),
    UATScenario(
        id="finch-57-pair",
        label="57-63 Finch Ave W — scope-reduction resubmission (original vs revised)",
        lat=43.77648429616059,
        lon=-79.42065239301269,
        description="",  # handled specially: runs _FINCH_ORIGINAL and _FINCH_REVISED
        confidence_min=0,  # pair-ordering check, not a single band
        confidence_max=100,
        expected_phrases=[],
        red_flag_phrases=[],
        human_rubric=[
            "The ORIGINAL (70-unit apartments) should score <= the REVISED "
            "(42 stacked towns).",
            "Does the original's summary hint at what changes would improve it?",
            "Does the revised summary reflect the unit reduction / type change well?",
            "Is the appeal history (OMB-approved after revision) mentioned or "
            "inferable from comps?",
        ],
    ),
    UATScenario(
        id="weston-1552",
        label="1552 Weston Rd — 8-storey affordable housing, as-of-right (approved)",
        lat=43.69233417292582,
        lon=-79.50653440850571,
        description=(
            "Proposed development for an 8 storey affordable housing residential "
            "building at 1550 Weston Road,  containing a total of  50 residential "
            "uses and amenity spaces. Severed via consent application."
        ),
        confidence_min=70,
        confidence_max=88,
        expected_phrases=["within"],
        red_flag_phrases=["refused", "unlikely"],
        human_rubric=[
            "Does the summary correctly identify this as within the 8-storey limit?",
            "Does it identify the as-of-right path (no rezoning required)?",
            "Is the affordable-housing context or the consent/severance noted?",
            "Would a planner trust a confidence >= 70 for this project?",
        ],
    ),
]

_FINCH_ORIGINAL = (
    "Original proposal for a 4-storey, 70 unit residential development "
    "with 68 underground parking spaces."
)
_FINCH_REVISED = (
    "Original proposal for a 4-storey, 70 unit residential development with "
    "68 underground parking spaces. Application has been appealed to the OMB "
    "and has been revised in accordance with the following: a reduction in the "
    "number of units from 70 to 42 and a shift from traditional apartment house "
    "dwelling units to back-to-back stacked townhouse units; a decrease in the "
    "proposed building coverage; the addition of a communal outdoor landscaped "
    "amenity area, doubling the overall amount of soft landscaping on the site; "
    "a shortened driveway due to a proposed car lift in replacement of the "
    "previously proposed car ramp; an increase in setbacks along the Finch and "
    "Elmview Avenue frontages and rear yard; removal of the previously proposed "
    "rooftop mechanical units; better integration of waste management facilities."
)


def _preflight(ref_dir: str, data_dir: str) -> None:
    """Skip cleanly (exit 0) when LLM/data prerequisites are missing."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("SKIP: ANTHROPIC_API_KEY not set.")
        sys.exit(0)
    if not Path(ref_dir).exists():
        print(f"SKIP: GIS reference data not found: {ref_dir}")
        sys.exit(0)
    enriched = Path(data_dir) / "enriched" / "dev_applications.parquet"
    if not enriched.exists():
        print(f"SKIP: enriched data not found: {enriched}")
        sys.exit(0)


def _run_single(
    scenario_id: str,
    lat: float,
    lon: float,
    description: str,
    ref_dir: Path,
    data_dir: Path,
    model_dir: Path,
    agents: NarratorAgents,
) -> dict:
    """Run one description through the full narrator pipeline."""
    from scripts.narrator_eval import _narrate_case

    case = {"id": scenario_id, "lat": lat, "lon": lon, "description": description}
    return _narrate_case(
        case,
        ref_dir=ref_dir,
        data_dir=data_dir,
        model_dir=model_dir,
        agents=agents,
    )


def _auto_checks(result: dict, scenario: UATScenario) -> list[str]:
    """Return auto-check failure messages (empty list = all passed)."""
    failures: list[str] = []
    score = result["score"]
    summary = result["summary"]

    if score is None:
        return ["narrator returned no confidence score"]
    if not summary or not summary.strip():
        return ["narrator returned empty summary"]
    if "CONFIDENCE:" in summary:
        failures.append("CONFIDENCE: line leaked into the summary")

    if scenario.id != "finch-57-pair":
        if not (scenario.confidence_min <= score <= scenario.confidence_max):
            failures.append(
                f"confidence {score} outside "
                f"[{scenario.confidence_min}, {scenario.confidence_max}]"
            )

    summary_lower = summary.lower()
    for phrase in scenario.expected_phrases:
        if phrase.lower() not in summary_lower:
            failures.append(f"expected phrase not found in summary: '{phrase}'")
    for phrase in scenario.red_flag_phrases:
        if phrase.lower() in summary_lower:
            failures.append(f"red-flag phrase found in summary: '{phrase}'")

    return failures


def _print_result(
    label: str,
    result: dict,
    scenario: UATScenario,
    failures: list[str],
    *,
    verbose: bool,
) -> None:
    from scripts.narrator_eval import _mechanism_trace

    score = result["score"]
    violations = result["violations"]
    n_struct = sum(1 for v in violations if v.severity != Severity.INFORMATIONAL)

    print(f"\n{'=' * 72}")
    print(f"  SCENARIO: {label}")
    print(
        f"  score={score}  band=[{scenario.confidence_min}, {scenario.confidence_max}]"
        f"  violations={len(violations)} ({n_struct} structural)"
    )
    print(f"  mechanism: {_mechanism_trace(result)}")
    if failures:
        for f in failures:
            print(f"  AUTO-CHECK FAIL: {f}")
    else:
        print("  AUTO-CHECKS: all passed")
    if verbose:
        print("\n  --- SUMMARY ---")
        for line in result["summary"].strip().splitlines():
            print(f"  {line}")
    print("\n  --- HUMAN RUBRIC (answer yes/no) ---")
    for item in scenario.human_rubric:
        print(f"  [ ] {item}")


def run_uat(
    ref_dir: str = "data/reference",
    data_dir: str = "data",
    model_dir: str = "models",
    case_ids: list[str] | None = None,
    *,
    verbose: bool = True,
) -> dict[str, object]:
    from zoneto.llm.agents import make_narrator_agents

    ref_p, data_p, model_p = Path(ref_dir), Path(data_dir), Path(model_dir)
    agents = make_narrator_agents()

    scenarios = _SCENARIOS
    if case_ids:
        scenarios = [s for s in _SCENARIOS if s.id in case_ids]
        if not scenarios:
            print(f"ERROR: no scenarios match {case_ids}")
            sys.exit(2)

    all_failures: dict[str, list[str]] = {}

    for scenario in scenarios:
        if scenario.id == "finch-57-pair":
            r_orig = _run_single(
                "finch-57-original",
                scenario.lat,
                scenario.lon,
                _FINCH_ORIGINAL,
                ref_p,
                data_p,
                model_p,
                agents,
            )
            r_rev = _run_single(
                "finch-57-revised",
                scenario.lat,
                scenario.lon,
                _FINCH_REVISED,
                ref_p,
                data_p,
                model_p,
                agents,
            )
            failures: list[str] = []
            so, sr = r_orig["score"], r_rev["score"]
            if so is None or sr is None:
                failures.append("a half of the pair returned no confidence score")
            elif so > sr:
                failures.append(
                    f"ordering violated: original score {so} > revised score {sr}"
                )
            _print_result(
                f"{scenario.label} — ORIGINAL", r_orig, scenario, [], verbose=verbose
            )
            _print_result(
                f"{scenario.label} — REVISED",
                r_rev,
                scenario,
                failures,
                verbose=verbose,
            )
            all_failures[scenario.id] = failures
        else:
            result = _run_single(
                scenario.id,
                scenario.lat,
                scenario.lon,
                scenario.description,
                ref_p,
                data_p,
                model_p,
                agents,
            )
            failures = _auto_checks(result, scenario)
            _print_result(scenario.label, result, scenario, failures, verbose=verbose)
            all_failures[scenario.id] = failures

    total = sum(len(v) for v in all_failures.values())
    clean = sum(1 for v in all_failures.values() if not v)
    print(f"\n{'=' * 72}")
    print(f"auto-checks: {clean}/{len(all_failures)} scenarios clean")
    if total:
        print(f"FAIL: {total} auto-check failure(s) — see above")
    return {"all_failures": all_failures, "total_failures": total}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case",
        action="append",
        dest="case_ids",
        metavar="ID",
        help="run only this scenario id (repeatable)",
    )
    parser.add_argument("--ref-dir", default="data/reference")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    _preflight(args.ref_dir, args.data_dir)

    results = run_uat(
        args.ref_dir,
        args.data_dir,
        args.model_dir,
        args.case_ids,
        verbose=not args.quiet,
    )
    if results["total_failures"]:
        sys.exit(1)


if __name__ == "__main__":
    main()

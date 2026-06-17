# Failure-Pattern Golden Cases & UAT Scenarios Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a golden case documenting the employment-OPA confidence blind spot, then build a UAT scenarios script that runs five known-failure/resubmission cases through the narrator and prints annotated output + a human rubric for a development professional to review.

**Architecture:** One new JSON stanza in the existing golden-case fixture exercises the "standalone Official Plan Amendment without a rezoning — refusal invisible to the compliance engine" failure mode. A new `scripts/uat_scenarios.py` calls the same `_narrate_case` pipeline as `narrator_eval.py` for five curated scenarios (four from existing golden cases, one new) and prints auto-checked metrics alongside a human-readable rubric. A new `just uat` target wires it into the workflow.

**Tech Stack:** Python 3.13, Polars, Pydantic AI, existing `narrator_eval.py` pipeline helpers, pytest parametrization (auto-picks up new fixture entries), `justfile`.

## Global Constraints

- Python ≥ 3.13.
- New fixture entries must follow the schema of existing entries exactly: `id`, `label`, `folderrsn`, `lat`, `lon`, `description`, `outcome`, `mechanism`, `expected_confidence` `{min, max}`, `advisory`, `ci` `{site, sim_stub, expected_overrides, expected_statutory}`, `notes`.
- `expected_overrides` must have exactly four keys: `floor_70`, `precedent_floor_55`, `unknown_limits_floor_55`, `cap_30`.
- `expected_statutory.additional` must be a list (not null).
- UAT script must skip gracefully (not error) when `ANTHROPIC_API_KEY` is absent or `data/reference/` is missing.
- `just test` (pre-commit, no integration marker) must pass after every task — the CI tier is network-free and must not be broken.
- One LLM call per scenario; total UAT cost ≈ $0.025 (5 × ~$0.005 at Haiku pricing).
- Do not add `sim_stub` fields to the fixture unless the case requires a specific precedent match; `null` is correct for passthrough / advisory cases.

---

## Task 1: Snapshot the dupont-328-opa candidate case

**Files:**
- No file changes — research only.

**Why this case:** folderrsn 2623559 is the standalone employment-OPA at 328 Dupont St (refused 2010, status="Refused" in dev_applications). It is the same address as existing golden case `dupont-328` (folderrsn 3693046, an OZ revision) but a completely different application type: a bare OPA requesting an employment-land designation change with no accompanying rezoning. This is the most common class of outright refusal that the current compliance engine cannot detect — there is no height, no unit count, and potentially no use mismatch in the description.

**Interfaces:**
- Produces: site-context snapshot dict for Task 2; confirmed `expected_overrides` values.

- [ ] **Step 1: Run the emit-case triage for folderrsn 2623559**

```bash
uv run python scripts/narrator_refused_triage.py --emit-case 2623559
```

Expected: JSON stanza printed to stdout. Record the `ci.site` block; it will have `zoning_class`, `zoning_max_height_m`, `permitted_use_category`, `in_heritage_district`, etc.

- [ ] **Step 2: Confirm which override fires (RESOLVED 2026-06-17)**

The triage reported `bucket = floor-55-unverified`. **This is the blind spot, and it differs from the plan's original prediction of floor-70.** The OPA-designation-change description extracts `proposed_use = mixed_use` with no storeys/units/height/FSI. In the R zone (13m height, 1.0 density, no storey/unit cap) there is nothing to verify the proposal against, and `mixed_use` is not structurally blocked, so zero structural violations → the unknown-limits floor pins confidence at **55**. A stub LLM returns score 55 for a 2010 *refused* application.

Record `expected_overrides`:
```json
{ "floor_70": false, "precedent_floor_55": false, "unknown_limits_floor_55": true, "cap_30": false }
```

- [ ] **Step 3: Note the violations and statutory path (RESOLVED 2026-06-17)**

```
violations: [op_use_nonconforming (INFORMATIONAL), heritage_district (INFORMATIONAL), zoning_exception (INFORMATIONAL)]
n_structural: 0
path: as_of_right | act_section: "Planning Act — no amendment required"
decider: "Toronto Building (Chief Building Official)" | non_decision_days: None
additional: ['site_plan']
```

Note the engine *does* fire `op_use_nonconforming` (mixed-use proposed against the site's employment OP designation) but only at INFORMATIONAL severity, so it does not move the path off `as_of_right`. The real process was a rezoning/OPA → `matches_reality: false`. **This means `test_statutory_process_detection_accuracy` must be updated in Task 2 (its misses set currently hard-codes only the two Finch cases).**

Full site snapshot (RESOLVED 2026-06-17):
```json
{
  "zoning_class": "R", "zoning_max_units": null, "zoning_max_density": 1.0,
  "zoning_max_storeys": null, "zoning_max_height_m": 13.0,
  "permitted_use_category": "Residential", "zoning_min_frontage_m": null,
  "zoning_min_lot_area_sqm": null, "zoning_max_coverage_pct": null,
  "zoning_min_sqm_per_unit": null, "zoning_holding": 0, "zoning_exception": 1,
  "zoning_exception_no": "900", "zoning_pct_res": null, "zoning_pct_comm": null,
  "zoning_pct_emp": null, "in_heritage_register": 0, "in_heritage_district": 1,
  "secondary_plan_name": null, "in_secondary_plan": 0, "in_mtsa": 0,
  "in_trca_regulated_area": 0, "in_greenbelt": 0
}
```

- [ ] **Step 4: No commit — Task 1 is research only.**

---

## Task 2: Add `dupont-328-opa` golden case and validate CI tests

**Files:**
- Modify: `tests/fixtures/narrator_eval_cases.json`
- Modify: `tests/api/test_narrator_regression.py` (update the hard-coded misses set)

**Interfaces:**
- Consumes: `ci.site` snapshot and `expected_overrides` from Task 1.
- Produces: new parameterized test case auto-picked up by `test_narrator_regression.py` (`test_deterministic_overrides`, `test_statutory_process`, `test_narrate_end_to_end_mocked`).

- [ ] **Step 1: Insert the new golden-case stanza**

Add this entry to the `"cases"` array in `tests/fixtures/narrator_eval_cases.json`, immediately after the existing `"dupont-328"` entry. Values are resolved from Task 1 — paste verbatim.

```json
{
  "id": "dupont-328-opa",
  "label": "328 Dupont St — standalone employment OPA without rezoning, refused 2010",
  "folderrsn": "2623559",
  "lat": 43.67277991586383,
  "lon": -79.40924372090099,
  "description": "Standard OPA application.  to change Official Plan designation from employment area to mixed use area.  No accompanying rezoning application.  ",
  "outcome": {
    "decision": "refused",
    "body": "City Council (OZ, Refused; AIC STATUS_DESC 'Refused'). Standalone OPA to convert an employment-area OP designation to mixed-use with no accompanying ZBA — a configuration the City consistently refuses because an OP designation change needs a companion rezoning to be actionable.",
    "year": 2010,
    "appealed": false,
    "verification": {
      "source": "Toronto AIC ArcGIS FeatureServer (live POST query, STATUS_DESC field)",
      "url": "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services/COTGEO_IBMS_AIC_POINT/FeatureServer/0/query",
      "verified_date": "2026-06-17"
    }
  },
  "mechanism": "KNOWN LIMITATION: unknown-limits floor-55 fires — the OPA-designation-change description carries no storeys/units/height, mixed_use is not structurally blocked in the R zone, so confidence floors at 55; the real refusal reason (employment conversion needs a companion ZBA) is invisible to the engine",
  "expected_confidence": {
    "min": 40,
    "max": 88
  },
  "advisory": true,
  "ci": {
    "site": {
      "zoning_class": "R",
      "zoning_max_units": null,
      "zoning_max_density": 1.0,
      "zoning_max_storeys": null,
      "zoning_max_height_m": 13.0,
      "permitted_use_category": "Residential",
      "zoning_min_frontage_m": null,
      "zoning_min_lot_area_sqm": null,
      "zoning_max_coverage_pct": null,
      "zoning_min_sqm_per_unit": null,
      "zoning_holding": 0,
      "zoning_exception": 1,
      "zoning_exception_no": "900",
      "zoning_pct_res": null,
      "zoning_pct_comm": null,
      "zoning_pct_emp": null,
      "in_heritage_register": 0,
      "in_heritage_district": 1,
      "secondary_plan_name": null,
      "in_secondary_plan": 0,
      "in_mtsa": 0,
      "in_trca_regulated_area": 0,
      "in_greenbelt": 0
    },
    "sim_stub": null,
    "expected_overrides": {
      "floor_70": false,
      "precedent_floor_55": false,
      "unknown_limits_floor_55": true,
      "cap_30": false
    },
    "expected_statutory": {
      "path": "as_of_right",
      "act_section_contains": "no amendment",
      "decider": "Toronto Building (Chief Building Official)",
      "non_decision_appeal_days": null,
      "actual_process": "rezoning",
      "matches_reality": false,
      "note": "LIMITATION: an OPA-designation-change description has no structural signal -> engine derives as_of_right; the real process was an OPA (+ companion ZBA). op_use_nonconforming fires but only INFORMATIONAL, so it doesn't move the path.",
      "additional": [
        "site_plan"
      ]
    }
  },
  "notes": "ADVISORY — pins the 'employment-OPA without ZBA is invisible to the compliance engine' blind spot. extract_project_features yields proposed_use=mixed_use with no storeys/units/height/FSI; in the R zone (13m, 1.0 density, no storey/unit cap) nothing can be verified and mixed_use is not structurally blocked, so the unknown-limits floor pins confidence at 55. A 2010 *refused* application thus floors at 55 — a mid-confidence 'maybe' where the right answer is low. The engine does fire op_use_nonconforming (mixed-use proposed against the employment OP designation) but only at INFORMATIONAL severity, so it neither moves the path off as_of_right nor lowers the floor. Refusal reason (policy: employment land conversion requires a companion ZBA) is unencoded. Compare dupont-328 (same address, the later OZ revision that correctly gets cap-30 via height inference). When unit-density or OP-severity escalation lands, tighten this band toward [10, 40] and add an ordering vs dupont-328. Band calibrated in Task 3."
}
```

- [ ] **Step 2: Update the process-detection misses set**

`test_statutory_process_detection_accuracy` hard-codes the set of cases whose derived process intentionally diverges from reality. Adding `dupont-328-opa` (`matches_reality: false`) expands that set. In `tests/api/test_narrator_regression.py`, change:

```python
    assert set(misses) == {"finch-57-revised", "finch-57-original"}, (
        f"process-detection misses changed: {sorted(misses)}"
    )
```

to:

```python
    assert set(misses) == {
        "finch-57-revised",
        "finch-57-original",
        "dupont-328-opa",
    }, f"process-detection misses changed: {sorted(misses)}"
```

Also update that test's docstring to reflect the third documented blind spot — add a sentence after the existing Finch description:

```
    dupont-328-opa adds the employment-OPA blind spot: a designation-change
    description has no structural signal, so the engine derives as_of_right
    where reality was an OPA + companion rezoning.
```

- [ ] **Step 3: Run the CI-safe test tier for the new case**

```bash
uv run pytest tests/api/test_narrator_regression.py -v -k "dupont-328-opa or process_detection"
```

Expected (all passing):
```
test_deterministic_overrides[dupont-328-opa] PASSED
test_statutory_process[dupont-328-opa] PASSED
test_narrate_end_to_end_mocked[dupont-328-opa] PASSED
test_statutory_process_detection_accuracy PASSED
```

If `test_deterministic_overrides` fails: re-check the four `expected_overrides` booleans against the Task 1 triage output.
If `test_statutory_process_detection_accuracy` fails: the misses set edit in Step 2 was missed or misspelled.

- [ ] **Step 4: Run the full CI test suite to confirm no regressions**

```bash
uv run pytest tests/api/test_narrator_regression.py -v
```

Expected: all existing tests still pass; `dupont-328-opa` adds 3 new passing tests.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/narrator_eval_cases.json tests/api/test_narrator_regression.py
git commit -m "test: add dupont-328-opa advisory golden case — employment OPA blind spot"
```

---

## Task 3: Calibrate dupont-328-opa confidence band

**Files:**
- Modify: `tests/fixtures/narrator_eval_cases.json` (update `expected_confidence` band after live calibration)

**Interfaces:**
- Consumes: the new golden case from Task 2.
- Produces: calibrated `expected_confidence.{min, max}` derived from ≥3 LLM runs.

- [ ] **Step 1: Run the live narrator for dupont-328-opa**

```bash
uv run python scripts/narrator_eval.py --case dupont-328-opa
```

Requires `ANTHROPIC_API_KEY` set in environment and `data/reference/` populated. Expected output:
```
  ~ [dupont-328-opa] [advisory] score=<N> band=[40, 88]
      328 Dupont St — standalone employment OPA without rezoning, refused 2010
      violations: 3 (0 structural) | floor-55 (compatible use, limits unverified)
```

The `unknown_limits_floor_55` override pins the integration score at **≥55**. Run the command 3 times total and record all three scores (expect them clustered in the 55–80 range, like the same-mechanism `finch-57-original` advisory case).

- [ ] **Step 2: Set the advisory band**

The advisory band must contain all observed scores. Because the floor pins ≥55, set `expected_confidence.min` to `min(40, lowest_observed − 5)` (40 keeps a margin below the floor for prompt drift) and `expected_confidence.max` to 88 (the system prompt ceiling). If all three runs land in a tight range (e.g. 60–72), you may narrow `max` to `highest_observed + 5` instead.

Edit `tests/fixtures/narrator_eval_cases.json` to update `"min": 40, "max": 88` only if the observed range justifies a tighter band.

Append to the `notes` field: `" Observed over 3 calibration runs (2026-06-17): <scores>."` replacing `<scores>` with the actual values.

- [ ] **Step 3: Confirm the advisory band passes**

```bash
uv run python scripts/narrator_eval.py --case dupont-328-opa
```

Expected output line: `  ~ [dupont-328-opa] [advisory] score=<N> band=[<min>, <max>]` — the `~` (advisory miss OK) marker should appear only if score is outside band.

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/narrator_eval_cases.json
git commit -m "test: calibrate dupont-328-opa advisory confidence band"
```

---

## Task 4: Write the UAT scenarios script

**Files:**
- Create: `scripts/uat_scenarios.py`

**Interfaces:**
- Consumes: `narrator_eval.py::_narrate_case`, `narrator_eval.py::_mechanism_trace`, `tests/fixtures/narrator_eval_cases.json` (reads golden-case metadata for lat/lon/description/label).
- Produces: per-scenario printed block: confidence vs expected, mechanism, violations, summary text, human rubric checklist.

The script does **not** make a pass/fail assertion on the human-rubric items — those require a development professional's judgment. It does assert: (a) confidence is not `None`; (b) summary is non-empty; (c) "CONFIDENCE:" did not leak into the summary. It exits 1 if any auto-check fails, 0 otherwise.

- [ ] **Step 1: Write the failing smoke-test first**

Create `scripts/uat_scenarios.py` with just the imports and a `main()` stub, then verify it fails:

```python
"""UAT scenario runner for failure-pattern cases.

Runs five curated narrator scenarios (covering employment use-mismatch,
extreme scale in a contested ward, employment-OPA blind spot, resubmission
scope reduction, and as-of-right success) through the full evaluate pipeline
and prints annotated output for a development professional to review.

Usage:
    uv run python scripts/uat_scenarios.py [--case mendota-2]

Requires ANTHROPIC_API_KEY, data/reference/, and data/enriched/ + models/.
Skips gracefully when prerequisites are missing.

Exit codes: 0 = all auto-checks passed, 1 = some auto-checks failed,
2 = missing prerequisites.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    raise NotImplementedError("not yet implemented")


if __name__ == "__main__":
    main()
```

```bash
uv run python scripts/uat_scenarios.py
```

Expected: `NotImplementedError: not yet implemented` — confirms the file runs.

- [ ] **Step 2: Add the scenario definitions and preflight**

Replace the stub with the full implementation:

```python
"""UAT scenario runner for failure-pattern cases.

Runs five curated narrator scenarios (covering employment use-mismatch,
extreme scale in a contested ward, employment-OPA blind spot, resubmission
scope reduction, and as-of-right success) through the full evaluate pipeline
and prints annotated output for a development professional to review.

Usage:
    uv run python scripts/uat_scenarios.py [--case mendota-2]

Requires ANTHROPIC_API_KEY, data/reference/, and data/enriched/ + models/.
Skips gracefully when prerequisites are missing.

Exit codes: 0 = all auto-checks passed, 1 = some auto-checks failed,
2 = missing prerequisites.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class UATScenario:
    id: str
    label: str
    lat: float
    lon: float
    description: str
    confidence_min: int
    confidence_max: int
    expected_phrases: list[str]       # should appear somewhere in the summary (case-insensitive)
    red_flag_phrases: list[str]       # should NOT appear in the summary
    human_rubric: list[str]           # questions for a planner to answer yes/no


# Five scenarios derived from the failure-pattern analysis (2026-06-17).
# lat/lon/description pulled from tests/fixtures/narrator_eval_cases.json
# for the four existing golden cases; dupont-328-opa uses the same.
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
        confidence_max=50,
        expected_phrases=["rezoning"],
        red_flag_phrases=["as of right", "as-of-right", "within limits"],
        human_rubric=[
            "Does the summary identify self-storage as the use and explain why it conflicts with the zone?",
            "Does it mention the rezoning (s.34) requirement?",
            "Would a planner agree confidence < 50 is appropriate for a just-refused application?",
            "Is there a comps section, and if so, are the comparable applications sensible?",
        ],
    ),
    UATScenario(
        id="sheppard-4155",
        label="4155 Sheppard Ave E — 46-storey OZ in RD zone, refused (Scarborough Centre)",
        lat=43.782625464310485,
        lon=-79.28115891671413,
        description=(
            "Combined Official Plan and Zoning By-law Amendment application to permit "
            "a 46-storey mixed-use building containing 726 residential units, "
            "1,226.63 sq.m. of indoor amenity space, 951.48 sq.m. of outdoor amenity "
            "space, and 520.66 sq.m. of commercial space resulting in a total gross "
            "floor area of 44,467.05 sq.m., or a FSI of 11.72. A total 232 vehicular "
            "parking spaces, and 550 bicycle parking spaces are proposed. Appealed to OLT"
        ),
        confidence_min=10,
        confidence_max=30,
        expected_phrases=["storey", "height"],
        red_flag_phrases=["as of right", "likely approved"],
        human_rubric=[
            "Does the summary quantify the height excess (46 storeys vs a 2-storey zone limit)?",
            "Does it mention the Scarborough/Agincourt secondary plan or the elevated appeal rate?",
            "Is the OLT/appeal process described?",
            "Would the confidence number (should be ≤30) feel right to a Scarborough development broker?",
        ],
    ),
    UATScenario(
        id="dupont-328-opa",
        label="328 Dupont St — standalone employment OPA without rezoning (refused 2010) [KNOWN GAP]",
        lat=43.67277991586383,
        lon=-79.40924372090099,
        description=(
            "Standard OPA application.  to change Official Plan designation from "
            "employment area to mixed use area.  No accompanying rezoning application.  "
        ),
        confidence_min=10,
        confidence_max=88,  # wide: this is the advisory blind-spot case
        expected_phrases=[],
        red_flag_phrases=[],  # no auto-checks on summary content — fully human-judged
        human_rubric=[
            "KNOWN GAP: this application was refused because standalone OPAs without "
            "a companion ZBA are not actionable. The engine cannot detect this.",
            "What confidence score did the system return? If > 60, flag as a false positive.",
            "Does the summary mention employment area, Official Plan Amendment, or land-use designation?",
            "Does it explain that an OPA alone is insufficient without a companion rezoning?",
            "If the summary is generic or over-confident, note it: this is a documented limitation "
            "that a future fix should move confidence into the [10, 40] range.",
        ],
    ),
    UATScenario(
        id="finch-57-pair",
        label="57-63 Finch Ave W — scope-reduction resubmission pair (original vs revised)",
        lat=43.77648429616059,
        lon=-79.42065239301269,
        description="",  # runs two descriptions; handled specially in _run_scenario
        confidence_min=0,  # pair comparison, not a single band
        confidence_max=100,
        expected_phrases=[],
        red_flag_phrases=[],
        human_rubric=[
            "The ORIGINAL proposal (70-unit apartments) should score lower than the REVISED "
            "(42 back-to-back stacked towns).",
            "Does the original's summary hint at what changes would improve the outcome?",
            "Does the revised summary reflect the unit reduction and building-type change positively?",
            "Is the appeal history (OMB-approved after revision) mentioned or inferable from comps?",
        ],
    ),
    UATScenario(
        id="weston-1552",
        label="1552 Weston Rd — 8-storey affordable housing, as-of-right (approved 2022)",
        lat=43.69233417292582,
        lon=-79.50653440850571,
        description=(
            "Proposed development for an 8 storey affordable housing residential building "
            "at 1550 Weston Road,  containing a total of  50 residential uses and amenity "
            "spaces. Severed via consent application."
        ),
        confidence_min=70,
        confidence_max=88,
        expected_phrases=["rezoning", "within"],
        red_flag_phrases=["refused", "unlikely"],
        human_rubric=[
            "Does the summary correctly identify this as within the zone's 8-storey limit?",
            "Does it identify the as-of-right path (no rezoning required)?",
            "Is affordable housing context mentioned or the consent/severance noted?",
            "Would a planner trust a confidence ≥ 70 for this project?",
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
    agents,
) -> dict:
    """Run one description through the full narrator pipeline."""
    from scripts.narrator_eval import _narrate_case

    case = {
        "id": scenario_id,
        "lat": lat,
        "lon": lon,
        "description": description,
    }
    return _narrate_case(
        case,
        ref_dir=ref_dir,
        data_dir=data_dir,
        model_dir=model_dir,
        agents=agents,
    )


def _auto_checks(result: dict, scenario: UATScenario) -> list[str]:
    """Return list of auto-check failure messages (empty = all pass)."""
    failures = []
    score = result["score"]
    summary = result["summary"]

    if score is None:
        failures.append("narrator returned no confidence score")
        return failures
    if not summary or not summary.strip():
        failures.append("narrator returned empty summary")
        return failures
    if "CONFIDENCE:" in summary:
        failures.append("CONFIDENCE: line leaked into the summary")

    # Confidence band (skip for pair-comparison scenario)
    if scenario.id != "finch-57-pair":
        if not (scenario.confidence_min <= score <= scenario.confidence_max):
            failures.append(
                f"confidence {score} outside [{scenario.confidence_min}, "
                f"{scenario.confidence_max}]"
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
    n_struct = sum(1 for v in violations if hasattr(v, "severity") and v.severity.name != "INFORMATIONAL")

    print(f"\n{'=' * 72}")
    print(f"  SCENARIO: {label}")
    print(f"  score={score}  band=[{scenario.confidence_min}, {scenario.confidence_max}]  "
          f"violations={len(violations)} ({n_struct} structural)")
    print(f"  mechanism: {_mechanism_trace(result)}")
    if failures:
        for f in failures:
            print(f"  AUTO-CHECK FAIL: {f}")
    else:
        print("  AUTO-CHECKS: all passed")
    print()
    if verbose:
        print("  --- SUMMARY ---")
        for line in result["summary"].strip().splitlines():
            print(f"  {line}")
        print()
    print("  --- HUMAN RUBRIC (answer yes/no) ---")
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
            # Run both halves and compare
            r_orig = _run_single(
                "finch-57-original", scenario.lat, scenario.lon,
                _FINCH_ORIGINAL, ref_p, data_p, model_p, agents,
            )
            r_rev = _run_single(
                "finch-57-revised", scenario.lat, scenario.lon,
                _FINCH_REVISED, ref_p, data_p, model_p, agents,
            )
            failures: list[str] = []
            if r_orig["score"] is not None and r_rev["score"] is not None:
                if r_orig["score"] >= r_rev["score"]:
                    failures.append(
                        f"ordering violated: original score {r_orig['score']} "
                        f">= revised score {r_rev['score']}"
                    )
            _print_result(
                f"{scenario.label} — ORIGINAL",
                r_orig, scenario, [], verbose=verbose,
            )
            _print_result(
                f"{scenario.label} — REVISED",
                r_rev, scenario, failures, verbose=verbose,
            )
            all_failures[scenario.id] = failures
        else:
            result = _run_single(
                scenario.id, scenario.lat, scenario.lon,
                scenario.description, ref_p, data_p, model_p, agents,
            )
            failures = _auto_checks(result, scenario)
            _print_result(scenario.label, result, scenario, failures, verbose=verbose)
            all_failures[scenario.id] = failures

    total_failures = sum(len(v) for v in all_failures.values())
    print(f"\nauto-checks: {sum(1 for v in all_failures.values() if not v)}/{len(all_failures)} scenarios clean")
    if total_failures:
        print(f"FAIL: {total_failures} auto-check failure(s) — see above")
    return {"all_failures": all_failures, "total_failures": total_failures}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--case", action="append", dest="case_ids", metavar="ID",
        help="run only this scenario id (repeatable)"
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
```

- [ ] **Step 3: Verify the script runs and skips cleanly when prerequisites are absent**

Temporarily unset the API key:

```bash
ANTHROPIC_API_KEY="" uv run python scripts/uat_scenarios.py
```

Expected:
```
SKIP: ANTHROPIC_API_KEY not set.
```
Exit code 0.

- [ ] **Step 4: Commit**

```bash
git add scripts/uat_scenarios.py
git commit -m "feat: add UAT scenario runner for five failure-pattern cases"
```

---

## Task 5: Add `just uat` target and run a live smoke test

**Files:**
- Modify: `justfile`

**Interfaces:**
- Consumes: `scripts/uat_scenarios.py`.
- Produces: `just uat` and `just uat --case <id>` commands.

- [ ] **Step 1: Add the target to justfile**

Open `justfile` and add immediately after the `narrator-triage` target (line 54):

```just
# Run UAT scenarios for failure-pattern cases through the full narrator pipeline
# Requires ANTHROPIC_API_KEY, data/reference/, data/enriched/, models/
# Usage: just uat                  # all five scenarios
#        just uat --case mendota-2 # one scenario
uat *ARGS:
    uv run python scripts/uat_scenarios.py {{ARGS}}
```

- [ ] **Step 2: Verify the target is wired up**

```bash
just uat --help
```

Expected: argparse help output from `scripts/uat_scenarios.py`.

- [ ] **Step 3: Run the full UAT suite (requires API key and data)**

```bash
just uat
```

Expected: five scenario blocks printed. Auto-checks should all pass. Record any `AUTO-CHECK FAIL` lines; if any appear, fix the scenario definition (`confidence_min/max` or `expected_phrases`).

- [ ] **Step 4: Run a single known-good scenario to validate --case filtering**

```bash
just uat --case weston-1552
```

Expected: only the weston-1552 block is printed, auto-checks pass, exit 0.

- [ ] **Step 5: Commit**

```bash
git add justfile
git commit -m "feat: add just uat target for failure-pattern scenario runner"
```

---

## Self-review

**Spec coverage check:**

| Requirement | Task |
|---|---|
| New low-confidence anchor for employment OPA blind spot | Task 2 |
| Confidence band calibrated from live LLM runs | Task 3 |
| CI-safe parametrized tests auto-pick up new case | Task 2 (no code change needed — test_narrator_regression.py parametrizes from JSON) |
| UAT script covering 5 failure-pattern scenarios | Task 4 |
| Employment use-mismatch scenario | Task 4 (mendota-2) |
| Extreme-scale contested-ward scenario | Task 4 (sheppard-4155) |
| Employment OPA blind-spot scenario | Task 4 (dupont-328-opa) |
| Resubmission scope-reduction pair | Task 4 (finch-57-pair) |
| As-of-right success baseline | Task 4 (weston-1552) |
| Graceful skip without API key or data | Task 4 step 3 |
| `just uat` target | Task 5 |
| Ordering assertion for resubmission pair | Task 4 (auto-check: revised > original) |
| Human rubric printed for planner review | Task 4 |

**Placeholder scan:** None. All code blocks are complete.

**Type consistency:**
- `_narrate_case` is imported from `scripts.narrator_eval` — this is a module-level import that only works when run from the repo root (same as all other scripts). The import path `from scripts.narrator_eval import _narrate_case` matches the existing import pattern used in no other file; verify it resolves by running `uv run python -c "from scripts.narrator_eval import _narrate_case"` before Task 4 step 3.
- `_mechanism_trace` is imported from the same module — it is a public function in `scripts/narrator_eval.py` (not prefixed with `__`).
- `UATScenario` is defined in the same file; no cross-file type dependency.
- `NarratorAgents` / `make_narrator_agents` are imported in `run_uat` inside the function body (same pattern as `narrator_eval.py`) — deferred to avoid ImportError when API key check fails before agents are needed.

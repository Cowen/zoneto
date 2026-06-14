# Planning Act Integration — 2026-06-13

Incorporates Ontario's **Planning Act (R.S.O. 1990, c. P.13)** into the Zoneto
evaluation pipeline as a deterministic *provincial* reference layer, alongside the
existing *municipal* By-law 569-2013 layer. Motivated by the model/product critiques in
`specs/2026-03-16-model-critique.md` and `specs/2026-06-13-product-critique-real-world-utility.md`.

## Decision: framing only, not a new score input

The 2026-06-13 critique argues the 0–100 confidence number is the product's weak idea and
must not carry more synthetic signal — its accuracy ceiling is set by open-data
availability, not by adding factors. So this work improves everything *around* the number
(process, appeal route, timeline, statutory correctness) and leaves the number's drivers
untouched. Concretely: `_apply_confidence_overrides` is unchanged, and a system-prompt guard
forbids the new statutory context from moving CONFIDENCE. The CI-safe
`test_narrate_end_to_end_mocked` proves the score is unaffected by the new prompt section.

## What changed

1. **New `analytics/planning_act.py`** — pure-data statute table (no network), counterpart to
   `compliance.py`. Exports `MINOR_VARIANCE_TESTS`, `StatutoryProcess`, `PROCESS_BY_PATH`,
   `APPLICATION_TYPE_PROCESS`, and helpers `path_for_violations`, `statutory_timeline_days`,
   `format_statutory_context`.

2. **`compliance.py` — s.45 framing fix (critique 2026-06-13 §8).** Removed the legally false
   "up to 10% deviation is typically considered minor" heuristic from the height/storeys/FSI
   remedies; replaced with the **four qualitative s.45(1) tests** (single-sourced from
   `planning_act` via a deferred import to avoid a cycle). Variation/rezoning violations now
   append the resolving Planning Act section to their `section_ref`
   (`s.45` for variances; `s.34` / `s.22` for rezonings; `s.36` for holding removal).
   *No severity logic changed* — both variance and rezoning are already "structural", so the
   confidence number is unaffected.

3. **`narrator.py` — statutory context block.** `_format_statutory_process` derives the required
   process from the violations and injects `## Statutory process & appeal route (Planning Act)`
   into the prompt (process, decider, OLT route, statutory non-decision clock, and a Bill 23
   read on the comparable appeal rate). Context only; guarded against affecting CONFIDENCE.

4. **`score.py` — statutory timeline anchor (critique 2026-03-16 §2).** New
   `statutory_min_decision_days` column written for **every** dev row via
   `statutory_timeline_days(application_type)`. Gives CD/SB a defensible deterministic baseline
   that the OZ/SA-only survival model leaves null — without dressing a statute up as a
   prediction. It is the statutory *floor* to a non-decision appeal right, explicitly distinct
   from the survival p50.

5. **`routes.py` — `/evaluate` response.** New `statutory_process` field
   (`StatutoryProcessResult`) so the front end can show "needs OPA + ZBA (s.22/s.34), decided by
   Council, OLT appeal, ~120-day statutory clock" — the "process classifier" framing the
   critique calls the honest product.

## Statutory values encoded

Verified 2026-06-13 against secondary sources (the e-Laws consolidation
`ontario.ca/laws/statute/90p13` is JS-rendered and could not be machine-fetched; CanLII blocks
automated fetches). Reflect the Planning Act as amended through **Bill 185 (2024)**, carrying the
**Bill 108/109** non-decision windows and **Bill 23 (2022)** third-party appeal removals.
**Re-confirm against e-Laws before relying on exact day counts — the SA/site-plan status is the
most volatile (Bill 185 changed it).**

| AIC type | Process | Act section | Decider | Non-decision appeal | Third-party appeal |
|---|---|---|---|---|---|
| OZ | OPA + ZBA (rezoning) | s.22 / s.34 | City Council | 120 days (90 standalone ZBA) | No (Bill 23) |
| SB | Plan of Subdivision | s.51 | Council | 120 days (s.51(34)) | No |
| CD | Plan of Condominium | s.51 (via Condominium Act) | City Planning | 120 days | No |
| SA | Site Plan Approval | s.41 | City Planning (delegated) | none (post-Bill 185) | No |
| PL | Part Lot Control Exemption | s.50 | Council (by by-law) | none | No (not OLT-appealable) |

- **Minor variance (s.45):** Committee of Adjustment; four s.45(1) tests; appeal to OLT within
  **20 days** (s.45(12)); no statutory non-decision clock (the COA must hold a hearing).
- **Bill 23 "specified persons":** the applicant, the municipality, the Minister, and certain
  public bodies/utilities retain OPA/ZBA appeal rights; residents and community groups do not.

## Out of scope / follow-ons

From the 2026-06-13 critique, the higher-value **correctness/data** items remain open and are
*not* Planning Act framing work:

1. Verify multiplex/EHON as-of-right against the zoning `UNITS` field (possible false "needs
   rezoning" on legal fourplexes).
2. Join the Official Plan Land Use polygon layer (the single largest missing signal).
3. Fix the OLT scraper (returns 0 rows; every label/model inherits the pessimism).
4. Reframe Section 37 comps as Community Benefits Charge (s.37 repealed by Bill 108) — also a
   Planning-Act-section framing fix; deferred here to keep scope tight.

ML-model items (appeal base rate, survival recency weighting, `production_ready` thresholds) are
tracked in `specs/2026-03-16-model-critique.md` and untouched by this change.

## How we evaluate it (the layer doesn't move the score, so band evals are blind)

`narrator-eval`/`narrator-triage` only assert the confidence *number* lands in a band; since
the Planning Act layer is framing-only, they cannot tell whether the derived *process* is
right. So evaluation is **content/process-based**, in two deterministic tiers:

1. **Golden-fixture statutory asserts (CI-safe).** Each of the 13 `narrator_eval_cases` carries
   an `expected_statutory` block: the asserted deterministic `path`/section/decider/days, plus a
   ground-truth `actual_process` + `matches_reality`. `test_statutory_process[case]` pins the
   mapping; `test_statutory_process_detection_accuracy` pins the known limitation — **11/13 match
   reality; the 2 misses are the Finch "limits-unknowable" cases** (no zoning limit in data →
   engine derives `as_of_right` where reality was a rezoning).

2. **Corpus process-match harness — `just planning-act-eval`** (`scripts/planning_act_eval.py`).
   Deterministic confusion matrix of derived process vs. actual `application_type` over all
   ~30.8k enriched applications (ground truth = the process the applicant actually filed).
   Headline: **OZ rezoning recall ≈ 10% (lower bound)**, with misses split into *data gap*
   (no zoning limit to check, ~3.8k) vs. *engine/extraction gap* (limit present, no violation
   derived, ~6.3k); **MV detection ≈ 0%** (every minor-variance row lacked a batch zoning limit).
   This is the honest baseline the open-data enrichment items (OP join, height overlay, multiplex
   UNITS) are meant to raise — and the harness will measure whether they do. Recall is a *lower
   bound* because the enriched set lacks `zoning_max_height_m`/`permitted_use_category`, so
   metre-height and use-mismatch violations can't fire there.

A latent bug the corpus harness surfaced: `compliance.py` divided by the zone limit in
`_check_height_m`/`_check_fsi` without guarding `limit == 0` (real enriched rows have
`zoning_max_density == 0`). Fixed by treating any `limit <= 0` as "no encoded limit" across the
storeys/height/units/density/advisory checks; pinned by `TestComplianceZeroLimits`.

The framing-only guarantee itself is still covered by
`test_narrate_end_to_end_mocked` (score unchanged with the statutory section present) and by
leaving `_apply_confidence_overrides` untouched.

## Verification

- `uv run pytest tests/analytics/test_planning_act.py` — 25 unit tests (process precedence,
  per-type timelines incl. CD/SB non-null, four-tests count, s.45 remedy/section_ref).
- `tests/api/test_narrator_regression.py::test_statutory_process*` — 14 CI-safe process asserts.
- `tests/test_extract_compliance.py::TestComplianceZeroLimits` — zero-limit regression.
- `just planning-act-eval` — corpus process-match diagnostic (needs `just enrich`).
- `just test && just lint` — full CI-safe suite + ruff/ty, all green.

---

# Phase 2 — hardening + multi-process (2026-06-13, follow-on)

A self-critique surfaced gaps in Phase 1. Addressed here: statutory-data correctness (2),
honest eval (3), and the multi-process trigger set (4a). The OP land-use join (4b) was
**deferred** after the data turned out not to exist on open endpoints (below).

## Item 2 — statutory data correctness

Day counts **verified 2026-06-13 against Ontario's Citizens' Guide to Land Use Planning**
(primary gov source; e-Laws is JS-rendered and CanLII 403s bots): ZBA **90**, combined
OPA+ZBA **120**, minor-variance appeal **20** (s.45(12)); OPA s.22 and subdivision s.51 = 120
(standard Bill 108 values). Changes:
- Third-party appeal removal re-attributed to **Bill 23 (2022) cemented by Bill 185 (2024)**;
  remaining appellants are the applicant landowner, the Minister/approval authority, and narrow
  "specified persons" (NAV Canada, airports, certain EPA/aggregate holders).
- **OZ split 90 vs 120**: `statutory_timeline_days("OZ", is_combined=...)` and the score column
  use `is_combined_application`; the narrator detects an OPA mention in the description.
- **MV → s.45, CO → s.53** added to `APPLICATION_TYPE_PROCESS` (were `None`); **TLAB**
  intentionally unmapped (it's an appeal body, not an application type).
- `_SOURCE_NOTE` now records the verification source; the s.41 site-plan appeal status (changed
  by Bill 185) is the one value still flagged for a primary-source check.

## Item 3 — honest corpus eval

`just planning-act-eval` now:
- Splits OZ rezoning recall by `dev_approved` to test the temporal-drift hypothesis (approved OZ
  may have upzoned its own site). Result: **clean 10.4% vs approved 10.6% — essentially identical,
  so drift is NOT the cause**; the low recall is genuine engine/data blindness.
- Reports **rezoning precision ≈ 65%** (of rows we call rezoning, share that actually filed OZ).
- Floors pinned in `tests/analytics/test_planning_act_eval.py` (integration): clean recall > 5%,
  precision > 40%.

## Item 4a — multi-process trigger set

`path_for_violations` (primary zoning path) was only one axis; a proposal can be as-of-right on
zoning yet still trigger other processes. Added `additional_processes(extracted)` —
deterministic feature/keyword triggers for **site_plan (s.41), subdivision (s.51), condominium,
consent (s.53), part_lot_control (s.50), rental_replacement (CoTA s.111 / Ch. 667)** — and
`statutory_processes()` (primary + orthogonal, deduped). Surfaced in the narrator prompt
("Likely also required …"), in `/evaluate` as `additional_processes: list[StatutoryProcessResult]`,
and in the golden fixtures (`expected_statutory.additional`, asserted). Corpus trigger rates:
site_plan 64.3%, condominium 14.0%, subdivision 8.4%, rental_replacement 3.6%, part_lot 2.1%,
consent 0.3% — signal the old single-bucket classifier discarded entirely.

## Item 4b — Official Plan land-use join — DONE (interim source)

The premise (OP land use is on Toronto Open Data as a polygon layer) is **wrong**. Re-verified
2026-06-13/14: neither the **COTGEO ArcGIS org** (`services3.arcgis.com/b9WvedVPoizGfvfD`, the
one the AIC source uses — only `COT_official_plan_areas`, the map-sheet index) nor
`gis.toronto.ca/.../cot_geospatial11` (zoning, secondary plans, SASP, MTSA, heritage) carries an
OP land-use **designation** layer. The license-clean long-term source is a **City Geospatial
Competency Centre (GCC) data request** (`gcc@toronto.ca`) — pending; the binding question is
whether commercial redistribution is permitted, which only the GCC can confirm.

**Built now against an interim source** (user decision: ingest Borealis now, full incorporation,
swap the official layer in later): the Borealis/Dataverse reconstruction (`doi:10.5683/SP3/1VMJAG`),
`LanduseParcelsMerged` layer — 10 designation multipolygons dissolved, EPSG:26917, **CC BY-NC 4.0**
(interim only). Implemented as the established reference-polygon pattern:
- `reference._fetch_op_land_use` (datafile 315976 → reproject UTM17N→WGS84 via DuckDB `ST_Transform`
  → `data/reference/op_land_use.geojson`, canonical OP designation names); `just op` / `zoneto op`.
  Optional/graceful in `fetch_reference` (absent → null everywhere), like TRCA/greenbelt.
- `spatial._add_op_land_use_feature` adds `op_land_use_designation` to enriched dev rows;
  `site_context.lookup_site_context` adds it (with the zoning-style ~200m snap fallback) for `/evaluate`.
- **s.24/s.22 conformity check** now built: `use_classifier.op_use_matches_designation` +
  `compliance._check_op_conformity` emit `op_use_nonconforming` (INFORMATIONAL — surfaces the OPA
  requirement + flips the narrator's combined 120-day clock, but does **not** move the confidence
  number on the uncertain interim layer; promote to `NEEDS_REZONING` with the authoritative layer).
- Eval: `planning-act-eval` reports `op_coverage` + OZ detection lift (`OZ_recall_path_only` vs
  `OZ_recall_with_op`); floors pinned in `tests/analytics/test_planning_act_eval.py` (integration).

**Measured on the real enriched corpus (n=30,772):** OP designation **coverage 60.9%** (the
dissolved layer omits roads/utility/water/gaps). OZ "needs a Planning Act amendment process"
detection lifts from **10.4% (zoning path only) → 35.4% (with the OP-conformity signal)** — a +25pt
lift that validates the "single largest missing signal" claim — while rezoning **precision is
unchanged at 65%** (the OP signal is orthogonal to the zoning-envelope path, by design).

## Also fixed

Latent bug found by the corpus harness on real data: `_check_height_m`/`_check_fsi` divided by a
zone limit of `0` (`zoning_max_density == 0` exists in enriched rows). All four
storeys/height/units/density checks now treat `limit <= 0` as "no encoded limit"; pinned by
`TestComplianceZeroLimits`.

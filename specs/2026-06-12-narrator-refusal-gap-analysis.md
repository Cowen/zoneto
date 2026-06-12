# Narrator Refusal Gap Analysis — 2026-06-12

How does the narrator's confidence score hold up against the applications
Toronto actually refused or forced into revision? This analysis ran every
unique refused application through the evaluate pipeline
(`just narrator-triage`, new in this batch), researched the real refusal
reasons externally, and codified the findings as five new golden cases
(fixture batch `2026-06-12 refused/revised triage`).

---

## Headline Numbers

| Population | N | Result |
|---|---|---|
| Unique refused applications (dev_approved=0) | **17** | 61 rows collapse to 17 folderrsns (multi-address parcels) |
| Refused apps scoring **>= 70** ("strong likelihood") | **8 of 17 (47%)** | every one forced by the floor-70 override |
| Refused apps scoring >= 55 | 9 of 17 | the 9th is a precedent-floor-55 case |
| Refused apps the narrator scores low (<= 35), correctly | 8 of 17 | 3 cap-30, 5 passthrough |
| LLM score distribution (n=17) | — | min 15, median 55, max 73 |
| Refused SA applications with a real decision milestone | **0 of 6** | all stop at "Notice of Complete Application Issued" |
| Revised-then-approved applications (description regex) | 54 unique | 145 rows; source for the finch-57 pair |

Mechanism buckets over the 17 (deterministic probe, no LLM):
`floor-70: 8, passthrough: 5, cap-30: 3, precedent-55: 1`.

The single biggest miscalibration in the system: **when the zone polygon
carries no structured storey/height/unit limit, any compatible-use proposal —
including a 73-storey tower — produces zero structural violations and the
floor-70 guarantees "strong likelihood".** Refusal correlates with what the
rule engine can see (cap-30 and use-mismatch cases score low); it is blind to
everything else.

---

## Deep-Dive: Why Were They Actually Refused?

Refusal reasons are not in any of our data sources (status says "Refused" and
nothing else; the AIC layer has no decision text; the OLT scraper returned 0
rows on 2026-06-12 — site changed, see Data Issues). Reasons below were
researched per case from staff refusal reports (`toronto.ca/legdocs`),
UrbanToronto, and councillor pages.

| Case | Refusal driver(s) | Narrator score | Can current inputs see it? |
|---|---|---|---|
| 372-378 Yonge (4997273, SA) — 73st over heritage row | Heritage: designated Victorian/Art Deco facades; "negative impact on heritage attributes of scale, form and massing" | **72** (floor-70) | Partially — heritage flags exist but are informational-only, and were 0 at this geocode |
| 5576 Yonge (5150376, OZ) — 35+25st, 608u | OP Avenues policy (mid-rise corridor), sanitary sewer easement through site, stormwater/water capacity, parking, parkland shortfall | **73** (floor-70) | No — Avenues designation, servicing, parkland all absent from data |
| 328-356 Dupont (3693046, OZ) — 13st | Dupont Secondary Plan: 20m rail-corridor safety setback; OMB capped corridor at 12st | **70** (floor-70) | No — in_secondary_plan=0 here (layer gap); setbacks unmodeled |
| 1061 The Queensway (5353820/39, SA twins) | No public record; no decision milestone → administrative closure | **72** (floor-70) | Label noise, not a real adjudicated refusal |
| 6 Howard Park (5316030, SA) | SA refused while parent OZ 4915577 was OMB-**approved** at the same site | **55** (precedent-55) | Label noise — the "refusal" does not mean the project died |
| 782 King W (5317113, SA) | No decision milestone; parent OZ active → administrative closure | 35 | Label noise |
| 2-4 Mendota (5559219, OZ) — self-storage | Use: 4st self-storage in residential context, Council refused 2026-05-21 | **28 — correct** | Yes — use_not_permitted violation |
| 5840-5870 Yonge (3893344, OZ) — 29st | Council refused 2017 (corridor context) | **28 — correct** | LLM mid-band judgment, no override |
| 195 Old Weston Rd (2962468, SA) — 50u in E zone | Employment-zone conversion era; adjudication unclear | **18 — correct** | Yes — use mismatch |
| 145 Sheppard E (4283373, OZ) — 11st in RD | Secondary-plan misfit, mid-rise on stable street; appealed to LPAT | **28 — correct** (cap-30, 5.5x) | Yes — ratio cap |
| 1539-41 Ave Rd / 272-96 Lawrence W (4766004, OZ) — 14+12st | Avenues stepping-down, transition/angular plane, shadow, rental replacement. **OLT then approved in principle (2025-04-02) — our label is stale** | **28** (cap-30, 7x) | Ratio yes; the OLT reversal no |
| 36-44 Eglinton W chain (4862745 + absent predecessor) | Predecessor (65st, FSI 32.27) refused: Yonge-Eglinton "Crossroads" height-peak policy, 0.31m setbacks, Eglinton Park shadow, no Functional Servicing Report. Identical 65st resubmission → OLT settlement at 59st | n/a (predecessor not in data) | No — FSI, setbacks, shadow, secondary-plan policy all invisible |

Refusal-reason taxonomy across adjudicated refusals: OP/secondary-plan area
policy (5576 Yonge, Dupont, 145 Sheppard, Eglinton, Lawrence), built
form/setbacks/transition (Lawrence, Eglinton, Dupont), heritage (372-378
Yonge), servicing/technical (5576 Yonge, Eglinton), shadow (Lawrence,
Eglinton), use (Mendota), parking/traffic (5576 Yonge). **Height/density
ratio — the one thing the narrator scores deterministically — was the sole
driver in zero of the researched cases.** It correlates (cap-30 cases were
refused) but the stated grounds are always policy, built form, heritage, or
servicing.

---

## What the Revised-Then-Approved Cases Show

54 unique approved applications carry an explicit revision note. The three
researched in depth:

1. **320 McCowan Rd (4724015)**: 27/31st, 520u, FSI 5.5 → revised to 25/29st,
   483u, FSI 5.0 "in response to City staff comments" → OMB approved. The
   revision currency was storeys-FSI-parking — modest deltas, not shape changes.
2. **57-63 Finch Ave W (3103677)**: 4st/70u apartments → council non-decision →
   OMB → 42 back-to-back stacked towns, larger setbacks, less coverage, more
   landscaping → approved. The revision currency was unit count and built form.
3. **36-44 Eglinton W (4862745)**: refused at 65st... resubmitted at the *same*
   65st, settled at OLT for 59st. Some "revisions" are process moves, not
   design concessions.

Empirical test (now pinned by the `finch-57-original` / `finch-57-revised`
golden cases): **the narrator scores the refused-shape original and the
approved revision identically** (75-78 vs 78-82, tied once in 5 calibration
runs). Two mechanisms compress the pair:

- Both halves floor at 70 when the zone encodes no limits (Finch: RM zone,
  only FSI 0.85 — never checked).
- The original text self-matches the approved revised row at >= 0.90 TF-IDF
  similarity within 250m (McCowan original: 0.98), so the precedent floor-55
  fires for the *original* too.

A comps-style "what did revisions buy" signal is therefore not derivable from
the current scoring path; it lives in the description text the LLM sees but
the deterministic layer ignores.

---

## Gaps, Mapped to Code

### 1. floor-70 equates "no encoded limit" with "as-of-right" (the 47% false-positive driver)

`_apply_confidence_overrides` (src/zoneto/api/narrator.py): zero structural
violations + compatible use → floor 70. But zero violations usually means the
zone polygon has **null** limits, not that the proposal complies. 374 Yonge
(73st, CR zone), 1061 Queensway (43st, UT), 5 Lamont (46st, RD geocoded to a
limit-less polygon) all floored at 70 and were all refused.

### 2. No storeys↔metres inference in extraction/compliance

372-378 Yonge: the zone **does** encode a 46.0m height limit, but the
description states "73-storey" and no height in metres, so no violation.
Dupont: 13.0m limit vs "13 storeys" — same blindness. A conservative
3m/storey inference would have produced an extreme-ratio cap in both.

### 3. FSI (`zoning_max_density`) is stored but never violation-checked

`check_compliance` (src/zoneto/analytics/compliance.py) checks storeys, units,
height — never density, although `zoning_max_density` is populated (Finch:
0.85; Dupont: 1.0; St Clair: 0.6). The 36 Eglinton predecessor was refused at
FSI **32.27**. FSI is also the revision currency (5.5 → 5.0 at McCowan).

### 4. First-match regex extraction misreads multi-tower and revision texts

`extract_project_features` (src/zoneto/analytics/extract.py) on the real
McCowan description returns **storeys=2** (from "reduced ... by 2 storeys")
and units=200 (the west-tower partial) for a 25+29st, 483-unit proposal. The
original text returns storeys=4 (from "4-storey parking structure"). Tallest-
value or proposal-clause-aware extraction would fix both.

### 5. Policy-overlay flags are informational-only, and sometimes point the wrong way

Heritage register/district, secondary plan, MTSA never block the floor-70 and
are worth at most ±8 — yet secondary-plan and heritage conflicts are the most
common stated refusal grounds. Worse, `in_secondary_plan` is treated as an
**upward** (+8) signal in the prompt's Step 3, while in the refused set it
flagged the binding constraint (Agincourt at 4155 Sheppard). Coverage is also
spotty: in_secondary_plan=0 at Dupont despite the Dupont Street Secondary
Plan being the refusal basis; in_heritage_register=0 at 372-378 Yonge despite
designated buildings on-site.

### 6. "Refused" status is not an adjudication record

- All 6 refused SAs stop at "Notice of Complete Application Issued" — they are
  administrative closures (parent OZ superseded/settled), not decisions.
  6 Howard Park's "refused" SA sits under an OMB-**approved** OZ.
- Labels go stale against tribunal outcomes: 4766004 is "Refused" but the OLT
  approved it in principle 2025-04-02.
- Refused applications can vanish entirely: the 36 Eglinton predecessor
  (file 20 165466 NNY 08 OZ, refused 2020, not appealed) is not in the
  dataset at all — refusal-label coverage (17 unique since 2010) badly
  understates actual refusals. Resubmission chains exist only as free-text
  mentions ("Resubmission of previously refused File No. ...").

---

## Proposed Scoring Changes (NOT implemented — proposals only)

Ordered by expected impact; each lists the golden cases it would move.

1. **Demote floor-70 to floor-55 when limits are unknowable.** Fire floor-70
   only when at least one hard limit (storeys/height/units) is encoded AND the
   proposal is within it; otherwise "compatible use, limits unknown" floors at
   55 with an explicit data-gap caveat. Moves: yonge-374, dupont-328,
   wellesley-68, finch-57-original/-revised would drop their floors to 55
   (advisory cases could then be re-banded ~[40, 70]); weston-1552 and
   fallingbrook-100 (limits encoded, compliant) keep 70. Would have cut the
   refused >= 70 rate from 8/17 to ~0/17.
2. **Storeys→metres inference (3m/storey, conservative) in compliance.**
   Converts yonge-374 (73st vs 46m → 4.8x) and dupont-328 (13st vs 13m → 3x)
   into cap-30 cases — both refused. No existing approved case regresses
   (weston/fallingbrook are within limits either way).
3. **Check FSI against `zoning_max_density`** when GFA or a stated FSI is
   extractable from the description (it usually is: "FSI of 5.4", "32.3 times
   the lot area"). Joins the cap-30 ratio trio. Moves: finch-57-original below
   finch-57-revised (the deferred pair ordering becomes assertable),
   livingston-408 tightens.
4. **Extraction hardening:** take the max across storey mentions, ignore
   "by N storeys" deltas and "existing N-storey" context, sum multi-tower unit
   counts, and prefer the revised clause when both original and revised stats
   appear. Pins: McCowan-style texts (currently storeys=2/units=200 for a
   25+29st/483u proposal).
5. **Make secondary-plan membership risk-neutral or negative** in Step 3
   (currently +8). The refused set shows it flags binding constraints at
   least as often as upside.
6. **Treat refused SAs with no decision milestone as label-null**, not
   dev_approved=0, in `labels.py`; and refresh `dev_approved` against OLT
   outcomes when the OLT source works again. Affects training labels and the
   comps appeal/approval stats, not just the narrator.

Items 1-3 are deterministic-layer changes validated by the existing +
new golden cases (`just narrator-eval` before/after, re-band advisory cases
that start scoring correctly). Item 6 is an enrichment change with its own
blast radius (training labels) — do not bundle it with narrator work.

---

## Data Issues Found Along the Way

- **AIC ArcGIS layer schema changed** (by 2026-06): the GET
  `?where=FOLDERRSN=...&outFields=*` form recorded in the original fixture
  verification URLs now returns 400. Use POST form data with
  `where=FOLDERRSN IN (...)` and the new field names (`STATUS_DESC`,
  `SUBMIT_DATE`, `FULL_ADDRESS`, ...). `src/zoneto/sources/aic.py` discovers
  fields dynamically so the scraper itself still works, but its
  `_AIC_FIELD_MAP` keys (`STATUS`, `LOCATION`, `DATE_SUBMITTED`) no longer
  exist on the layer — `just aic-full` would currently fetch only the
  surviving mapped fields. Worth a follow-up.
- **OLT scraper returns 0 records** (`just olt`, 2026-06-12) — olt.gov.on.ca
  presumably changed structure. Until fixed, `olt_outcome` matching is dead
  and refusal labels cannot be cross-checked against tribunal reversals.
- The refused set has heavy folderrsn duplication (61 rows / 17 unique);
  anything that aggregates refused rows without dedupe is overweighting
  multi-address parcels.

---

## Follow-up (same day): Proposals 1–3 Implemented and Proven Out

Proposals 1–3 were implemented immediately after this analysis and validated
against the eval suite and the refused-set triage.

**What landed:**

1. *Floor-70 demotion* (`narrator.py::_limits_verified`): the as-of-right
   floor-70 now fires only when at least one encoded limit (storeys, height —
   stated or inferred, units, FSI) was actually checked against an extracted
   value; compatible-use proposals with unverifiable limits floor at 55. The
   system prompt's Step 2 gained a matching "limits unknown → 55–65" band.
2. *Storeys→metres inference* (`compliance.py::effective_height_m`, 3.0m/storey):
   a new `height_exceeds_max_inferred` check fires when no explicit height was
   stated, the zone has no storey limit, and the estimate exceeds the metre
   limit by >25%; the cap-30 ratio uses the inferred height too.
3. *FSI checking* (`extract.py::proposed_fsi` + `compliance.py::_check_fsi`):
   stated FSI ("FSI of 5.4", "32.27 times the lot area") is extracted and
   checked against `zoning_max_density`; FSI joins the cap-30 ratio trio.

**Refused-set triage, before → after** (`just narrator-triage --llm`, n=17):

| Metric | Before | After |
|---|---|---|
| Refused scoring >= 70 | **8/17 (47%)** | **0/17** |
| Refused scoring >= 55 | 9/17 | 6/17 (all in the honest 55–64 "limits unknown" band, plus the precedent case) |
| Median / max LLM score | 55 / 73 | **30 / 64** |
| Buckets | floor-70: 8, cap-30: 3, passthrough: 5, precedent: 1 | cap-30: 7, floor-55-unverified: 5, passthrough: 4, precedent: 1 |

The inference converted four additional refusals into deterministic caps
(68 Wellesley 28st vs 18m, 372-378 Yonge 73st vs 46m, 328 Dupont 13st vs 13m,
782 King W 18st vs its E-zone height limit). No approved case regressed:
weston-1552 and fallingbrook-100 keep floor-70 (limits encoded and respected),
st-clair-1613 and dorney-43 keep their precedent floors — 13/13 bands and 3/3
orderings green post-change.

**Golden-case movement:** wellesley-68, yonge-374, and dupont-328 were
re-banded from advisory [70, 88] to **assertable [10, 30]** — three documented
limitations became regression-tested correct behavior. The finch pair demoted
to floor-55; it remains compressed (revised 78–80 vs original 60–78, advisory)
because neither text states an FSI — separating it needs unit-density
inference (units vs lot area), which stays on the proposal list. yonge-374 now
passes for the height reason, not the heritage reason; the heritage and
secondary-plan blind spots (gaps #5–6 above) remain open.

---

## Artifacts From This Batch

- `scripts/narrator_refused_triage.py` + `just narrator-triage` — rerun after
  any scoring change lands; the headline number to watch is "refused apps
  scoring >= 70".
- 5 new golden cases (13 total) + 1 new ordering in
  `tests/fixtures/narrator_eval_cases.json`: yonge-374 (heritage, advisory),
  dupont-328 (secondary plan, advisory), mendota-2 (use mismatch, assertable),
  finch-57-revised / finch-57-original (revision pair, original advisory).
- `test_confidence_in_band` now xfails advisory band misses instead of
  failing, matching the eval script's semantics.

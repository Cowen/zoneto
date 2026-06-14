# Product Critique: Real-World Utility of the Zoneto Scoring System
_2026-06-13_

## Up Front

The product has two ideas fused together that deserve different answers:

1. **"Tell me what process this site/proposal will need, and show me the comps"** — sensible, genuinely useful, and the data mostly supports it. Site acquisition teams triage dozens of candidate sites; compressing "what zone is this, what overlays apply, what did similar applications nearby do, how long did it take, what's the appeal exposure" from hours of manual lookup into seconds is real value.

2. **"Score the proposal 0–100 on likelihood of approval"** — this is the weak idea, and the refusal gap analysis (2026-06-12) essentially proves it. The killer line: _height/density ratio — the one thing the narrator scores deterministically — was the sole refusal driver in zero of the researched cases._ The actual refusal grounds were Official Plan policy, built form/transition, heritage, servicing, shadow. The system scores the one dimension that doesn't decide outcomes, because it's the one dimension open data encodes.

There's a deeper structural problem with the scoring concept for this user base: **in Toronto, exceeding the base zoning envelope is not a risk signal — it's the business model.** Virtually every project the target user cares about (anything over ~6 storeys) requires a ZBA, often an OPA. The base zone in much of the city reflects 1950s–2000s built form; the planning conversation happens entirely in the delta above it. So "73 storeys proposed, 46m allowed" tells a development professional almost nothing they didn't know from the address. What they're paying for is: _what excess gets approved here, by whom (Council vs OLT), at what cost in concessions, and on what timeline._ The comps/appeal-rate/survival-model side of the stack is pointed at exactly that question; the compliance score isn't.

---

## Large Processes and Laws the System Doesn't See

Roughly in order of how often they actually decide outcomes:

### 1. The Official Plan — the largest single gap

The system checks By-law 569-2013 but not the OP, and under the Planning Act, zoning must conform to the OP. The land use designation (_Neighbourhoods_ vs _Apartment Neighbourhoods_ vs _Mixed Use Areas_ vs _Avenues_ vs _Employment Areas_) is the real ceiling:

- A site designated _Neighbourhoods_ is effectively closed to tall buildings no matter what the comps say.
- An _Employment Areas_ conversion to residential needs a Municipal Comprehensive Review — close to impossible mid-cycle.

The refusal set shows this directly (5576 Yonge refused on Avenues policy; 195 Old Weston was an employment conversion).

The good news: **Official Plan Land Use is on Toronto Open Data as a polygon layer**, the same shape as the existing zoning join. This is probably the single highest-value enrichment not yet done, and it's cheap.

### 2. Secondary plans as content, not flags

The system has `in_secondary_plan` as a boolean (with coverage holes — the gap analysis found it was 0 at Dupont, where the Dupont Street Secondary Plan was the refusal basis), and the narrator prompt currently treats it as a +8 _upside_. The refused set shows the opposite polarity.

There are ~40 secondary plans and hundreds of Site and Area Specific Policies, and they contain the binding numbers (the Yonge-Eglinton height peaks that killed the 65-storey Eglinton proposal, the Dupont rail-corridor setback). The flag tells you a rulebook exists; the system never reads the rulebook. That's a hard problem (the documents are PDFs of prose), but it's where the actual law lives for high-value sites.

### 3. Built form guidelines — the negotiation currency

Angular planes, transition to Neighbourhoods, tower separation (25m), floorplate caps (~750m²), shadow on parks, setbacks. The revised-then-approved analysis showed this is what revisions actually trade in (McCowan: storeys/FSI/parking deltas; Finch: apartment → stacked towns). None of this is modelable from a text description plus a zone polygon. It may bound how good _any_ description-scoring approach can get.

### 4. The provincial layer, which is currently rewriting the game board

Bill 23 eliminated third-party appeals and restructured parkland; Bill 109 created fee-refund timelines that changed how the City processes applications; Bill 185 killed pre-application requirements; MZOs bypass everything.

More concretely dangerous for correctness: **Toronto's citywide multiplex amendments (four units as-of-right citywide since 2023, sixplex in parts of the city since 2025), garden/laneway suites, Major Streets, and the removal of parking minimums may not be reflected in the zoning GeoJSON's UNITS field.** If `zoning_max_units` still says 1 in an RD zone, the `unit_limit_advisory` and units cap-30 will flag a fourplex as needing rezoning when it's now as-of-right. That's not a missing feature — it's the system being **wrong in the strict-compliance lane it claims to own**. Worth verifying against the source data before anything else.

### 5. Section 37 / Community Benefits Charge

Section 37 density bonusing was repealed (Bill 108) and replaced by the Community Benefits Charge — a formulaic 4% of land value — in September 2022. The `_format_community_benefits` block in the narrator presents negotiated s.37 comps as forward guidance; for any new application it is a historical artifact. Either reframe it as CBC context or drop it.

### 6. The economics that decide go/no-go

Development charges (~$50–90k/unit), parkland dedication, Inclusionary Zoning in PMTSAs (note: `in_mtsa` is scored as pure upside, but PMTSA status also triggers IZ set-asides — it cuts both ways), rental replacement under Chapter 667 (demolishing 6+ rental units triggers replacement at like rents — a project-killer the description often reveals), Toronto Green Standard. A due-diligence tool that is silent on all of these is answering "can I?" when the user is asking "should I?"

### 7. The OLT is the actual endgame, and the scraper is broken

Most large contested Toronto projects are approved via OLT settlement, not Council. The labels show the damage: 4766004 marked "Refused" but OLT-approved in 2025; six of seventeen "refusals" are administrative closures, not decisions. With the OLT scraper returning 0 rows (`just olt`, 2026-06-12 — olt.gov.on.ca changed structure), the system's outcome labels are systematically pessimistic about exactly the projects users care most about. Fixing that scraper matters more than any narrator change, because every downstream label, comp, and trained model inherits the error.

### 8. Smaller but real

- The COA "10% is typically minor" heuristic in the remedies isn't law — the Planning Act s.45 four tests are qualitative, and Toronto COA panels routinely grant larger variances (and refuse smaller ones).
- Site Plan Approval applies to nearly everything and is often the schedule driver but isn't modeled as a process step.
- Airport height limits, contamination/Record of Site Condition, and tree bylaws occasionally bite.

---

## What This Implies for the Product

The honest product is **"process classifier + overlay screener + comps engine"**, and the dishonest part is the 0–100 number. Target users are professionals; they will catch the first wrong score (a floored 70 on a refused tower, or a "needs rezoning" on an as-of-right fourplex) and discount the tool permanently.

The same machinery, reframed as: _"this needs an OPA + ZBA + SPA; here are 6 same-zone OZ comps, their outcomes, appeal rates, and p50 timelines; flags: Avenues designation, PMTSA (IZ applies), rental replacement likely"_ — with no synthetic probability attached — is both more defensible and closer to what due diligence actually consumes.

If the score is kept, the gap analysis already gives it a more accurate name: **compliance-path viability**, not approval probability. The June 12 fixes (floor demotion, height inference, FSI) made it much better at not being _confidently_ wrong — refused-set ≥70 went from 47% to 0% — but they improved calibration on the dimension that doesn't drive decisions. The ceiling on this approach is set by what's in open data, and the decisive variables (OP policy fit, built form, servicing, political context) mostly aren't.

---

## Concrete Priority Order

1. **Verify the multiplex/EHON as-of-right reality against zoning UNITS data** — correctness bug; the system may be flagging legal fourplexes as needing rezoning.
2. **Join the Official Plan Land Use layer** — cheapest large signal missing; same spatial join pattern as zoning.
3. **Fix the OLT scraper** — labels are wrong without it; every trained model inherits the error.
4. **Replace the s.37 framing with CBC context** — the historic comps are misleading for new applications.
5. **Flip or neutralize the secondary-plan/MTSA scoring polarity** — currently +8 upside; refused set shows it often flags binding constraints instead.

Items 1–3 are correctness fixes. Items 4–5 are calibration fixes. The bigger strategic decision — whether the confidence number should be a number at all — is upstream of all of them.

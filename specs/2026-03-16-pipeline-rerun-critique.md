# Pipeline Re-Run Critique — 2026-03-16

## Pipeline Results Summary

| Model | N | Primary Metric | Production Ready |
|---|---|---|---|
| dev_applications_appealed | 18,322 | AUC 0.692 ± 0.063 | Yes |
| dev_days_to_decision (survival) | 7,642 | C-index 0.741 ± 0.052 | Yes |
| coa_approved | 4,609 | AUC 0.535 ± 0.192 | **No** |
| coa_days_to_approval | 4,251 | R² -0.268 ± 0.600 | **No** |
| permit_issuance_days | 133,006 | R² 0.039 ± 0.212 | **No** |

Only 2 of 5 models are production-ready. The COA and permit models are effectively broken.

---

## Critique: Data Scientist & Toronto Real Estate Developer

### What I would not pay for

**1. The COA models are worthless in their current state.**

AUC 0.535 is barely above random (0.50). The R² of -0.268 means the regression model is *worse* than predicting the mean every time. The enormous standard deviations (0.192 for AUC, 0.600 for R²) tell me these models are unstable across folds — some folds perform reasonably while others are catastrophic. This is not a model I would show to a client, let alone charge for.

**Root cause:** The COA dataset has only 4,749 rows spanning 2022-2023 closed applications, with a 94.5% approval rate (4,350 approved vs 259 denied vs 140 unknown). The features available — zoning designation, ward, application type — don't capture what actually drives COA decisions: site-specific facts (neighbour impact, lot geometry deviations), the quality of the planner's report, and whether neighbours objected. These are fundamentally soft, qualitative factors that the structured data doesn't record.

**What a developer actually wants from COA:** A COA minor variance for a 1.2m rear-yard setback reduction on a detached lot in Leaside is a completely different animal than a consent to sever in Scarborough. The model treats them identically because it has no feature for "magnitude of requested variance relative to the by-law standard."

**2. The permit issuance model explains almost nothing.**

R² of 0.039 means the model captures 3.9% of the variance in permit processing time. The median is 30 days but the range goes to 1,909 days. The heavy right tail (mean 59 vs median 30) suggests a mixture of fast-track permits (plumbing, minor renos) and complex permits (new construction) that the model can't distinguish well enough.

Structure type and permit type are the top features (importance 0.23 and 0.16), which makes sense — a plumbing permit clears faster than a new-build. But ward_grid being the #2 feature (0.17) is suspicious: it likely captures staffing/backlog differences at district offices rather than anything inherent about the application. This means the model's "predictions" would degrade the moment the city rebalances workloads.

**What a developer actually wants:** "If I submit a new-build permit for a 6-storey mid-rise in Ward 10 in March, when should I realistically expect issuance?" The model can't answer this with any useful precision at R² 0.039.

**3. The appeal model (AUC 0.692) is useful but not compelling.**

An appeal probability model is genuinely valuable — appeals add 1-3 years and six-figure legal costs. AUC 0.692 is above the 0.65 production threshold but not by much. More concerning: the average precision is only 0.258, meaning the model generates a lot of false positives when flagging likely appeals. For a developer doing due diligence, a model that cries wolf too often loses credibility fast.

The feature importance reveals the core problem: `year_submitted` dominates at 0.317 importance. This isn't a real predictor — it's a proxy for policy and tribunal regime changes over time (OMB → LPAT → OLT transitions, provincial policy changes like Bill 23). The model is really saying "applications from era X had different appeal rates than era Y," which isn't actionable for a new application.

**4. The survival model is the strongest product — but the output is confusing.**

C-index 0.741 is genuinely solid for survival analysis. The model correctly ranks which applications will take longer. But the scored output shows `pred_dev_days_to_decision` with a median of 1,083 days (~3 years) and a p75 of 2,415 days (~6.6 years). These are *median survival times*, not "expected days." Most developers would misinterpret these numbers as point predictions and conclude the model is insane.

Also: the survival model only applies to OZ and SA applications. CD, SB, and PL types (4,730 applications, 18% of the dataset) get no time estimate at all.

**5. Nothing is scored for COA — the entire dataset is a dead end in the UI.**

Because both COA models fail the `production_ready` check, the scored COA parquet has zero prediction columns. A user querying "will my minor variance be approved?" gets nothing. This is the single most common question from homeowners and small developers approaching the Committee of Adjustment.

**6. The dev_approved model was retired but the problem it solved wasn't.**

The CLAUDE.md says dev_approved was retired due to 97.3% class imbalance and the frozen dataset. But look at the label distribution: only 2,446 approved vs 69 denied out of 26,161 rows, with 23,646 nulls. The null count is enormous — it appears the "approved" label only applies to a narrow subset (likely only those with explicit "Council Approved" status), while the vast majority of closed applications get null. This isn't really 97.3% imbalance in the real world; it's a labeling problem where most approvals are coded as something other than "approved."

### What would make me pay

**A. Dramatically improve feature engineering for the appeal model.**

The appeal model is the most commercially valuable. To get it from "interesting" to "I'd embed this in my due diligence workflow":
- Add **proposed density/height** extracted from the description field (NLP or regex for "XX storeys", "XX units")
- Add **neighbourhood appeal history** — what fraction of OZ/SA apps in the same ward were appealed in the last 3 years
- Add **councillor-level features** — some councillors are associated with higher appeal rates
- Add **application age at scoring** — older applications that haven't been appealed yet are less likely to be

**B. Fix the COA model or drop it honestly.**

Either:
- Enrich with variance magnitude features (what specific provisions are being varied and by how much), which would require parsing the application description
- Bring in neighbour objection counts if available through TLAB/AIC data
- Or be honest and present COA as "data-only" without predictions, rather than training a model that can't beat a coin flip

**C. Segment the permit model by permit type.**

Instead of one model for all 133K permits, train separate models for:
- Small residential (the bulk of the volume, relatively predictable)
- New construction (fewer rows, longer timelines, different drivers)
- Plumbing/mechanical (fast-track, mostly a function of backlog)

Per-segment R² would likely be much higher than the pooled 0.039.

**D. Make the survival model output interpretable.**

Present survival curves, not just median times. Show "70% chance of decision within X days, 90% within Y days." This is what a developer actually needs for project finance timelines.

**E. Add a "confidence" or "reliability" indicator to each prediction.**

Not all predictions are created equal. An OZ application in a ward with 500 training examples is much more reliable than one in a ward with 20. Expose this to the user.

**F. Score active/under-review applications separately and prominently.**

The 4,731 active applications are where the commercial value lives — developers want to know about *their* pending application. The 18,175 closed applications are historical validation. The current output mixes them together.

---

## Product Manager Critique

### Triage: What's worth building?

The data scientist raised six suggestions (A–F). I've investigated feasibility against the actual codebase and data. Here's my assessment, ordered by impact-to-effort ratio.

---

### P0 — Do immediately (high impact, low effort)

#### Extract storeys and units from dev_applications descriptions

**Verdict: YES — this is the single highest-ROI improvement.**

- 51.8% of dev_applications descriptions contain storey counts ("12 storey", "28-storey")
- 40.2% contain unit counts ("551 units", "186 dwelling units")
- These are simple regex extractions, not NLP
- Proposed density/height is the #1 factor real estate developers evaluate. Adding it as a feature to the appeal model could meaningfully improve AUC beyond 0.692
- Also valuable for the survival model (larger projects take longer — this gives the model a way to distinguish "3-storey infill" from "40-storey tower")

**Implementation:** Add `_extract_storeys(description)` and `_extract_units(description)` in `enrich.py`. Add columns to `DEV_NUM_COLS` in `features.py`. Straightforward — a few hours of work.

#### Output survival percentiles, not just median

**Verdict: YES — low effort, dramatically better UX.**

The survival model already computes full survival functions in memory (`predict_survival_function()`). The scoring code discards them and only keeps the median. Outputting p25/p50/p75 (or specific time horizons like "probability of decision within 1 year / 2 years / 3 years") is a code change in `score.py`, not a modeling change.

**The data scientist is correct** that "1,083 days" as a point prediction is confusing and unhelpful. "70% chance of decision within 2 years" is actionable.

---

### P1 — Do next (high impact, moderate effort)

#### Ward-level rolling appeal rates as a feature

**Verdict: YES — strong data, clear signal.**

- 18,322 labeled applications across 33 wards
- Appeal rates vary from 0.6% to 17.1% by ward — this is real signal, not noise
- Ward-level 3-year rolling appeal rates would capture local political dynamics (NIMBYism hotspots, councillor attitudes) without the `year_submitted` temporal leakage problem the data scientist flagged
- Requires a windowed aggregation in `enrich_dev()` — moderate complexity but well within the existing Polars pipeline

**Caution:** Must be careful about temporal leakage. The rolling rate for a given application should only use data *before* that application's submission date.

#### Score active applications separately and prominently

**Verdict: YES — this is a presentation/product issue, not a modeling issue.**

The 4,731 active dev_applications are the commercial product. The 18,175 closed ones are training data. Currently they're mixed together in one parquet file. Adding a simple filter or separate output file is trivial. This should be a new CLI command or flag: `zoneto score --active-only`.

---

### P2 — Investigate further (moderate impact, moderate effort)

#### Permit model segmentation by type

**Verdict: PARTIALLY — segment the top 3, don't bother with the long tail.**

- Small Residential (35K rows), Plumbing (31K), Mechanical (29K) are each large enough for robust models
- The remaining 16 types have <17K rows each and diminishing returns
- Architecture supports this easily — add a `segment_col` parameter to `train_source()`
- **The data scientist may be wrong** that per-segment R² will be "much higher." The low R² may be fundamental: permit processing time is driven by inspector availability, seasonal backlog, and applicant responsiveness — none of which are in the data. Worth testing on the top 3 segments before committing

#### Add application age at scoring as a feature

**Verdict: YES — trivial and useful.**

For the appeal model: `days_since_submission = today - date_submitted`. Applications that have been open for 2+ years without an appeal are much less likely to be appealed. This is a single column addition in scoring, not even a training change (use `days_since_submission` as a feature at training time too, computed from submission date relative to the training data cutoff).

---

### P3 — Defer or drop (low impact or infeasible)

#### Fix the COA model

**Verdict: DEFER — the data doesn't support it.**

The data scientist correctly identified the problem (94.5% approval rate, no variance-magnitude features). My investigation confirms:
- Only 4.9% of COA descriptions mention storeys — there's no structural data to extract
- The features that matter (magnitude of variance, neighbour objections, site-specific planning context) are not in the dataset and not available through open data
- **The honest move:** Remove COA prediction from the product entirely. Display the raw data (approval rates by ward, by type, by zoning designation) as descriptive statistics, not predictions. A 94.5% base rate means "it'll probably be approved" is already a better predictor than the model

#### Confidence/reliability indicators

**Verdict: DEFER — nice to have, not core.**

Useful in a mature product. Right now we have 3 of 5 models that don't work. Fix the models first, then add confidence intervals. This is a polish feature for v2.

#### Councillor-level features

**Verdict: DATA NOT AVAILABLE — drop for now.**

Ward number is already a feature. Individual councillor behavior would require mapping councillors to wards over time (they change with elections) and would be a proxy for ward anyway. Not worth the complexity.

---

### Summary: Recommended build order

| Priority | Item | Effort | Expected Impact |
|---|---|---|---|
| P0 | Extract storeys/units from descriptions | Small | Appeal model AUC improvement, survival model improvement |
| P0 | Survival model percentile output | Small | Dramatically better UX |
| P1 | Ward rolling appeal rates | Medium | Appeal model AUC improvement, reduces year leakage |
| P1 | Separate active-application scoring | Small | Core product value |
| P2 | Permit segmentation (top 3 types) | Medium | Permit R² improvement (unproven) |
| P2 | Application age feature | Small | Appeal model marginal improvement |
| P3 | Drop COA predictions, show descriptive stats | Small | Honesty > bad predictions |
| P3 | Confidence indicators | Medium | Polish for v2 |

### Where the data scientist is wrong

1. **"Councillor-level features"** — Ward number already captures this. Councillors change every 4 years; ward demographics are more stable and more predictive.
2. **"NLP for COA descriptions"** — Only 4.9% of COA descriptions have extractable structural data. The text is about zoning variances ("to permit a reduced side yard setback"), not buildings. NLP would yield almost nothing.
3. **The implied claim that COA can be fixed with better features** — The 94.5% approval rate means the model's ceiling is inherently low. Even a perfect model would have minimal discrimination. The right product decision is to present base rates, not predictions.

# Model Critique — 2026-03-16

As a data scientist and Toronto real estate developer, this is my critique of the
models after a full pipeline rerun today. This run reflects all prior fixes
(CRS correction, 730-day cap, ward profiles, retired dev_approved, survival model).

---

## Current Model Metrics (2026-03-16 rerun)

| Model | N | Key metric | Secondary |
|---|---|---|---|
| `dev_applications_appealed` | 3,625 | ROC-AUC **0.871 ±0.057** | Brier 0.169 |
| `coa_approved` | 4,609 | ROC-AUC **0.535 ±0.192** | Brier 0.066 |
| `coa_days_to_approval` | 4,251 | R² **-0.268 ±0.600** | MAE 62d |
| `permit_issuance_days` | 133,006 | R² **0.039 ±0.212** | MAE 49d |
| `dev_days_to_decision` | 7,642 | C-index **0.743 ±0.049** | — |

---

## What Changed Since Last Spec (2026-03-15)

Minor metric drift only — no code changes between runs:
- `dev_applications_appealed` AUC: 0.865 → 0.871 (within noise)
- `coa_days_to_approval`: R² -0.465 → -0.268, MAE 83d → 62d — the 730-day cap
  is clearly working. Max `coa_days_to_approval` is now 729 (down from 2,992).
  Still negative R² but meaningfully better.
- `dev_days_to_decision` survival model: C-index 0.743 ±0.049 on 7,642 rows.
  This is the new model requested in the last spec's P0. It exists and it works.

---

## Critical Issues Found

### 1. Appeal Model Has a Fabricated 50/50 Base Rate

This is the most serious problem in the pipeline. The training data has
**1,830 appealed vs 1,795 not appealed** — a nearly perfect 50/50 split. This
does not reflect reality. In Toronto, the vast majority of development
applications are *not* appealed. The true appeal rate for OZ applications is
roughly 15-25% depending on the year.

The 50/50 split arises because the `dev_appealed` label is only non-null for
3,625 of 26,161 rows. The labeling logic (in `enrich_dev`) selectively assigns
labels to applications where appeal status can be determined from the `status`
field — which biases toward applications that *did* enter the appeal process
being labeled, while the many quiet approvals get null labels and are dropped.

**The consequence is visible in scoring output:** the model predicts >70%
appeal probability for **7,969 of 26,161 applications (30.5%)**. Mean predicted
appeal probability is 0.373. This is wildly inflated. A developer using these
scores would believe nearly a third of all applications are high-risk for
appeal, which would make the tool useless for prioritization — everything
looks dangerous.

The isotonic calibration (CalibratedClassifierCV) is faithfully calibrating to
the training distribution, which is the wrong distribution. The model is
well-calibrated to a fictional world where 50% of applications are appealed.

**Fix required:** Either (a) engineer the `dev_appealed` label to cover all
closed applications (not just those where appeal status is explicit in the
status field), ensuring the true base rate is preserved, or (b) apply class
weight rebalancing during training if the biased label set is unavoidable.
Without this fix, the appeal model cannot be shown to users.

### 2. Survival Model Scores Out-of-Domain Applications

The `dev_days_to_decision` model is trained exclusively on OZ and SA
applications (7,642 rows). This is correct — only OZ and SA have the relevant
AIC decision milestones.

However, scoring applies the model to **all 26,161 dev applications**, including
CD (2,734), SB (1,320), and PL (676) applications. These application types have
fundamentally different timelines: a Part Lot Control Exemption (PL) typically
takes weeks, not years. The model has never seen a PL or CD during training, so
predictions for these types are extrapolations from OZ/SA patterns.

**The result:** predicted median time to decision is **1,192 days (3.3 years)**
across all applications. The actual observed median for the training set (OZ+SA
only) is 945 days. The model inflates even further for out-of-domain types
because it has no learned basis for shorter timelines.

Only 56 of 26,161 scored applications get a prediction under 1 year. For CD/SB/PL
applications that routinely close within months, this is absurd.

**Fix required:** Either (a) only score OZ+SA applications with the survival
model and leave others null, or (b) train separate models per application type
if there is enough labeled data.

### 3. COA Model Predicts "Approved" for Every Single Application

`pred_coa_approved` has a mean of **1.0000** — literally every application is
predicted as approved. At a 94.4% base rate, the model has learned that the
optimal strategy is to always predict the majority class. AUC 0.535 with ±0.192
variance confirms there is no discrimination.

This was flagged in the last spec but it is worth restating: this model is not
merely weak, it is degenerate. The `prob_coa_approved` column (mean 0.936) is
nearly identical to the base rate for every application, meaning the model adds
zero information over simply displaying "94% of applications are approved."

The `production_ready: false` flag in metrics.json correctly gates this model.
But the model is still trained, serialized, and scored — consuming pipeline
resources for a result that is definitionally useless.

### 4. Permit Model Explains Nothing (R² = 0.039)

133,006 training rows. R² = 0.039. Feature importance shows structure_type
(0.232), ward_grid (0.168), and permit_type (0.162) as the top features — but
even these strong categorical signals only explain 4% of variance.

The problem is structural: permit issuance time is driven by queue depth,
application completeness, revision cycles, and zoning examiner workload — none
of which are in the data. No feature engineering on the existing columns will
fix this. The `production_ready: true` flag is set because R² ≥ 0.0, but the
threshold is too permissive — a model explaining 4% of variance should not be
labeled production-ready.

---

## What These Models Get Wrong as a Product

### The Appeal Model Creates False Urgency

If I'm a developer evaluating whether to file an OZ for a midrise condo at
Yonge and Eglinton, and the model tells me "78% chance of appeal," I would
budget an extra $200,000–$500,000 for LPAT counsel and 12-18 additional months
of carrying costs. If the true probability is closer to 20%, I've massively
over-provisioned. The inflated base rate means the model systematically
overstates risk, which ironically makes it *less* useful than no model at all —
at least without a model, I'd use my professional judgment and industry
base rates.

### The Survival Model Provides No Useful Discrimination

Predicted days to decision ranges from 334 to 6,549, but the distribution is
compressed: p25=866d, p75=2,044d. For a developer, the useful question is
"will this take 1 year or 4 years?" The model's interquartile range spans
866-2,044 days (2.4-5.6 years), which is so wide that it provides little
actionable discrimination. Application type alone (OZ vs SA) explains 50.6%
of the model's gain — meaning the model's primary signal is "OZ takes longer
than SA," which any developer already knows.

The C-index of 0.743 means the model correctly ranks 74.3% of pairs by
duration. This is decent for survival analysis but the absolute predictions
are inflated and the model has no mechanism to account for the post-2022
planning reform speedups (Bill 23, MTSA streamlining). Applications filed
after 2022 are in a structurally faster policy environment that the historical
training data underweights.

### Ward Demographics Add Measurable Signal — Keep Them

Contrary to the hypothesis in the last spec, ward demographic features
(`ward_pct_detached`, `ward_median_income`, `ward_pop_density`,
`ward_pct_renters`) collectively contribute meaningful permutation importance
to the appeal model: 0.0174 + 0.0136 + 0.0073 + 0.0042 = 0.0425 combined,
which exceeds `ward_number` alone (0.0109). These features capture
neighborhood character that goes beyond ward identity — they should be kept.

---

## What Would Make Me Pay (Updated Priorities)

### P0: Fix the appeal model base rate

The appeal model is the flagship product and its calibration is broken. Without
fixing the biased 50/50 training distribution, the model actively misleads.
This is a label engineering problem, not a modeling problem:

- Audit the `dev_appealed` labeling logic in `enrich_dev` to understand which
  closed applications are excluded (null label) and whether those exclusions
  systematically drop non-appealed cases
- Re-engineer the label to cover all closed OZ/SA applications, assigning
  `dev_appealed=0` to applications that completed without appeal
- Verify the resulting base rate matches known Toronto appeal rates (~15-25%)
- Retrain and check that scored probability distribution shifts to a realistic
  range (mean ~0.15-0.25, not 0.37)

### P1: Restrict survival model scoring to OZ+SA only

Do not score CD, SB, PL applications with a model trained only on OZ+SA. Either:
- Filter to `application_type IN ('OZ', 'SA')` before scoring, leaving others null
- Or, at minimum, add an `application_type` filter warning in the scored output

This is a one-line fix with large trust impact.

### P1: Tighten `production_ready` thresholds

- Classifiers: keep AUC ≥ 0.65
- Regressors: raise to R² ≥ 0.10 (from 0.0). A model explaining <10% of
  variance is not production-ready by any reasonable standard.
- Survival: keep C-index ≥ 0.65

`permit_issuance_days` (R² = 0.039) should fail the tightened threshold and be
excluded from scoring.

### P2: Retire or stop training degenerate models

`coa_approved` and `permit_issuance_days` consume training compute and disk for
results that add zero value. If they fail `production_ready` checks:
- Skip scoring them entirely (don't write columns that are known to be noise)
- Log a warning during training indicating the model is below threshold
- Keep the training step (so metrics are tracked) but skip serialization

### P2: Add temporal recency weighting to survival model

Applications filed after 2022 are in a structurally different planning regime
(Bill 23, provincial MTSA rezonings, streamlined site plan approvals). The
survival model treats a 2010 application and a 2024 application identically.
Adding sample weights that upweight recent years or using a rolling training
window (e.g., 2015+) would improve relevance for current applications.

### P3: Investigate the appeal label coverage gap

3,625 out of 26,161 rows (13.9%) have non-null `dev_appealed`. Of these, the
50/50 split suggests systematic selection bias. The remaining 22,536 rows include
both active applications (which correctly have no label) and closed applications
whose appeal status is ambiguous. Understanding exactly which `status` values
map to "no appeal" vs "unknown" is essential for the P0 fix above.

---

## Summary of Actionable Findings

| # | Finding | Severity | Fix effort |
|---|---|---|---|
| 1 | Appeal model trained on 50/50 data; real rate is ~20% | **Critical** | Medium |
| 2 | Survival model scored on CD/SB/PL (never trained on) | **High** | Small |
| 3 | `permit_issuance_days` passes production_ready at R²=0.039 | **Medium** | Small |
| 4 | COA approval predicts majority class for all rows | **Medium** | — (structural) |
| 5 | Survival predictions compressed; low discrimination | **Low** | — (data limit) |
| 6 | `importance-all` fails on retired `dev_applications_approved` | **Low** | Small |

---

## Product Manager Review

*Added 2026-03-16 — critique of the user feedback above from a product perspective.*

### Factual Verification

**"Appeal model has a fabricated 50/50 base rate"** — Verified. The training
data shows 1,830 appealed vs 1,795 not appealed. The label `dev_appealed` is
derived from the `status` field in `enrich_dev`. To confirm the mechanism, I
need to check which status values map to appealed vs not-appealed vs null.

This is a real and critical finding. However, calling it "fabricated" is
slightly misleading — the 50/50 split is an emergent property of the labeling
logic, not intentional design. The correct framing: **the label definition
creates selection bias that overrepresents appealed applications in the
training set.**

The user's claim that Toronto's true appeal rate is "roughly 15-25%" is
plausible for OZ applications but would need verification. Appeal rates vary
significantly by application type and era. The key point stands regardless of
the exact rate: 50/50 is far from reality.

**"Survival model scores out-of-domain applications"** — Verified. The model
trains on OZ+SA only (7,642 rows) but `score_all` applies it to all 26,161
dev application rows. This is a genuine bug in the scoring logic. The training
code correctly filters to non-null `dev_decision_event` (OZ+SA), but the
scoring code does not apply the same filter. **Clear fix, high confidence.**

**"Predicted median is 1,192 days (3.3 years)"** — Verified from scored output.
The user's interpretation that this is "inflated" needs nuance: OZ applications
genuinely take 2-5+ years in Toronto. The median observed time for the training
set is 945 days (2.6 years). The scored median being 1,192 days (3.3 years) is
higher because (a) it includes predictions for all types (inflating), and (b)
the survival model's median prediction is at the 50th percentile of the
survival function, which overestimates when censoring is heavy (3,069 of 7,642
are censored = 40.2%). **The fix for out-of-domain scoring will largely resolve
the distribution issue; the censoring effect is an inherent survival analysis
property and not a bug.**

**"COA model predicts approved for all rows"** — Verified. `pred_coa_approved`
mean is exactly 1.0000. This is expected behavior for a model with AUC ~0.5
on a 94.4% positive class — it learns to always predict the majority. The
`production_ready: false` flag is correctly set. **No new action needed beyond
what was already flagged.**

**"Ward demographics add measurable signal"** — Verified via permutation
importance. Combined importance of ward demographic features (0.0425) exceeds
`ward_number` alone (0.0109). This resolves the open question from the last spec
("measure impact before removing"). **Decision: keep ward demographics. Close
the Hold item.**

**"permit_issuance_days passes production_ready at R²=0.039"** — Verified.
The current threshold for regressors is R² ≥ 0.0, which this model technically
passes. The user's proposal to raise the threshold to R² ≥ 0.10 is reasonable —
R² < 0.10 means the model explains less than 10% of variance, which is not
useful for any practical decision. **Approve threshold tightening.**

**"`importance-all` fails on dev_applications_approved"** — Verified. The
model file still exists in `models/` but the enriched data no longer has the
`postal_fsa` column (removed in a prior cleanup). This is a stale artifact.
Either delete the joblib file or exclude it from the importance loop.
**Trivial fix.**

### What Is Worthwhile (PM-Approved)

| # | Item | Decision | Rationale |
|---|---|---|---|
| 1 | Fix appeal label to reflect true base rate | **Approve — P0** | Flagship model is miscalibrated; misleads users |
| 2 | Restrict survival scoring to OZ+SA | **Approve — P0** | Scoring bug; one-line fix |
| 3 | Raise regressor production_ready to R² ≥ 0.10 | **Approve — P1** | Prevents useless models from being scored |
| 4 | Skip scoring for non-production-ready models | **Approve — P1** | Don't write noise columns to output |
| 5 | Delete stale `dev_applications_approved.joblib` | **Approve — P1** | Cleanup; fixes importance-all |
| 6 | Keep ward demographics (close Hold) | **Approve** | Empirically validated via permutation importance |
| 7 | Temporal weighting for survival model | **Hold — P2** | Valid but not urgent; survival C-index is acceptable |
| 8 | Retire COA training entirely | **Reject** | Keep training for metric tracking; just don't score |

### What Needs Revision

**"Fix effort: Medium" for appeal base rate** — The user underestimates the
complexity. The fix requires:
1. Auditing which `status` values in `dev_applications` correspond to "completed
   without appeal" vs "completed with appeal" vs "ambiguous/active"
2. Re-engineering the label to cover all closed applications, not just those
   with explicit appeal signals
3. Verifying the resulting distribution against external data
4. Retraining and re-evaluating

This is a label engineering project that touches `enrich_dev`, requires domain
knowledge of Toronto planning status codes, and may reveal that the current data
simply does not distinguish "closed without appeal" from "status unknown." If the
latter, no amount of engineering will fix the base rate without external data.
**Effort is Medium-to-High, with a risk of discovering the data doesn't support
the fix.**

**"Retire or stop training degenerate models" as P2** — For COA models, I
disagree with full retirement. The COA data problem may be resolved if
additional year CSVs become available or if the city reopens the dataset. Keep
training (to track metrics over time) but agree to skip serialization and
scoring when below threshold. For `permit_issuance_days`, the user is right:
there is no path to improvement without queue-depth data. Retire scoring but
keep the training/evaluation step as a diagnostic.

### Prioritized Action Plan (PM-Approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| P0 | Audit `dev_appealed` label logic; re-engineer to cover all closed OZ/SA apps | Realistic base rate; calibrated probabilities | **Yes** |
| P0 | Restrict survival model scoring to OZ+SA only | Eliminate out-of-domain predictions | **Yes** |
| P1 | Raise regressor `production_ready` threshold to R² ≥ 0.10 | `permit_issuance_days` correctly flagged | **Yes** |
| P1 | Skip scoring for models where `production_ready: false` | No noise columns in output | **Yes** |
| P1 | Delete stale `dev_applications_approved.joblib` from models/ | Fix `importance-all` | **Yes** |
| P1 | Close "ward demographics" investigation — keep features | Simplify backlog | **Yes** |
| P2 | Add temporal recency weighting to survival model | Better predictions for post-2022 applications | **Yes — after P0/P1** |
| Hold | Retire COA model training | Keep for metric tracking | **No — keep training, skip scoring** |

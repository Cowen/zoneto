# Model Critique — 2026-03-15 (Ward Profiles Run)

**Role:** Data scientist / Toronto real estate developer
**Pipeline run:** 2026-03-15 — post ward-profiles enrichment, CRS fix, work_type COA feature

---

## Current Model Metrics

| Model | N | Primary metric | Secondary |
|---|---|---|---|
| `dev_applications_approved` | 2,515 | ROC-AUC **0.691 ±0.177** | Brier 0.024 |
| `dev_applications_appealed` | 3,625 | ROC-AUC **0.868 ±0.052** | Brier 0.174 |
| `coa_approved` | 4,609 | ROC-AUC **0.534 ±0.229** | Brier 0.068 |
| `coa_days_to_approval` | 4,350 | R² **−0.552 ±1.136** | MAE 85d |
| `permit_issuance_days` | 133,006 | R² **0.008 ±0.245** | MAE 57d |

---

## What Changed Since the Last Critique

The following code changes landed since 2026-03-15:

- **EPSG:2952 CRS fix** applied in `_spatial_join_dev` — spatial features
  (`zoning_class`, `secondary_plan_name`, `in_heritage_register`, `in_heritage_district`)
  should now be populated for dev applications
- **Ward profiles** (`ward_pct_renters`, `ward_median_income`, `ward_pop_density`,
  `ward_pct_detached`) added to `DEV_NUM_COLS` and `COA_NUM_COLS`
- **`work_type`** added to `COA_CAT_COLS`
- **`application_year`** added to `PERMIT_NUM_COLS`
- **`KFold` replaced `TimeSeriesSplit`** for COA cross-validation
- **`commercial` → `mercantile`** in permit feature columns
- **StratifiedKFold cap** applied to prevent fold warnings on small minority classes

---

## Issue 1: The CRS Fix Did Not Help Dev_Approved — And May Have Hurt It

The CRS fix was the single highest-priority item from the prior spec. Now that
EPSG:2952 is correctly applied, spatial features should contribute real signal.
Yet dev_applications_approved **declined** from 0.710 to 0.691 (and the prior
run showed 0.596 — variance makes trend-reading unreliable here).

Two explanations:

**A) Spatial features are still mostly null.** If the zoning join succeeds for
some addresses but not others (e.g., permits with lat/lon outside the zoning
polygon coverage area), the model gets a partially-populated categorical column
with many `__missing__` values. OrdinalEncoder with `__missing__` fill adds noise
rather than signal when the missingness is structured (properties outside the
polygon coverage are systematically different from those inside it).

**B) 69 refused applications make the AUC unstable.** A ±0.177 standard deviation
on AUC means the true signal cannot be measured with this class balance. The small
improvement or decline run-to-run is statistical noise, not model improvement.
Adding real spatial features cannot be evaluated until the class imbalance is fixed.

**Bottom line:** I cannot tell whether the CRS fix helped. The class imbalance
(97.3% approved) swamps any signal from spatial features.

---

## Issue 2: Ward Profiles Add Noise, Not Signal, to COA

Ward demographics (`ward_pct_renters`, `ward_median_income`, `ward_pop_density`,
`ward_pct_detached`) were added to both COA and DEV feature sets. The hypothesis
is that ward-level socioeconomic context predicts application outcomes.

This is plausible in theory but wrong in practice for these models:

- **`ward_number` is already in `COA_CAT_COLS`.** The model already learns
  per-ward approval patterns from the ward identifier. Ward demographics are a
  continuous approximation of the same information. A tree model will prefer
  the explicit ward category and partially ignore the continuous proxies.
- **COA panels decide on individual site conditions**, not ward demographics.
  The relevant signal is the specific application type × zoning interaction at
  the parcel level, not whether the surrounding neighbourhood is 60% renters.
- **COA metrics didn't improve.** AUC remains 0.534 ±0.229 — exactly where it
  was before. If ward profiles were adding signal, we'd see at least a directional
  improvement.

The ward profiles dataset is genuinely interesting for a different product
(ward-level trend analysis, development pressure mapping) but it is not the
right feature for predicting individual application outcomes.

---

## Issue 3: The ±0.229 Variance on COA Approval is a Disqualifying Red Flag

AUC 0.534 ±0.229 means the model is performing at **0.305 AUC in the worst
cross-validation fold.** That is substantially worse than random (0.5). This
is not a model quality problem — it is a training data pathology.

The root cause was identified in the prior spec: only 2022–2023 closed
applications were fetched. The survivorship bias means early-year applications
in the dataset are specifically the contested ones that dragged on for years.
Cross-validation folds trained on these systematically mispredict folds
containing recent, typical applications.

The KFold switch from TimeSeriesSplit doesn't fix this — it just redistributes
the bias across folds rather than isolating it. **The fix is the data sync**
(P1 from the prior spec), which has not been executed.

No amount of feature engineering resolves a fundamental training data problem.

---

## Issue 4: COA Days R² Standard Deviation of ±1.136 Is Pathological

The standard deviation of R² **exceeds the absolute value of the mean**
(−0.552 ±1.136). This means at least one CV fold produces R² catastrophically
below −1, i.e., predictions are several times worse than the naive mean
in some folds. This indicates the fold selection is encountering the
survivorship-biased old applications as test data and the model, trained on
recent data in that fold, has learned the wrong relationship entirely.

A model with R² standard deviation larger than 1 should not be reported as a
performance metric at all. It signals that the model is not generalizing — it is
memorizing fold-specific artifacts.

---

## Issue 5: `application_year` Made the Permit Model Worse

R² for `permit_issuance_days` declined from 0.042 to 0.008 after adding
`application_year` as a feature. This is the opposite of the expected outcome.

Why it happened: `application_year` is highly correlated with processing time
in the training data (2020 COVID disruptions, 2021–2022 backlog, 2023 recovery).
When `TimeSeriesSplit` trains on years 2020–2021 and tests on 2022+, the model
has learned that high `application_year` → long processing time. In the test
folds, high `application_year` predicts _shorter_ times (backlog cleared).
The feature reversed sign across the temporal split.

This is the same pathology that killed `hearing_month` in the COA model: a
feature that captures real historical correlation fails in temporal holdout
because the relationship is driven by a time-varying confounder (queue depth,
staffing, policy).

**`application_year` should be reverted from `PERMIT_NUM_COLS`.** It makes the
model less stable, not more informative.

---

## Issue 6: StratifiedKFold Cap Is Partially Ineffective

The training output still shows:

```
UndefinedMetricWarning: Only one class is present in y_true. ROC AUC score is not defined in that case.
```

This fires for `dev_applications_approved`, confirming that some CV test folds
still contain only one class (all approved). Capping the number of splits
prevents the `StratifiedKFold` initialisation from crashing, but does not
guarantee that small minority-class instances are distributed across test folds.

With 69 refused applications out of 2,515 rows, even `cv=5` will sometimes
produce test folds with zero refusals if they cluster chronologically. The
warning is not an artifact — it reflects genuine data scarcity. The fix is
not a technical workaround; it is accepting that this model cannot be evaluated
reliably and retiring it from the product.

---

## Issue 7: KFold for COA Introduces Future Data Leakage

Switching from `TimeSeriesSplit` to `KFold` for COA evaluation was done to avoid
fold failures caused by the survivorship-biased data distribution. But KFold
randomly mixes past and future applications across train and test folds. A model
trained on 2022 applications can predict 2018 applications in the same fold — the
opposite of real deployment conditions.

The correct fix was to address the data bias (P1 sync), not to change the
evaluation methodology in a way that hides the temporal structure. The reported
AUC 0.534 ±0.229 is not even optimistically useful — but with proper temporal
holdout it would likely be lower.

---

## What Is Usable Today

**`dev_applications_appealed` (AUC 0.868 ±0.052):** Still the only model I
would act on. AUC improved slightly from 0.843 to 0.868 with lower variance
(±0.052 vs ±0.060). For a developer monitoring which applications are likely
to attract TLAB opposition, this provides real value. The Brier score of 0.174
is acceptable.

**Everything else:** Not ready for any product surface.

---

## What Would Make Me Pay

1. **Run `zoneto sync --source coa`** to get the full historical COA dataset.
   This was the P1 fix from two specs ago. Until it is done, all COA metrics
   are meaningless and no COA feature engineering is interpretable.

2. **Revert `application_year` from `PERMIT_NUM_COLS`.** It made R² worse by
   0.034. Remove it and re-evaluate.

3. **Check actual zoning join hit rate after CRS fix.** How many dev applications
   now have a non-null `zoning_class`? If the fix worked, this should be > 90%.
   If it is still near 0%, the zoning polygon coverage is still incomplete.

4. **Retire `dev_applications_approved` from the product.** The UndefinedMetricWarning
   and ±0.177 variance confirm the model cannot be reliably evaluated. It is a
   technical liability, not a product feature.

5. **Remove ward profile features from COA `NUM_COLS`.** They add computation,
   increase feature dimensionality, and show zero measurable benefit. Keep them
   in DEV features only, where the demographic context is more plausible as an
   explanatory variable.

---

## High-ROI Improvements (Ranked)

| # | Improvement | Expected Impact | Effort |
|---|-------------|-----------------|--------|
| 1 | Run `zoneto sync --source coa` (data-only) | Fix selection bias; enable COA eval | Data only |
| 2 | Revert `application_year` from PERMIT_NUM_COLS | Recover R² ~0.042 | Trivial |
| 3 | Remove ward profiles from COA_NUM_COLS | Reduce noise, simplify model | Trivial |
| 4 | Verify zoning join hit rate post-CRS fix | Confirm fix worked or diagnose residual | Diagnostic |
| 5 | Retire `dev_applications_approved` from scoring | Remove misleading output | Small |

---

## Product Manager Review

*Added 2026-03-15 — critique of the data scientist feedback above.*

### What Is Validated

**COA data sync is still the blocking dependency (Item #1).** Every critique
session identifies this as the root cause of COA model failure. Every session
ends without it being executed. This is not a technical problem — it is a
prioritisation failure. The pipeline has the code to fetch all COA years (P1
was marked "code complete" in a prior spec). Running `uv run zoneto sync
--source coa` is a five-minute operation that unlocks three models. It should
be done before any other change is made. **Approve immediately; no code changes
required.**

**Revert `application_year` (Item #2).** The R² regression from 0.042 to 0.008
is real and the temporal confounding mechanism is clearly explained. The prior
PM review approved adding this feature "as a diagnostic" — the diagnostic result
is negative. Reverting is the correct response to a diagnostic that fails.
**Approve.**

**Remove ward profiles from COA_NUM_COLS (Item #3).** The spec correctly argues
that `ward_number` already captures per-ward patterns as a categorical feature.
The continuous ward demographics are a weaker proxy of the same geography and
showed no measurable improvement. The COA decision process (individual variance
hearings by panels) is not well explained by neighbourhood demographics.
However, ward profiles in DEV_NUM_COLS are a reasonable experiment — the
development application process involves policy review at a planning district
level where demographic context could matter. Keep ward profiles in DEV features;
remove only from COA. **Approve for COA; hold the DEV judgment until post-CRS
baseline.**

**Zoning join hit rate diagnostic (Item #4).** This should have been done
immediately after the CRS fix was applied. It is a one-line log message or a
row count. If `zoning_class` is still near 100% null, we are building models
that report spatial feature columns with zero signal — wasting model capacity
and giving developers false confidence in "zoning-aware" predictions.
**Approve as an immediate diagnostic step.**

**Retire `dev_applications_approved` from scoring (Item #5).** Previous PM
reviews approved this. The model produces calibrated probabilities that are
not calibrated (AUC 0.691 ±0.177, UndefinedMetricWarning still firing). Serving
it in `score_all` output harms developer trust when predictions are wrong and
provides false confidence when they happen to be right. Retirement means:
do not write `pred_dev_approved` / `prob_dev_approved` columns to scored output.
The model file can remain on disk for research. **Approve.**

### What Needs Revision

**KFold vs TimeSeriesSplit for COA — the spec is right, but the recommended
fix is wrong.** The spec correctly identifies that KFold allows future data
leakage. But reverting to TimeSeriesSplit with the current biased data will
recreate the catastrophic fold failures. The correct sequencing is: (a) sync
COA data first, (b) evaluate whether the resulting distribution supports
TimeSeriesSplit (it should, given ~15,000+ well-distributed rows), (c) revert
to TimeSeriesSplit then. Do not change the evaluation method without the data.

**"Ward profiles are noise for COA" is underspecified.** The claim is directionally
correct but the mechanism needs qualification: ward demographics correlate with
*application volume* and *application type mix*, not with committee decisions.
Ward panels (Etobicoke York vs North York vs Toronto & East York vs Scarborough)
have genuinely different approval rates. But ward demographics don't capture
this — the actual panel is what matters. If `planning_district` (already in
COA_CAT_COLS) is correctly populated, it already encodes the panel. Check
whether `planning_district` has good coverage before concluding that geographic
features are useless for COA.

**Permit model framing:** R² = 0.008 with MAE = 57 days on a process that
averages ~100–200 days is useless for any operational decision. No feature
tweak will fix this without a source that proxies queue depth. The honest product
call is: **retire the permit model from all user-facing output** until an external
signal (monthly permit volume from a rolling window, or a lagged average issuance
time per `permit_type`) is added that captures administrative throughput trends.
The spec does not make this call explicitly; it should.

### Prioritized Action Plan (PM-approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| P0 | Run `uv run zoneto sync --source coa` then `just pipeline` | Establish unbiased COA baseline; may recover R² > 0.5 | **Yes — do this first** |
| P1 | Revert `application_year` from `PERMIT_NUM_COLS` | Recover permit R² ~0.042 | **Yes** |
| P1 | Remove ward profiles from `COA_NUM_COLS` only | Reduce noise in COA features | **Yes** |
| P2 | Log zoning join hit rate after CRS fix; add assertion in `enrich_dev` | Confirm spatial features are populated | **Yes** |
| P2 | Retire `dev_applications_approved` from `score_all` output | Stop serving unreliable predictions | **Yes — already approved in prior review** |
| P3 | Re-evaluate KFold vs TimeSeriesSplit for COA post-sync | Restore temporal holdout | **Yes — but only after P0** |
| Hold | Any new COA feature engineering | Cannot evaluate until post-sync baseline | **No** |
| Hold | Permit model improvements beyond diagnostic | No signal path identified | **No — unless queue-depth proxy found** |

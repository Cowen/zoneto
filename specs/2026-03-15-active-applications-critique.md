# Model Critique — 2026-03-15 (Active Applications + New Features Run)

**Role:** Data scientist / Toronto real estate developer
**Pipeline run:** 2026-03-15 — post active-applications enrichment features (is_active, has_parent_application, postal_fsa)

---

## Current Model Metrics

| Model | N | Primary metric | Secondary |
|---|---|---|---|
| `dev_applications_approved` | 2,515 | ROC-AUC **0.720 ±0.267** | Brier 0.022 |
| `dev_applications_appealed` | 3,625 | ROC-AUC **0.874 ±0.054** | Brier 0.164 |
| `coa_approved` | 4,609 | ROC-AUC **0.534 ±0.229** | Brier 0.068 |
| `coa_days_to_approval` | 4,350 | R² **−0.552 ±1.136** | MAE 85d |
| `permit_issuance_days` | 133,006 | R² **0.008 ±0.245** | MAE 57d |

---

## What Changed Since the Last Critique

Commits since `2026-03-15-ward-profiles-critique.md`:

- **`is_active` flag** added to enriched dev output (identifies pending applications;
  output-only, not a model feature)
- **`has_parent_application`** added to `DEV_NUM_COLS` (1 if `parent_folder_number` is not null)
- **`postal_fsa`** added to `DEV_CAT_COLS` (first 3 chars of postal, e.g. "M5V")
- **StratifiedKFold cap** applied to prevent fold-count warnings

Two PM-approved changes from the previous review were **not implemented**:
- Remove ward profiles from `COA_NUM_COLS` — **not done**
- Revert `application_year` from `PERMIT_NUM_COLS` — **not done**

---

## Issue 1: Dev Approved Variance Exploded — New Features Are Destabilizing the Model

`dev_applications_approved` went from AUC **0.691 ±0.177** to **0.720 ±0.267**.
The mean improved marginally; the standard deviation increased by 51%.

A ±0.267 standard deviation on AUC means:
- Best fold: ~0.987 AUC (near perfect)
- Worst fold: ~0.453 AUC (worse than random)

This is not a model that improved. This is a model that got more erratic. The
addition of `postal_fsa` and `has_parent_application` added capacity to memorize
training data in some folds while failing catastrophically in others.

The model was already untrainable due to class imbalance (69 refusals in 2,515
rows, 97.3% approval rate). Adding high-cardinality categorical features to an
already imbalanced dataset is the opposite of what helps here.

---

## Issue 2: `postal_fsa` Is High-Cardinality Poison for This Dataset Size

There are roughly 300+ unique FSA codes in Toronto (M1B through M9W range). With
2,515 training examples (of which only 69 are in the minority class), the model
has roughly 8 examples per FSA on average — and far fewer for many minority-class
FSAs. OrdinalEncoder assigns ordinal integers to categories it has seen. In
cross-validation, many FSAs appear in only one fold's training set, forcing the
test fold to fall back to the `unknown_value=-1` path.

The result: `postal_fsa` looks like a strong predictor in folds where it's been
trained (memorization of the neighbourhood) and fires the unknown-value path in
others. This is the mechanism generating the ±0.267 variance explosion.

**For a model with 69 minority-class examples, no categorical feature with more
than ~10 unique values is reliably learnable with temporal CV.** Ward and
application type are arguably worth the cardinality. Postal FSA is not.

---

## Issue 3: `has_parent_application` Needs Feature Importance Validation Before Production

The hypothesis is plausible: applications linked to a parent folder may represent
larger, more professionally managed developments and could have different approval
profiles. But there are two failure modes worth checking before trusting this:

**A) Post-hoc labeling:** If parent-linked applications are more likely to have
been batched by experienced planners, they may simply be correlated with the
approved set because experienced applicants tend to file approvable applications.
This is signal, but it's signal about the applicant, not the site.

**B) Data sparsity:** How many of the 69 refused applications have a non-null
`parent_folder_number`? If very few (or very many), the feature may contribute
nothing after balancing — or it may be the primary driver of the ±0.267 variance.

Neither of these is fatal, but before shipping `has_parent_application` as a
product feature, run `feature_importance("dev_applications_approved")` and
confirm it contributes positive permutation importance, not just gain-based
importance (which can be gamed by high-cardinality or correlated features).

---

## Issue 4: The Two PM-Approved Removals Were Skipped

The previous PM review approved two changes as P1:

1. **Remove ward profiles from `COA_NUM_COLS`** — `COA_NUM_COLS` still contains
   `ward_pct_renters`, `ward_median_income`, `ward_pop_density`, `ward_pct_detached`.
   The COA metrics are completely unchanged from before these features were added
   (AUC 0.534 ±0.229 in both runs), consistent with the argument that they add
   noise but not signal. They are still in the model consuming feature capacity.

2. **Revert `application_year` from `PERMIT_NUM_COLS`** — `application_year` is
   still in `PERMIT_NUM_COLS`. The permit model's R² = 0.008 is unchanged from the
   run where this feature was identified as the cause of R² regression (from 0.042
   to 0.008). This feature has been confirmed harmful and was approved for removal.
   It is still there.

Neither removal requires any data changes. Both are one-line edits to `features.py`.
When PM-approved changes pile up unimplemented across critique cycles, the
feedback loop breaks down — the PM's time is wasted if approved actions are not
taken.

---

## Issue 5: COA Sync Is Running — But the Data Structure Is the Problem

Previous critiques accused the sync of not being run. This was wrong. The COA
data was synced today (5,093 rows, 11:32), and the `year_start=2018` configuration
is correctly fetching closed-application CSVs back to 2018.

The actual data tells a different story:

| year_submitted | rows |
|---|---|
| 2014–2020 | 119 (2.5%) |
| 2021 | 1,356 (28.6%) |
| 2022 | 2,773 (58.4%) |
| 2023 | 289 (6.1%) |

87% of records are from 2021–2022. This is not a sync failure — it reflects what
the CKAN CSVs actually contain. The city's bulk CSV exports are sparse before 2021.

More damning: **the class imbalance is 94.4%** (4,350 approved, 259 refused).
This is worse than dev_approved's 97.3% rate. With 259 refusals spread across 4,749
rows in a temporally skewed dataset, no model can be reliably trained or evaluated.

The root cause of AUC 0.534 ±0.229 and R² −0.552 ±1.136 is not missing data — it
is that:
1. The COA almost never refuses applications (94.4% approve)
2. Historical CSVs are sparse, so cross-validation folds are unbalanced
3. The "Active Applications" CSV (no year in filename) is still not fetched,
   because `_fetch_bulk_csv` requires a 4-digit year in the resource name

The active applications CSV would add currently-pending (undecided) applications.
These would all have null outcomes, deepening the imbalance problem further unless
they are explicitly excluded from training.

As a developer: COA approval prediction is not learnable from this dataset
regardless of sync frequency. The signal simply is not there.

---

## What Is Usable Today

**`dev_applications_appealed` (AUC 0.874 ±0.054):** Still the only model worth
considering. The Brier score improved slightly (0.174 → 0.164). This model
tells me which applications are at elevated TLAB opposition risk. For a developer
doing pre-application due diligence — is this rezoning going to attract opposition?
— this is the one metric I would monitor.

**Everything else remains below the bar for any practical use.**

---

## What Would Make Me Pay

1. **Remove `application_year` from `PERMIT_NUM_COLS`** (one line, already PM-approved).
   This adds zero work but recovers ~0.034 R² and removes a known confounder.

2. **Remove ward profiles from `COA_NUM_COLS`** (one line, already PM-approved).
   The COA model is already broken. Stop adding noise to a model with negative R²
   and 0.534 AUC.

3. **Remove `postal_fsa` from `DEV_CAT_COLS`** or replace with a lower-cardinality
   geographic proxy. The variance explosion from 0.177 to 0.267 is a clear signal
   that this feature is not learning anything useful — it is memorizing folds.

4. **Run feature importance on `dev_applications_appealed`** to confirm `postal_fsa`
   and `has_parent_application` contribute positive permutation importance. If they
   don't, remove them. If they do, then `dev_approved`'s variance problem is about
   class imbalance and these features are innocent bystanders.

5. **Retire `dev_applications_approved` from scoring output** (PM-approved since
   at least two cycles ago). The `UndefinedMetricWarning` is still firing. AUC
   variance is still ±0.267. This model should not reach any user surface.

6. **Assess `coa_approved` viability by segment** — check refusal rates for
   consents vs. minor variances, and across the four geographic panels. If any
   segment has > 10% refusal rate and sufficient volume, a sub-model may be
   learnable. If not, retire COA approval prediction entirely.

---

## High-ROI Improvements (Ranked)

| # | Improvement | Expected Impact | Effort |
|---|-------------|-----------------|--------|
| 1 | Remove `application_year` from `PERMIT_NUM_COLS` | Recover R² ~0.042 | 1 line |
| 2 | Remove ward profiles from `COA_NUM_COLS` | Reduce noise | 4 lines |
| 3 | Remove `postal_fsa` from `DEV_CAT_COLS` | Reduce variance in dev_approved | 1 line |
| 4 | Retire `dev_applications_approved` from `score_all` | Stop shipping misinformation | Small code change |
| 5 | Run feature importance on dev_appealed (postal_fsa, has_parent_application) | Confirm new features add signal | Diagnostic |
| 6 | Assess `coa_approved` sub-segmentation viability (consents vs variances, per-panel) | Determine if 94.4% approval rate is learnable in any segment | Analysis |

---

## Product Manager Review

*Added 2026-03-15 — critique of the data scientist feedback above.*

### What Is Validated

**Issue 4: PM-approved changes not implemented.** This is the most important
finding in this critique. Two P1 changes — removing ward profiles from `COA_NUM_COLS`
and reverting `application_year` from `PERMIT_NUM_COLS` — were approved in the
previous review cycle and were not executed. This is a process failure, not a
technical problem. Both changes are confirmed-beneficial, zero-risk, and take
less than five minutes. They should be the first code changes made in any future
development session. **Both approved; implement immediately as prerequisite to
all other work.**

**Issue 1 and Issue 2: `postal_fsa` variance explosion.** The data scientist's
analysis is technically sound. The variance increase from ±0.177 to ±0.267 is
direct evidence of overfitting to fold-specific postal distributions. With 69
minority-class examples and 300+ FSAs, the feature cannot generalize. The risk
is that future critique sessions will spend time on this feature without
acknowledging it is actively harming the model. **Approve removal of `postal_fsa`
from `DEV_CAT_COLS`.** The feature can be revisited if and when the dev_applications
dataset is replenished with live data (and the minority class grows substantially).

**Issue 5: COA sync diagnosis correction.** Prior critique cycles incorrectly
claimed the COA sync was not being run. This was a misdiagnosis: the sync is
running, the data is current (5,093 rows as of today), and `year_start=2018`
is correctly configured. The stagnant metrics reflect the actual data structure,
not a missing sync.

The corrected root causes are: (a) 94.4% class imbalance making the classifier
uninformative, (b) 87% of records concentrated in 2021–2022, and (c) active
applications not being fetched due to the year-in-filename requirement.

This changes the recommended action: **COA approval prediction should be
assessed for viability rather than data acquisition.** With 259 refusals out of
4,749 applications, the signal-to-noise ratio is fundamentally poor. The
appropriate question is whether the city publishes refusal rates that are high
enough in any sub-segment (application type, ward, planner) to make a
sub-model viable. If not, COA approval prediction should be retired as a
product feature, and `coa_days_to_approval` retained as the only COA model.

**PM-approved direction: retire `coa_approved` model; investigate whether a
sub-segmented model (e.g., consents only, or a specific ward panel) achieves
AUC > 0.65 with sufficient minority-class samples.**

**Issue 6: `dev_applications_approved` retirement.** This has been PM-approved in
two prior reviews. The `UndefinedMetricWarning` is still firing; the ±0.267 variance
is worse than before. There is no condition under which continuing to ship this
model output is acceptable. **Approve immediately; no further discussion needed.**

**Issue 3: `has_parent_application` importance validation.** The PM concurs that
shipping features without permutation importance validation is speculative. However,
`has_parent_application` is a more theoretically grounded feature than `postal_fsa`
(application complexity as a proxy for developer experience). The appropriate
action is to run the diagnostic, not to remove the feature pre-emptively.
**Approve diagnostic; hold removal decision pending results.**

### What Needs Revision

**Issue 2 framing on high-cardinality categoricals:** The critique correctly
identifies postal FSA cardinality as a problem but does not acknowledge that
`ward_number` has similar cardinality (~25 wards) and is retained. The distinction
is that ward is explicitly meaningful (it maps to planning policy areas, CoA panels,
planner assignments) whereas postal FSA adds geographic granularity that duplicates
what `zoning_class` and `ward_number` already encode. The critique is right on the
conclusion but the reasoning should distinguish "useful cardinality" from
"redundant cardinality" rather than applying a blanket 10-category rule.

**Permit model framing:** The critique notes that `application_year` was approved
for removal and wasn't done. This is correct and the fix is simple. However, the
critique does not revisit the prior PM direction to retire the permit model from
user-facing output until a queue-depth proxy is added. R² = 0.008 is still the
same. The permit model is still being trained and scored. The PM-approved hold
on permit model improvements has not been acted on either. The permit model
situation — training a model we know is wrong, scoring it, writing predictions to
Parquet — is worse than not having a permit model at all, because it creates the
appearance of a working feature while delivering predictions that carry no
information.

### Prioritized Action Plan (PM-approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| **P0-immediate** | Remove `application_year` from `PERMIT_NUM_COLS` | Recover permit R² ~0.042 | **Yes — overdue** |
| **P0-immediate** | Remove ward profiles from `COA_NUM_COLS` | Reduce COA feature noise | **Yes — overdue** |
| **P0-immediate** | Remove `postal_fsa` from `DEV_CAT_COLS` | Reduce dev_approved variance | **Yes** |
| **P0-immediate** | Retire `dev_applications_approved` from `score_all` | Stop serving misleading output | **Yes — overdue × 2** |
| **P1** | Run `feature_importance` on `dev_applications_appealed` | Validate has_parent_application and ward profiles in DEV | **Yes — diagnostic only** |
| **P1** | Assess `coa_approved` viability by segment (consent vs variance, per-panel) | Determine if minority class is learnable anywhere | **Yes** |
| **P2** | Retire `coa_approved` model if no viable sub-segment found | Stop training a 94.4%-baseline classifier | **Yes — pending P1 assessment** |
| **P2** | Re-evaluate permit model: add queue-depth proxy feature or retire it | Either R² > 0.15 or model removed | **Yes — queue-depth path approved; retire if not found** |
| **Hold** | Any new COA feature engineering | Class imbalance is the blocker, not features | **No** |
| **Hold** | Any new dev_applications features | Cannot evaluate while variance ±0.267 | **No — blocked on removing postal_fsa and re-baselining** |

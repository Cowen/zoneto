# Model Critique — 2026-03-15 (After P0/P2 Code Fixes, Pre-P1 Data Sync)

As a data scientist and Toronto real estate developer, this is my critique of the
models after the P0 (revert `hearing_month`), P1 (COA dedup code), and P2 (zoning
URL fix) code changes were applied and the pipeline was re-run. The COA data has
**not** been re-synced from CKAN (the data-layer part of P1 is pending).

---

## Current Model Metrics (2026-03-15)

| Model | N | Key metric | Secondary |
|---|---|---|---|
| `dev_applications_approved` | 2,515 | ROC-AUC **0.710 ±0.178** | Brier 0.024 |
| `dev_applications_appealed` | 3,625 | ROC-AUC **0.843 ±0.060** | Brier 0.177 |
| `coa_approved` | 4,609 | ROC-AUC **0.496 ±0.094** | Brier 0.035 |
| `coa_days_to_approval` | 4,350 | R² **−0.776 ±0.934** | MAE 64d |
| `permit_issuance_days` | 133,006 | R² **0.042 ±0.207** | MAE 49d |

---

## What Changed Since the Last Critique

### P0: Reverting `hearing_month` — Partial Improvement

`coa_days_to_approval` R² improved from −4.12 to −0.776. This confirms that
`hearing_month` was the primary driver of the catastrophic regression, not the
underlying data quality alone. The regression model is no longer terminally broken
by a spurious feature, but −0.776 still means predictions are substantially worse
than predicting the mean. The selection bias in the COA training data (only 2022–2023
closed applications) is the remaining root cause.

`coa_approved` barely budged: 0.500 → 0.496. The selection bias dominates here too.
Reverting `hearing_month` was the right call; it confirmed that the COA model quality
is fundamentally a data problem, not a feature engineering problem.

### P2: Zoning GeoJSON Downloaded — But Still 100% Null

The correct full-city `zoning-area-4326.geojson` (50MB) was downloaded during this
pipeline run. Despite this, `zoning_class` is **still null for all 26,161 dev
application rows**. The training logs show 50+ repetitions of:

> "Skipping features without any observed values: `zoning_class`, `secondary_plan_name`"

This rules out the previous hypothesis (wrong resource URL) as the sole cause.

**Root cause identified:** The `_spatial_join_dev` function reprojects dev application
coordinates from **EPSG:26917** (UTM Zone 17N) to WGS84 before the spatial join.
But the actual coordinates are in **EPSG:2952** (NAD83 / MTM Zone 10, Ontario):

| Statistic | x (easting) | y (northing) |
|---|---|---|
| Minimum | 293,785 | 318,239 |
| Median | 313,123 | ~4,835,000 |
| Maximum | 647,028 | 4,855,741 |
| Values < 400k | 25,344 (99.8%) | — |
| Values ≥ 500k | 43 (0.2%) | — |

In UTM Zone 17N (EPSG:26917), Toronto eastings are ~625,000–645,000. The observed
median of 313,123 is consistent with EPSG:2952 (MTM Zone 10 false easting: 304,800),
where Toronto parcels fall in the 302,000–326,000 range.

When x=313,123 is treated as UTM Zone 17N easting:
- Actual position: ~−83.3° longitude (Michigan), 43.6° latitude
- No Toronto zoning polygon matches Michigan coordinates

When correctly interpreted as MTM Zone 10:
- Actual position: ~−79.4° longitude, 43.7° latitude — downtown Toronto

**The spatial features (`zoning_class`, `secondary_plan_name`, `in_heritage_register`,
`in_heritage_district`) have been contributing zero signal to every dev application
model ever trained by this pipeline.** The fix is to change the pyproj transformer
source CRS from `EPSG:26917` to `EPSG:2952` in `_spatial_join_dev`.

---

## Why I Would Not Pay For These Results Today

### What is Usable

**`dev_applications_appealed` (AUC 0.843 ±0.060):** This is the only production-ready
model. The variance is acceptable (worst-fold AUC ~0.78), Brier score of 0.177 is
reasonable for a rare outcome. If I'm a developer deciding which applications to
monitor for community opposition and TLAB risk, this model provides real value.

### What is Not Usable

**`coa_approved` (AUC 0.496):** Literally worse than a coin flip. If I'm scheduling
a Committee of Adjustment hearing and want to know my odds, this model cannot tell me
anything. I would not use it, even to pre-screen applications.

**`coa_days_to_approval` (R² −0.776):** Predictions are worse than the naive mean.
For construction financing purposes (where I need to know when I can break ground),
a model with negative R² actively misleads. MAE of 64 days on a process that
averages ~130–200 days is too imprecise to inform any business decision.

**`permit_issuance_days` (R² 0.042):** 133,000 training rows and essentially zero
predictive power. The permit office queue depth is the real driver and it's nowhere
in the data. This model should not be surfaced to any user.

**`dev_applications_approved` (AUC 0.710 ±0.178):** The ±0.178 variance is disqualifying.
In a bad cross-validation fold this is AUC 0.532 (barely above random). With only 69
refused applications out of 2,515 labeled rows, the model is trained on an extreme
class imbalance and cannot be trusted for any specific prediction. The underlying
dataset is also retired (no new applications), so even fixing this model doesn't
create ongoing value.

---

## New Issues Not in Previous Specs

### 1. Coordinate System Mismatch (Newly Confirmed)

Previous specs speculated about a CRS mismatch; today's data confirms it. The fix
is well-defined (change EPSG:26917 → EPSG:2952 in the pyproj transformer). This
should unlock `zoning_class`, `secondary_plan_name`, `in_heritage_register`, and
`in_heritage_district` as real features in the dev application models. Given that
AUC 0.710 is achieved with zero spatial signal, these features may substantially
improve the model.

### 2. COA Data Sync Still Pending

The P1 data sync (`uv run zoneto sync --source coa`) has not been run. The pipeline
is still trained on 2022–2023 closed applications with survivorship bias. Until
re-synced with full historical data, the COA metrics cannot be meaningfully evaluated.

### 3. The 43 Dev Application Rows With x ≥ 500k

A small minority (43 rows, 0.17%) have x coordinates that look like UTM Zone 17N
values. These are likely bad data or entries from a different source. They should be
filtered or flagged rather than reprojected as if they were MTM coordinates.

---

## What Would Make Me Pay

1. **Fix the CRS bug** and re-run dev application models with real spatial features.
   If the dev_appealed AUC holds or improves above 0.85 with actual zoning data, that's
   a material upgrade. If zoning class is a strong predictor of appeal risk (residential
   vs commercial zones, heritage overlay), this model becomes significantly more useful.

2. **Re-sync COA data** (15,000+ rows across all years) and evaluate whether the
   COA approval model reaches AUC > 0.70. A working COA model would be directly
   monetizable for planning consultants advising on variance applications.

3. **For permit issuance:** Either find a data source that proxies queue depth (e.g.,
   monthly permit application volume as a derived feature) or explicitly retire this
   model from the product. A model with R² = 0.042 destroys credibility if surfaced.

---

## High-ROI Improvements (Ranked)

| # | Improvement | Expected Impact | Effort |
|---|---|---|---|
| 1 | Fix CRS: EPSG:26917 → EPSG:2952 in `_spatial_join_dev` | Enable spatial features for dev models | Small |
| 2 | Re-sync COA data (run `zoneto sync --source coa`) | Fix selection bias; enable COA model eval | Data only |
| 3 | Re-run `just pipeline` after #1 and #2 | Establish true baseline with real features | Operations |
| 4 | Add `year` to PERMIT_NUM_COLS; derive from `application_date` | Capture temporal queue-depth signal | Small |
| 5 | Retire dev_applications_approved from product | Avoid misleading users with unstable model | Policy |

---

## Product Manager Review

*Added 2026-03-15 — critique of the user feedback above from a product perspective.*

### What is Validated

**CRS mismatch root cause (Item #1):** The coordinate analysis is decisive. x=313,123
being the median with a 304,800 MTM false easting leaves no ambiguity: these are MTM
Zone 10 (EPSG:2952) coordinates. The previous spec's CRS diagnosis was directionally
correct ("coordinate mismatch") but identified the wrong side of the mismatch (it
blamed the zoning polygons' CRS, not the dev application input coordinates). The fix
is a one-line change in the pyproj transformer; no zoning data changes needed. This
is the highest-leverage fix in the codebase and has been invisible across every prior
pipeline run. **Approve immediately.**

**COA data sync (Item #2):** Still the critical dependency for the COA models.
The code fixes from P0/P1 are in place; only the data is missing. This is a pure
operations step with no code risk. **Approve — run the sync before the next pipeline.**

**Pipeline re-run after fixes (Item #3):** Obviously correct. Cannot evaluate model
quality until both the CRS fix and COA sync are applied. The current metrics are a
pre-fix baseline, not a product milestone. **Approve.**

**Permit year feature (Item #4):** R² = 0.042 with 133,000 rows signals that the
current feature set explains almost nothing. Adding `application_year` as a numeric
feature is low-effort and directly addresses the queue-depth hypothesis: permit office
throughput changed materially between 2020 (COVID slowdowns) and 2022–2023 (recovery).
If `year` is a strong predictor, it validates the queue-depth theory and the R² should
jump. **Approve as a diagnostic experiment; do not add more features until year signal
is quantified.**

**Retire dev_applications_approved (Item #5):** The dataset is retired (city confirmed
no new records), the class imbalance is extreme (97.3% approved), and the model
variance (±0.178) is too high for any reliable prediction. Continuing to train and
serve this model wastes compute and risks misleading users. **Approve — deprecate
from scoring output until a live replacement dataset is identified.**

### What Needs Revision

**The 43 high-x rows need investigation, not just filtering.** Before filtering
them as "bad data," check whether they correspond to specific application types or
geographic areas. If they're legitimate applications recorded in a different coordinate
format (e.g., geocoded by a different City system), blindly filtering them would
silently drop valid records. The right fix is to detect both CRS variants at read time
and handle both. However, 43 rows out of 26,161 is 0.17% — low enough that a
comment noting the anomaly is sufficient for now. Filtering is acceptable short-term.
**Accept the filter; add a code comment explaining the anomaly.**

**MAE 64d for COA regression is not as bad as presented.** If the COA model is
retrained on non-biased data (post-sync, post-dedup) and R² recovers to ~0.7, then
a MAE of 50–70 days on a 100–200 day process is a reasonable planning estimate
(within one hearing cycle). The critique correctly identifies that the current MAE
is unusable, but the threshold for "usable" is not AUC or R² alone — it's whether
the confidence interval is narrow enough to inform financing decisions. **Revise the
acceptance criteria: R² > 0.5 AND MAE < 60 days would be acceptable for a draft
planning timeline estimate.**

### Prioritized Action Plan (PM-approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| P0 | Fix CRS: `EPSG:26917` → `EPSG:2952` in `_spatial_join_dev` | Enable spatial features in dev models | **Yes** |
| P1 | Run `uv run zoneto sync --source coa` then `just pipeline` | Establish unbiased COA baseline | **Yes** |
| P2 | Add `application_year` to `PERMIT_NUM_COLS`; derive in `enrich_permits` | Test queue-depth hypothesis | **Yes — diagnostic** |
| P3 | Retire `dev_applications_approved` from `score` output | Remove misleading model from product | **Yes** |
| Hold | Any further COA feature engineering | Cannot evaluate until post-sync baseline | **No — hold** |
| Hold | Any permit model features beyond `year` | Establish year-signal baseline first | **No — hold** |

# Model Critique and Improvement Plan — 2026-03-14 (Post-Improvement Run)

As a data scientist and Toronto real estate developer, this is my critique of the
models **after** the improvements proposed in the earlier today's spec were
implemented and the pipeline was re-run. The situation is materially different from
the pre-improvement state.

---

## Pipeline Status

Two bugs were fixed during this run:

1. **COA hearing_date parse failure**: `hearing_date` is a string (`'Nov 12, 2015'`
   format) but the code called `.dt.month()` directly. Fixed by parsing with
   `str.to_date("%b %d, %Y", strict=False)` first.
2. **Permit numeric columns as strings**: `est_const_cost`, `dwelling_units_created`,
   `dwelling_units_lost` are stored as strings with comma thousands separators
   (`'15,000'`). Fixed in `enrich_permits` by stripping commas before casting to Float64.
3. **`commercial` not in dataset**: `PERMIT_NUM_COLS` included `commercial` but the
   actual column is absent from `permits_cleared` (it uses `business_and_personal_services`
   and `mercantile`). Fixed by removing `commercial` from `PERMIT_NUM_COLS`.

---

## Current Model Metrics (2026-03-14, Post-Improvement)

| Model | N | Key metric | Value |
|---|---|---|---|
| `dev_applications_approved` | 2,515 | ROC-AUC | 0.613 ±0.149 |
| `dev_applications_appealed` | 3,625 | ROC-AUC | 0.847 ±0.050 |
| `coa_approved` | 4,609 | ROC-AUC | 0.500 ±0.041 |
| `coa_days_to_approval` | 4,350 | R² / MAE | -4.12 / 101 days |
| `permit_issuance_days` | 133,006 | R² / MAE | 0.042 / 49 days |

**This is a significant regression from the 2026-03-04 baseline.** Four of five models
are now materially worse. Only `dev_applications_appealed` remains usable (AUC 0.847).

---

## What Went Wrong

### 1. COA Dataset Has Catastrophic Selection Bias (Root Cause of Regression)

The pipeline fetches only "Closed Applications 2022" and "Closed Applications 2023"
CSVs from the Toronto COA dataset. These contain applications that were *closed in
those years*, regardless of when they were filed. The year-submitted distribution
reveals the problem:

| Year submitted | N | Mean days to approval |
|---|---|---|
| 2014 | 1 | 2,992 |
| 2015 | 10 | 2,375 |
| 2016 | 5 | 2,077 |
| 2017 | 21 | 1,793 |
| 2019 | 59 | 1,031 |
| 2021 | 1,385 | 224 |
| 2022 | 2,993 | 130 |
| 2023 | 384 | 87 |

A 2014 application in the 2022 closed CSV took ~2,992 days — nearly 8 years. This
is **survivorship bias**: the only 2014 applications that appear here are the ones
that dragged on long enough to still be open in 2022. Typical 2014 COA applications
that resolved normally (in 100–200 days) are not in the dataset at all.

When `TimeSeriesSplit` trains on early-year rows (2,000+ day processing times) and
tests on recent rows (100-200 day processing times), the model predicts catastrophically
wrong values. **This is why `coa_days_to_approval` R² = -4.12** (predictions are
5× worse than predicting the mean). This is not a model quality problem — it is a
fundamental training data problem.

The same bias contaminates `coa_approved`: old applications in the dataset are
specifically the contested or complex ones (which is why they took years). They may
have systematically different approval rates than representative applications from
those years.

### 2. `hearing_month` Made Both COA Models Worse

The previous spec proposed adding `hearing_month` as a feature. This was implemented
and degraded `coa_approved` from AUC 0.695 → 0.500 (random) and `coa_days_to_approval`
from R² 0.786 → -4.12.

The mechanism: `hearing_month` is correlated with approval in training folds (older
years with particular seasonal hearing patterns) but this correlation reverses or
disappears in test folds (recent years with different seasonal mix). The model learned
a spurious correlation. For the regression, `hearing_month` interacts badly with the
selection bias — old applications heard in certain months happened to have extreme
processing times, but the model cannot generalize this to test folds.

**Assessment: `hearing_month` should be reverted until the selection bias is fixed.**
A feature that reliably degrades models is worse than no feature.

### 3. Spatial Join for Dev Applications Produces Zero Signal

`zoning_class` is null for 26,139 of 26,161 dev applications (99.9%). Despite valid
WGS84 coordinates (25,386 rows with parseable coords), the `ST_Within(point, polygon)`
join against the zoning GeoJSON matches almost nothing.

**Likely cause:** The zoning CSV from the CKAN datastore exports geometry in the city's
internal CRS (likely EPSG:2952 or similar), not WGS84. The code reprojects dev
application coordinates to WGS84 but compares against zoning polygons that are stored
in a different CRS. A point at (-79.4°, 43.7°) is outside any Toronto polygon stored
in UTM or provincial coordinate systems.

Result: `zoning_class` and `secondary_plan_name` (listed as key model features) have
been providing **zero signal** in all prior runs. The spatial enrichment warnings during
training ("Skipping features without any observed values: zoning_class, secondary_plan_name")
confirm this.

### 4. Permit Issuance Model Has Essentially No Predictive Power

R² = 0.042 with 133,006 training rows is essentially zero. A naive model predicting
the mean would do almost as well. With 133k rows, a proper model should explain
substantially more variance.

**Why it fails:** The available features (`permit_type`, `structure_type`, `ward_grid`,
`est_const_cost`, `dwelling_units_created/lost`, use flags) do not capture the primary
driver of permit issuance time: **City of Toronto permit office queue depths and review
staffing**. A permit for a complex high-rise might be issued in 3 months during a slow
period, or take 18 months during a backlog. The City's processing capacity is not
captured in any available open dataset.

Secondary missing features: renovation complexity (available partially via `description`),
whether the permit required plan review vs. standard review, professional certification.

### 5. Dev Approved: Only 2,515 Usable Labels

18,175 of 26,161 dev_application rows have status "Closed" — excluded as ambiguous.
This leaves only 2,515 rows with clear approved/refused labels (2,446 approved, 69 refused).

The AUC = 0.613 ±0.149 is not reliable. The `UndefinedMetricWarning: Only one class
present in y_true` triggered during training, meaning some TimeSeriesSplit folds
contain only one class. This happens when the 69 refused applications cluster in
certain time periods, leaving test folds without any negatives.

---

## Why I Would Still Not Pay For These Results

The previous spec's improvements were implemented correctly, but they revealed deeper
problems in the data rather than improving outcomes. I am closer to a working product
in understanding but further in delivered metrics.

What I need:
1. A regression model that can actually predict COA timeline (R² > 0.5)
2. A COA approval model I can trust (AUC > 0.70)
3. Some explanatory power in permit timing (R² > 0.3)

I do not yet have any of these.

---

## High-ROI Improvements (Ranked)

| # | Improvement | Expected Impact | Effort |
|---|-------------|-----------------|--------|
| 1 | Fix COA selection bias: fetch all available COA history (2019–2021 CSVs + Active) | Restore R² > 0.7, AUC > 0.70 | Medium |
| 2 | Revert `hearing_month` until selection bias is fixed | Restore AUC 0.695 baseline immediately | Trivial |
| 3 | Fix spatial join CRS mismatch for dev applications | Enable zoning_class feature to contribute | Medium |
| 4 | Investigate permit model — add `description` text signal or temporal lag features | R² > 0.2 | Medium |

---

## Implementation Plan

### 1. Fix COA Temporal Coverage (Highest Priority)

**Root issue:** Only 2022 and 2023 closed application CSVs are fetched. Must fetch all
available years to remove selection bias.

**Files:** `sources/ckan.py`, `sources/registry.py`

Current behavior: `bulk_csv` mode skips resources not matching a year pattern, and
the COA dataset has "Closed Applications 2019", "Closed Applications 2020",
"Closed Applications 2021" CSVs that are not being fetched.

**Changes:**
- Audit the COA CKAN dataset resource list to find all available closed-application CSV years.
- Update the `bulk_csv` mode year filter in `CKANSource` to include years 2019+ (not just 2022–2023).
- Alternatively, update the `year_start` filter in the registry to `2019` and ensure
  the `bulk_csv` resource discovery includes CSVs from 2019 onward.
- This should increase COA training data to ~15,000+ rows and fix the survivorship bias
  by including applications that resolved quickly in each year.

**Expected outcome:** coa_days_to_approval R² should recover to ~0.7, coa_approved
AUC should recover to ~0.70.

### 2. Revert `hearing_month` (Trivial, Immediate)

**Files:** `analytics/features.py`, `analytics/enrich.py`

- Remove `"hearing_month"` from `COA_NUM_COLS`.
- Remove the `hearing_month` column creation from `enrich_coa`.
- This restores the pre-improvement feature set (without the degrading feature).
- After fixing the selection bias (#1), `hearing_month` can be re-evaluated.

**Expected outcome:** coa_approved AUC returns to ~0.695 baseline.

### 3. Fix Zoning Spatial Join CRS Mismatch

**Files:** `analytics/enrich.py`

The zoning CSV geometry column is almost certainly not in WGS84. Investigate:
- Read the first zoning polygon geometry and check its coordinate ranges.
- If coordinates are large numbers (e.g., 600000, 4800000), the geometry is in
  EPSG:26917 or EPSG:2952 (Toronto's projected CRS).
- If so, either:
  - Reproject zoning polygons to WGS84 using DuckDB's `ST_Transform`, or
  - Reproject dev application points to the zoning CRS instead of WGS84.
- Use the DuckDB spatial function `ST_SRID` / `ST_Transform` if the SRS is embedded,
  or use `pyproj` to manually transform the zoning coordinates before loading.

**Expected outcome:** `zoning_class` populated for most dev applications, adding
genuine spatial signal. Also unlocks `secondary_plan_name`.

### 4. Investigate Permit Issuance Model

**Files:** `analytics/features.py`, `analytics/enrich.py`

Diagnostic step first:
- Run feature importance (`just importance`) on `permit_issuance_days` to see which
  features are actually contributing signal.
- Check variance explained per feature using built-in importance.

If `est_const_cost` is the primary driver but has comma-encoded values (now fixed),
the model may improve after this run's bug fix. Check new metrics after fix.

If still R² < 0.1, consider:
- Add `year` (application year) as numeric feature — permit office backlogs are strongly
  correlated with year (COVID disruptions, policy changes, staffing changes). This is
  the most likely high-signal addition.
- Add `application_month` (month of year) as a cyclical numeric feature — seasonal
  patterns in permit application volumes affect queue depth.

---

## Product Manager Review

*Added 2026-03-14 — critique of the spec above from a product/business perspective.*

### What is Validated

**COA selection bias (Item #1 — highest priority):** This analysis is correct and
well-reasoned. The survivorship bias mechanism is real and explains the catastrophic
metrics. Research confirms that Toronto's COA CKAN dataset has closed-application CSVs
available from **2001 through 2023** (not just 2022–2023), plus a convenient consolidated
file "Closed Applications since 2017.csv" (32,410 records). Fetching 2019–2021 at
minimum (or the since-2017 consolidated file for a quick win) is straightforward,
low-risk, and has the highest expected impact. **Approve.**

**Revert `hearing_month` (Item #2):** Confirmed. The feature unambiguously made both
COA models worse. Reverting is trivial and should be done immediately. The spec is
correct that it can be reconsidered after the selection bias is fixed, because hearing
month is legitimately informative (CoA panels have seasonal scheduling). **Approve.**

**Spatial join investigation (Item #3):** The spec's proposed fix is **factually
incorrect**. Research confirms the Toronto zoning GeoJSON exports ARE in WGS84
(EPSG:4326), not in a different CRS as the spec speculates. The actual cause of the
99.9% null `zoning_class` is that the CKAN datastore resource being downloaded
(ID `76a2620f-...`) is an incomplete subset — 11,719 features — likely a specific
table within the zoning bylaw database (perhaps only special zones or policy areas),
not full parcel coverage. Testing confirms that even a known Toronto address (City Hall)
is outside all polygons in this dataset. The fix is to use the correct CKAN resource:
the "Zoning Area - 4326.geojson" download, which provides full zoning polygon
coverage for all Toronto parcels. This is a medium-effort fix with high expected
impact once done. **Approve the fix, correct the CRS diagnosis.**

**Permit issuance model investigation (Item #4):** The R² = 0.042 finding is real
and the diagnosis is plausible — administrative queue depth isn't captured in permit
fields. Adding `year` and `application_month` are low-cost experiments worth doing.
The spec correctly notes that the comma-encoding bug fix (now done) might improve
`est_const_cost` signal. **Approve the diagnostic approach; defer feature additions
pending bug-fix-only results.**

### What Needs Revision or Rejection

**Zoning CRS explanation is wrong.** The spec claims zoning polygons may use
"EPSG:2952 or similar" and suggests using `ST_Transform`. This is incorrect — the
geometry IS WGS84 but the resource is incomplete. The implementation plan for Item #3
should be updated to: (a) identify the correct CKAN resource URL for the full zoning
polygon coverage, (b) download the "Zoning Area - 4326.geojson" resource instead of
the datastore dump. No coordinate reprojection changes are needed.

**The business case for dev_applications_approved is understated.** The spec notes only
2,515 labeled rows with high variance (AUC 0.613 ±0.149), which is correct. But the
spec doesn't conclude clearly: this model should be **deprioritized or retired** as
long as the dev_applications dataset is retired (no new records). Fixing the COA and
permit models delivers more value to current decision-making. A separate recommendation
should explicitly say: do not invest in the dev_applications_approved model until a
live replacement dataset is identified.

**permit_issuance_days business framing:** The spec correctly identifies that
administrative queue depth is the missing driver, but doesn't address this at the
product level. For a developer using this model to plan construction financing, an
R² of 0.042 means predictions are essentially useless. The spec should explicitly say:
"this model should not be surfaced to end users or included in product outputs until
R² exceeds 0.3." Until then it is a research artifact, not a product feature.

**Missing: data freshness recommendation.** The pipeline has not been synced
(re-fetched from CKAN) as part of this run. The dev_applications dataset is retired
(no new data). The permits_cleared and permits_active datasets are updated daily but
the last sync date is unknown. The product roadmap should include a data freshness
check and a scheduled sync cadence. This matters for permit predictions since queue
depths change year-to-year.

### Prioritized Action Plan (PM-approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| P0 | Revert `hearing_month` from COA features | Restore AUC ~0.695 immediately | Yes |
| P1 | Fetch COA 2019–2021 CSVs (or since-2017 consolidated) | Fix selection bias; recover R² > 0.5 | Yes |
| P2 | Fix zoning resource URL to full polygon coverage | Enable zoning_class for dev model | Yes — with corrected CRS diagnosis |
| P3 | Feature-importance run on permit model post bug-fix | Confirm R² improvement from est_const_cost fix | Yes |
| Hold | Improve dev_applications_approved model | Dataset is retired; no new data to predict | No — hold until live dataset found |
| Hold | Surface permit_issuance_days in product | R² too low to be useful | No — set R² ≥ 0.3 bar before release |

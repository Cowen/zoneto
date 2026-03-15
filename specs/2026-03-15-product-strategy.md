# Product Strategy — 2026-03-15

**Role:** Product Owner
**Author:** Claude (claude-sonnet-4-6)
**Date:** 2026-03-15

---

## Executive Summary

This pipeline has a structural crisis that incremental bug fixes cannot solve: its
most important dataset is retired, and its best model may have learned from a data
leak. The previous three critique cycles have been productive at the code level but
have not addressed the strategic question: **what is this product actually for, and
who uses it?**

Without answers to those questions, continued ML investment delivers diminishing
returns. This proposal argues for a reorientation of the product goal before the next
phase of development.

---

## Current State Assessment

### What's Working

- **`dev_appealed` model (AUC 0.843±0.060):** The only model with reliable discriminative
  power. Even if it has mild label leakage from `status`, it captures real signal.
  Worth keeping.
- **`coa_days_to_approval` (pending fix):** Once the COA data re-sync is complete and
  the dedup is applied, this is potentially a genuinely useful planning timeline
  estimate (R² > 0.5, MAE < 60 days is achievable).
- **The pipeline itself:** The sync → enrich → train → score architecture is sound,
  well-tested, and maintainable.

### What's Not Working

- **`dev_applications` is retired.** No new records. The dev models predict history.
  Any forward-looking use requires an application to have been filed before the dataset
  cutoff. This is a fatal flaw for a predictive product.
- **`dev_applications_approved` (AUC 0.710±0.178):** 69 refusals in 2,515 rows.
  Extreme imbalance, high variance across folds. The dataset retirement means this will
  never improve. Should be removed.
- **Spatial features have been silent since day one.** The EPSG mismatch meant zoning,
  heritage, and secondary plan features contributed zero signal to every dev model ever
  trained. This is now fixed in code but results haven't been re-validated.
- **Permit issuance (R² 0.042 on 133,000 rows):** 133k rows and near-zero predictive
  power means the features don't explain this outcome. Queue depth (how many permits
  were in the system that day) is missing and is likely the dominant driver.
- **No user surface.** Predictions live in Parquet files. The only consumer is someone
  who can write Python. The `score_one` function exists but nothing calls it.

### The Underlying Strategic Gap

The product has been built as a data pipeline with ML bolted on. There is no defined
user, no usage interface, and no mechanism for predictions to reach anyone who could
act on them. A developer deciding whether to pursue a rezoning application cannot use
this pipeline without Python skills and a local data sync.

---

## What a Real User Needs

Taking the perspective of the named target users (Toronto real estate developers,
planning consultants, community groups):

**Developer pre-application:**
> "I own a parcel at King and Bathurst, zoned CL. I want to convert to mixed-use
> residential. Should I pursue an OZ/SA? What are my chances, and how long will this
> take?"

The honest answer from the current system: "I can't tell you, because the dev
applications dataset is retired and the model variance is too high."

**Planning consultant:**
> "My client has a Committee of Adjustment minor variance hearing in North York next
> month. What are comparable applications and what were the outcomes?"

The honest answer: "I have 4,630 COA records from 2022–2023 only, with poor model
accuracy (AUC 0.496)."

**Community group opposing a development:**
> "An OZ application just appeared on the city's planning website. What's the history
> of similar applications near me and what can we do?"

The honest answer: "This application isn't in our dataset. We can't track it."

In each case, the limiting factor is data coverage and access, not model sophistication.

---

## Large Changes Proposed

### Change 1: Shift Primary Goal from Prediction to Exploration

**Current goal:** "Predict likelihood of approval using ML models."
**Proposed goal:** "Help users understand comparable applications and planning outcomes."

The ML models are a downstream feature of having good data, not the core value. The
core value is: **"Find me 10 applications near mine, with similar attributes, and tell
me what happened to them."**

This shift matters because:
- Comparable applications ("comps") are understandable without ML expertise
- They work even when model quality is low
- They are forward-looking even when the base dataset is retired (as long as the user
  understands the data cutoff)
- They are the mental model that planning professionals already use

**Implementation implication:** Add a `comps` command to the CLI and an endpoint to
the (proposed) API that accepts an application's attributes and returns the N most
similar historical applications with their outcomes, sorted by similarity. Similarity
metric: cosine distance over embeddings of (ward, zoning, application_type) with
spatial proximity as a secondary sort.

### Change 2: Replace the Retired Dev Applications Dataset

The dev_applications dataset is the product's Achilles heel. The fix is not to improve
the model — it's to find live data.

Options (to be researched):
1. **Toronto Planning Activity Portal scraper.** The city's planning application search
   at `app.toronto.ca/DevelopmentApplicationSearch` exposes individual application
   records with full history. Not a formal API but scrapeable.
2. **New CKAN datasets.** The city may have published replacement datasets. Worth
   auditing `open.toronto.ca` for planning datasets published or updated since 2024.
3. **Hybrid:** Keep the retired dataset for historical training, add a live scraper
   for new applications, and stitch them together on `application` number.

Until a live source is integrated, all dev_applications models must be labeled clearly
as "historical only — data through [cutoff date]" in any user-facing output.

### Change 3: Add CoA Active Applications

The pipeline fetches only closed applications from 2022–2023. The city also publishes
an "Active Applications" resource on the same CKAN dataset. Fetching active
applications would:
- More than double the COA dataset size
- Provide current applications that are decision-pending (the most valuable to users)
- Enable tracking how long pending applications have already been waiting

This is a medium-effort data source addition: update the registry to also fetch the
active CSV, extend `enrich_coa` to handle the union, add an `is_active` flag.

### Change 4: Add a Serving Layer

Even a minimal FastAPI application with three endpoints would transform usability:

```
GET  /health
POST /score     {"source": "coa", "features": {...}}  →  {predictions}
GET  /comps     {"ward": "10", "type": "Minor Variance", "lat": 43.6, "lon": -79.4}  →  [{application}]
```

The `score_one` function already exists. The `comps` query is a DuckDB spatial query
against the enriched parquet. A Dockerfile and a `just serve` command in the justfile
would make this runnable.

This does not require a React frontend. A simple HTML page that calls the API via
fetch() is sufficient for a working demo.

### Change 5: Instrument the Permit Queue Depth Problem

The permit model's R² = 0.042 is explained by a missing feature: queue depth. The
number of permit applications in the system at any given time is the primary driver of
issuance wait time. This can be derived from the existing data:

For each permit application, count how many other applications were submitted in the
same 90-day window and in the same geographic area. This is a computable lagged
feature from the `permits_active` + `permits_cleared` data without any new data
sources.

If queue depth derived this way produces R² > 0.20, the permit model becomes usable.
If it doesn't, the permit model should be permanently retired.

---

## What to Stop Doing

### Stop training `dev_applications_approved`

- 97.3% approval rate makes the model uninformative
- Retired dataset means it will never improve
- High variance (±0.178) makes it untrustworthy for any specific application
- Remove from `train_all`, `score_all`, and scoring output

### Stop improving the permit model without addressing queue depth

- 133,000 training rows and R² = 0.042 is conclusive: the current features explain
  nothing. Feature engineering on top of a broken model is wasted effort.
- Gate further permit model work on the queue-depth experiment.

### Stop treating data sync as a one-time operation

- The pipeline currently does full replace on every sync. There is no incremental sync,
  no change detection, and no record of when specific applications changed status.
- Without status-change tracking, the pipeline cannot support alerting or monitoring
  use cases.

---

## Prioritized Roadmap

| Priority | Work | Why Now |
|---|---|---|
| **P0** | Fix CRS (EPSG:26917 → EPSG:2952) | Already in progress; unblocks spatial features |
| **P0** | Re-sync COA + re-run pipeline | Unblocks COA model evaluation |
| **P1** | Remove `dev_applications_approved` from scoring | Prevents shipping a misleading model |
| **P1** | Add "comparable applications" query (`comps`) | Delivers value without depending on model quality |
| **P2** | Add CoA active applications to the registry | Roughly doubles COA dataset; enables current use |
| **P2** | Queue-depth feature for permit model | Either fixes the model or definitively retires it |
| **P3** | Research live dev_applications replacement | Existential for the product's long-term relevance |
| **P3** | Add FastAPI serving layer with `/score` and `/comps` | Makes the product accessible to non-Python users |
| **Hold** | Any further dev_approved model work | Dataset is retired; not worth improving |
| **Hold** | React frontend or hosted deployment | Wait until API is stable and models are validated |

---

## Success Criteria for Next Review

The product is in a measurably better state when ALL of the following are true:

1. `dev_applications_approved` is no longer trained or served
2. `coa_approved` ROC-AUC > 0.65 (post-sync, post-dedup baseline)
3. `coa_days_to_approval` R² > 0.50 (post-sync baseline)
4. `dev_appealed` AUC holds at ≥ 0.83 after CRS fix (spatial features may shift it)
5. At least one of: (a) a `comps` feature exists, or (b) a live dev_applications
   source is identified and integrated
6. The permit model decision is made: either R² > 0.15 with queue-depth feature
   (keep it) or the model is explicitly removed from the product

These are not stretch goals. They are minimum requirements for the product to be
credible to its target users.

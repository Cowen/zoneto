# Product Strategy Review — 2026-03-17

**Role:** Product Owner
**Date:** 2026-03-17

---

## Executive Summary

After reviewing 9 rounds of user feedback spanning March 14-16, the README,
and the current model state, I recommend a fundamental reorientation of this
product. The ML models are not the problem. The product is.

Zoneto has spent three days in an optimization loop: improve features, retrain,
critique, repeat. The result is two barely-passing models (appeal AUC 0.673,
survival C-index 0.744) and three broken ones. Meanwhile, **no human user has
ever consumed a prediction from this pipeline.** The predictions live in Parquet
files that require Python to read. The critique has been entirely self-referential:
AI models critiqued by AI playing the role of users who don't exist.

The product needs to answer a prior question before the next round of model
engineering: **who is the customer, what decision are they making, and how do
they access the answer?**

---

## Current State (As Of 2026-03-17)

### What Exists

| Model | Metric | Production Ready | Useful to a Developer? |
|---|---|---|---|
| `dev_applications_appealed` | AUC 0.673 | Yes (barely) | Marginally — appeal probabilities realistic (mean 0.113) but discrimination is weak |
| `dev_days_to_decision` | C-index 0.744 | Yes | Yes — correctly ranks OZ/SA timelines; best model in the pipeline |
| `coa_approved` | AUC 0.535 | No | No — worse than base rate |
| `coa_days_to_approval` | R² -0.268 | No | No — worse than predicting the mean |
| `permit_issuance_days` | R² 0.039 | No | No — explains nothing |

The scored output has:
- **26,161 dev applications** with appeal predictions and survival percentiles
- **4,217 active (under-review) applications** — the commercially valuable subset
- **4,749 COA applications** with zero predictions (both models failed)
- **154,430 permits** with zero predictions (model failed)
- **No user interface.** No API. No web page. No way for a non-developer to query anything.

### What 9 Critique Rounds Accomplished

The feedback loop has been productive at the engineering level:

- CRS bug found and fixed (spatial features went from 0% to 69% populated)
- Appeal label bias corrected (50/50 artificial rate → realistic ~11% base rate)
- Survival model added (new, and the strongest model)
- `dev_applications_approved` retired (correctly — 97.3% class imbalance, frozen dataset)
- Production-ready thresholds tightened (R² ≥ 0.10 for regressors)
- Survival scoring restricted to OZ+SA only (eliminated out-of-domain predictions)
- Storeys/units extraction added
- Ward appeal rates added
- Multiple broken features identified and removed

**But the model quality ceiling has been reached with available data.** Appeal AUC
went from 0.847 → 0.673 after fixing the label bias — meaning the high AUC was an
artifact, and the real discriminative power was always modest. Further feature
engineering on the same data will not produce a breakthrough.

---

## Step Back: Three Strategic Questions

### 2a. Should we consider new data sources or different models?

**Yes — but not the ones the critique cycle has been requesting.**

The feedback specs repeatedly ask for: MTSA boundaries, queue-depth proxies,
variance magnitude parsing, councillor features, application-age features. These
are incremental improvements worth fractions of an AUC point. They will not
change the product's value proposition.

The data source that would change everything is **live development application
data.** The `dev_applications` dataset is retired. Without a live feed of new
applications, the product cannot serve its stated purpose — predicting outcomes
for applications being filed *now*.

**New data sources worth investigating (ranked by impact):**

1. **Toronto Application Information Centre (AIC) scraper expansion.** The pipeline
   already scrapes AIC for milestone dates (decision_date, complete_date). The AIC
   portal also contains current application status, public hearing schedules, and
   planning staff reports. Expanding the scraper to pull full application records
   would replace the retired CKAN dataset with a live feed. This is the single
   highest-impact data investment.

2. **Ontario Land Tribunal (OLT) published decisions.** Appeal outcomes, hearing
   durations, and decision reasoning are published on the OLT website. This data
   directly labels the appeal model's prediction target with more granularity than
   the binary status field. It would also enable predicting *appeal outcomes*
   (does the appellant win?), which is more valuable than predicting whether an
   appeal is filed.

3. **Toronto Building Division monthly statistics.** The city publishes aggregate
   permit processing statistics that could proxy queue depth — the missing driver
   for the permit model. Whether this is granular enough to be useful is unknown.

**Model architecture is not the bottleneck.** HistGradientBoosting is appropriate
for this data (tabular, mixed types, moderate N). The problem is features and
labels, not the learner. Switching to XGBoost, LightGBM, or neural networks
would not materially change outcomes on this data.

The one model that *should* be reconsidered architecturally is `coa_approved`.
At 94.4% approval rate, binary classification is the wrong framing entirely. The
interesting question is not "will it be approved?" but "under what conditions?" —
and that requires a different kind of model (clustering or condition-extraction
from text), not a better classifier.

### 2b. Should we consider new users or optimize targeting?

**Yes — this is the most important strategic question, and it hasn't been answered.**

The README states the goal is to "track and ultimately predict the likelihood of
a given development application being approved." But *for whom?*

The critique specs roleplay as a "data scientist and Toronto real estate
developer." This is a useful fiction, but it reveals that the product has been
designed for a user who:
- Can run a CLI pipeline
- Can read Parquet files with Python
- Understands ML metrics (AUC, R², C-index)
- Has enough context to interpret probabilities

This user does not exist in the real world. Real potential users fall into
distinct segments with different needs:

**Segment 1: Development firms doing site acquisition due diligence**
- Decision: "Should I buy this parcel and file a rezoning?"
- Need: Comparable application outcomes for the ward/zone, typical timelines,
  appeal risk relative to baseline
- Willingness to pay: High ($500-5,000/report)
- Access requirement: Web interface or API, one-off queries
- **What they'd actually use:** A lookup that says "8 OZ applications were filed
  within 500m of your parcel in the last 5 years. 6 were approved (avg 2.4 years),
  1 was appealed (took 4.1 years), 1 is still active."

**Segment 2: Planning consultants advising clients**
- Decision: "What should I tell my client about their application's chances?"
- Need: Base rates by type/ward, comparable outcomes, timeline ranges
- Willingness to pay: Moderate ($100-500/month subscription)
- Access requirement: Web interface with search
- **What they'd actually use:** A dashboard showing approval rates and timelines
  by application type and ward, with drill-down to individual applications

**Segment 3: Neighbourhood groups monitoring development pressure**
- Decision: "What's being built near me and should I oppose it?"
- Need: Active application feed, ward-level development trends
- Willingness to pay: Low (ad-supported or free tier)
- Access requirement: Email alerts, simple map view
- **What they'd actually use:** Alerts when new OZ applications are filed in
  their ward, with historical context on similar applications

**Segment 4: Planning lawyers evaluating appeal risk**
- Decision: "Should I advise my client to appeal / defend against appeal?"
- Need: Appeal probability, comparable appeal outcomes, OLT decision patterns
- Willingness to pay: High ($1,000-10,000/case)
- Access requirement: Detailed report per application
- **What they'd actually use:** An appeal risk report with comparable OLT cases
  and win rates for similar application profiles

**Recommendation: Target Segment 1 first.** Development firms make the
highest-stakes decisions, are willing to pay the most, and their core need
(comparable applications + timeline intelligence) plays to the pipeline's data
strengths, not its model weaknesses. Segment 4 is the most model-dependent and
should wait until the appeal model is stronger.

### 2c. Should we change the goals themselves?

**Yes. The goal should shift from prediction to intelligence.**

The current goal — "predict the likelihood of a given development application
being approved" — has been thoroughly stress-tested through 9 critique rounds.
The findings are clear:

1. **Approval prediction is nearly worthless.** 97% of dev applications are
   approved. 94% of COA applications are approved. Predicting "yes" every time
   is already extremely accurate. The rare refusals are driven by site-specific
   factors (proposal scale, neighbourhood opposition, policy conflicts) that are
   not in the structured data.

2. **Appeal prediction is marginally useful but not compelling.** AUC 0.673 means
   the model correctly ranks appeal risk for 67% of pairs. This is better than
   chance but not enough to base a business decision on. The mean appeal
   probability (0.113) is realistic, but the discrimination between "this OZ has
   5% appeal risk" and "this OZ has 25% appeal risk" is not reliable enough.

3. **Timeline prediction is the most useful output.** The survival model (C-index
   0.744) correctly ranks which applications will take longer. Developers care
   about "how long" far more than "will it be approved" — because the answer to
   the latter is almost always yes.

4. **Comparable applications are more trusted than probabilities.** A developer
   will more readily act on "here are 8 similar applications and their outcomes"
   than on "your application has a 23% appeal risk." Comps are interpretable,
   verifiable, and match how the industry already makes decisions.

**Proposed new goal:**

> Help Toronto development professionals make informed decisions by providing
> structured intelligence on comparable planning applications, outcome patterns,
> and expected timelines — using ML models to rank and prioritize where the data
> supports it, and presenting raw data where it doesn't.

This goal:
- Remains valuable even when models are weak (comps don't require prediction)
- Leverages the pipeline's strongest asset (26,000+ indexed applications with
  spatial and outcome data)
- Naturally segments the product (data-rich features for everyone, ML-enhanced
  features where models pass threshold)
- Creates a credibility path: users trust the data, then learn to trust the
  predictions over time

---

## What I'd Actually Build Next

### Phase 1: Make the data accessible (Weeks 1-2)

The highest-ROI investment is not model improvement — it's making the existing
data queryable by non-technical users.

**1. FastAPI serving layer with three endpoints:**

```
GET  /health
GET  /comps?ward=10&type=OZ&lat=43.65&lon=-79.38&radius_m=500&years=5
POST /score  {"source": "dev_applications", "features": {...}}
```

The `/comps` endpoint is the product. It returns the N most similar historical
applications to a query, with their outcomes, timelines, and key attributes.
Similarity is based on: spatial proximity, application type, zoning class, and
ward. The data already exists in enriched Parquet; this is a DuckDB query.

The `/score` endpoint wraps the existing `score_one` function. It only returns
predictions for models where `production_ready: true`.

**2. Minimal HTML frontend:**

A single HTML page with a form: enter an address or lat/lon, select application
type, see comparable applications and (where available) predictions. No React,
no build system. Vanilla HTML + fetch() calling the API.

**3. `just serve` command in the justfile.**

### Phase 2: Fix the data source crisis (Weeks 2-4)

**1. Expand AIC scraper to replace retired dev_applications.**

The scraper already hits the AIC portal for decision dates. Extend it to pull
full application records (status, description, address, dates) for all active
and recently-closed applications. This creates a live feed that replaces the
retired CKAN dataset.

**2. Add OLT decision scraping.**

OLT publishes decisions with case numbers that can be linked to `folderrsn`.
This directly improves the appeal model's training data with more precise
labels (appeal filed, appeal upheld, appeal dismissed, appeal settled).

### Phase 3: Model improvements worth making (Weeks 4-6)

Only after Phases 1 and 2 — these improvements depend on having a user surface
and better data:

1. **MTSA/PMTSA spatial feature** for the appeal model (transit adjacency is
   the strongest policy signal in current Toronto planning)
2. **SHAP explanations** for the appeal model (users need to know *why* an
   application scores high-risk, not just that it does)
3. **Application description NLP** — even simple TF-IDF features from the
   description field could improve discrimination for both appeal and timeline
   models

### What to stop doing

1. **Stop training COA approval model.** AUC 0.535 with 94% base rate will never
   be useful. Present COA as descriptive statistics (approval rates by type, ward,
   panel) — not predictions. Keep `coa_days_to_approval` training for metric
   tracking only (it may recover with more data).

2. **Stop training permit issuance model.** R² 0.039 with 133K rows is conclusive:
   the features don't explain processing time. Queue depth data doesn't exist in
   open data. Retire from training until an external signal is found.

3. **Stop the critique loop.** 9 rounds of model critique have produced
   diminishing returns. The next marginal improvement is not in the models — it's
   in making the existing output accessible to a human user who can provide real
   feedback.

---

## Risks and Counter-Arguments

**"We should perfect the models before shipping."** No. The models will never be
perfect with this data. AUC 0.673 for appeal prediction is useful-enough when
combined with comparable applications that let users verify the prediction against
their domain knowledge. Shipping early gets real feedback; waiting gets more AI-
critiquing-AI loops.

**"A FastAPI layer is scope creep."** The README goal says "predict the likelihood."
A prediction that lives in a Parquet file and requires Python to read is not a
prediction anyone can use. The API is not scope creep — it is the minimum viable
delivery mechanism for the product's stated purpose.

**"The dev_applications dataset might come back."** It might. But the CKAN portal
has marked it "Retired" and the product cannot wait for a city bureaucracy to
reverse a data publication decision. The AIC scraper expansion is a self-reliant
alternative.

**"We're abandoning 3 of 5 models."** We're acknowledging that 3 of 5 models
don't work and can't be fixed with available data. Training them wastes compute
and creates the illusion of a product with five features when only two function.
Honesty about capability is a product feature.

---

## Summary

| Decision | Reasoning |
|---|---|
| **Change the goal** from "predict approval likelihood" to "provide development application intelligence" | Approval prediction is nearly useless at 97% base rate; intelligence (comps, timelines, trends) is valuable with current data |
| **Target development firms first** (Segment 1) | Highest willingness to pay, needs align with data strengths |
| **Build a serving layer before improving models** | No user has ever consumed a prediction; cannot get real feedback without access |
| **Expand AIC scraper** to replace retired dev_applications | Existential data source risk; scraper infrastructure already exists |
| **Retire COA approval and permit models** from training | Neither can be fixed with available data; consuming compute for noise |
| **Keep appeal + survival models** as ML-enhanced features | Both pass production threshold; serve real user needs |
| **Stop the model critique loop** | 9 rounds produced diminishing returns; next value is in user access, not model tuning |

The product's best features today are not its models — they are 26,000 indexed
development applications with spatial data, outcome labels, and timeline
information. Making that data queryable is worth more than the next 10% of AUC.

# Model Critique — 2026-03-15 (Post-CRS-Fix Pipeline Rerun)

As a data scientist and Toronto real estate developer, this is my critique of the
models after the full pipeline (`just pipeline`) was re-run today. This run reflects:
- CRS fix applied (EPSG:26917 → EPSG:2952) — spatial features now working for dev applications
- Ward profiles enrichment added (ward demographics)
- Active applications included in dev_applications dataset
- `dev_applications_approved` retired from training and scoring
- `postal_fsa` and `application_year` (permit) features removed (P0 cleanup)

---

## Current Model Metrics (2026-03-15 rerun)

| Model | N | Key metric | Secondary |
|---|---|---|---|
| `dev_applications_appealed` | 3,625 | ROC-AUC **0.865 ±0.045** | Brier 0.177 |
| `coa_approved` | 4,609 | ROC-AUC **0.535 ±0.192** | Brier 0.066 |
| `coa_days_to_approval` | 4,350 | R² **−0.465 ±1.117** | MAE 83d |
| `permit_issuance_days` | 133,006 | R² **0.039 ±0.212** | MAE 49d |

---

## What Changed Since Last Spec

### CRS Fix Partially Resolved Spatial Feature Nulls

The EPSG:26917 → EPSG:2952 fix did work: `zoning_class` is now populated for
**18,136 of 26,161 dev application rows (69.3%)**. Before the fix, every row was
null. Heritage register coverage is complete (24,900 not in register, 1,261 in
register — 4.8% flagged). This is a real improvement.

However, 8,025 rows (30.7%) still have null `zoning_class`. These are likely
applications whose parcels fall in coverage gaps in the city's zoning GeoJSON
(unzoned land, area-specific by-laws, or boundary effects). The model silently
encodes these as `"__missing__"` — this is correct behaviour for
HistGradientBoosting but the null rate is high enough to warrant investigation.

`dev_applications_appealed` AUC improved from 0.843 to 0.865 after the CRS fix,
confirming that spatial features contribute signal. The variance also tightened
(±0.060 → ±0.045). This is a concrete improvement from the fix.

### COA Model Still Broken — This Is A Structural Data Limit, Not A Sync Problem

`coa_approved` AUC: 0.496 → 0.535. Marginal improvement, still useless.
`coa_days_to_approval` R²: −0.776 → −0.465. Improved but still worse than predicting the mean.

The COA data **was synced today** (2026-03-15). The narrow date distribution is not
a sync gap — it is the complete picture the CKAN source provides. The city only
publishes closed applications in two CSV files ("Closed Applications 2022" and
"Closed Applications 2023"). The resulting dataset has 5,093 rows with `in_date`
ranging from 2014 to 2023, but 58.4% filed in 2022 and ~28.5% in 2021. Pre-2020
rows total 119. This is what exists in the open data portal.

This is a structural limitation, not an operational fix. Re-syncing will not change
the distribution. Any improvement to the COA models requires either:
- Finding a supplementary data source (the city's internal records, which are not
  open data), or
- Accepting that the COA models can only speak to recent (2021–2023) application
  patterns and labeling them accordingly.

TimeSeriesSplit with 5 folds on this concentrated dataset means early folds train
on ~400 rows and test on the 2022 mass — COVID-era 2021 patterns vs. 2022 recovery
is a distributional shift that will hurt generalization.

Additionally, `coa_days_to_approval` has an outlier at **2,992 days (~8.2 years)**
in a distribution with a mean of 187 days and a 90th percentile of 351 days. This
single outlier is almost certainly a data error (perhaps an application filed in 2014
and not formally closed until 2022) and is destabilizing the regression across CV folds.

---

## Why I Would Not Pay For These Results

### What Is Usable

**`dev_applications_appealed` (AUC 0.865 ±0.045):** The only model worth putting
in front of a user. AUC 0.865 with ±0.045 variance means the worst-case fold is
still AUC ~0.82 — that's reliable enough to rank applications by appeal risk. For
me as a developer, knowing *before* I file whether my OZ application will attract
TLAB opposition is genuinely valuable: it affects how much time I budget for the
planning process, whether to retain an LPAT consultant pre-emptively, and whether
to engage the community proactively.

But I'm paying for actionability, not just AUC. The model outputs a probability
but gives me no context: appeal risk compared to *what*? Which similar applications
were appealed, and why? Which zoning classes or ward patterns dominate the high-risk
cases? Without interpretability, a probability score is hard to act on.

### What Is Not Usable

**`coa_approved` (AUC 0.535 ±0.192):** Marginally better than flipping a coin —
and with ±0.192 variance, some folds are below 0.5. This model will actively mislead.
If I'm a planning consultant trying to advise clients on variance likelihood, I'd
be better served by the Committee of Adjustment's published approval statistics
(which consistently run ~94% for minor variances). A model that tells me "94% base
rate" is more useful than one with AUC 0.535.

**`coa_days_to_approval` (R² −0.465 ±1.117):** Negative R² means predictions are
literally worse than the naive mean. The ±1.117 standard deviation on R² means in
the worst folds this model is catastrophically wrong. For a developer trying to
model carrying costs on a severance or minor variance application, this model is
hazardous — it will confidently produce wrong timelines.

**`permit_issuance_days` (R² 0.039 ±0.212):** 133,006 training rows and 96.1% of
variance is unexplained. The features used (permit_type, structure_type, ward_grid,
est_const_cost, dwelling_units, use-type flags) simply do not contain the
information needed to predict processing time. Permit timelines are driven by:

1. **Permit office queue depth** — not in the data
2. **Application completeness** — not in the data
3. **Project complexity requiring engineering review** — est_const_cost is a weak
   proxy at best, and 55.5% of rows have null cost
4. **Whether revisions were requested** — not in the data

A model with R² ≈ 0 cannot be surfaced to users without destroying trust in the
product. The 49-day MAE looks acceptable in isolation but the mean issuance time
varies by permit type — for a complex new build this MAE is inadequate.

---

## What the Models Get Wrong

### 1. The Appeal Model Trains on Historical Outcomes, Not Current Risk

The 3,625 rows with labeled `dev_appealed` outcomes are all **closed** applications —
cases that have completed the planning cycle. The 4,217 active applications (now
included in the dataset) are excluded from training. This means the model has never
seen a labelled example of an application that is *currently in the phase where
appeals are most likely to occur*.

The model captures aggregate historical patterns (ward, type, zoning, heritage) but
cannot capture situational risk: the nature of the specific variance, the degree of
deviation from by-law, or the current political climate in a ward. Two OZ applications
in the same ward with the same zoning class can have wildly different appeal profiles
depending on whether the development is 30 storeys or 12 storeys, and the model
currently cannot distinguish between them.

### 2. No Time-to-Decision for Development Applications

For a developer, the most operationally critical question is not "will this be
approved?" (we know almost everything gets approved eventually) — it is "how long
will this take?" Carrying costs on a Toronto development site run $100,000–$500,000+
per month depending on land value. A 6-month forecast error on a planning timeline
is material.

There is no `dev_applications_days_to_decision` model, and the data to build one
exists: `date_submitted` and implied decision dates can be derived from the `status`
field or CKAN update metadata. The absence of this model is the largest single gap
between what the pipeline offers and what a developer would pay for.

### 3. Zoning Class Is a Coarse Signal

The top zoning classes in the dev application training data are RD (residential
detached), R (residential), CR (commercial-residential), RM (residential multiple).
These broad class codes don't capture:

- The specific zone's height permission vs. the application's proposed height
- Whether the application is within a Major Transit Station Area (MTSA) or Protected
  Major Transit Station Area (PMTSA) — the single most important planning policy
  context for Toronto applications since 2022
- The degree of variance from the applicable by-law (which drives opposition)

A developer filing an OZ for a 12-storey building in a CR zone with a 20-storey
permission is in a completely different risk profile from one filing in a residential
R zone requesting a rezoning from 2 to 40 storeys. The current features treat both
identically.

### 4. COA Feature Set Misses What The Committee Actually Weighs

The Committee of Adjustment's decisions on minor variances are primarily driven by
four tests under section 45(1) of the *Planning Act*:

1. Is the variance minor in nature?
2. Is it desirable for the use of the property?
3. Does it conform to the general intent of the Official Plan?
4. Does it conform to the general intent of the Zoning By-law?

None of these factors are in the feature set. What's in the feature set:
`application_type`, `sub_type`, `ward_number`, `zoning_designation`,
`planning_district`, `work_type`, `year_submitted`.

`ward_number` is a weak proxy for local planning culture but misses the specifics.
`zoning_designation` is more useful but is one-hot encoded without the actual
numerical parameters that the variance deviates from. The result is that the model
cannot learn what CoA actually adjudicates — it learns only aggregate ward-level
and type-level patterns.

### 5. The Ward Profile Features May Introduce Leakage or Noise

The ward demographic features (`ward_pct_renters`, `ward_median_income`,
`ward_pop_density`, `ward_pct_detached`) are census-derived static values joined
by ward. These are not time-varying, meaning a 2010 dev application and a 2024 one
in the same ward get identical demographics despite 14 years of neighborhood change.
More importantly, it's unclear these demographics are predictive of planning outcomes
after controlling for ward fixed effects (which `ward_number` already captures).
These features may add noise rather than signal.

---

## What Would Make Me Pay

### P0: A time-to-decision model for development applications

Derive `dev_days_to_decision` from `date_submitted` to implied close date, train a
regression model on the ~22,000 closed applications. Even a model explaining 40%
of variance (R² = 0.40) would be worth paying for given the financial stakes.

### P1: Appeal model interpretability

Add SHAP or feature importance visualization so I can see *why* a specific application
scores high-risk. If the model says my OZ has a 78% appeal probability, I need to
know if it's because of the ward, the zoning class, or the heritage flag — because
each has a different response strategy.

### P2: MTSA/PMTSA spatial feature

Whether an application falls within a Major Transit Station Area fundamentally
changes the planning policy context and the likelihood of council support. This is
available as open data from the city and would be a single spatial join — similar
in structure to what's already done for heritage and secondary plans.

### P3: Accept the COA data structural limit or find a new source

The COA CKAN source only publishes 2022 and 2023 closed application CSVs — this is
fully synced and it is all that exists in open data. Older years are not available
from CKAN. The options are: (a) check if the city publishes older CSVs elsewhere,
(b) accept the 2021–2023 window as the permanent training horizon and label models
accordingly, or (c) look for a supplementary commercial or FOI-sourced dataset.

At 94% approval rate for minor variances, the real value would be in predicting
*processing time* and *conditions*, not binary approval — but even the timeline model
cannot be fixed without more training data spanning more years.

### P4: Retire or gate `coa_approved` and `coa_days_to_approval`

Do not surface these models to users in the current state. A system that displays
a COA approval probability of 0.72 when the actual signal is noise is actively
harmful to trust. Hide them behind a "not enough data" message until the COA data
problem is resolved.

---

## High-ROI Improvements (Ranked)

| # | Improvement | Expected Impact | Effort |
|---|---|---|---|
| 1 | Add `dev_days_to_decision` regression model | Highest-value missing feature | Medium |
| 2 | Cap `coa_days_to_approval` outliers (>730 days) | Remove data-error distortion from regression | Small |
| 3 | Add MTSA/PMTSA spatial join for dev applications | Strong planning policy signal | Medium |
| 4 | Add SHAP feature importance to dev_appealed scoring output | Actionable interpretability | Medium |
| 5 | Gate `coa_approved` / `coa_days_to_approval` (hide from product if AUC < 0.65) | Protect trust | Small |
| 6 | Investigate 30.7% zoning_class null rate | May recover signal from gap parcels | Small |
| 7 | Retire `permit_issuance_days` or find queue-depth proxy | Remove R²≈0 model from product | Small |

---

## Product Manager Review

*Added 2026-03-15 — critique of the user feedback above from a product perspective.*

### Factual Review

**"The appeal model trains on historical outcomes, not current risk"** — Correct
and important. The 4,217 active applications are scored but not trained on (by
construction: they have no outcome labels yet). This is not a bug — it is the
correct use of a supervised model — but the user's point about missing situational
features is valid. The model generalizes from historical patterns. This is inherent
to the approach, not a fixable code issue.

**"No time-to-decision for development applications"** — Verified: there is no such
model and no `dev_days_to_decision` feature in the enriched data. The data to build
it likely exists (status transitions, submitted date), but it requires label
engineering. This is the strongest gap identified. **Actionable.**

**"MTSA/PMTSA is the single most important planning policy context since 2022"** —
This is directionally correct. Toronto's Bill 23 (2022) and the associated
provincially-directed MTSA rezoning significantly changed approval dynamics for
transit-adjacent applications. However, calling it "the single most important"
factor may be overstated — ward-level councilor support, application type, and
heritage designations also drive outcomes materially. The point that MTSA/PMTSA
is a missing feature with available data is valid. **Actionable.**

**"Ward demographics may introduce noise rather than signal"** — Plausible but
unverified. Ward-level demographics are correlated with ward identity, so they may
add little marginal signal over `ward_number`. The right test is to measure AUC
with and without the ward demographic features. This is a hypothesis, not a
confirmed problem. **Flag for investigation — do not remove features without
measuring impact.**

**"Committee of Adjustment adjudicates four Planning Act tests, none of which are
in the feature set"** — Accurate description of CoA's statutory mandate. However,
the conclusion may be too pessimistic: `zoning_designation` and `ward_number` are
imperfect but real proxies for the planning policy context that drives those tests.
The key missing data is the *degree of variance* (how much does the application
deviate from the by-law limit?) — that is the most actionable gap within CoA features.
The four-test framework cannot be mechanized from open data alone. **Partially valid;
focus on degree-of-variance gap, not the statutory framework.**

**"2,992-day COA outlier corrupts the regression"** — Verified. The `coa_days_to_approval`
distribution shows mean 187d, but max 2,992d (8.2 years). This is almost certainly
a data error or an extreme legacy case that should be capped or excluded. Outliers
of this magnitude inflate RMSE and destabilize R² across CV folds. This is a cheap
fix: cap the regression target at, say, 730 days (2 years), flag anything beyond
as "exceptional case." **High-ROI fix, low effort.**

### What Is Worthwhile (PM-Approved)

| # | Item | Decision | Rationale |
|---|---|---|---|
| 1 | `dev_days_to_decision` regression model | **Approve — P0** | Highest stated user value; data likely exists |
| 2 | Cap `coa_days_to_approval` at 730d | **Approve — P0** | Cheap fix, removes outlier distortion |
| 3 | Investigate CKAN for additional COA year CSVs beyond 2022–2023 | **Approve — P1** | Check if 2019–2021 files exist; if not, accept structural limit |
| 4 | MTSA/PMTSA spatial join | **Approve — P1** | Valid planning signal, open data available |
| 5 | Gate COA models if AUC < 0.65 | **Approve — P1** | Protect product trust |
| 6 | Investigate 30.7% zoning null rate | **Approve — P1** | Unknown coverage gap, cheap to measure |
| 7 | Retire `permit_issuance_days` | **Approve — P1** | R²=0.039 with 133k rows; no path to improvement without queue data |
| 8 | SHAP feature importance | **Hold — P2** | Valid but requires shipping the base model first; add after P0/P1 |
| 9 | Ward demographics removal | **Hold** | Do not remove without A/B measuring impact on AUC |

### What Needs Revision

**"Degree of variance from by-law" as a CoA feature** — The user is right that
this is the key missing signal, but wrong to assume it's derivable from the current
data. The CoA dataset does not include the specific by-law standard being deviated
from or the proposed value. This would require scraping the individual application
documents (PDFs) from the city's application portal — a meaningfully larger
engineering effort than a spatial join. **Acknowledge the gap; mark as out-of-scope
for now pending a data availability check.**

**"Retire permit_issuance_days"** — The user is correct that the model should not
be shown to users in its current state. However, full retirement is premature: if
`application_year` is added as a feature (to capture queue depth over time), it is
plausible that R² could improve materially (the permit office timeline changed
dramatically between 2020 COVID slowdowns and 2022–2023 recovery). The right action
is: add `application_year`, re-run, measure — if R² is still < 0.1 after that, retire.

### Prioritized Action Plan (PM-approved)

| Priority | Action | Expected outcome | Approved? |
|---|---|---|---|
| P0 | Engineer `dev_days_to_decision` label; train regression model | New highest-value model | **Yes** |
| P0 | Cap `coa_days_to_approval` at 730 days (exclude outliers) | Stable regression target | **Yes** |
| P1 | Check CKAN for COA CSVs beyond 2022–2023 (2019–2021) | More training data if available | **Yes** |
| P1 | Add MTSA/PMTSA spatial join for dev applications | Stronger planning policy signal | **Yes** |
| P1 | Gate COA approval/timeline models behind AUC threshold | Protect user trust | **Yes** |
| P1 | Investigate zoning null rate (30.7%) — check GeoJSON coverage | Potential signal recovery | **Yes** |
| P1 | Add `application_year` to permit features; re-evaluate permit model | Test queue-depth hypothesis | **Yes — diagnostic** |
| P2 | Add SHAP explanations to dev_appealed scoring output | Interpretability | **Yes — after P0/P1** |
| Hold | Degree-of-variance CoA feature | Requires PDF scraping, out-of-scope | **No** |
| Hold | Ward demographics removal | Measure impact before removing | **No** |

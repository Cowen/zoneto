# Model Critique and Improvements — 2026-03-14

As a data scientist and Toronto real estate developer, here is my honest assessment
of the current models and what I would pay for.

---

## Why I Would Not Pay For These Results Today

### 1. dev_approved: Essentially Useless — 69 refusals in 20,690 rows

99.7% of labeled dev_applications were approved. The model predicts "approved" for
everything and is right nearly all the time. The 0.970 ROC-AUC sounds impressive but
masks the core problem: there are only 69 refusals in the training set. A model cannot
reliably learn to distinguish the rare refusal from the overwhelming approval majority
on six generic features. This tells me nothing I don't already know by reading the
class balance.

**What I care about:** "Is this specific application likely to be refused — and why?"
That requires understanding the nature of the proposal relative to planning policy,
not just knowing that almost everything gets approved.

### 2. dev_appealed: Suspiciously Strong, Likely Leaking Status

ROC-AUC of 0.957 for predicting OMB/TLAB appeals is suspiciously high. The appeal
label is derived from the current `status` field, which includes values like
"OMB Appeal" and "OMB Approved". The status field is a *current* snapshot, not the
status at filing time. An application filed in 2015 that is still "Under Review" in
2024 and has never been appealed is labeled 0 — but a 2015 application that was
appealed and settled long ago is labeled 1.

**The leak:** The current status of an application contains information about its
entire history, including whether an appeal was filed. The model may have learned
that applications with certain `ward_number` × `year` combinations are more likely
to be labelled "OMB Appeal" simply because of how data was entered, not because of
anything predictable at filing time.

Even granting that the model is legitimate, a developer wants to know at filing time:
"will my application attract an appeal?" — not retroactively on a retired dataset.

### 3. dev_applications Dataset is Retired — No New Applications

The entire `dev_applications` dataset is marked "Retired" on the Toronto Open Data
Portal. No new records will be added. Scoring a new OZ or SA application against a
model trained on a retired dataset is only possible if the application happened to
be filed before the dataset cutoff. This makes both dev models practically useless
for forward-looking developer decisions.

### 4. COA Approved: AUC 0.695 on 4,630 Rows is Barely Better Than Ward Base Rates

The COA approval model has only one numeric feature (`year_submitted`) and four
categorical features. It is essentially a look-up table by application_type ×
ward_number × zoning_designation. The 0.695 AUC is close to what you'd get from a
simple conditional probability table.

What's missing:
- **Panel** (Etobicoke York, North York, Toronto & East York, Scarborough) — the
  four CoA panels have distinctly different approval rates and member compositions.
  `planning_district` is already in the raw data.
- **Seasonality** — hearing month affects scheduling delays and is correlated with
  the composition of panels sitting that day.
- **Only 2022–2023 data** — the pipeline fetches only two years of closed
  applications. Toronto CoA has publicly available data going back further. Broader
  temporal coverage would dramatically improve the temporal CV and the model.

### 5. Probabilities are Uncalibrated — Useless for Financial Decisions

`prob_dev_approved = 0.84` from a HistGradientBoosting classifier means "relatively
more likely" but does not mean 84% probability. HistGradientBoosting is not
calibrated by default; its raw outputs underestimate uncertainty. A developer who
plugs these probabilities into a pro-forma model (expected revenue = prob × value)
will systematically mis-estimate project risk.

To be paid for, probabilities must be calibrated: "80% predicted" should correspond
to ~80% empirical approval frequency.

### 6. is_tlab_era is 100% Redundant

`is_tlab_era` is defined as `year_submitted >= 2017` and `year_submitted` is already
in the numeric features. The tree model sees two perfectly correlated features and
will use whichever it encounters first. This wastes a split and adds noise to
feature importance analysis.

### 7. No Permit Issuance Timeline Model

For a developer, the most operationally useful prediction is: "If I submit this
building permit today, when will it be issued?" This affects construction financing
timelines, draw schedules, and contractor availability. The pipeline has 161k cleared
permits with both `application_date` and `issued_date` — enough to train a strong
regressor. This model is absent.

---

## Improvements With Highest Return on Investment

Ranked by (value to developer) × (feasibility):

| # | Improvement | ROI | Effort |
|---|-------------|-----|--------|
| 1 | COA feature expansion (planning_district, hearing_month) | High | Low |
| 2 | Model calibration for classifiers | High | Low |
| 3 | Remove redundant is_tlab_era feature | Medium | Trivial |
| 4 | Permit issuance timeline model (new regressor) | Very High | Medium |

---

## Implementation Plan

### 1. COA Feature Expansion

**Files:** `analytics/enrich.py`, `analytics/features.py`

- `enrich_coa`: preserve `planning_district` column as a categorical feature;
  extract `hearing_month` (int, 1–12) from the first hearing_date where available.
- `COA_CAT_COLS`: add `"planning_district"`
- `COA_NUM_COLS`: add `"hearing_month"`

Expected impact: AUC 0.695 → 0.72+ based on the large between-panel variance in
approval rates.

### 2. Model Calibration

**Files:** `analytics/train.py`

- Add `calibrate: bool = True` parameter to `train_source`.
- For classifiers with `calibrate=True`: after training the base pipeline on 80% of
  training data, wrap it in `CalibratedClassifierCV(pipeline, cv='prefit',
  method='isotonic')` and fit the calibrator on the held-out 20%.
- The serialized `.joblib` file is the calibrated model (still implements
  `predict_proba`).
- `evaluate_source` is unaffected — ranking metrics (AUC, avg_precision) are
  invariant to monotone probability scaling; Brier score will reflect improvement.

### 3. Remove is_tlab_era

**Files:** `analytics/features.py`, `analytics/enrich.py`

- Remove `"is_tlab_era"` from `DEV_NUM_COLS`.
- Remove the `is_tlab_era` column creation from `enrich_dev`.
- `year_submitted` already captures the TLAB-era signal.

### 4. Permit Issuance Timeline Model

**Files:** `analytics/features.py`, `analytics/enrich.py`, `analytics/train.py`,
`analytics/score.py`, `cli.py`

New regression model: `permit_issuance_days`

- `enrich_permits(data_dir)`: read `permits_cleared` parquet; compute
  `permit_issuance_days = (issued_date - application_date).days`; drop rows where
  either date is null or `permit_issuance_days <= 0`; write
  `data/enriched/permits_cleared.parquet`.
- `PERMIT_CAT_COLS`: `["permit_type", "structure_type", "ward_grid"]`
- `PERMIT_NUM_COLS`: `["est_const_cost", "dwelling_units_created",
  "dwelling_units_lost", "residential", "commercial", "industrial",
  "institutional"]`
- Add job to `train_all`: train `permit_issuance_days.joblib` regressor.
- Add to `score_all`: output `pred_permit_issuance_days` (float) on active permits.
- Update `enrich` CLI command to call `enrich_permits`.

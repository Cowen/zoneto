# Model Metrics

Each entry is a dated snapshot of 5-fold cross-validated performance on the
full enriched dataset at that point in time. Add a new dated section when
re-running after data updates or model changes.

Metrics are cross-validated (not training-set scores):
- **Classifiers**: StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
- **Regressor**: KFold(n_splits=5, shuffle=True, random_state=42)

---

## 2026-03-04

**Data snapshot:**
- `dev_applications`: 26,161 rows enriched (2008 – ~2024)
- `coa`: 5,093 rows enriched (closed applications 2022–2023)

**Features used:**

| Source | Categorical | Numeric |
|---|---|---|
| dev | application_type, ward_number, zoning_class, secondary_plan_name | year_submitted, in_heritage_register, in_heritage_district, in_secondary_plan, has_community_meeting |
| coa | application_type, sub_type, ward_number, zoning_designation | year_submitted |

---

### dev_applications_approved (HistGradientBoostingClassifier)

Target: `dev_approved` — 1 = closed/approved, 0 = refused
Class balance: 69 negative / 20,621 positive (99.7% positive)
N = 20,690

| Metric | Mean | ± Std |
|---|---|---|
| Accuracy | 0.998 | 0.001 |
| F1 | 0.999 | 0.000 |
| ROC-AUC | **0.970** | 0.033 |
| Avg Precision | 1.000 | 0.000 |

> Note: Accuracy and F1 are inflated by the extreme class imbalance (69 refusals
> in 20,690 rows). ROC-AUC is the meaningful signal here. The model can reliably
> separate the rare refusals from the large approval majority.

---

### dev_applications_no_appeal (HistGradientBoostingClassifier)

Target: `dev_no_appeal` — 1 = OMB/TLAB appeal filed, 0 = no appeal
Class balance: 19,970 negative / 1,830 positive (8.4% positive)
N = 21,800

| Metric | Mean | ± Std |
|---|---|---|
| Accuracy | 0.946 | 0.003 |
| F1 | 0.622 | 0.017 |
| ROC-AUC | **0.957** | 0.003 |
| Avg Precision | 0.712 | 0.024 |

> Best-performing model. AUC of 0.957 and average precision of 0.712 indicate
> strong discrimination despite the class imbalance. The model ranks high-risk
> applications (likely to attract an appeal) very well.

---

### coa_approved (HistGradientBoostingClassifier)

Target: `coa_approved` — 1 = approved (any form), 0 = refused/withdrawn
Class balance: 280 negative / 4,350 positive (94.0% positive)
N = 4,630

| Metric | Mean | ± Std |
|---|---|---|
| Accuracy | 0.947 | 0.005 |
| F1 | 0.972 | 0.003 |
| ROC-AUC | **0.695** | 0.041 |
| Avg Precision | 0.965 | 0.007 |

> Weakest classifier. ROC-AUC of 0.695 is only modestly above chance. The
> available features (ward, application type, zoning designation, year) do not
> capture what CoA panels actually weigh: site-specific facts, neighbour impact,
> and planning policy context. Use with caution for prediction; more useful as a
> baseline for future feature engineering.

---

### coa_days_to_approval (HistGradientBoostingRegressor)

Target: `coa_days_to_approval` — calendar days from intake to final decision
(approved rows only)
N = 4,350 | mean = 187 days | median = 127 days | p90 = 351 days

| Metric | Mean | ± Std |
|---|---|---|
| MAE | **54.6 days** | 3.9 |
| RMSE | 93.0 days | 11.8 |
| R² | **0.786** | 0.026 |

> R² of 0.786 means the features explain ~79% of variance in processing time.
> MAE of 55 days on a median of 127 days is reasonable. The gap between MAE and
> RMSE reflects outlier long-duration applications (p90 = 351 days). The model
> is most reliable in the 60–200 day range.

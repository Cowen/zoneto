# Analytics — Zoneto

## Feature Columns (`features.py`)

- `DEV_CAT_COLS` — application_type, ward_number, zoning_class, secondary_plan_name
- `DEV_NUM_COLS` — spatial flags, ward demographics, proposed_storeys/units, excess ratios, ward_appeal_rate_3y, in_mtsa, desc_svd_0..desc_svd_19
- `COA_CAT_COLS` — application_type, sub_type, ward_number, zoning_designation, planning_district, work_type
- `COA_NUM_COLS` — year_submitted
- `PERMIT_CAT_COLS` — permit_type, structure_type, ward_grid
- `PERMIT_NUM_COLS` — est_const_cost, dwelling_units_created/lost, use flags

## Enrichment (`enrich.py`)

**Reference datasets** (cached in `data/reference/`):
- `zoning.geojson` — ZN_ZONE, UNITS (max units), DENSITY (max FSI); **-1 means no limit → treated as null**
- `zoning_height.geojson` — HT_STORIES (max storeys, ~15% coverage); -1 treated as null
- Heritage register/districts (ZIP→SHP), secondary plans (GeoJSON), `mtsa/` (ZIP→SHP)
- `aic_decisions.parquet` — `folderrsn, decision_date, complete_date, scraped_at`
- `olt_decisions.parquet` — `case_number, municipality, hearing_date, decision_date, outcome, address, scraped_at`

**Non-obvious facts:**
- Dev application x/y are **EPSG:2952** (City of Toronto internal CRS); `_spatial_join_dev` reprojects to EPSG:4326 before zoning join.
- `dev_appealed` label: OZ+SA only. 1=appeal filed, 0=any closed non-appeal status, null=active/non-OZ/SA. Covers ALL closed OZ+SA to preserve true base rate (~15–25%). Earlier versions only labeled explicitly-approved rows as 0 — caused 50/50 bias.
- `ward_appeal_rate_3y` uses only years strictly before `year_submitted` — temporal leakage-safe.
- `unit_excess_ratio` = proposed_units / zoning_max_units; `storey_excess_ratio` = proposed_storeys / zoning_max_storeys (>1.0 exceeds by-law).
- `coa_days_to_approval` capped at 730 days; `dev_days_to_decision` capped at 3,650 days.
- NLP: descriptions TF-IDF→20-dim TruncatedSVD (`desc_svd_0..desc_svd_19`); pipeline serialized to `models/desc_tfidf.joblib`.
- BERT: `compute_bert_embeddings()` uses `BAAI/bge-small-en-v1.5` (384-dim) → `data/enriched/desc_bert_embeddings.npy` + `desc_bert_index.parquet`. Idempotent.

## Training (`train.py`)

| File | Type | Target | Status |
|---|---|---|---|
| `dev_applications_appealed.joblib` | CalibratedClassifierCV(HGBC) | `dev_appealed` | **production** |
| `coa_days_to_approval.joblib` | HGBRegressor | `coa_days_to_approval` | **tracking only** (not served) |
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | optional (requires AIC scrape) |

**Retired** (not trained/scored): `dev_applications_approved` (97.3% imbalance), `coa_approved` (AUC 0.535), `permit_issuance_days` (R²=0.039, queue depth not in open data).

`production_ready` thresholds: classifiers roc_auc_mean ≥ 0.65, regressors r2_mean ≥ 0.10, survival c-index ≥ 0.65. `coa_days_to_approval` forced `production_ready: false`. CV uses `TimeSeriesSplit` on `year_col` to avoid leakage.

## Scoring (`score.py`)

Skips `production_ready: false` models. Only dev_applications scored. Survival model: OZ+SA rows only; outputs p25/p50/p75. Writes `dev_applications.parquet` and `dev_applications_active.parquet` (active apps only).

| Column | Type |
|---|---|
| `pred_dev_appealed` | int (0/1) |
| `prob_dev_appealed` | float |
| `pred_dev_days_p25/p50/p75` | float |

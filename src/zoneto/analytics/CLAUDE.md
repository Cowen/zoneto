# Analytics — Zoneto

_Last verified: 2026-05-22_

## Enrichment Module Map

The enrichment pipeline is split across modules; `enrich.py` is a thin orchestrator
(`enrich_coa`, `enrich_dev`, `enrich_permits`, `_compute_ward_appeal_rate_3y`).

| Module | Responsibility | Public API |
|---|---|---|
| `reference.py` | Reference dataset URLs + downloads | `fetch_reference()` |
| `spatial.py` | Spatial joins, height/MTSA/ward features | `_spatial_join_dev`, `_add_height_feature`, `_add_mtsa_feature`, `_enrich_ward_features` (internal) |
| `labels.py` | Outcome status sets, OLT matching | `match_olt_to_dev()`; `_label_from_sets`, `_DEV_DAYS_CAP` (internal) |
| `nlp.py` | TF-IDF/SVD text features, BERT embeddings | `compute_bert_embeddings()`; `_extract_text_features` (internal) |
| `enrich.py` | Orchestration | `enrich_coa`, `enrich_dev`, `enrich_permits` |

`enrich.py` re-exports `fetch_reference` and `match_olt_to_dev` for backward
compatibility, so `from zoneto.analytics.enrich import ...` still resolves them.

## Feature Columns (`features.py`)

- `DEV_CAT_COLS` — application_type, ward_number, zoning_class, secondary_plan_name
- `DEV_NUM_COLS` — spatial flags, ward demographics, proposed_storeys/units, excess ratios, ward_appeal_rate_3y, in_mtsa, desc_svd_0..desc_svd_19

(COA/permit feature-column lists were removed with the deleted COA/permit models — only the DEV columns feed the survival model.)

## Enrichment

**Reference datasets** (fetched by `reference.py`, cached in `data/reference/`):
- `zoning.geojson` — ZN_ZONE, UNITS (max units), DENSITY (max FSI); **-1 means no limit → treated as null**
- `zoning_height.geojson` — HT_STORIES (max storeys, ~15% coverage); -1 treated as null
- Heritage register/districts (ZIP→SHP), secondary plans (GeoJSON), `mtsa/` (ZIP→SHP)
- `op_land_use.geojson` — Official Plan land-use **designation** polygons (`op_designation`); **interim Borealis source** (CC BY-NC, 10 designations dissolved, EPSG:26917→WGS84 via `ST_Transform`). Optional/graceful: absent → `op_land_use_designation` null. **Acquired by `just op` (NOT `enrich`)** so the download isn't forced. No official City polygon layer exists (verified 2026-06-13); swap for a GCC data-request layer when secured. See `specs/2026-06-13-planning-act-integration.md` item 4b.
- `aic_decisions.parquet` — `folderrsn, decision_date, complete_date, scraped_at`
- `olt_decisions.parquet` — `case_number, municipality, hearing_date, decision_date, outcome, address, scraped_at`

**Non-obvious facts:**
- Dev application x/y are **EPSG:2952** (City of Toronto internal CRS); `_spatial_join_dev` (spatial.py) reprojects to EPSG:4326 before zoning join.
- `dev_appealed` label (labels.py `_label_from_sets`): OZ+SA only. 1=appeal filed, 0=any closed non-appeal status, null=active/non-OZ/SA. Covers ALL closed OZ+SA to preserve true base rate (~15–25%). Earlier versions only labeled explicitly-approved rows as 0 — caused 50/50 bias.
- `op_land_use_designation` (spatial.py `_add_op_land_use_feature`): OP designation from a point-in-polygon join against `op_land_use.geojson`. Written to enriched parquet for the conformity check + eval, but **NOT a model feature** (kept out of `features.py` `DEV_*_COLS` — adding it would change the model; framing-only stance).
- `ward_appeal_rate_3y` uses only years strictly before `year_submitted` — temporal leakage-safe.
- `unit_excess_ratio` = proposed_units / zoning_max_units; `storey_excess_ratio` = proposed_storeys / zoning_max_storeys (>1.0 exceeds by-law).
- `dev_days_to_decision` capped at 3,650 days (labels.py `_DEV_DAYS_CAP`).
- NLP (nlp.py): descriptions TF-IDF→20-dim TruncatedSVD (`desc_svd_0..desc_svd_19`); pipeline serialized to `models/desc_tfidf.joblib`.
- BERT: `compute_bert_embeddings()` (nlp.py) uses `BAAI/bge-small-en-v1.5` (384-dim) → `data/enriched/desc_bert_embeddings.npy` + `desc_bert_index.parquet`. Idempotent.

## Training (`train.py`)

| File | Type | Target | Status |
|---|---|---|---|
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | **production** (requires AIC scrape) |
| `desc_tfidf.joblib` | TF-IDF → TruncatedSVD | description text → `desc_svd_0..19` | feature extractor (not a predictor) |

`dev_days_to_decision` is the only predictive model. **Deleted** (never cleared the quality bar — training-data limitations): `dev_applications_appealed` (AUC 0.559, survivorship-biased labels), `coa_days_to_approval` (R² −0.27), `dev_applications_approved` (97.3% imbalance), `coa_approved` (AUC 0.535), `permit_issuance_days` (R²=0.039, queue depth not in open data). The generic classifier/regressor training machinery (`train_source`/`evaluate_source`/`build_pipeline`) was removed with them. See `tests/analytics/test_retirement.py` for the guard.

`production_ready` threshold: survival c-index ≥ 0.65. CV uses `TimeSeriesSplit` on `year_col` to avoid leakage.

## Scoring (`score.py`)

Skips `production_ready: false` models. Only dev_applications scored. Survival model: OZ+SA rows only; outputs p25/p50/p75. Writes `dev_applications.parquet` and `dev_applications_active.parquet` (active apps only).

| Column | Type |
|---|---|
| `pred_dev_days_p25/p50/p75` | float |
| `statutory_min_decision_days` | int (or null) |

`statutory_min_decision_days` comes from `planning_act.statutory_timeline_days(application_type)` and is written for **every** row (OZ 120, SB/CD 120, SA/PL null) — unlike the OZ/SA-only survival model. It is the **statutory floor** to an applicant's non-decision appeal right under the Planning Act, **not** a predicted decision time; never conflate it with the survival `p50`.

## Planning Act layer (`planning_act.py`)

Pure-data provincial-statute reference (no network), the counterpart to the municipal `compliance.py`. `MINOR_VARIANCE_TESTS` (s.45(1) four tests) is single-sourced here and pulled into `compliance.py` remedies via a deferred import (planning_act imports `Severity`/`Violation` from compliance — deferred import breaks the cycle). `path_for_violations()` derives the primary **zoning path** (prohibited > rezoning > variance > as_of_right, mirroring the narrator's override precedence). `_check_op_conformity` (in `compliance.py`) compares the proposed use against the site's OP **designation** (`op_use_matches_designation` in `use_classifier.py`, the provincial counterpart to `use_matches_zone`); a non-conforming use emits an `op_use_nonconforming` Violation at **INFORMATIONAL** severity — it signals an OPA (s.22) / s.24 conformity issue and flips the narrator's combined-application clock to 120 days, but deliberately does **not** move the confidence number while on the interim Borealis layer (promote to `NEEDS_REZONING` when an authoritative City layer lands). `additional_processes(extracted)` adds **orthogonal** Planning Act/Toronto processes a proposal also triggers — `site_plan` (s.41), `subdivision` (s.51), `condominium`, `consent` (s.53), `part_lot_control` (s.50), `rental_replacement` (CoTA s.111 / Ch. 667) — via deterministic feature/keyword heuristics (advisory "likely applies"); `statutory_processes()` returns primary + orthogonal, deduped. `PROCESS_BY_PATH`/`APPLICATION_TYPE_PROCESS`/`ADDITIONAL_PROCESS` carry decider, appeal body, non-decision clock, and post-Bill 23/185 third-party-appeal status. **Framing only** — never feeds the confidence number. OZ splits 90 (standalone ZBA) vs 120 (combined OPA) via `is_combined`; MV→s.45, CO→s.53 mapped; TLAB intentionally unmapped. Day counts verified 2026-06-13 vs Ontario's Citizens' Guide (see `_SOURCE_NOTE`); re-confirm s.41 site-plan status against e-Laws.

**Eval:** `just planning-act-eval` (`scripts/planning_act_eval.py`) — deterministic confusion matrix of derived process vs actual `application_type` over the enriched corpus (ground truth = process the applicant filed). Reports OZ rezoning recall **split by `dev_approved`** (approved OZ may have upzoned its own site → temporal drift; clean vs drift came out ~identical at ~10.4%, so the low recall is real blindness, not drift — a lower bound since the batch set lacks `zoning_max_height_m`/`permitted_use_category`), rezoning **precision** (~65%), data-gap vs engine-gap miss split, orthogonal-process trigger rates, and (item 4b) **OP-conformity coverage + OZ detection lift** (`op_coverage`, `OZ_recall_path_only` vs `OZ_recall_with_op`) — the OP signal fires in the batch set because `op_land_use_designation` is enriched (unlike `permitted_use_category`), adding an orthogonal OPA-detection axis on top of the zoning path. Since the layer doesn't move the confidence number, this process-match harness — not the band-based `narrator-eval` — is how its correctness is tracked; floors pinned in `tests/analytics/test_planning_act_eval.py` (integration). See `specs/2026-06-13-planning-act-integration.md`.

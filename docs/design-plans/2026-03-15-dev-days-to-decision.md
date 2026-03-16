# Dev Application Days-to-Decision Design

## Overview

Predict how many days a Toronto development application (OZ or SA type) will take
from submission to planning decision. This requires: (1) scraping decision dates from
the city's Application Information Centre, (2) engineering survival labels for both
closed and active applications, and (3) training a gradient-boosted survival model.

**Goals:**
- Deliver `pred_dev_days_to_decision` (predicted median days) in `data/scores/dev_applications.parquet`
- Use all OZ+SA applications — closed (events) and active (right-censored) — not just closed
- Evaluate via Harrell's c-index; gate production readiness at c-index ≥ 0.65

## Architecture

AIC scraping is a reference-data fetch step (like `fetch_reference`), not a CKAN source.
Decision dates are cached in `data/reference/aic_decisions.parquet`, joined to enriched
dev_applications during `enrich_dev()`. The survival model lives in the existing training
and scoring pipeline with two new functions: `train_survival()` and `evaluate_survival()`.

**Data flow:**

```
dev_applications (CKAN parquet)
  + aic_decisions.parquet (scraped from app.toronto.ca/AIC)
    → enrich_dev() → dev_days_to_decision, dev_decision_event, dev_days_observed
      → train_survival() → dev_days_to_decision.joblib
        → score_all() → pred_dev_days_to_decision column
```

**Key decisions:**
- OZ + SA only (11,436 + 9,995 applications). Other types excluded by `application_type` filter.
- "Decision date" = "City Council Decision Made" for OZ; "Statement of Approval Issued" for SA.
  These are the final AIC milestones for each type. "By-law in Force" is out of scope: the AIC
  does not track it and no structured open-data equivalent exists.
- Active applications (4,217 rows) are right-censored: `dev_decision_event=False`,
  `dev_days_observed = today - date_submitted`. They contribute to the survival model.
- Outlier cap: `dev_days_to_decision > 3,650 days` → null (same rationale as 730-day COA cap).

## Existing Patterns

The design follows established patterns throughout:

**Reference-data fetching** (`analytics/enrich.py`): `fetch_aic_decisions()` mirrors `_fetch_ward_profiles_csv()` and `_download()` — downloads to `data/reference/`, idempotent (skips existing), called from `enrich`. `beautifulsoup4` is a new dependency; `httpx` is already present.

**Enrichment columns** (`analytics/enrich.py`): New label columns (`dev_days_to_decision`, `dev_decision_event`, `dev_days_observed`) follow the same `with_columns()` / `map_elements()` pattern as existing labels (`dev_approved`, `dev_appealed`, `coa_days_to_approval`).

**Training functions** (`analytics/train.py`): `train_survival()` and `evaluate_survival()` follow the same signature as `train_source()` and `evaluate_source()` (path, columns, name, dir → count / metrics dict). `train_all()` calls the new survival function and writes `production_ready` to metrics.json the same way.

**Scoring** (`analytics/score.py`): `_predict_survival_median()` follows the `_predict_regressor()` pattern; `score_all()` appends `pred_dev_days_to_decision` to the dev_applications scored parquet.

**Divergence from existing pattern — model estimator:** The existing pipeline uses `HistGradientBoostingRegressor`/`Classifier` from sklearn. The survival model uses `GradientBoostingSurvivalAnalysis` from scikit-survival, which requires a structured numpy label array `[(event: bool, time: int)]`. This is the only new estimator family in the codebase. The `ColumnTransformer` preprocessing and `OrdinalEncoder` strategy are unchanged.

**Protocol rename:** `Source` in `sources/base.py` is renamed to `CKANSource` to clarify that the protocol governs CKAN ingest only. The AIC scraper deliberately does not implement this protocol.

## Implementation Phases

### Phase 1: Rename Source Protocol to CKANSource

**Goal:** Clarify that the Source protocol governs CKAN ingest only, before adding a
non-CKAN data source that could otherwise be confused with it.

**Components:**
- Modify: `src/zoneto/sources/base.py` — rename `Source` class/protocol to `CKANSource`
- Modify: `src/zoneto/sources/ckan.py` — update `CKANSource` implementation reference
- Modify: `src/zoneto/sources/registry.py` — update `SOURCES: dict[str, CKANSource]`
- Modify: `src/zoneto/cli.py` — update any `Source` type annotations
- Modify: `CLAUDE.md` — update all references to `Source` protocol
- Modify: `tests/test_registry.py` — update protocol name references

**Dependencies:** None (pure rename, no new functionality)

**Done when:** `just test` passes with no changes to test logic; `just lint` clean.

---

### Phase 2: AIC Scraper Module

**Goal:** Implement and test `fetch_aic_decisions()` — the function that scrapes
decision milestone dates from the city's AIC portal and caches them locally.

**Components:**
- Add: `beautifulsoup4` to `pyproject.toml` (runtime dependency)
- Add: `scikit-survival` to `pyproject.toml` (runtime dependency — added here so Phase 3
  can import it; no model code yet)
- Create: `src/zoneto/sources/aic.py`
  - `fetch_aic_decisions(data_dir: Path, *, delay: float = 1.0) -> int`
    - Reads OZ+SA rows with non-null `application_url` from `data/dev_applications` parquet
    - Loads existing `data/reference/aic_decisions.parquet` if present; skips already-scraped `folderrsn` values
    - For each un-scraped application: fetches AIC page via `httpx`, parses HTML with `beautifulsoup4`
    - Extracts `decision_date` (OZ: "City Council Decision Made"; SA: "Statement of Approval Issued") and `complete_date` ("Notice of Complete Application Issued")
    - Stores result parquet schema: `folderrsn (String), decision_date (Date | null), complete_date (Date | null), scraped_at (Date)`
    - Sleeps `delay` seconds between requests; skips with logged warning on HTTP error or parse failure
    - Returns count of newly scraped rows
- Create: `tests/test_aic.py`
  - Mock `httpx` responses via `pytest-httpx` (synthetic AIC milestone HTML fixture)
  - Test: OZ application extracts correct `decision_date`
  - Test: SA application extracts correct `decision_date` (different milestone name)
  - Test: Already-scraped rows are skipped (idempotency)
  - Test: HTTP errors logged and skipped without crash
  - Test: Missing milestone produces null `decision_date`

**Dependencies:** Phase 1 (clean rename before adding new files)

**Done when:** All `tests/test_aic.py` tests pass; `just lint` clean.

---

### Phase 3: Label Engineering and Feature Update

**Goal:** Extend `enrich_dev()` to join AIC decision dates and compute survival labels.
Add `is_combined_application` feature. Update `DEV_NUM_COLS` in `features.py`.

**Components:**
- Modify: `src/zoneto/analytics/enrich.py`
  - `enrich_dev()`: after spatial join, left-join `data/reference/aic_decisions.parquet`
    on `folderrsn`; filter to OZ+SA for label computation only
  - Add column `dev_days_to_decision (Int32 | null)`: `decision_date - date_submitted`
    for closed OZ+SA with valid decision_date; null otherwise; cap at 3,650 days
  - Add column `dev_decision_event (Int8 | null)`: 1 if decision_date present, 0 if
    active (`is_active == 1`), null for non-OZ/SA types
  - Add column `dev_days_observed (Int32 | null)`: `dev_days_to_decision` for events;
    `today - date_submitted` for censored (active); null for non-OZ/SA
  - Add column `is_combined_application (Int8)`: 1 if `application_type == "OZ"` and
    `application` or `description` field contains "OPA" (combined OPA+Rezoning); 0 otherwise
- Modify: `src/zoneto/analytics/features.py`
  - Add `"is_combined_application"` to `DEV_NUM_COLS`
- Modify: `tests/analytics/test_enrich.py`
  - `_make_dev_parquet()` fixture: add `folderrsn` column; create minimal `aic_decisions.parquet` fixture
  - Test: `dev_days_to_decision` computed correctly for OZ with decision date
  - Test: `dev_decision_event = 0` for active OZ/SA; null for non-OZ/SA
  - Test: `dev_days_observed` uses today's date for active applications
  - Test: cap at 3,650 days applied
  - Test: `is_combined_application = 1` when description contains OPA
- Modify: `tests/analytics/test_features.py`
  - Test: `is_combined_application` present in `DEV_NUM_COLS`

**Dependencies:** Phase 2 (aic_decisions.parquet must be writable/readable)

**Done when:** All modified tests pass; `just lint` clean.

---

### Phase 4: Survival Model Training

**Goal:** Implement `train_survival()`, `evaluate_survival()`, and wire into `train_all()`.

**Components:**
- Modify: `src/zoneto/analytics/train.py`
  - Add import: `from sksurv.ensemble import GradientBoostingSurvivalAnalysis`
  - Add: `train_survival(enriched_path, time_col, event_col, cat_cols, num_cols, model_name, model_dir) -> int`
    - Filters to rows where `event_col` is not null (OZ+SA only)
    - Builds structured label array `[(event: bool, time: int)]` via numpy
    - Reuses `build_pipeline()` with `GradientBoostingSurvivalAnalysis(random_state=42)` as estimator
    - No `CalibratedClassifierCV` wrapper (survival models are not calibrated)
    - Serializes to `model_dir/<model_name>.joblib`
    - Returns row count used
  - Add: `evaluate_survival(enriched_path, time_col, event_col, cat_cols, num_cols, *, cv, year_col) -> dict`
    - Uses `TimeSeriesSplit` (same temporal CV strategy as `dev_applications_appealed`)
    - Returns `{"concordance_index_mean": float, "concordance_index_std": float, "n": int}`
    - Uses `sksurv.metrics.concordance_index_censored` for scoring
  - Modify: `train_all()` — add optional survival job for `dev_days_to_decision`:
    - Calls `train_survival()` if `dev_days_observed` column present in enriched dev parquet
    - Adds `production_ready = concordance_index_mean >= 0.65` to metrics
    - Skips silently if column absent (AIC scrape not yet run)
- Modify: `tests/analytics/test_train.py`
  - Update `_make_dev_enriched()` fixture: add `dev_decision_event`, `dev_days_observed`, `is_combined_application` columns
  - Test: `train_survival()` creates `dev_days_to_decision.joblib`
  - Test: `evaluate_survival()` returns dict with `concordance_index_mean` key
  - Test: `train_all()` includes `dev_days_to_decision` in counts and metrics when label present
  - Test: `train_all()` skips survival model gracefully when label absent

**Dependencies:** Phase 3 (enriched parquet schema with survival columns)

**Done when:** All modified tests pass; `just lint` clean.

---

### Phase 5: Scoring Update

**Goal:** Add `pred_dev_days_to_decision` to batch scoring output.

**Components:**
- Modify: `src/zoneto/analytics/score.py`
  - Add: `_predict_survival_median(pipe, X) -> list[float]`
    - Calls `pipe.predict_survival_function(X)` to get per-row survival functions
    - Extracts median (time at which survival probability crosses 0.5) for each row
    - Falls back to `pipe.predict(X)` (partial hazard) if survival function median
      is undefined (survival stays > 0.5 throughout observed range)
  - Modify: `_DEV_MODELS` registry: add `("dev_days_to_decision", "pred_dev_days_to_decision", True)`
  - Modify: `score_all()`: load `dev_days_to_decision.joblib` if present; call
    `_predict_survival_median()`; append `pred_dev_days_to_decision` column
  - Modify: `score_one()`: handle `dev_days_to_decision` model via `_predict_survival_median()`
- Modify: `tests/analytics/test_score.py`
  - Update fixtures to include `dev_decision_event`, `dev_days_observed`, `is_combined_application`
  - Test: `score_all()` writes `pred_dev_days_to_decision` column when model exists
  - Test: `score_all()` skips gracefully when `dev_days_to_decision.joblib` absent
  - Test: `score_one()` returns `pred_dev_days_to_decision` for dev_applications source

**Dependencies:** Phase 4 (model must exist to score)

**Done when:** All modified tests pass; `just lint` clean.

---

### Phase 6: CLI Integration and Documentation

**Goal:** Expose `fetch_aic_decisions` as `zoneto aic` CLI command; add `--fetch-aic`
flag to `zoneto enrich`; update justfile and CLAUDE.md.

**Components:**
- Modify: `src/zoneto/cli.py`
  - Add command: `@app.command() def aic(delay: float = 1.0)` — calls
    `fetch_aic_decisions(DATA_DIR, delay=delay)`; prints Rich progress summary
    (applications scraped, skipped, failed)
  - Modify: `enrich` command — add `--fetch-aic / --no-fetch-aic` flag (default: True);
    call `fetch_aic_decisions()` before spatial enrichment when flag is set
- Modify: `justfile`
  - Add: `aic` task — `zoneto aic`
  - Update: `enrich` task — no change needed (passes through to CLI)
  - Update: `pipeline` task comment to note AIC scrape is included in enrich
- Modify: `CLAUDE.md`
  - Update `CKANSource` protocol section (renamed from `Source`)
  - Add `fetch_aic_decisions` to Enrichment section
  - Add `dev_days_to_decision` model to Training table
  - Add `pred_dev_days_to_decision` to Scoring output columns table
  - Add `zoneto aic` to CLI section
- Modify: `tests/test_cli.py`
  - Test: `zoneto aic` command runs without error (mock `fetch_aic_decisions`)
  - Test: `zoneto enrich --no-fetch-aic` skips AIC fetch

**Dependencies:** Phases 1–5 (all components must exist before wiring CLI)

**Done when:** All modified tests pass; `just test` clean; `just lint` clean; README/CLAUDE.md accurate.

## Additional Considerations

**AIC page rendering:** The AIC portal's rendering method (server-side HTML vs AJAX) is unknown without live inspection. If milestone dates load via JavaScript, `httpx` alone will not suffice and a headless browser (e.g., playwright) will be needed. This should be verified manually before beginning Phase 2 implementation. If AJAX is confirmed, Phase 2 scope expands to include playwright as a dependency.

**Scrape rate and robots.txt:** `robots.txt` at `app.toronto.ca` is inaccessible via standard HTTP; content is unknown. Default delay of 1.0s/request is conservative. The `--delay` flag allows tuning. For the full 16k OZ+SA URLs, a 1s delay means ~4.5 hours of runtime; this is expected and the idempotent design means it can be interrupted and resumed.

**`is_combined_application` signal:** Combined OPA+Rezoning applications (OZ type with OPA in description) are derived from free-text fields and may be imperfect. If the field proves noisy, it can be dropped from `DEV_NUM_COLS` without affecting any other model.

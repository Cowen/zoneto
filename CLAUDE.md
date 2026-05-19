# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-05-19 -->
<!-- Last reviewed against: main (comps spatial context fields) -->

## Purpose

Zoneto is a development application intelligence platform for Toronto. It provides development professionals with structured data on comparable planning applications, outcome patterns, and expected timelines — using ML models to rank and prioritize where the data supports it, and presenting raw data where it doesn't.

The pipeline fetches planning and permit data from the City of Toronto CKAN portal and AIC (Application Information Centre), normalizes it into Hive-partitioned Parquet files, trains ML models on enriched data, and serves predictions and comparables via a FastAPI HTTP API.

**Target user:** Development firms doing site acquisition due diligence.

## Quick Start

```bash
uv sync                    # install deps
just test                  # run pytest
just lint                  # ruff check + ty check
just sync                  # fetch all sources -> data/
just status                # show row counts and last-modified
just aic                   # scrape AIC portal for decision dates
just aic-full              # scrape AIC + fetch full application records from ArcGIS
just olt                   # scrape Ontario Land Tribunal decisions
just enrich                # enrich raw parquet with spatial + outcome labels
just train                 # train ML models from enriched parquet
just score                 # batch inference -> data/scores/
just serve                 # start FastAPI serving layer (port 8000)
just pipeline              # enrich -> train -> score in sequence
just importance <model>    # permutation feature importance for one model
just importance-all        # feature importance for all models
just regression            # performance regression tests (synthetic data, CI-safe)
just regression-integration # performance regression tests against real enriched data
just update-baselines      # regenerate tests/fixtures/model_baselines.json
just summary               # print score distributions (percentiles)
just docker-build          # build Docker image
just docker-run            # run Docker container (port 8000)
just fmt                   # ruff format
```

Run `just` with no arguments to list all available tasks. The full task definitions
are in `justfile` at the repo root.

The CLI entrypoint is `zoneto` (mapped to `zoneto.cli:app` in pyproject.toml).

### Checking data freshness

Always run `just status` before analyzing model results or running the pipeline.
It shows row counts and last-modified timestamps for all five sources.

**COA freshness caveat:** The `coa` source will always show a narrow date range
(in_date spanning ~2014–2023, heavily concentrated in 2022) even when fully synced.
This is the complete picture from the CKAN source — the city only publishes closed
application CSVs for 2022 and 2023. A 2022-heavy distribution is **not** a sign
the data needs re-syncing. A source is fresh if its Last Modified timestamp is
within the last two weeks.

## Architecture

```
src/zoneto/
  cli.py             Typer app: `sync`, `status`, `aic`, `olt`, `enrich`, `train`, `score`, `summary`, `serve` commands
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/
    aic.py           AIC scraper: fetch_aic_decisions_arcgis(), fetch_aic_applications()
    aic_source.py    AICSource: Source protocol impl for AIC ArcGIS FeatureServer
    base.py          Source protocol (runtime_checkable)
    ckan.py          CKANSource (datastore + bulk_csv modes)
    olt.py           OLT scraper: fetch_olt_decisions() for Ontario Land Tribunal cases
    registry.py      SOURCES dict -- the single source of truth for datasets
  analytics/
    __init__.py      Analytics subpackage (empty)
    explain.py       SHAP feature importance explanations
    features.py      Canonical feature column lists for ML models
    enrich.py        Reference data downloads, NLP vectorization, and enrichment pipelines
    importance.py    Feature importance (permutation + built-in gain)
    train.py         sklearn pipelines and training functions
    score.py         Batch and single-application scoring
  api/
    __init__.py      API subpackage
    app.py           FastAPI app factory with lifespan
    comps.py         DuckDB query builder for comparables
    routes.py        GET /health, GET /ready, GET /geocode, GET /comps, POST /score
static/
  index.html         Frontend: comps search, score, SHAP explanations
Dockerfile           Production container (Python 3.13-slim, uvicorn)
```

**Serving layer** (`src/zoneto/api/`):
- `GET /health` — returns `{"status": "ok"}`
- `GET /ready` — returns 200 when models + data loaded, 503 otherwise
- `GET /geocode?address=441+King+St+W` — proxies to Nominatim, returns `{lat, lon, display_name}`. Errors: 404 (not found), 504 (timeout), 502 (upstream error)
- `GET /comps?ward=10&type=OZ&lat=43.65&lon=-79.38&radius_m=500&years=5` — comparable applications
- `POST /score` — predictions from production-ready models only
- `POST /score?explain=true` — includes top-5 SHAP contributions per model

Static frontend: `static/index.html` served at `/`. Uses address search (via `/geocode`) instead of raw lat/lon inputs. Includes a "What could I build here?" panel that scores 5 hypothetical development scenarios using the nearest comp's spatial context fields.

Data flows:
- Ingest: CLI -> registry -> source.fetch() -> storage.write_source() -> data/<name>/year=YYYY/*.parquet
- Analytics: data/<name>/ -> enrich -> data/enriched/*.parquet -> train -> models/*.joblib -> score -> data/scores/*.parquet
- Serving: models/*.joblib + data/enriched/*.parquet -> FastAPI endpoints

## Contracts

### Source Protocol (`sources/base.py`)

Any data source must satisfy this `@runtime_checkable` protocol:

- `name: str` -- human/machine identifier for the source
- `fetch() -> pl.DataFrame` -- returns a normalized polars DataFrame

The returned DataFrame **must** contain at least a `year` column (Int32) and a
`source_name` column (String) so storage partitioning works.

### CKANConfig (`models.py`)

Pydantic model with four fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataset_id` | `str` | required | CKAN package name |
| `access_mode` | `Literal["datastore", "bulk_csv"]` | required | fetch strategy |
| `year_start` | `int` | 2015 | year floor: skip CSV resources and filter rows below this year |
| `year_column` | `str` | `"application_date"` | column name to extract year from; parsed to `pl.Date` before year extraction |

### Storage (`storage.py`)

- `write_source(df, name, data_dir)` -- writes Hive-partitioned Parquet under
  `data_dir/name/year=YYYY/`. Deletes existing source dir first (full replace).
  Returns 0 immediately if the DataFrame is empty. Returns row count.
- `source_row_counts(name, data_dir)` -- returns total rows or None.
- `last_modified(name, data_dir)` -- returns most recent mtime or None.

Storage uses native polars Parquet writer (not pyarrow) because polars 1.38+
creates correct Hive directories while pyarrow creates flat files.

### Registry (`sources/registry.py`)

`SOURCES: dict[str, Source]` maps logical names to Source instances:

| Key | Dataset | Mode | year_start | year_column |
|---|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 | `application_date` (default) |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 | `application_date` (default) |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2018 | `application_date` (default) |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |
| `aic_applications` | AIC ArcGIS FeatureServer | AICSource | — | — |

NOTE: Though the CKAN data source identifies `dev_applications` as retired, it still has data indicating it is being actively updated.
`aic_applications` fetches live records from the AIC ArcGIS REST API (COTGEO_IBMS_AIC_POINT),
providing a live alternative to the retired CKAN dev_applications dataset.

### CLI (`cli.py`)

- `zoneto sync [--source NAME]` -- fetches one or all sources, writes Parquet
  to `./data/`. Prints colored output via Rich.
- `zoneto status` -- prints a Rich table of row counts and last-modified times.
- `zoneto aic [--delay FLOAT] [--full/--no-full]` -- scrapes AIC portal (ArcGIS FeatureServer)
  for OZ/SA decision milestone dates. Caches results to `data/reference/aic_decisions.parquet`.
  With `--full`: also fetches complete application records to `data/aic_applications/`.
- `zoneto olt [--delay FLOAT]` -- scrapes Ontario Land Tribunal decisions for Toronto.
  Writes `data/reference/olt_decisions.parquet`. Default delay: 2.0s/request.
- `zoneto enrich [--fetch-ref/--no-fetch-ref] [--fetch-aic/--no-fetch-aic] [--fetch-olt/--no-fetch-olt]` --
  enriches raw parquet with outcome labels and spatial features. Downloads reference
  datasets to `data/reference/` if `--fetch-ref` (default). Scrapes AIC portal for
  decision dates if `--fetch-aic` (default). With `--fetch-olt`: fuzzy-matches OLT
  decisions to dev_applications (requires prior `zoneto olt` run). Enriches COA,
  dev_applications, and permits_cleared. Writes enriched parquet to `data/enriched/`.
- `zoneto train [--model-dir PATH]` -- trains 2-3 outcome-prediction models from
  enriched parquet (survival model optional). Serializes to `models/*.joblib` (default: `./models`).
  Retired models: dev_applications_approved, coa_approved, permit_issuance_days.
  coa_days_to_approval is trained for metric tracking only (not served).
- `zoneto score [--model-dir PATH]` -- runs batch inference on enriched parquet using
  trained models. Writes scored parquet to `data/scores/`.
- `zoneto summary` -- prints percentile distributions (p5/p25/p50/p75/p95) and mean
  for all `pred_*` and `prob_*` columns in scored parquet files under `data/scores/`.
- `zoneto serve [--port INT] [--host STR] [--data-dir PATH] [--model-dir PATH] [--static-dir PATH] [--reload]` --
  starts the FastAPI serving layer. Default: `0.0.0.0:8000`. With `--reload`,
  sets `ZONETO_DATA_DIR`/`ZONETO_MODEL_DIR`/`ZONETO_STATIC_DIR` env vars and
  launches uvicorn with the import string `zoneto.api.app:create_app_from_env`
  and `factory=True` so module changes hot-reload.

`DATA_DIR` defaults to `Path("data")` (cwd-relative).

### Analytics Features (`analytics/features.py`)

Canonical feature column lists for machine learning models:

- `DEV_CAT_COLS` -- categorical features for development applications (application_type, ward_number, zoning_class, secondary_plan_name)
- `DEV_NUM_COLS` -- numeric features for development applications (year_submitted, in_heritage_register, in_heritage_district, in_secondary_plan, has_community_meeting, ward_pct_renters, ward_median_income, ward_pop_density, ward_pct_detached, has_parent_application, is_combined_application, proposed_storeys, proposed_units, unit_excess_ratio, storey_excess_ratio, ward_appeal_rate_3y, in_mtsa, desc_svd_0..desc_svd_19)
- `COA_CAT_COLS` -- categorical features for COA (application_type, sub_type, ward_number, zoning_designation, planning_district, work_type)
- `COA_NUM_COLS` -- numeric features for COA (year_submitted)
- `PERMIT_CAT_COLS` -- categorical features for permits (permit_type, structure_type, ward_grid)
- `PERMIT_NUM_COLS` -- numeric features for permits (est_const_cost, dwelling_units_created, dwelling_units_lost, residential, mercantile, industrial, institutional)

### Enrichment (`analytics/enrich.py`)

Downloads reference datasets from CKAN and enriches raw source parquet:

**Reference datasets** (cached in `data/reference/`):
- Zoning (GeoJSON, full-city WGS84 — `zoning.geojson`) -- for spatial point-in-polygon join via DuckDB ST_Read.
  Fields: ZN_ZONE (zone code), UNITS (max dwelling units, -1=no limit), DENSITY (max FSI, -1=no limit).
  Data dictionary: `docs/zoning_readme.txt`
- Zoning height overlay (GeoJSON, WGS84 — `zoning_height.geojson`) -- max permitted storeys/height per area.
  Fields: HT_STORIES (max storeys, -1=no limit). Separate overlay from zoning area — fewer polygons, ~15% coverage
- Heritage register (ZIP → SHP with WGS84 points) -- flag properties in register
- Heritage districts (ZIP → SHP) -- flag properties in district
- Secondary plans (GeoJSON) -- flag properties in plan area
- MTSA boundaries (ZIP -> SHP — `mtsa/`) -- Major Transit Station Areas; flag properties in MTSA zones
- AIC decisions (scraped via `fetch_aic_decisions_arcgis()` — `data/reference/aic_decisions.parquet`)
  Schema: `folderrsn (String), decision_date (Date|null), complete_date (Date|null), scraped_at (Date)`
  OZ: "City Council Decision Made"; SA: "Statement of Approval Issued"
- OLT decisions (scraped via `fetch_olt_decisions()` — `data/reference/olt_decisions.parquet`)
  Schema: `case_number (String), municipality (String), hearing_date (Date|null), decision_date (Date|null), outcome (String), address (String), scraped_at (Date)`

**Enrichment functions**:
- `fetch_reference(data_dir)` -- downloads/extracts all reference datasets including MTSA (idempotent)
- `fetch_aic_decisions_arcgis(data_dir, *, batch_size=200)` -- scrapes AIC ArcGIS FeatureServer for OZ+SA milestone dates.
  Idempotent: skips already-scraped `folderrsn` values. Returns count of newly scraped rows.
- `fetch_aic_applications(data_dir, *, batch_size=200)` -- fetches ALL AIC application records from ArcGIS
  FeatureServer. Writes Hive-partitioned Parquet to `data/aic_applications/year=YYYY/`. Provides a live
  replacement for the retired CKAN dev_applications dataset.
- `enrich_coa(data_dir)` -- deduplicates on `reference_file` (handles consolidated CSV overlap),
  enriches COA with outcome labels, ward_number, year_submitted,
  planning_district (preserved from source), coa_approved (1/0/null), coa_days_to_approval regression target.
  `coa_days_to_approval` is capped at 730 days (values beyond → null); >730d outliers are near-certain data errors.
- `enrich_dev(data_dir)` -- enriches dev_applications with year_submitted,
  has_community_meeting, spatial features (zoning, heritage, secondary plan), dev_approved and dev_appealed labels.
  Dev application x/y are in EPSG:2952 (NAD83 / MTM Zone 10, City of Toronto internal CRS);
  `_spatial_join_dev` reprojects from EPSG:2952 → EPSG:4326 before joining zoning polygons.
  New survival columns (requires AIC scrape): `dev_days_to_decision` (Int32|null, capped at 3,650 days),
  `dev_decision_event` (Int8|null, 1=closed/0=active/null=non-OZ/SA),
  `dev_days_observed` (Int32|null, days_to_decision for events; today-submitted for censored).
  New feature: `is_combined_application` (Int8, 1 if OZ with OPA in description).
  New features: `proposed_storeys` (Int32|null, regex-extracted from description),
  `proposed_units` (Int32|null, regex-extracted from description).
  New spatial features from zoning GeoJSON: `zoning_max_units` (Int32|null, max
  allowable units from by-law UNITS field), `zoning_max_density` (Float64|null,
  max FAR from by-law DENSITY field).
  New derived feature: `unit_excess_ratio` (Float64|null, proposed_units / zoning_max_units;
  values > 1.0 indicate proposals exceeding zoning limits — a strong appeal signal).
  Per by-law data dictionary, -1 means "no limit" for UNITS/DENSITY/HT_STORIES — treated as null.
  New spatial feature from height overlay: `zoning_max_storeys` (Int32|null, max permitted storeys
  from HT_STORIES field; -1 treated as null).
  New derived feature: `storey_excess_ratio` (Float64|null, proposed_storeys / zoning_max_storeys;
  ~7% coverage vs 0.6% for unit_excess_ratio, as proposed_storeys is more commonly specified).
  New feature: `ward_appeal_rate_3y` (Float64|null, rolling 3-year appeal rate for
  the same ward using only OZ/SA data from years strictly before the application's
  year_submitted; null when no prior data exists). Temporal leakage-safe.
  `dev_appealed` label (Int8|null): restricted to OZ+SA only. 1=appeal filed, 0=closed without appeal
  (any non-active closed status), null=active/non-OZ/SA. Covers ALL closed OZ+SA apps to preserve
  the true base rate (~15-25%); previously only explicitly-approved rows got 0, causing 50/50 bias.
  New spatial feature: `in_mtsa` (Int8, 1 if application point falls within MTSA boundary polygon).
  NLP features: `desc_svd_0..desc_svd_19` (Float64) -- TF-IDF vectorization of application
  description text, reduced to 20 dimensions via TruncatedSVD. The TF-IDF+SVD pipeline is
  serialized to `models/desc_tfidf.joblib` for reuse during scoring.
- `match_olt_to_dev(dev_df, data_dir, *, confidence_threshold=0.75)` -- fuzzy-matches OLT decisions
  to dev_applications via address similarity (difflib.SequenceMatcher). Adds columns:
  `olt_case_number` (String|null), `olt_outcome` (String|null), `olt_decision_date` (Date|null).
  Indexed by street number for performance. Returns enriched DataFrame.
- `enrich_permits(data_dir)` -- enriches permits_cleared with application_year (Int32,
  from application_date year) and permit_issuance_days (Int32, issued_date - application_date
  in calendar days). Drops rows with non-positive issuance days. Writes
  data/enriched/permits_cleared.parquet. Returns row count

### Training (`analytics/train.py`)

Trains sklearn HistGradientBoosting classifiers and regressors from enriched parquet:

**Models**:
| File | Type | Target | Source | Status |
|---|---|---|---|---|
| `dev_applications_appealed.joblib` | CalibratedClassifierCV(HistGradientBoostingClassifier) | `dev_appealed` | enriched dev_applications | **production** |
| `coa_days_to_approval.joblib` | HistGradientBoostingRegressor | `coa_days_to_approval` | enriched coa | **tracking only** (not served) |
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | enriched dev_applications | optional (requires AIC scrape) |

**Retired models** (not trained or scored):
- `dev_applications_approved` -- dataset frozen (no new records), 97.3% class imbalance, +/-0.267 AUC variance
- `coa_approved` -- AUC 0.535 with 94% base rate; worse than majority class. Cannot be improved with structured features
- `permit_issuance_days` -- R-squared 0.039 on 133K rows; queue depth (primary driver) not in open data

**Pipeline architecture**:
- ColumnTransformer with OrdinalEncoder for categorical features
  (fills nulls with "__missing__", encodes unknown as -1)
- Passthrough for numeric features (HistGradientBoosting handles NaN natively)
- Classifiers are wrapped in `CalibratedClassifierCV(cv=5, method='isotonic')` by default
  (when calibrate=True and >= 20 rows). Regressors are never calibrated.
- Random seed: 42 for reproducibility

**Functions**:
- `build_pipeline(cat_cols, num_cols, estimator)` -- returns unfitted Pipeline
- `train_source(enriched_path, label_col, cat_cols, num_cols, model_name, model_dir, *, regressor, calibrate)` -- trains one model, returns row count. When calibrate=True (default) and not regressor and >= 20 rows, wraps pipeline in CalibratedClassifierCV.
- `train_survival(enriched_path, time_col, event_col, cat_cols, num_cols, model_name, model_dir)` -- trains a GradientBoostingSurvivalAnalysis model. Filters to non-null event rows (OZ+SA only). Labels are structured numpy array `[(event: bool, time: int)]`. No calibration. Returns row count.
- `evaluate_source(enriched_path, label_col, cat_cols, num_cols, *, regressor, cv, year_col)` -- temporal CV evaluation; returns per-metric mean/std dict. Uses `TimeSeriesSplit` when `year_col` is set and present (avoids future-data leakage). Caps cv at n_samples - 1 for all splitter types. Classifiers return roc_auc, neg_brier_score, avg_precision; regressors return r2, neg_mae, neg_rmse.
- `evaluate_survival(enriched_path, time_col, event_col, cat_cols, num_cols, *, cv, year_col)` -- temporal CV for survival model. Uses `concordance_index_censored` for scoring each fold. Returns concordance_index_mean/std and n.
- `train_all(data_dir, model_dir)` -- trains 2-3 models (dev_applications_appealed + coa_days_to_approval always; survival optional). Evaluates with temporal CV, returns ({model_name: row_count}, {model_name: metrics_dict}). Each metrics dict includes `production_ready` (bool): classifiers require roc_auc_mean >= 0.65; regressors require r2_mean >= 0.10. `coa_days_to_approval` is forced `production_ready: false` regardless of metrics (tracking only). Retired: dev_approved, coa_approved, permit_issuance_days.
  Optional survival model (trained if `dev_days_observed` present in enriched dev parquet):
  dev_days_to_decision. Survival model uses c-index threshold (>= 0.65) for `production_ready`.

### Scoring (`analytics/score.py`)

Batch and single-application inference from trained joblib models:

**Batch scoring** (`score_all`):
- Reads enriched parquet from `data/enriched/`, loads models from `models/`
- Checks `models/metrics.json` for `production_ready` flags; skips models where `production_ready: false`
- Only dev_applications models are scored (coa and permit models are retired/tracking-only)
- Applies NLP vectorizer (`models/desc_tfidf.joblib`) to add `desc_svd_*` columns if not already present
- For classifiers: outputs `pred_<label>` (int) and `prob_<label>` (float) columns
- For regressors: outputs `pred_<label>` (float) column only
- Survival model (`dev_days_to_decision`) only scores OZ+SA rows; non-OZ/SA rows get null.
  Outputs p25/p50/p75 percentile columns instead of single median for better UX.
- Writes scored parquet to `data/scores/dev_applications.parquet`
- Writes `data/scores/dev_applications_active.parquet` containing only active (under-review)
  applications — the commercially valuable subset for developers querying pending apps

**Single scoring** (`score_one`):
- `score_one(source, features, model_dir)` -- scores one application dict
- `source` must be `"dev_applications"`, `"coa"`, or `"permits_cleared"`
- `features` is a dict with keys matching the feature column lists
- Survival model only runs if `features["application_type"]` is `"OZ"` or `"SA"`
- Returns dict of prediction/probability values

**Output columns added by scoring**:
| Source | Column | Type | Description |
|---|---|---|---|
| dev_applications | `pred_dev_appealed` | int | 0/1 appeal prediction |
| dev_applications | `prob_dev_appealed` | float | appeal probability |
| dev_applications | `pred_dev_days_p25` | float | predicted 25th percentile days to decision (survival) |
| dev_applications | `pred_dev_days_p50` | float | predicted median days to decision (survival) |
| dev_applications | `pred_dev_days_p75` | float | predicted 75th percentile days to decision (survival) |

Note: COA and permit scoring columns are no longer produced (models retired).

### Explanations (`analytics/explain.py`)

Per-application SHAP feature contributions via TreeExplainer:

- `explain_one(source, features, model_dir, model_name, *, top_n)` -- returns top-N SHAP values for a single application
- Returns list of dicts with keys: `feature` (str), `shap_value` (float), `direction` (str: "increases_risk" or "decreases_risk")
- Note: `explain_one()` currently only supports `source="dev_applications"`. Other sources return `[]`.

### Feature Importance (`analytics/importance.py`)

Computes feature importance for trained models via two methods:

- `feature_importance(model_name, data_dir, model_dir, *, builtin, n_repeats, random_state)` -- returns polars DataFrame with columns: feature, importance_mean, importance_std (sorted descending)
- `builtin=True`: gain-based importance from HistGradientBoosting internal tree structure (fast, no data needed). Unwraps `CalibratedClassifierCV` wrapper automatically.
- `builtin=False` (default): permutation importance on enriched parquet (slower, more reliable)

**Supported models** (`_MODEL_META` registry): dev_applications_appealed, coa_approved, coa_days_to_approval, permit_issuance_days, dev_days_to_decision

Note: `dev_days_to_decision` only supports `--builtin` mode (gain-based). Permutation importance raises an error because sksurv does not support standard sklearn scorers.

### OLT Scraper (`sources/olt.py`)

Scrapes Ontario Land Tribunal decisions for Toronto:

- `fetch_olt_decisions(data_dir, *, delay=2.0, municipality="Toronto", max_pages=500)` -- paginates OLT search results, rate-limited. Writes `data/reference/olt_decisions.parquet`. Returns count of decisions fetched.
- Schema: `case_number, municipality, hearing_date, decision_date, outcome, address, scraped_at`

### AIC Source (`sources/aic_source.py`)

`AICSource` implements the Source protocol for the AIC ArcGIS FeatureServer:

- `name = "aic_applications"`
- `fetch()` calls `fetch_aic_applications()` and returns the full DataFrame
- Registered in `SOURCES` dict alongside CKAN sources

### Comps Query Builder (`api/comps.py`)

- `query_comps(enriched_path, *, application_type, ward_number, lat, lon, radius_m, years, limit)` -- queries comparable development applications from enriched Parquet via DuckDB
- Supports spatial filtering with bounding box approximation (lat/lon + radius_m)
- Deduplicates by `folderrsn` via `QUALIFY ROW_NUMBER() OVER (PARTITION BY folderrsn)`, keeping the most recent row per application
- Returns applications sorted by proximity (when lat/lon provided) or recency (year_submitted DESC)
- Returns list of dicts with: folderrsn, application_type, ward_number, zoning_class, status, year_submitted, lat, lon, dev_approved, dev_appealed, dev_days_to_decision, proposed_storeys, proposed_units, description, street_address, application_url, dist_sq, plus spatial/demographic context fields (see below)
- **Optional columns** (`_OPTIONAL_COLS`): columns that may be absent in older enriched parquet files are handled via a null-safe pattern -- each column is introspected with `DESCRIBE` and either selected from the parquet or replaced with a typed NULL. Current optional columns: application_url, in_heritage_register, in_heritage_district, in_secondary_plan, secondary_plan_name, in_mtsa, ward_pct_renters, ward_median_income, ward_pop_density, ward_pct_detached, ward_appeal_rate_3y, has_community_meeting, zoning_max_units, zoning_max_density, unit_excess_ratio, zoning_max_storeys, storey_excess_ratio

### App Factory (`api/app.py`)

- `create_app(data_dir, model_dir, static_dir)` -- creates configured FastAPI application
- Lifespan loads `production_ready` flags from `metrics.json` into app state
- Mounts `static/` directory for HTML frontend if it exists
- Static files mounted at `/` with `html=True` (serves `index.html` as default)
- `create_app_from_env() -> FastAPI` -- zero-argument factory that reads
  `ZONETO_DATA_DIR`, `ZONETO_MODEL_DIR`, and `ZONETO_STATIC_DIR` env vars
  (defaults: `data`, `models`, `static`) and delegates to `create_app(...)`.
  Required by uvicorn `--reload` mode, which needs an import string
  (`zoneto.api.app:create_app_from_env`) and cannot receive Path arguments
  directly. `zoneto serve --reload` sets the three env vars before invoking
  uvicorn with `factory=True`.

### Project Feature Extraction (`analytics/extract.py`)

`ProjectFeatures` dataclass — structured fields extracted via regex from a
free-text project description:

| Field | Type | Notes |
|---|---|---|
| `proposed_storeys` | `int \| None` | regex-extracted |
| `proposed_units` | `int \| None` | regex-extracted |
| `proposed_use` | `str \| None` | one of: residential/commercial/mixed_use/employment/institutional |
| `has_ground_floor_retail` | `bool` | |
| `description` | `str \| None` | original text (default None) |
| `proposed_height_m` | `float \| None` | regex-extracted metres value (default None) |
| `building_type` | `str \| None` | one of: apartment/duplex/triplex/fourplex/multiplex/semi_detached/townhouse/detached (default None) |

- `extract_project_features(description: str | None) -> ProjectFeatures` --
  returns a populated dataclass; missing fields are `None` / `False`.

### Description Similarity (`api/desc_similarity.py`)

Computes cosine similarity between a project description and the enriched
dev_applications corpus using the trained TF-IDF + SVD pipeline.

- `score_description_similarity(description, data_dir, model_dir, *, top_n=20, min_similarity=0.1) -> dict[str, Any] | None`
- Loads `models/desc_tfidf.joblib`, transforms description into the 20-D SVD
  space, then computes cosine similarity against `desc_svd_0..desc_svd_19`
  columns in `data/enriched/dev_applications.parquet` via DuckDB.
- Returns `None` when the TF-IDF model or enriched parquet is unavailable, when
  no SVD columns are present, or on any internal error (defensive — endpoint
  treats similarity as optional context).
- On success returns `{"top_matches": list[dict], "appeal_rate": float | None, "approval_rate": float | None, "n_similar": int}`.
  Each match contains `similarity` (rounded to 3 d.p.) plus whichever of
  `folderrsn`, `application_type`, `street_address`, `dev_appealed`, `dev_approved`
  are available.
  `appeal_rate` is the share of labelled top matches that were appealed (None when
  no labelled matches). `approval_rate` is the share of labelled top matches that
  were Council-approved (None when no labelled matches).

### Site Context Lookup (`api/site_context.py`)

- `lookup_site_context(lat, lon, data_dir) -> dict[str, Any]` -- spatial
  point-in-polygon lookup against zoning, heritage, secondary-plan, and MTSA
  GeoJSON/SHP layers in `data/reference/` via DuckDB `ST_Read`. Returns a dict
  populated with whichever of the following are resolved: `zoning_class`,
  `zoning_max_units`, `zoning_max_density`, `zoning_max_storeys`,
  `zoning_max_height_m`, `permitted_use_category`, `zoning_holding`,
  `in_heritage_register`, `in_heritage_district`, `in_secondary_plan`,
  `secondary_plan_name`, `in_mtsa`.
- **Zoning nearest-polygon fallback:** when `ST_Within(point, zoning.geom)`
  returns no hit (off-parcel geocoded coordinates, road/right-of-way points),
  the lookup retries with `ST_DWithin(point, geom, 0.002)` (~200m at Toronto's
  latitude) ordered by `ST_Distance`, snapping to the nearest zoning polygon.
  Points more than ~200m from any polygon still resolve as "unknown zone".
  Heritage, secondary-plan, and MTSA lookups remain strict `ST_Within` (no
  snapping) — those flags should not bleed across boundaries.

### Narrator (`api/narrator.py`)

- `narrate_evaluation(site, extracted, violations, chunks, llm_client, *, data_gaps=None, description_similarity=None) -> tuple[str, int | None]`
- Generates a markdown compliance summary plus an integer 0-100 confidence
  score parsed from a trailing `CONFIDENCE: <n>` line emitted by the LLM
  (returns `None` for the score if the line is missing). Parsing is lenient:
  scans backward from the last non-blank line and tolerates optional markdown
  bold (`**CONFIDENCE: 75**`) and trailing punctuation (`.`, `,`, `)`, `%`).
- The system prompt defines a tiered 0-100 confidence scale (90-100 = as-of-right,
  70-89 = strong/well-supported rezoning, 50-69 = probable rezoning with solid
  precedent, 30-49 = uncertain, 10-29 = low probability, 0-9 = effectively
  prohibited) and enumerates explicit confidence-raising and -lowering signals.
- When `data_gaps` is provided (list of human-readable strings), a "Known data
  gaps (do not speculate beyond these)" section is injected into the LLM prompt
  so the model is anchored to the missing-information scope and does not
  fabricate beyond it.
- When `description_similarity` is provided (the dict returned by
  `score_description_similarity`), a "Comparable application outcomes" section is
  injected via `_format_description_similarity()` — summarising the number of
  similar OZ/SA applications, highlighting strongest-match outcomes when
  similarity >= 0.95, and reporting `appeal_rate` and `approval_rate`. The
  section is omitted when the dict is None or `n_similar == 0`.
- LLM `max_tokens` is 800 for evaluation narration (400 for follow-up questions
  via `narrate_question`).

### Evaluate Endpoint (`api/routes.py`)

`POST /evaluate` — runs the full address-to-compliance pipeline: geocode,
site-context lookup, project-feature extraction, rule-engine compliance check,
dual-query bylaw retrieval, description-similarity scoring, and LLM narration.
Description-similarity is computed before narration and forwarded to
`narrate_evaluation` via `description_similarity=...` so the LLM can weight
comparable-application outcomes when assigning its confidence score.

`EvaluateResponse` fields:

| Field | Type | Notes |
|---|---|---|
| `lat`, `lon` | `float \| None` | geocoded coordinates |
| `site_context` | `dict[str, Any]` | from `lookup_site_context` |
| `extracted` | `dict[str, Any]` | includes `building_type` |
| `violations` | `list[ViolationResult]` | rule-engine output |
| `relevant_sections` | `list[RelevantSection]` | retrieved bylaw chunks |
| `summary_md` | `str` | LLM-generated markdown summary |
| `suggestions` | `list[str]` | suggested remedies from violations |
| `confidence_score` | `int \| None` | 0-100 from LLM, parsed by narrator |
| `data_gaps` | `list[str]` | always-populated; see below |
| `description_similarity` | `dict[str, Any] \| None` | from `score_description_similarity` |

`_compute_data_gaps(site, extracted) -> list[str]` always includes the lot
area/frontage caveat. It additionally adds:
- A height-overlay caveat when both `zoning_max_storeys` and
  `zoning_max_height_m` are `None` **and** `zoning_class` is set (i.e. the site
  is outside the Schedule B height overlay).
- A building-type caveat when `extracted.building_type is None`.

The same `data_gaps` list is passed to `narrate_evaluation` and returned in the
response so the frontend and the LLM see the same set of caveats.

## Dependencies

| Package | Role |
|---|---|
| beautifulsoup4 | HTML parsing for AIC scraper |
| duckdb | OLAP database for analytics |
| fastapi[standard] | HTTP API framework |
| httpx | HTTP client for CKAN API |
| joblib | Serialization and parallel computing for ML models |
| pandas | DataFrame interchange with scikit-learn |
| polars | DataFrames + Parquet I/O |
| pyarrow | Required by polars for Parquet support |
| pydantic | Config validation |
| pyproj | Coordinate reference system transformations |
| rich | Terminal formatting |
| scikit-learn | Machine learning library |
| scikit-survival | Survival analysis (GradientBoostingSurvivalAnalysis, concordance_index_censored) |
| shap | SHAP feature importance explanations |
| shapely | Spatial geometry operations |
| typer | CLI framework |
| uvicorn[standard] | ASGI server for FastAPI |

Dev: pytest, pytest-httpx, ruff, ty.

## Docker

Production container uses `python:3.13-slim` with `uv` for dependency management.
Build requires `data/enriched/`, `data/scores/`, `models/`, and `static/` to exist
(generated by the pipeline). Runs as non-root `appuser`.

```bash
just docker-build          # builds zoneto:latest
just docker-run            # runs on port 8000
```

## Invariants

- Python >= 3.13 required (uses `X | Y` union syntax).
- All column names are normalized to snake_case before storage; duplicate
  snake_case names get `_2`/`_3` suffixes.
- Date columns (any column name containing "date") are parsed to `pl.Date`
  best-effort; unrecognizable formats leave the column as String.
- `year` is derived from the column specified in `CKANConfig.year_column` (defaults to
  `application_date`) only if it was successfully parsed as `pl.Date`; otherwise defaults to 0.
- `fetch()` applies a rolling year filter: keeps rows with `year == 0` (unknown)
  or `year >= year_start`. Datastore mode auto-discovers the resource UUID via
  `package_show`. Bulk CSV mode skips non-CSV format resources.
- Storage is always full-replace per source (rmtree + rewrite).
- Tests use `pytest-httpx` to mock all HTTP calls; no network in CI.
- CKAN base URL: `https://ckan0.cf.opendata.inter.prod-toronto.ca`

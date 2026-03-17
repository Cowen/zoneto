# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-03-16 -->
<!-- Last reviewed against: main branch (model-critique phase: dev_appealed OZ/SA restriction, production_ready gating, summary command) -->

## Purpose

Zoneto is a CLI pipeline that fetches City of Toronto building-permit and
planning-application datasets from the city's CKAN open-data portal, normalizes
them, stores them as Hive-partitioned Parquet files, and trains ML models to
predict development application outcomes (approval likelihood, appeal risk,
and processing time).

## Quick Start

```bash
uv sync                    # install deps
just test                  # run pytest
just lint                  # ruff check + ty check
just sync                  # fetch all sources -> data/
just status                # show row counts and last-modified
just aic                   # scrape AIC portal for decision dates
just enrich                # enrich raw parquet with spatial + outcome labels
just train                 # train ML models from enriched parquet
just score                 # batch inference -> data/scores/
just pipeline              # enrich -> train -> score in sequence
just importance <model>    # permutation feature importance for one model
just importance-all        # feature importance for all models
just regression            # performance regression tests (synthetic data, CI-safe)
just regression-integration # performance regression tests against real enriched data
just update-baselines      # regenerate tests/fixtures/model_baselines.json
just summary               # print score distributions (percentiles)
just fmt                   # ruff format
```

Run `just` with no arguments to list all available tasks. The full task definitions
are in `justfile` at the repo root.

The CLI entrypoint is `zoneto` (mapped to `zoneto.cli:app` in pyproject.toml).

### Checking data freshness

Always run `just status` before analyzing model results or running the pipeline.
It shows row counts and last-modified timestamps for all four sources.

**COA freshness caveat:** The `coa` source will always show a narrow date range
(in_date spanning ~2014–2023, heavily concentrated in 2022) even when fully synced.
This is the complete picture from the CKAN source — the city only publishes closed
application CSVs for 2022 and 2023. A 2022-heavy distribution is **not** a sign
the data needs re-syncing. A source is fresh if its Last Modified timestamp is
within the last two weeks.

## Architecture

```
src/zoneto/
  cli.py             Typer app: `sync`, `status`, `aic`, `enrich`, `train`, `score`, `summary` commands
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/
    aic.py           AIC scraper: fetch_aic_decisions() for OZ/SA milestone dates
    base.py          Source protocol (runtime_checkable)
    ckan.py          CKANSource (datastore + bulk_csv modes)
    registry.py      SOURCES dict -- the single source of truth for datasets
  analytics/
    __init__.py      Analytics subpackage (empty)
    features.py      Canonical feature column lists for ML models
    enrich.py        Reference data downloads and enrichment pipelines
    importance.py    Feature importance (permutation + built-in gain)
    train.py         sklearn pipelines and training functions
    score.py         Batch and single-application scoring
```

Data flows:
- Ingest: CLI -> registry -> source.fetch() -> storage.write_source() -> data/<name>/year=YYYY/*.parquet
- Analytics: data/<name>/ -> enrich -> data/enriched/*.parquet -> train -> models/*.joblib -> score -> data/scores/*.parquet

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
| `coa` | committee-of-adjustment-applications | bulk_csv | 2015 | `application_date` (default) |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |

NOTE: Though the CKAN data source identifies `dev_applications` as retired, it still has data indicating it is being actively updated.

### CLI (`cli.py`)

- `zoneto sync [--source NAME]` -- fetches one or all sources, writes Parquet
  to `./data/`. Prints colored output via Rich.
- `zoneto status` -- prints a Rich table of row counts and last-modified times.
- `zoneto aic [--delay FLOAT]` -- scrapes AIC portal for OZ/SA decision milestone dates.
  Caches results to `data/reference/aic_decisions.parquet`. Default delay: 1.0s/request.
- `zoneto enrich [--fetch-ref/--no-fetch-ref] [--fetch-aic/--no-fetch-aic]` -- enriches raw parquet with outcome
  labels and spatial features. Downloads reference datasets to `data/reference/` if
  `--fetch-ref` (default). Scrapes AIC portal for decision dates if `--fetch-aic` (default).
  Enriches COA, dev_applications, and permits_cleared.
  Writes enriched parquet to `data/enriched/`.
- `zoneto train [--model-dir PATH]` -- trains 3-5 outcome-prediction models from
  enriched parquet (permit and survival models optional). Serializes to `models/*.joblib` (default: `./models`).
  dev_applications_approved is retired (dataset frozen, 97.3% class imbalance).
- `zoneto score [--model-dir PATH]` -- runs batch inference on enriched parquet using
  trained models. Writes scored parquet to `data/scores/`.
- `zoneto summary` -- prints percentile distributions (p5/p25/p50/p75/p95) and mean
  for all `pred_*` and `prob_*` columns in scored parquet files under `data/scores/`.

`DATA_DIR` defaults to `Path("data")` (cwd-relative).

### Analytics Features (`analytics/features.py`)

Canonical feature column lists for machine learning models:

- `DEV_CAT_COLS` -- categorical features for development applications (application_type, ward_number, zoning_class, secondary_plan_name)
- `DEV_NUM_COLS` -- numeric features for development applications (year_submitted, in_heritage_register, in_heritage_district, in_secondary_plan, has_community_meeting, ward_pct_renters, ward_median_income, ward_pop_density, ward_pct_detached, has_parent_application, is_combined_application)
- `COA_CAT_COLS` -- categorical features for COA (application_type, sub_type, ward_number, zoning_designation, planning_district, work_type)
- `COA_NUM_COLS` -- numeric features for COA (year_submitted)
- `PERMIT_CAT_COLS` -- categorical features for permits (permit_type, structure_type, ward_grid)
- `PERMIT_NUM_COLS` -- numeric features for permits (est_const_cost, dwelling_units_created, dwelling_units_lost, residential, mercantile, industrial, institutional)

### Enrichment (`analytics/enrich.py`)

Downloads reference datasets from CKAN and enriches raw source parquet:

**Reference datasets** (cached in `data/reference/`):
- Zoning (GeoJSON, full-city WGS84 — `zoning.geojson`) -- for spatial point-in-polygon join via DuckDB ST_Read
- Heritage register (ZIP → SHP with WGS84 points) -- flag properties in register
- Heritage districts (ZIP → SHP) -- flag properties in district
- Secondary plans (GeoJSON) -- flag properties in plan area
- AIC decisions (scraped via `fetch_aic_decisions()` — `data/reference/aic_decisions.parquet`)
  Schema: `folderrsn (String), decision_date (Date|null), complete_date (Date|null), scraped_at (Date)`
  OZ: "City Council Decision Made"; SA: "Statement of Approval Issued"

**Enrichment functions**:
- `fetch_reference(data_dir)` -- downloads/extracts all reference datasets (idempotent)
- `fetch_aic_decisions(data_dir, *, delay=1.0)` -- scrapes AIC portal for OZ+SA milestone dates.
  Idempotent: skips already-scraped `folderrsn` values. Returns count of newly scraped rows.
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
  `dev_appealed` label (Int8|null): restricted to OZ+SA only. 1=appeal filed, 0=closed without appeal
  (any non-active closed status), null=active/non-OZ/SA. Covers ALL closed OZ+SA apps to preserve
  the true base rate (~15-25%); previously only explicitly-approved rows got 0, causing 50/50 bias.
- `enrich_permits(data_dir)` -- enriches permits_cleared with application_year (Int32,
  from application_date year) and permit_issuance_days (Int32, issued_date - application_date
  in calendar days). Drops rows with non-positive issuance days. Writes
  data/enriched/permits_cleared.parquet. Returns row count

### Training (`analytics/train.py`)

Trains sklearn HistGradientBoosting classifiers and regressors from enriched parquet:

**Models**:
| File | Type | Target | Source | Label filter |
|---|---|---|---|---|
| `dev_applications_appealed.joblib` | CalibratedClassifierCV(HistGradientBoostingClassifier) | `dev_appealed` | enriched dev_applications | drop null |
| `coa_approved.joblib` | CalibratedClassifierCV(HistGradientBoostingClassifier) | `coa_approved` | enriched coa | drop null |
| `coa_days_to_approval.joblib` | HistGradientBoostingRegressor | `coa_days_to_approval` | enriched coa | drop null |
| `permit_issuance_days.joblib` | HistGradientBoostingRegressor | `permit_issuance_days` | enriched permits_cleared | drop null (optional, skip if absent) |
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | enriched dev_applications | OZ+SA only (null event = excluded); trained only if AIC scraped |

Note: `dev_applications_approved` is retired — dataset frozen (no new records), 97.3% class imbalance, ±0.267 AUC variance. Not trained or scored.

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
- `train_all(data_dir, model_dir)` -- trains 3-5 models (dev_approved retired; permit and survival models optional), evaluates with temporal CV, returns ({model_name: row_count}, {model_name: metrics_dict}). Each metrics dict includes `production_ready` (bool): classifiers require roc_auc_mean >= 0.65; regressors require r2_mean >= 0.10 (raised from 0.0 — models explaining <10% of variance are not useful).
  Optional survival model (trained if `dev_days_observed` present in enriched dev parquet):
  dev_days_to_decision. Survival model uses c-index threshold (>= 0.65) for `production_ready`.

### Scoring (`analytics/score.py`)

Batch and single-application inference from trained joblib models:

**Batch scoring** (`score_all`):
- Reads enriched parquet from `data/enriched/`, loads models from `models/`
- Checks `models/metrics.json` for `production_ready` flags; skips models where `production_ready: false`
- For classifiers: outputs `pred_<label>` (int) and `prob_<label>` (float) columns
- For regressors: outputs `pred_<label>` (float) column only
- Survival model (`dev_days_to_decision`) only scores OZ+SA rows; non-OZ/SA rows get null
- Writes scored parquet to `data/scores/dev_applications.parquet`, `data/scores/coa.parquet`,
  and optionally `data/scores/permits_cleared.parquet` (skips if enriched file absent)

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
| dev_applications | `pred_dev_days_to_decision` | float | predicted median days to decision (survival model) |
| coa | `pred_coa_approved` | int | 0/1 approval prediction |
| coa | `prob_coa_approved` | float | approval probability |
| coa | `pred_coa_days_to_approval` | float | predicted days to approval |
| permits_cleared | `pred_permit_issuance_days` | float | predicted days to permit issuance |

### Feature Importance (`analytics/importance.py`)

Computes feature importance for trained models via two methods:

- `feature_importance(model_name, data_dir, model_dir, *, builtin, n_repeats, random_state)` -- returns polars DataFrame with columns: feature, importance_mean, importance_std (sorted descending)
- `builtin=True`: gain-based importance from HistGradientBoosting internal tree structure (fast, no data needed). Unwraps `CalibratedClassifierCV` wrapper automatically.
- `builtin=False` (default): permutation importance on enriched parquet (slower, more reliable)

**Supported models** (`_MODEL_META` registry): dev_applications_appealed, coa_approved, coa_days_to_approval, permit_issuance_days, dev_days_to_decision

Note: `dev_days_to_decision` only supports `--builtin` mode (gain-based). Permutation importance raises an error because sksurv does not support standard sklearn scorers.

## Dependencies

| Package | Role |
|---|---|
| beautifulsoup4 | HTML parsing for AIC scraper |
| duckdb | OLAP database for analytics |
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
| shapely | Spatial geometry operations |
| typer | CLI framework |

Dev: pytest, pytest-httpx, ruff, ty.

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

# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-03-04 -->
<!-- Last reviewed against: development-outcome-prediction branch (Phase 4) -->

## Purpose

Zoneto is a CLI pipeline that fetches City of Toronto building-permit and
planning-application datasets from the city's CKAN open-data portal, normalizes
them, and stores them as Hive-partitioned Parquet files.

## Quick Start

```bash
uv sync                    # install deps
just test                  # run pytest
just lint                  # ruff check + ty check
just sync                  # fetch all sources -> data/
just status                # show row counts and last-modified
just enrich                # enrich raw parquet with spatial + outcome labels
just train                 # train ML models from enriched parquet
just score                 # batch inference -> data/scores/
just pipeline              # enrich -> train -> score in sequence
```

The CLI entrypoint is `zoneto` (mapped to `zoneto.cli:app` in pyproject.toml).

## Architecture

```
src/zoneto/
  cli.py             Typer app: `sync`, `status`, `enrich`, `train`, `score` commands
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/
    base.py          Source protocol (runtime_checkable)
    ckan.py          CKANSource (datastore + bulk_csv modes)
    registry.py      SOURCES dict -- the single source of truth for datasets
  analytics/
    __init__.py      Analytics subpackage (empty)
    features.py      Canonical feature column lists for ML models
    enrich.py        Reference data downloads and enrichment pipelines
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
| `coa` | committee-of-adjustment-applications | bulk_csv | 2020 | `application_date` (default) |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |

### CLI (`cli.py`)

- `zoneto sync [--source NAME]` -- fetches one or all sources, writes Parquet
  to `./data/`. Prints colored output via Rich.
- `zoneto status` -- prints a Rich table of row counts and last-modified times.
- `zoneto enrich [--fetch-ref/--no-fetch-ref]` -- enriches raw parquet with outcome
  labels and spatial features. Downloads reference datasets to `data/reference/` if
  `--fetch-ref` (default). Writes enriched parquet to `data/enriched/`.
- `zoneto train [--model-dir PATH]` -- trains all 4 outcome-prediction models from
  enriched parquet. Serializes to `models/*.joblib` (default: `./models`).
- `zoneto score [--model-dir PATH]` -- runs batch inference on enriched parquet using
  trained models. Writes scored parquet to `data/scores/`.

`DATA_DIR` defaults to `Path("data")` (cwd-relative).

### Analytics Features (`analytics/features.py`)

Canonical feature column lists for machine learning models:

- `DEV_CAT_COLS` -- categorical features for development applications
- `DEV_NUM_COLS` -- numeric features for development applications
- `COA_CAT_COLS` -- categorical features for committee-of-adjustment applications
- `COA_NUM_COLS` -- numeric features for committee-of-adjustment applications

### Enrichment (`analytics/enrich.py`)

Downloads reference datasets from CKAN and enriches raw source parquet:

**Reference datasets** (cached in `data/reference/`):
- Zoning (CSV with GeoJSON) -- for spatial point-in-polygon join
- Heritage register (ZIP → SHP with WGS84 points) -- flag properties in register
- Heritage districts (ZIP → SHP) -- flag properties in district
- Secondary plans (GeoJSON) -- flag properties in plan area

**Enrichment functions**:
- `fetch_reference(data_dir)` -- downloads/extracts all reference datasets (idempotent)
- `enrich_coa(data_dir)` -- enriches COA with outcome labels, ward_number, year_submitted,
  coa_approved (1/0/null), coa_days_to_approval regression target
- `enrich_dev(data_dir)` -- enriches dev_applications with year_submitted, has_community_meeting,
  spatial features (zoning, heritage, secondary plan), dev_approved and dev_no_appeal labels

### Training (`analytics/train.py`)

Trains sklearn HistGradientBoosting classifiers and regressors from enriched parquet:

**Models**:
| File | Type | Target | Source | Label filter |
|---|---|---|---|---|
| `dev_applications_approved.joblib` | HistGradientBoostingClassifier | `dev_approved` | enriched dev_applications | drop null |
| `dev_applications_no_appeal.joblib` | HistGradientBoostingClassifier | `dev_no_appeal` | enriched dev_applications | drop null |
| `coa_approved.joblib` | HistGradientBoostingClassifier | `coa_approved` | enriched coa | drop null |
| `coa_days_to_approval.joblib` | HistGradientBoostingRegressor | `coa_days_to_approval` | enriched coa | drop null |

**Pipeline architecture**:
- ColumnTransformer with OrdinalEncoder for categorical features
  (fills nulls with "__missing__", encodes unknown as -1)
- Passthrough for numeric features (HistGradientBoosting handles NaN natively)
- Random seed: 42 for reproducibility

**Functions**:
- `build_pipeline(cat_cols, num_cols, estimator)` -- returns unfitted Pipeline
- `train_source(enriched_path, label_col, cat_cols, num_cols, model_name, model_dir, *, regressor)` -- trains one model, returns row count
- `train_all(data_dir, model_dir)` -- trains all 4 models, returns {model_name: row_count}

### Scoring (`analytics/score.py`)

Batch and single-application inference from trained joblib models:

**Batch scoring** (`score_all`):
- Reads enriched parquet from `data/enriched/`, loads models from `models/`
- For classifiers: outputs `pred_<label>` (int) and `prob_<label>` (float) columns
- For regressors: outputs `pred_<label>` (float) column only
- Writes scored parquet to `data/scores/dev_applications.parquet` and `data/scores/coa.parquet`

**Single scoring** (`score_one`):
- `score_one(source, features, model_dir)` -- scores one application dict
- `source` must be `"dev_applications"` or `"coa"`
- `features` is a dict with keys matching the feature column lists
- Returns dict of prediction/probability values

**Output columns added by scoring**:
| Source | Column | Type | Description |
|---|---|---|---|
| dev_applications | `pred_dev_approved` | int | 0/1 approval prediction |
| dev_applications | `prob_dev_approved` | float | approval probability |
| dev_applications | `pred_dev_no_appeal` | int | 0/1 no-appeal prediction |
| dev_applications | `prob_dev_no_appeal` | float | no-appeal probability |
| coa | `pred_coa_approved` | int | 0/1 approval prediction |
| coa | `prob_coa_approved` | float | approval probability |
| coa | `pred_coa_days_to_approval` | float | predicted days to approval |

## Dependencies

| Package | Role |
|---|---|
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

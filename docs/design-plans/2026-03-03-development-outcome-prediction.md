# Development Outcome Prediction Design

## Overview

Build an analytical layer on top of the zoneto pipeline that predicts the likelihood of
success for Toronto development applications. The layer enriches raw application data with
spatial and demographic features, trains scikit-learn models offline, and exposes predictions
via new CLI commands and enriched Parquet files.

Goals:
- Produce three predictions per application: probability of approval, probability of
  approval without appeal, and estimated days to approval
- Cover two source types: `dev_applications` (OZ/SA/CD/SB/PL types) and `coa` (Minor
  Variances and Consents)
- Provide a `zoneto score` CLI command for both batch scoring of all known applications
  and ad-hoc scoring of a single hypothetical application

Success criteria:
- `zoneto enrich` produces `data/enriched/dev_applications.parquet` and
  `data/enriched/coa.parquet` with feature columns and outcome labels
- `zoneto train` produces 6 fitted sklearn pipelines saved under `models/`
- `zoneto score` prints predictions for all known applications (batch) or a single
  hypothetical application (ad-hoc via flags)

## Architecture

The analytical layer is a new `src/zoneto/analytics/` subpackage. Three new CLI commands
are added to the existing `cli.py` via `@app.command()` decorators. Data flows through
four stages on top of the existing ETL pipeline:

```
zoneto sync     (existing) → data/dev_applications/, data/coa/
zoneto enrich   (new)      → data/enriched/
zoneto train    (new)      → models/
zoneto score    (new)      → stdout (+ predictions written into data/enriched/)
```

**New subpackage structure:**

```
src/zoneto/analytics/
  __init__.py
  enrich.py      # fetch reference datasets; spatial + ward joins; outcome labels
  train.py       # sklearn pipeline training; joblib persistence
  score.py       # batch and ad-hoc prediction
  features.py    # shared feature column lists (cat_cols, num_cols per source)
```

**Reference datasets** (downloaded once by `enrich`, cached in `data/reference/`):

| Dataset | Toronto Open Data slug | Format | Join method |
|---------|------------------------|--------|-------------|
| Zoning By-law | `zoning-by-law` | GeoJSON polygons | point-in-polygon |
| Heritage Register | `heritage-register` | CSV | address match |
| Heritage Conservation Districts | `heritage-conservation-districts` | GeoJSON polygons | point-in-polygon |
| Secondary Plans | `secondary-plans` | GeoJSON polygons | point-in-polygon |
| Ward Profiles | `ward-profiles` | CSV | ward_number join |

**Spatial coordinate handling:** dev_applications `x`/`y` columns are UTM strings in
NAD83 / UTM Zone 17N (EPSG:26917). Toronto Open Data GeoJSON files use WGS84 (EPSG:4326).
`pyproj` reprojects the coordinates before loading into DuckDB. DuckDB's spatial
extension (`ST_Within`) performs point-in-polygon joins.

**COA spatial limitation:** COA applications have no x/y coordinates. COA enrichment
uses ward profile joins only (non-spatial). The `zoning_designation` field already present
in raw COA data is passed through as-is.

**6 trained models** — one per (source × metric):

| Model file | Target | Algorithm | Training filter |
|-----------|--------|-----------|-----------------|
| `dev_applications_approved.joblib` | `approved` (binary) | HistGradientBoostingClassifier | exclude in-progress statuses |
| `dev_applications_no_appeal.joblib` | `appeal_received=0` (binary) | HistGradientBoostingClassifier | approved only |
| `dev_applications_days_to_approval.joblib` | days from submission to close (int) | HistGradientBoostingRegressor | approved only |
| `coa_approved.joblib` | `approved` (binary) | HistGradientBoostingClassifier | exclude in-progress |
| `coa_no_appeal.joblib` | `omb_descision` null (binary) | HistGradientBoostingClassifier | decided only |
| `coa_days_to_approval.joblib` | `finaldate - in_date` (int) | HistGradientBoostingRegressor | decided only |

`HistGradientBoostingClassifier`/`Regressor` are chosen because they handle missing
values natively (no imputation step) and perform well on mixed categorical/numeric
tabular data. Each model is wrapped in a `Pipeline([ColumnTransformer, estimator])`
and saved as a single joblib file so inference only requires `joblib.load` + `.predict`.

**`zoneto score` modes:**

- *Batch* (no flags): loads `data/enriched/`, runs all 6 models, writes `p_approved`,
  `p_no_appeal`, `est_days_to_approval` columns back into enriched Parquet, prints a
  Rich summary table.
- *Ad-hoc* (flags `--source`, `--application-type`, `--ward`, `--x`, `--y`, `--year`):
  constructs a single-row feature DataFrame, resolves spatial features via shapely
  point-in-polygon (faster than DuckDB for one point), returns JSON to stdout.

**New justfile recipes:** `enrich`, `train`, `score` (mirrors existing `sync`, `status`).

## Existing Patterns

Investigation found these patterns already in use; this design follows all of them:

- **Typer CLI** (`cli.py`): subcommands added with `@app.command()`. `Console()` from
  Rich used for all terminal output. New commands follow the same pattern.
- **Hive-partitioned Parquet** (`storage.py`): enriched Parquet files land in
  `data/enriched/` as flat files (no year partitioning — enriched data is not
  partitioned by year since it combines across all years).
- **Pydantic config** (`models.py`): existing `CKANConfig` model is unchanged. No new
  config models are required for the analytics layer.
- **No geospatial or ML code exists yet.** This design introduces DuckDB, scikit-learn,
  joblib, and pyproj as new dependencies.

## Implementation Phases

### Phase 1: Project setup

**Goal:** Add new dependencies and create the analytics subpackage skeleton.

**Components:**
- Modify: `pyproject.toml` — add `duckdb>=1.0`, `scikit-learn>=1.4`, `joblib>=1.3`,
  `pyproj>=3.6`
- Create: `src/zoneto/analytics/__init__.py` (empty)
- Create: `src/zoneto/analytics/features.py` — define `DEV_CAT_COLS`, `DEV_NUM_COLS`,
  `COA_CAT_COLS`, `COA_NUM_COLS` as module-level constants (lists of column names)

**Dependencies:** Phase 0 (zoneto sync must have run to populate `data/`)

**Done when:** `uv sync` succeeds; `python -c "import duckdb, sklearn, joblib, pyproj"`
runs without error.

---

### Phase 2: Enrichment pipeline

**Goal:** Fetch all reference datasets, perform spatial and ward joins, derive outcome
labels, write `data/enriched/dev_applications.parquet` and `data/enriched/coa.parquet`.

**Components:**
- Create: `src/zoneto/analytics/enrich.py`
  - `fetch_reference(data_dir)` — downloads 5 reference datasets to
    `data_dir/reference/` if not already present; uses `httpx` (already a dep)
  - `enrich_dev(data_dir)` — reads raw dev Parquet; reprojects x/y with pyproj;
    DuckDB spatial joins for zoning class, heritage district, secondary plan;
    ward profile join; outcome label columns; writes enriched Parquet
  - `enrich_coa(data_dir)` — reads raw COA Parquet; ward profile join; outcome label
    columns; writes enriched Parquet
- Modify: `src/zoneto/cli.py` — add `zoneto enrich` command calling both enrich functions
- Create: `data/reference/` directory (gitignored)
- Create: `data/enriched/` directory (gitignored)

**Outcome label logic:**
- `approved`: dev_applications `status` in known-approved values (to be determined by
  inspecting unique status values during implementation); COA `c_of_a_descision`
  contains "Approved"
- `appeal_received`: dev_applications `status` contains "Appeal" or "OMB"; COA
  `omb_descision` is not null/empty
- `days_to_approval`: dev_applications `STRPTIME(date_submitted, '%Y-%m-%dT%H:%M:%S')`
  to close date; COA `finaldate - in_date`

**Dependencies:** Phase 1

**Done when:** `zoneto enrich` runs without error; enriched Parquet files exist and
contain at least `approved`, `appeal_received`, `days_to_approval` columns alongside
all feature columns.

---

### Phase 3: Training pipeline

**Goal:** Train 6 sklearn pipelines on enriched data and save to `models/`.

**Components:**
- Create: `src/zoneto/analytics/train.py`
  - `build_pipeline(cat_cols, num_cols, task)` — returns a `Pipeline` with
    `ColumnTransformer` (OrdinalEncoder for categoricals, passthrough for numerics)
    and `HistGradientBoostingClassifier` or `HistGradientBoostingRegressor`
  - `train_source(source, enriched_path, models_dir)` — trains all 3 models for a
    source, applies training set filters, saves joblib files
- Modify: `src/zoneto/cli.py` — add `zoneto train` command
- Create: `models/` directory (gitignored)

**Training set filters per model:**
- `_approved`: drop rows where `approved` is null (in-progress applications have no
  definitive outcome)
- `_no_appeal`: filter to `approved == 1` only
- `_days_to_approval`: filter to `approved == 1` and `days_to_approval > 0`

**Dependencies:** Phase 2

**Done when:** `zoneto train` completes without error; `models/` contains 6 `.joblib`
files; each file loads cleanly with `joblib.load()` and responds to `.predict()`.

---

### Phase 4: Score CLI

**Goal:** Implement `zoneto score` with batch and ad-hoc modes.

**Components:**
- Create: `src/zoneto/analytics/score.py`
  - `score_all(data_dir, models_dir)` — loads enriched Parquet, runs all models,
    writes prediction columns back to enriched Parquet, returns summary stats
  - `score_one(source, features_dict, models_dir)` — constructs single-row DataFrame,
    loads models, returns dict of predictions
- Modify: `src/zoneto/cli.py` — add `zoneto score` command with optional flags
  (`--source`, `--application-type`, `--ward`, `--x`, `--y`, `--year`)
- Modify: `justfile` — add `enrich`, `train`, `score` recipes

**Ad-hoc spatial resolution:** single-point lookups use `shapely.geometry.Point` +
`shapely.prepared.prep(polygon).contains(point)` against cached GeoJSON loaded into
memory — faster than spinning up a DuckDB connection for one row.

**Dependencies:** Phase 3

**Done when:** `zoneto score` (batch) prints a Rich table and writes prediction columns;
`zoneto score --source dev_applications --application-type OZ --ward 10 --x 308668 --y 4834783 --year 2025`
prints JSON with `p_approved`, `p_no_appeal`, `est_days_to_approval`.

## Additional Considerations

**Status value mapping:** The exact set of `status` values in `dev_applications` that
constitute "approved" vs "in-progress" vs "refused" must be determined by inspecting
the live data during Phase 2 implementation. The enrich step should log the full status
distribution before applying the label mapping.

**Model quality:** With ~26k dev_applications rows and ~5k COA rows, and only a subset
having definitive outcomes, training set sizes will be modest. Feature importances should
be printed after training to catch degenerate models.

**`data/reference/` staleness:** Reference datasets (zoning, heritage) change
infrequently. Re-running `zoneto enrich` re-uses cached files. To force a re-download,
delete `data/reference/`.

**Gitignore additions:** `data/reference/`, `data/enriched/`, `models/` are all derived
data and should not be committed.

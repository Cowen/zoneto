# Toronto Building Data Pipeline Design

## Overview

A src-layout Python package (`zoneto`) that fetches Toronto building application data from the City of Toronto Open Data CKAN API and stores it locally as Hive-partitioned Parquet files. The pipeline supports three datasets (building permits active, building permits cleared, committee of adjustment applications) and is designed to add new data sources without modifying the CLI or storage layers.

**Goals:**
- Build a local dataset of Toronto building applications from 2015 to present
- Support periodic re-runs to keep data current (full re-download strategy)
- Be extensible for future scraping-based sources

## Architecture

Source protocol pattern: each data source implements a typed `Source` protocol with a `fetch() -> pl.DataFrame` method. A central registry maps source names to instances. The CLI and storage layers interact only with the protocol, not concrete implementations.

Two CKAN access modes handle the datasets:
- **datastore**: Paginated `datastore_search` API calls for rolling snapshots (active permits)
- **bulk_csv**: `package_show` to discover year-based CSV resources, then download and concatenate (cleared permits, COA)

Data flows: fetch raw → normalize (snake_case columns, typed dates, derived `year`) → write partitioned Parquet.

```
CKAN API
  └─> CKANSource.fetch()
        └─> _normalize()
              └─> storage.write_source()
                    └─> data/<source>/year=YYYY/0.parquet
```

## Existing Patterns

This is a greenfield project. No existing codebase patterns to follow. The design introduces:
- `src/` layout with `zoneto` package
- `typing.Protocol` for source abstraction
- Hive-partitioned Parquet under `data/` (gitignored)

## Implementation Phases

### Phase 1: Project Scaffolding
**Goal:** Installable package with working CLI skeleton and dev tooling configured.

**Components:**
- Create: `pyproject.toml` — package metadata, dependencies (polars, httpx, typer, pydantic, rich), dev dependencies (ruff, ty, pytest, pytest-httpx), ruff + ty config, entry point `zoneto = "zoneto.cli:app"`
- Create: `justfile` — `sync`, `status`, `test`, `lint`, `fmt` recipes
- Create: `.gitignore` — `data/`, `__pycache__/`, `.venv/`, `*.pyc`
- Create: `src/zoneto/__init__.py`
- Create: `src/zoneto/cli.py` — Typer app with stubbed `sync` and `status` commands that print placeholder text
- Create: `README.md` — brief description and quickstart

**Dependencies:** None

**Done when:** `uv sync` succeeds; `uv run zoneto sync` prints placeholder output; `uv run zoneto status` prints placeholder output; `uv run ruff check src/` passes; `uv run ty check src/` passes

---

### Phase 2: Source Protocol and Config Models
**Goal:** Typed Source protocol and CKANConfig model that form the contract for all data sources.

**Components:**
- Create: `src/zoneto/sources/__init__.py`
- Create: `src/zoneto/sources/base.py` — `Source` protocol (`name: str`, `fetch() -> pl.DataFrame`)
- Create: `src/zoneto/models.py` — `CKANConfig` pydantic model (`dataset_id: str`, `access_mode: Literal["datastore", "bulk_csv"]`, `year_start: int = 2015`)
- Create: `tests/__init__.py`
- Create: `tests/test_models.py` — validate `CKANConfig` construction and field constraints

**Dependencies:** Phase 1

**Done when:** `pytest tests/test_models.py` passes; `uv run ty check src/` passes with no errors on `base.py` and `models.py`

---

### Phase 3: CKAN Datastore Source
**Goal:** `CKANSource` in datastore mode fetches paginated records from the CKAN `datastore_search` endpoint.

**Components:**
- Create: `src/zoneto/sources/ckan.py` — `CKANSource` class:
  - `__init__(self, config: CKANConfig)`
  - `name` property returns `config.dataset_id`
  - `_fetch_datastore(self, client: httpx.Client) -> pl.DataFrame` — loops `offset` until response `records` is empty, accumulates pages, returns concatenated DataFrame
  - `fetch(self) -> pl.DataFrame` — dispatches to `_fetch_datastore` when `config.access_mode == "datastore"`
  - `_normalize(self, df: pl.DataFrame) -> pl.DataFrame` — renames columns to snake_case, parses date columns with `strict=False`, derives `year` from application date (null → 0), adds `source_name` literal column
- Create: `tests/test_ckan_datastore.py` — use `pytest-httpx` to mock CKAN responses:
  - Multi-page pagination terminates correctly
  - Empty response terminates loop
  - Normalization produces snake_case columns and correct `year` values
  - Null dates produce `year=0`

**Dependencies:** Phase 2

**Done when:** `pytest tests/test_ckan_datastore.py` passes; `uv run ty check src/` passes

---

### Phase 4: CKAN Bulk CSV Source
**Goal:** `CKANSource` in bulk_csv mode discovers year-based CSV resources via `package_show` and downloads them.

**Components:**
- Extend: `src/zoneto/sources/ckan.py`:
  - `_fetch_bulk_csv(self, client: httpx.Client) -> pl.DataFrame` — calls `package_show` for `config.dataset_id`, filters resources where name contains a year >= `config.year_start`, downloads each CSV with httpx, reads each as `pl.read_csv(..., infer_schema_length=10000)`, concatenates all
  - `fetch()` dispatches to `_fetch_bulk_csv` when `config.access_mode == "bulk_csv"`
- Create: `tests/test_ckan_bulk_csv.py` — mock `package_show` response with 3 year resources, mock CSV downloads:
  - Only resources with year >= year_start are downloaded
  - All qualifying CSVs are concatenated
  - Non-year resources in package_show are skipped

**Dependencies:** Phase 3

**Done when:** `pytest tests/test_ckan_bulk_csv.py` passes; full test suite passes; `uv run ty check src/` passes

---

### Phase 5: Source Registry
**Goal:** Named registry mapping the three Toronto datasets to configured `CKANSource` instances.

**Components:**
- Create: `src/zoneto/sources/registry.py`:
  ```python
  SOURCES: dict[str, Source] = {
      "permits_active": CKANSource(CKANConfig(
          dataset_id="building-permits-active-permits",
          access_mode="datastore",
      )),
      "permits_cleared": CKANSource(CKANConfig(
          dataset_id="building-permits-cleared-permits",
          access_mode="bulk_csv",
      )),
      "coa": CKANSource(CKANConfig(
          dataset_id="committee-of-adjustment-applications",
          access_mode="bulk_csv",
      )),
  }
  ```
- Create: `tests/test_registry.py` — assert all three keys present, each value satisfies `Source` protocol, `name` attributes are correct

**Dependencies:** Phase 4

**Done when:** `pytest tests/test_registry.py` passes

---

### Phase 6: Storage Layer
**Goal:** Write normalized DataFrames to Hive-partitioned Parquet, overwriting previous data on each run.

**Components:**
- Create: `src/zoneto/storage.py`:
  - `write_source(df: pl.DataFrame, name: str, data_dir: Path) -> int` — `shutil.rmtree` the source subdir, `df.write_parquet(data_dir / name, partition_by=["year"], use_pyarrow=True)`, returns row count
  - `source_row_counts(name: str, data_dir: Path) -> int | None` — `pl.scan_parquet(data_dir / name).select(pl.len()).collect()[0, 0]` if dir exists, else `None`
  - `last_modified(name: str, data_dir: Path) -> datetime | None` — latest mtime across partition files if dir exists
- Create: `tests/test_storage.py`:
  - `write_source` creates correct Hive directory structure
  - Second call overwrites (no stale partitions)
  - `source_row_counts` returns correct count
  - `last_modified` returns a datetime when data exists, `None` when not

**Dependencies:** Phase 1 (path conventions)

**Done when:** `pytest tests/test_storage.py` passes; `uv run ty check src/` passes

---

### Phase 7: CLI Integration
**Goal:** Working `sync` and `status` commands wired to registry and storage.

**Components:**
- Rewrite: `src/zoneto/cli.py`:
  - `sync(source: Annotated[str | None, typer.Option()] = None)` — iterates `SOURCES` (or single if `--source` given), calls `source.fetch()`, then `write_source()`, prints Rich progress per source with row count; catches and prints exceptions per source without aborting others
  - `status()` — iterates `SOURCES`, prints table (Rich `Table`) with columns: source name, row count, last modified; shows "no data" if not yet synced
  - Uses `rich.console.Console` for all output with color

**Dependencies:** Phases 5 and 6

**Done when:**
- `uv run zoneto sync --source permits_active` runs without error against live CKAN API (smoke test, not in pytest)
- `uv run zoneto status` prints table with correct row counts after sync
- `uv run pytest` (full suite) passes

## Additional Considerations

**Year detection for bulk_csv resources:** Resource names from `package_show` are not consistently formatted. Year extraction uses a regex (`\b(20\d{2})\b`) on the resource `name` field. Resources with no 4-digit year in their name are skipped.

**Extensibility:** Adding a new source requires implementing the `Source` protocol and adding one entry to `SOURCES` in `registry.py`. No changes to CLI or storage are needed.

**Scheduling:** The `justfile` `sync` recipe is suitable for cron or a systemd timer. No built-in scheduler is included.

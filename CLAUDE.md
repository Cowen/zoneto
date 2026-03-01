# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-03-01 -->
<!-- Last reviewed against: 4b630df -->

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
```

The CLI entrypoint is `zoneto` (mapped to `zoneto.cli:app` in pyproject.toml).

## Architecture

```
src/zoneto/
  cli.py             Typer app: `sync` and `status` commands
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/
    base.py          Source protocol (runtime_checkable)
    ckan.py          CKANSource (datastore + bulk_csv modes)
    registry.py      SOURCES dict -- the single source of truth for datasets
```

Data flows: CLI -> registry -> source.fetch() -> storage.write_source() -> data/<name>/year=YYYY/*.parquet

## Contracts

### Source Protocol (`sources/base.py`)

Any data source must satisfy this `@runtime_checkable` protocol:

- `name: str` -- human/machine identifier for the source
- `fetch() -> pl.DataFrame` -- returns a normalized polars DataFrame

The returned DataFrame **must** contain at least a `year` column (Int32) and a
`source_name` column (String) so storage partitioning works.

### CKANConfig (`models.py`)

Pydantic model with three fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataset_id` | `str` | required | CKAN package name |
| `access_mode` | `Literal["datastore", "bulk_csv"]` | required | fetch strategy |
| `year_start` | `int` | 2015 | year floor: skip CSV resources and filter rows below this year |

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

| Key | Dataset | Mode | year_start |
|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2020 |

### CLI (`cli.py`)

- `zoneto sync [--source NAME]` -- fetches one or all sources, writes Parquet
  to `./data/`. Prints colored output via Rich.
- `zoneto status` -- prints a Rich table of row counts and last-modified times.

`DATA_DIR` defaults to `Path("data")` (cwd-relative).

## Dependencies

| Package | Role |
|---|---|
| httpx | HTTP client for CKAN API |
| polars | DataFrames + Parquet I/O |
| pyarrow | Required by polars for Parquet support |
| pydantic | Config validation |
| typer | CLI framework |
| rich | Terminal formatting |

Dev: pytest, pytest-httpx, ruff, ty.

## Invariants

- Python >= 3.13 required (uses `X | Y` union syntax).
- All column names are normalized to snake_case before storage; duplicate
  snake_case names get `_2`/`_3` suffixes.
- Date columns (any column name containing "date") are parsed to `pl.Date`
  best-effort; unrecognizable formats leave the column as String.
- `year` is derived from `application_date` only if it was successfully parsed
  as `pl.Date`; otherwise defaults to 0.
- `fetch()` applies a rolling year filter: keeps rows with `year == 0` (unknown)
  or `year >= year_start`. Datastore mode auto-discovers the resource UUID via
  `package_show`. Bulk CSV mode skips non-CSV format resources.
- Storage is always full-replace per source (rmtree + rewrite).
- Tests use `pytest-httpx` to mock all HTTP calls; no network in CI.
- CKAN base URL: `https://ckan0.cf.opendata.inter.prod-toronto.ca`

# zoneto

Toronto building application data pipeline. Fetches building permit and Committee of Adjustment data from the City of Toronto Open Data API and stores it locally as Hive-partitioned Parquet files.

## Quickstart

```bash
uv sync
uv run zoneto sync        # fetch all sources
uv run zoneto status      # print row counts and last sync time
```

## Data sources

| Key | Dataset | Mode |
|-----|---------|------|
| `permits_active` | Building Permits — Active | CKAN datastore |
| `permits_cleared` | Building Permits — Cleared | Bulk CSV by year |
| `coa` | Committee of Adjustment | Bulk CSV by year |

Data is stored in `data/` as Hive-partitioned Parquet (`data/<source>/year=YYYY/`).

## Dev tasks

```bash
just test    # run pytest
just lint    # ruff + ty
just fmt     # ruff format
```

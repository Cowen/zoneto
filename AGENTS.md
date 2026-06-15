# Zoneto -- Toronto Building Data Pipeline

<!-- Freshness: 2026-05-21 -->

## Purpose

Zoneto is a development application intelligence platform for Toronto. It provides development professionals with structured data on comparable planning applications, outcome patterns, and expected timelines — using ML models to rank and prioritize where the data supports it, and presenting raw data where it doesn't.

**Target user:** Development firms doing site acquisition due diligence.

## Quick Start

```bash
uv sync && just test && just lint
just sync        # fetch all sources -> data/
just status      # always run before analyzing model results
just aic         # scrape AIC (prerequisite for survival model in enrich)
just olt         # scrape Ontario Land Tribunal decisions
just enrich      # enrich raw parquet with spatial + outcome labels
just train && just score
just serve       # FastAPI on port 8000
just pipeline    # enrich -> train -> score in sequence
just regression  # CI-safe regression tests (synthetic data)
just regression-integration  # real enriched data — not CI-safe
just narrator-eval  # narrator confidence calibration vs golden cases (LLM calls)
```

`just test` excludes `-m integration` (pre-commit safe, no network); `just test-all` runs everything.

**COA freshness caveat:** The `coa` source always shows data concentrated in 2022 — this is the complete CKAN picture (city only publishes closed CSVs for 2022–2023). A 2022-heavy distribution is not a sign of stale data.

## Architecture

```
src/zoneto/
  cli.py             Typer CLI
  models.py          CKANConfig pydantic model
  storage.py         write_source / source_row_counts / last_modified
  sources/           Source protocol, CKAN/AIC/OLT scrapers, registry
  analytics/         Enrichment, feature extraction, training, scoring
  api/               FastAPI app, routes, comps, narrator, site context
  llm/               Pydantic AI agents: agents.toml config, typed schemas
static/
  index.html         Frontend: address search, /evaluate, comps
Dockerfile
```

## Registry (`sources/registry.py`)

| Key | Dataset | Mode | year_start | year_column |
|---|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 | default |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 | default |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2018 | default |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |
| `aic_applications` | AIC ArcGIS FeatureServer | AICSource | — | — |

`dev_applications` is marked retired in CKAN but still actively updated. `aic_applications` (`COTGEO_IBMS_AIC_POINT`) is the live alternative.

## Invariants

- Python ≥ 3.13.
- Column names normalized to snake_case; duplicates get `_2`/`_3` suffixes.
- Date columns parsed to `pl.Date` best-effort; unrecognizable formats stay String.
- `year` derived from `year_column` only if Date-parsed; otherwise 0.
- `fetch()` keeps `year == 0` or `year >= year_start`.
- Storage is full-replace per source (rmtree + rewrite). Uses native polars Parquet writer — pyarrow creates flat files, not Hive dirs.
- Tests mock all HTTP via `pytest-httpx`; no network in CI.
- CKAN base URL: `https://ckan0.cf.opendata.inter.prod-toronto.ca`

# Product Strategy Pivot: Prediction to Intelligence

## Overview

Reorient Zoneto from a prediction-focused ML pipeline to a development application
intelligence platform. The core product becomes comparable application lookup with
timeline intelligence, augmented by ML predictions where models are strong enough.
The serving layer (FastAPI + minimal HTML frontend) makes the data accessible to
non-technical users for the first time.

**Goals:**
- Make 26,000+ indexed development applications queryable by non-developers
- Retire underperforming models (COA approval, permit issuance) from production
- Expand data sources (AIC full records, OLT decisions) to replace retired CKAN dataset
- Add MTSA spatial features, SHAP explanations, and description NLP to strengthen remaining models

**Success criteria:**
- `/comps` endpoint returns relevant comparable applications within 500ms
- `/score` endpoint serves predictions only from production-ready models
- AIC scraper produces a live feed of application records independent of CKAN
- OLT decisions matched to Toronto applications via address-based fuzzy matching
- Appeal model AUC improves with new features (MTSA, NLP)

**Target user:** Development firms doing site acquisition due diligence (Segment 1
from product strategy review).

## Architecture

Single FastAPI application (`src/zoneto/api/`) serving three endpoints plus static
HTML frontend. Models loaded at startup, DuckDB queries enriched Parquet at request
time. Deployed as a Docker container (serving layer only — pipeline runs separately).

```
                    Docker Container
┌─────────────────────────────────────────────┐
│  FastAPI (uvicorn)                           │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐  │
│  │ /health  │  │ /comps   │  │ /score    │  │
│  │ /ready   │  │ (DuckDB) │  │ (joblib)  │  │
│  └──────────┘  └────┬─────┘  └─────┬─────┘  │
│                     │              │         │
│  ┌──────────────────┴──────────────┴──────┐  │
│  │  data/enriched/*.parquet               │  │
│  │  data/scores/*.parquet                 │  │
│  │  models/*.joblib + metrics.json        │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  static/index.html (served by FastAPI)       │
└─────────────────────────────────────────────┘
```

**Key decisions:**
- DuckDB on Parquet at request time (no persistent database, data fits in memory)
- Models loaded at startup via joblib (26K applications is trivial)
- `/ready` endpoint confirms models + data loaded (for container orchestration)
- Vanilla HTML + CSS + fetch() frontend (no framework, no build step)

## Existing Patterns

Investigation found well-established patterns in the codebase:

**Source protocol** (`sources/base.py`): Runtime-checkable protocol requiring `name: str`
and `fetch() -> pl.DataFrame`. The new `AICSource` class follows this protocol.

**ArcGIS REST API** (`sources/aic.py`): `fetch_aic_decisions_arcgis()` already queries
`COTGEO_IBMS_AIC_POINT` FeatureServer in batches of 200 with structured field extraction.
AIC scraper expansion builds directly on this approach — same endpoint, more fields, no
folderrsn filter.

**DuckDB spatial joins** (`analytics/enrich.py`): `_spatial_join_dev()` uses DuckDB's
`ST_Read` and `ST_Contains` for point-in-polygon joins with CRS transformation. The MTSA
feature and `/comps` proximity filter follow this exact pattern.

**Reference data downloads** (`analytics/enrich.py`): `fetch_reference()` downloads
GeoJSON/ZIP files idempotently to `data/reference/`. MTSA boundary download follows
the same pattern.

**CalibratedClassifierCV unwrapping** (`analytics/importance.py`): Already unwraps
calibrated classifiers to access underlying tree models. SHAP explanation uses the
same unwrapping.

**Hive-partitioned storage** (`storage.py`): `write_source()` writes to
`data/<name>/year=YYYY/*.parquet`. New AIC application data follows this convention.

**CLI structure** (`cli.py`): Typer app with commands that delegate to analytics
functions. New `serve` and `olt` commands follow the same pattern.

**Divergence:** The `/comps` DuckDB query at HTTP request time is new — all existing
DuckDB usage is in batch enrichment. This is justified because the data volume is small
(26K rows) and DuckDB handles concurrent reads well on Parquet.

## Implementation Phases

### Phase 1: FastAPI Serving Layer

**Goal:** Make existing data and predictions queryable via HTTP API.

**Components:**
- Create: `src/zoneto/api/__init__.py`
- Create: `src/zoneto/api/app.py` — FastAPI app factory, startup event loads models + DuckDB connection
- Create: `src/zoneto/api/routes.py` — `GET /health`, `GET /ready`, `GET /comps`, `POST /score`
- Create: `src/zoneto/api/comps.py` — DuckDB query builder for comparable applications
- Modify: `src/zoneto/cli.py` — add `zoneto serve [--port 8000]` command
- Modify: `justfile` — add `just serve`
- Modify: `pyproject.toml` — add `fastapi`, `uvicorn` dependencies
- Create: `tests/api/test_routes.py` — endpoint tests (health, ready, comps, score)
- Create: `tests/api/test_comps.py` — comps query builder tests with fixture Parquet

**Dependencies:** None (first phase)

**Done when:**
- `GET /health` returns `{"status": "ok"}`
- `GET /ready` returns 200 when models + data loaded, 503 otherwise
- `GET /comps?ward=10&type=OZ&lat=43.65&lon=-79.38` returns comparable applications with outcomes and timelines
- `POST /score` returns predictions for production-ready models only
- All endpoint tests pass
- `just serve` starts the server

### Phase 2: Minimal HTML Frontend

**Goal:** Non-technical users can query comparable applications via a web form.

**Components:**
- Create: `static/index.html` — form (address/lat-lon, application type, radius, years), results table, prediction display
- Modify: `src/zoneto/api/app.py` — mount `static/` directory for static file serving

**Dependencies:** Phase 1 (API must exist)

**Done when:**
- Frontend loads at `http://localhost:8000/`
- Form submits to `/comps` and renders results as a table
- Predictions from `/score` displayed where available
- No build step required — vanilla HTML + CSS + fetch()

### Phase 3: Model Retirement

**Goal:** Remove underperforming models from production pipeline, keep code intact.

**Components:**
- Modify: `src/zoneto/analytics/train.py` — remove `coa_approved` and `permit_issuance_days` from `train_all()` default loop; `coa_days_to_approval` trains but `production_ready` forced to `false`
- Modify: `src/zoneto/analytics/score.py` — remove COA and permits scoring blocks; only score dev_applications
- Modify: `src/zoneto/cli.py` — update `train` output table to mark tracking-only models; `score` and `summary` only process dev_applications
- Modify: `tests/analytics/test_regression.py` — update baselines and expectations for retired models
- Create: `tests/analytics/test_retirement.py` — verify retired models not scored, tracking-only model has `production_ready: false`

**Dependencies:** None (can run in parallel with Phases 1-2, but sequenced here for clarity)

**Done when:**
- `just train` trains only: `dev_applications_appealed`, `dev_days_to_decision`, `coa_days_to_approval` (tracking)
- `coa_days_to_approval` has `production_ready: false` in metrics.json regardless of R2
- `just score` only produces `data/scores/dev_applications.parquet` and `data/scores/dev_applications_active.parquet`
- `just summary` only reports dev_applications scores
- Retirement tests pass

### Phase 4: Docker Container

**Goal:** Package the serving layer as a deployable Docker image.

**Components:**
- Create: `Dockerfile` — `python:3.13-slim` base, install deps via `uv`, copy `src/`, `data/enriched/`, `data/scores/`, `models/`, `static/`, expose port 8000, run `zoneto serve`
- Create: `.dockerignore` — exclude `tests/`, `.git/`, `data/` (except enriched/scores), `*.pyc`
- Modify: `justfile` — add `just docker-build`, `just docker-run`

**Dependencies:** Phases 1-2 (serving layer + frontend must exist)

**Done when:**
- `just docker-build` produces a working image
- `just docker-run` starts the container and `/health`, `/ready`, `/comps` respond correctly
- Container size is reasonable (< 2GB including data)

### Phase 5: AIC Scraper Expansion

**Goal:** Replace retired CKAN dev_applications with live AIC feed.

**Components:**
- Modify: `src/zoneto/sources/aic.py` — new `fetch_aic_applications()` function querying ArcGIS FeatureServer without folderrsn filter; discovers available fields via metadata endpoint; pages all records in batches of 200
- Create: `src/zoneto/sources/aic_source.py` — `AICSource` class implementing Source protocol; outputs `data/aic/year=YYYY/*.parquet`
- Modify: `src/zoneto/sources/registry.py` — add `aic_applications` to `SOURCES` dict
- Modify: `src/zoneto/cli.py` — `zoneto aic` gains `--full` flag for complete application records
- Modify: `src/zoneto/analytics/enrich.py` — `enrich_dev()` prefers AIC data over CKAN for same folderrsn
- Create: `tests/sources/test_aic_source.py` — mock ArcGIS responses, verify Source protocol compliance and Parquet output
- Create: `tests/analytics/test_enrich_aic_preference.py` — verify AIC-over-CKAN preference logic

**Dependencies:** None (independent of serving layer)

**Done when:**
- `zoneto aic --full` fetches complete application records from ArcGIS
- `aic_applications` source produces Hive-partitioned Parquet conforming to Source protocol
- `enrich_dev()` uses AIC records when available, CKAN as fallback
- All tests pass with mocked HTTP responses

### Phase 6: OLT Decision Scraping

**Goal:** Collect Ontario Land Tribunal decisions and match to Toronto applications.

**Components:**
- Create: `src/zoneto/sources/olt.py` — `fetch_olt_decisions()` scrapes OLT decisions page by municipality ("Toronto"), rate-limited (default 2.0s delay); outputs `data/reference/olt_decisions.parquet`
- Modify: `src/zoneto/analytics/enrich.py` — new `match_olt_to_dev(data_dir)` function; fuzzy address matching via `difflib.SequenceMatcher` with date proximity filter; produces `folderrsn -> olt_case_number` mapping; new columns on dev_applications: `olt_case_number`, `olt_outcome`, `olt_decision_date`
- Modify: `src/zoneto/cli.py` — new `zoneto olt [--delay FLOAT]` command; `zoneto enrich` gains `--fetch-olt/--no-fetch-olt` flag (default: no-fetch)
- Create: `tests/sources/test_olt.py` — mock OLT HTML responses, verify scraping
- Create: `tests/analytics/test_olt_matching.py` — verify fuzzy matching logic with synthetic addresses

**Dependencies:** Phase 5 (AIC data provides the application records to match against)

**Done when:**
- `zoneto olt` scrapes OLT decisions and writes Parquet
- Fuzzy matching produces high-confidence folderrsn-to-case mappings
- Enriched dev_applications include OLT columns (null when no match)
- All tests pass with mocked HTTP responses

### Phase 7: Model Improvements (MTSA + NLP)

**Goal:** Add MTSA spatial feature and description NLP features to strengthen remaining models.

**Components:**
- Modify: `src/zoneto/analytics/enrich.py` — `fetch_reference()` downloads MTSA boundary GeoJSON to `data/reference/mtsa.geojson`; `enrich_dev()` gains `in_mtsa` (Int8) via DuckDB spatial join; new `_extract_text_features()` applies TF-IDF + TruncatedSVD (20 components) to description column; serializes vectorizer to `models/desc_tfidf.joblib`
- Modify: `src/zoneto/analytics/features.py` — add `in_mtsa` and `desc_svd_0..19` to `DEV_NUM_COLS`
- Modify: `src/zoneto/analytics/score.py` — load `desc_tfidf.joblib` and transform descriptions before scoring
- Create: `tests/analytics/test_mtsa_feature.py` — verify spatial join with synthetic geometry
- Create: `tests/analytics/test_nlp_features.py` — verify TF-IDF + SVD pipeline with sample descriptions

**Dependencies:** Phase 5 (AIC data for fresh applications to enrich)

**Done when:**
- `in_mtsa` populated in enriched dev_applications via spatial join
- `desc_svd_0..19` populated from TF-IDF + SVD on description column
- Models retrain successfully with new features
- Appeal AUC evaluated (improvement expected but not guaranteed)
- All tests pass

### Phase 8: SHAP Explanations + Documentation

**Goal:** Add per-application explanations and update all documentation.

**Components:**
- Create: `src/zoneto/analytics/explain.py` — `explain_one(source, features, model_dir)` using `shap.TreeExplainer`; unwraps CalibratedClassifierCV; returns top-5 SHAP contributors with values and direction
- Modify: `src/zoneto/api/routes.py` — `/score` gains `?explain=true` query param; response includes `explanations` key
- Modify: `static/index.html` — render SHAP explanations as ranked feature list below predictions
- Modify: `pyproject.toml` — add `shap` dependency
- Modify: `CLAUDE.md` — revised purpose, new architecture section, updated model table, new CLI commands, new dependencies, endpoint contracts
- Modify: `README.md` — revised project description reflecting intelligence focus, updated quick start with `just serve`, Docker instructions
- Create: `tests/analytics/test_explain.py` — verify SHAP output structure, CalibratedClassifierCV unwrapping
- Create: `tests/api/test_explain_endpoint.py` — verify `/score?explain=true` returns explanations

**Dependencies:** Phases 1-7 (all functionality must exist)

**Done when:**
- `/score?explain=true` returns top-5 SHAP feature contributions
- Frontend renders explanations
- CLAUDE.md reflects all changes from all phases
- README.md reflects new product direction
- All tests pass

## Additional Considerations

**Rate limiting for scrapers:** OLT scraping uses 2.0s default delay (government site).
AIC ArcGIS queries are batched (200/request) which is respectful. Both are configurable
via CLI flags.

**OLT matching quality:** Fuzzy address matching will have false positives. The
`--no-fetch-olt` default during enrich means OLT data is opt-in until matching quality
is validated manually. A confidence score threshold gates which matches are used.

**NLP feature stability:** TF-IDF vocabulary is fitted on training data and serialized.
New applications with novel terms get zero weights for those terms, which is acceptable
behavior for tree-based models.

**Implementation scoping:** This design has exactly 8 phases. Each phase is self-contained
with clear verification criteria. If any phase proves larger than expected during
implementation planning, it can be split across multiple implementation plans.

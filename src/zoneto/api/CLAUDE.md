# API — Zoneto

## App Factory (`app.py`)

BERT: if `desc_bert_embeddings.npy` exists, loads `SentenceTransformer("BAAI/bge-small-en-v1.5")`; reuses `bylaw_index.model` if already loaded (same weights, avoids double load). LLM client created when `ANTHROPIC_API_KEY` set; otherwise `None`. `create_app_from_env()` reads `ZONETO_DATA_DIR`/`ZONETO_MODEL_DIR`/`ZONETO_STATIC_DIR` env vars for uvicorn `--reload` (`factory=True`).

## Site Context (`site_context.py`)

_Last verified: 2026-05-27_

Spatial point-in-polygon against zoning, heritage, secondary-plan, MTSA via DuckDB `ST_Read`. **Zoning fallback:** `ST_Within` miss (off-parcel geocodes) → retries `ST_DWithin(point, geom, 0.002)` (~200m) nearest-first. Heritage/secondary-plan/MTSA remain strict `ST_Within` — those flags must not bleed across boundaries.

`nearby_applications(lat, lon, enriched_path, *, radius_m=500, years=5, limit=20)` — queries `data/enriched/dev_applications.parquet` for applications within `radius_m` metres using a bounding-box pre-filter and planar distance. Returns `[]` when enriched_path absent (graceful). `is_active` column treated as optional (NULL when absent). Results sorted ascending by `distance_m`. Fields: `folderrsn, application_type, status, street_address, date_submitted, description, is_active, distance_m`.

## Description Similarity (`desc_similarity.py`)

BERT scorer preferred; TF-IDF+SVD fallback. Both accept `zoning_class` for zone-matched stats.

`approval_rate` is returned for API compat but **NOT surfaced in narrator** — `dev_approved` covers only ~9.6% of OZ/SA (survivorship bias). `zone_matched_appeal_rate` IS surfaced (no survivorship bias).

## Narrator (`narrator.py`)

Confidence: Step 1 extreme violation check (≥3× limit → cap 10–30); Step 2 base band (70–80 no violations + compatible use, 55–65 mismatched, 35–55 moderate); Step 3 ±8 (MTSA/exception up; high appeal/heritage down).

**Deterministic overrides (applied after LLM parse):**
- Floor: `violations == []` and `permitted_use_category` contains "mixed"/"commercial residential" → `max(score, 70)`.
- Cap: proposed_storeys/zoning_max_storeys OR proposed_units/zoning_max_units ≥ 3.0 → `min(score, 30)`.

Cross-zone: outcome line suppressed when best comparable's zone ≠ site zone — prevents cross-zone bleed. Data gaps do NOT deduct from confidence score.

## Evaluate Endpoint (`routes.py`)

`POST /evaluate` — geocode → site context → feature extraction → rule check → bylaw retrieval → description similarity → narration → nearby applications. BERT scorer used when `app.state.bert_model` set; otherwise TF-IDF+SVD. Response schema: `EvaluateResponse` in `routes.py`.

`_compute_data_gaps()` adds height-overlay caveat (when `zoning_max_storeys` and `zoning_max_height_m` both None and zone is known) and building-type caveat (when `building_type` is None). Same list passed to narrator and returned in response.

`EvaluateResponse.nearby_active_applications` — list of `NearbyApplication` (see `routes.py`) within 500m of the geocoded point, last 5 years, from enriched dev_applications. Empty list when enriched data absent. **Source is dev_applications only** (OZ/SA/CD/SB/PL); COA not included until `aic_applications` is fetched via `just aic --full` and integrated.

# API — Zoneto

## App Factory (`app.py`)

BERT: if `desc_bert_embeddings.npy` exists, loads `SentenceTransformer("BAAI/bge-small-en-v1.5")`; reuses `bylaw_index.model` if already loaded (same weights, avoids double load). Narrator agents (`app.state.narrator`, a `NarratorAgents` bundle from `zoneto.llm.agents.make_narrator_agents`) created when `ANTHROPIC_API_KEY` set; otherwise `None` (routes 503). `create_app_from_env()` reads `ZONETO_DATA_DIR`/`ZONETO_MODEL_DIR`/`ZONETO_STATIC_DIR` env vars for uvicorn `--reload` (`factory=True`).

## LLM Agents (`zoneto.llm`)

LLM calls go through **Pydantic AI** agents, not a hand-rolled client. `agents.toml` (packaged; override path via `ZONETO_AGENTS_CONFIG`) declares one table per agent (`narrator_evaluation`, `narrator_question`) with `model`/`max_tokens`/`retries`/`temperature`; `load_agents_config` reads it via stdlib `tomllib` into `AgentConfig` models, and `ZONETO_LLM_MODEL` still overrides every agent's model. System prompts stay in `narrator.py` (coupled to the formatters). `narrate_evaluation` returns a **typed** `NarrationResult{summary_md, confidence}` (`zoneto.llm.schemas`) — the old free-text `CONFIDENCE:` line + regex parser are gone; `_apply_confidence_overrides` still clamps the structured `confidence`. Tests stub agents with `TestModel`/`FunctionModel` via `tests/stubs.py` (no key, no network).

## Site Context (`site_context.py`)

_Last verified: 2026-05-27_

Spatial point-in-polygon against zoning, heritage, secondary-plan, MTSA, and the Official Plan land-use designation via DuckDB `ST_Read`. **Zoning fallback:** `ST_Within` miss (off-parcel geocodes) → retries `ST_DWithin(point, geom, 0.002)` (~200m) nearest-first. The **OP designation** (`op_land_use_designation`, from `op_land_use.geojson`) uses the **same snap fallback** (parcel/designation polygons miss off-parcel geocodes too); heritage/secondary-plan/MTSA remain strict `ST_Within` — those flags must not bleed across boundaries.

`nearby_applications(lat, lon, enriched_path, *, radius_m=500, years=5, limit=20)` — queries `data/enriched/dev_applications.parquet` for applications within `radius_m` metres using a bounding-box pre-filter and planar distance. Returns `[]` when enriched_path absent (graceful). `is_active` column treated as optional (NULL when absent). Results sorted ascending by `distance_m`. Fields: `folderrsn, application_type, status, street_address, date_submitted, description, is_active, distance_m`.

## Description Similarity (`desc_similarity.py`)

BERT scorer preferred; TF-IDF+SVD fallback. Both accept `zoning_class` for zone-matched stats and an optional `application_type` that **additionally** restricts the zone-matched subset to that process type (rezoning's exposure read against OZ comps, not site-plan SA). When applied, the result carries `zone_matched_application_type` and the narrator's zone-matched line names it ("same zone and process type"). `/evaluate` passes it via `_expected_app_type(path)` in `routes.py` (rezoning→`OZ`, else None — dev appeal labels are OZ+SA only, so only rezoning maps cleanly). Retrieval quality on the structured axes (zone/type/scale) is measured by `just comps-eval` (`scripts/comps_eval.py`); the harness showed text retrieval captures type well (+~40pp over random) but zone only weakly (+~8pp), which is why zone+type stratification of the *rate* matters.

`approval_rate` is returned for API compat but **NOT surfaced in narrator** — `dev_approved` covers only ~9.6% of OZ/SA (survivorship bias). `zone_matched_appeal_rate` IS surfaced (no survivorship bias) and is the rate **fed into the Planning Act statutory block** (`_format_statutory_process` prefers it over the zone-diluted pooled `appeal_rate`, falling back to pooled only when no same-zone rate exists).

## Narrator (`narrator.py`)

_Last verified: 2026-05-29_

`narrate_evaluation(..., description=None)` — when `description` is provided, injects a `## Project description` section (raw proposal text verbatim) right after `## Extracted project features`. Lets the LLM reason about details regex extraction misses (parking, laneway access, accessibility).

Confidence: Step 1 extreme violation check (≥3× limit → cap 10–30, including inferred-height and FSI); Step 2 base band (70–80 zero structural + compatible use + ≥1 verified limit; 55–65 compatible use with no checkable limit OR mismatched use; 35–55 structural violations); Step 3 ±8 (MTSA/exception up; high appeal/heritage down).

**Deterministic overrides (applied to the structured `confidence` after the agent returns):**
- Floor: compatible use + zero structural violations → `max(score, 70)` when `_limits_verified` (≥1 encoded limit checked against an extracted value), else `max(score, 55)` — a zone with all-null limits is "unknowable", not as-of-right.
- Cap: any of storeys, units, height (stated metres or `effective_height_m` = storeys × 3.0), or FSI ≥ 3.0× its zone limit → `min(score, 30)`. Compliance also emits `height_exceeds_max_inferred` (no stated metres, no storey limit, estimate >125% of metre limit) and `fsi_exceeds_max` violations.

Cross-zone: outcome line suppressed when best comparable's zone ≠ site zone — prevents cross-zone bleed. Data gaps do NOT deduct from confidence score.

**Statutory process block (Planning Act):** `_format_statutory_process(violations, extracted, desc_sim, description)` derives the primary process via `planning_act.path_for_violations` and injects a `## Statutory process & appeal route (Planning Act)` section (process, decider, OLT route, non-decision clock, Bill 23/185 read on the comparable appeal rate). It also lists **orthogonal** processes from `planning_act.additional_processes` (site plan, subdivision, consent, rental replacement, …) and picks the 90 vs 120-day rezoning clock from an OPA mention **or an `op_use_nonconforming` violation** (an Official-Plan non-conformity *is* the OPA trigger → combined 120-day clock). `/evaluate` returns `statutory_process` (primary) + `additional_processes` (list of `StatutoryProcessResult`). **Context only** — a system-prompt guard forbids it from changing CONFIDENCE, and `_apply_confidence_overrides` is untouched. See `specs/2026-06-13-planning-act-integration.md`.

**Narrator eval:** `just narrator-eval` runs 13 golden cases (real applications with AIC-verified outcomes, `tests/fixtures/narrator_eval_cases.json`) through the full pipeline with per-case mechanism traces; `tests/api/test_narrator_regression.py` is the pytest twin (CI-safe override tests + `-m integration` LLM band tests; advisory band misses xfail). Advisory cases (68 Wellesley, 372-378 Yonge, 328 Dupont, 57 Finch original) pin the documented limitation that confidence measures compliance-path viability, not approval probability — see `specs/2026-06-12-narrator-refusal-gap-analysis.md`. `just narrator-triage` runs every unique refused application through the same pipeline (deterministic by default, `--llm` for real scores) to measure refused-set miscalibration.

## Evaluate Endpoint (`routes.py`)

`POST /evaluate` — geocode → site context → feature extraction → rule check → bylaw retrieval → description similarity → narration → nearby applications. BERT scorer used when `app.state.bert_model` set; otherwise TF-IDF+SVD. Response schema: `EvaluateResponse` in `routes.py`. Passes `description=body.description` to the narrator so raw proposal text reaches the LLM.

`_retrieve_chunks()` merges exception → zone → description chunks. `exception_chunks` capped at `[:2]` so site-specific exception text doesn't crowd out zone and description-based bylaw sections.

`_compute_data_gaps()` adds height-overlay caveat (when `zoning_max_storeys` and `zoning_max_height_m` both None and zone is known), an **OP-designation caveat** (when `op_land_use_designation` is None but the zone is known — the s.24/s.22 conformity read couldn't run), and a building-type caveat (when `building_type` is None). Same list passed to narrator and returned in response. The OP designation rides in the raw `site_context` dict and the conformity finding (`op_use_nonconforming`, INFORMATIONAL) in `violations` — no `EvaluateResponse` schema change.

`EvaluateResponse.nearby_active_applications` — list of `NearbyApplication` (see `routes.py`) within 500m of the geocoded point, last 5 years, from enriched dev_applications. Empty list when enriched data absent. **Source is dev_applications only** (OZ/SA/CD/SB/PL); COA not included until `aic_applications` is fetched via `just aic --full` and integrated.

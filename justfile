set dotenv-load

default:
	@just --list

# Pull all data sources
sync:
    uv run zoneto sync

status:
    uv run zoneto status

# Scrape AIC portal for decision dates
aic:
    uv run zoneto aic

# Fetch full AIC application records (replacement for CKAN dev_applications)
aic-full:
    uv run zoneto aic --full

# Scrape OLT decisions for Toronto
olt:
    uv run zoneto olt

# Fetch Official Plan land-use designation polygons (interim Borealis source)
op:
    uv run zoneto op

# Build the bylaw index for semantic search
bylaw-index:
    uv run zoneto bylaw-index

# Evaluate bylaw retrieval quality (recall@3 and recall@5 against labelled queries)
bylaw-eval:
    uv run python scripts/bylaw_eval.py

# Evaluate comparable-application retrieval: structured concordance (zone, type,
# scale) of top-k comps vs a random baseline, leave-one-out (needs `just enrich`+train)
comps-eval *ARGS:
    uv run python scripts/comps_eval.py {{ARGS}}

# Regenerate BERT description embeddings + index from enriched data. Run after
# `just enrich` — the artifacts must stay in sync with the corpus row count.
bert:
    uv run python -c "from zoneto.analytics.nlp import compute_bert_embeddings as f; f()"

# Evaluate narrator confidence calibration against golden cases
# (requires ANTHROPIC_API_KEY + data/reference; one LLM call per case)
narrator-eval *ARGS:
    uv run python scripts/narrator_eval.py {{ARGS}}

# Triage refused / revised-then-approved applications against the narrator
# (deterministic by default; --llm narrates each, --emit-case prints a fixture stanza)
narrator-triage *ARGS:
    uv run python scripts/narrator_refused_triage.py {{ARGS}}

# Run UAT scenarios for failure-pattern cases through the full narrator pipeline
# (requires ANTHROPIC_API_KEY, data/reference/, data/enriched/, models/)
# Prints auto-checks + a human rubric per scenario. Skips cleanly if prereqs absent.
# Usage: just uat                  # all five scenarios
#        just uat --case mendota-2 # one scenario
uat *ARGS:
    uv run python scripts/uat_scenarios.py {{ARGS}}

# Evaluate the Planning Act process classifier: derived process vs actual
# application_type over the enriched corpus (deterministic; needs `just enrich`)
planning-act-eval *ARGS:
    uv run python scripts/planning_act_eval.py {{ARGS}}

# Enrich data
enrich:
    uv run zoneto enrich

# Train models
train:
    uv run zoneto train

# Score models
score:
    uv run zoneto score

# Show score distributions
summary:
    uv run zoneto summary

# Start the FastAPI serving layer
serve:
    uv run zoneto serve --reload

# Evaluate an address + project description against zoning rules
# Usage: just evaluate "123 Main St, Toronto" "12-storey mixed-use with 200 units"
evaluate address description:
    uv run zoneto evaluate "{{address}}" "{{description}}" | jq .

# Build the Docker image for the serving layer
# Run `just bylaw-index` first to bake the bylaw index into the image.
# The image still builds without it (app starts with bylaw_index = None).
docker-build:
    mkdir -p data/bylaw_index
    podman build -t zoneto:latest .

# Run the serving layer in Docker (requires prior docker-build and pipeline run)
# Pass ANTHROPIC_API_KEY via --env for the LLM narrator.
docker-run:
    podman run --rm -p 8000:8000 --env ANTHROPIC_API_KEY zoneto:latest

# Measure importance of all model input features
importance-all:
	ls models/ | grep joblib | sed 's/.joblib//g' | xargs -n1 just importance

# Measure importance of model input features
importance model:
    uv run zoneto importance {{model}}

# Run the full analytics pipeline: enrich (includes AIC scrape) → train → score
pipeline:
    just enrich
    just train
    just score

# CI-safe test suite (no network, no API keys) — run by the pre-commit hook
test:
    uv run pytest -qq -m "not integration"

# Full suite including integration tests (requires data/ + ANTHROPIC_API_KEY)
test-all:
    uv run pytest -qq

# Run performance regression tests (CI-safe, synthetic data)
regression:
    uv run pytest tests/analytics/test_regression.py -m "not integration" -v

# Run performance regression tests against real enriched data
regression-integration:
    uv run pytest tests/analytics/test_regression.py tests/api/test_narrator_regression.py tests/api/test_uat_scenarios.py -m integration -v

# Regenerate tests/fixtures/model_baselines.json from current enriched data
update-baselines:
    uv run python scripts/update_baselines.py

lint:
    uv run ruff check && uv run ruff format --check && uv run ty check src/ tests/

fmt:
    uv run ruff format

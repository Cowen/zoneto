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

# Build the bylaw index for semantic search
bylaw-index:
    uv run zoneto bylaw-index

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
    source .env && uv run zoneto serve --reload

# Build the Docker image for the serving layer
# Run `just bylaw-index` first to bake the bylaw index into the image.
# The image still builds without it (app starts with bylaw_index = None).
docker-build:
    mkdir -p data/bylaw_index
    docker build -t zoneto:latest .

# Run the serving layer in Docker (requires prior docker-build and pipeline run)
# Pass ANTHROPIC_API_KEY via --env for the LLM narrator.
docker-run:
    docker run --rm -p 8000:8000 --env ANTHROPIC_API_KEY zoneto:latest

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

test:
    uv run pytest -qq

# Run performance regression tests (CI-safe, synthetic data)
regression:
    uv run pytest tests/analytics/test_regression.py -m "not integration" -v

# Run performance regression tests against real enriched data
regression-integration:
    uv run pytest tests/analytics/test_regression.py -m integration -v

# Regenerate tests/fixtures/model_baselines.json from current enriched data
update-baselines:
    uv run python scripts/update_baselines.py

# FIXME types are good for tests/ too, not just src/
lint:
    uv run ruff check && uv run ty check src/

fmt:
    uv run ruff format

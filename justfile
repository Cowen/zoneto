default:
	@just --list

# Pull all data sources
sync:
    uv run zoneto sync

status:
    uv run zoneto status

# Enrich data
enrich:
    uv run zoneto enrich

# Train models
train:
    uv run zoneto train

# Score models
score:
    uv run zoneto score

# Measure importance of all model input features
importance-all:
	ls models/ | grep joblib | sed 's/.joblib//g' | xargs -n1 just importance

# Measure importance of model input features
importance model:
    uv run zoneto importance {{model}}

# Run the full analytics pipeline: enrich → train → score
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

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
	ls models/*.joblib | sed 's/.joblib//g' | xargs -n1 just importance

# Measure importance of model input features
importance model:
    uv run zoneto importance {{model}}

# Run the full analytics pipeline: enrich → train → score
pipeline:
    just enrich
    just train
    just score

test:
    uv run pytest

lint:
    uv run ruff check src/ && uv run ty check src/

fmt:
    uv run ruff format src/

sync:
    uv run zoneto sync

status:
    uv run zoneto status

enrich:
    uv run zoneto enrich

train:
    uv run zoneto train

score:
    uv run zoneto score

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

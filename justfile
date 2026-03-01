sync:
    uv run zoneto sync

status:
    uv run zoneto status

test:
    uv run pytest

lint:
    uv run ruff check src/ && uv run ty check src/

fmt:
    uv run ruff format src/

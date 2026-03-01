# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Scaffold an installable `zoneto` Python package with working CLI skeleton and dev tooling configured.

**Architecture:** src-layout Python package managed with uv. Typer for CLI, ruff + ty for code quality, pytest for testing. All project tooling configured in `pyproject.toml`. A justfile wraps common developer commands.

**Tech Stack:** Python 3.13, uv, typer, ruff, ty, pytest, hatchling (build backend)

**Scope:** Phase 1 of 7 (project scaffolding — infrastructure phase, verified operationally)

**Codebase verified:** 2026-02-28 — workspace is greenfield; only `docs/design-plans/2026-02-28-toronto-building-data-pipeline.md` exists alongside `.git/` and `.claude/settings.local.json`.

---

### Task 1: Create pyproject.toml

**Files:**
- Create: `pyproject.toml`

**Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "zoneto"
version = "0.1.0"
description = "Toronto building application data pipeline"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "httpx>=0.27",
    "polars>=1.0",
    "pyarrow>=17.0",
    "pydantic>=2.0",
    "rich>=13.0",
    "typer>=0.12",
]

[project.scripts]
zoneto = "zoneto.cli:app"

[tool.hatch.build.targets.wheel]
packages = ["src/zoneto"]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-httpx>=0.30",
    "ruff>=0.8",
    "ty>=0.0.1",
]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
src = ["src"]
target-version = "py313"

[tool.ruff.lint]
select = ["E", "W", "F", "I"]
```

**Step 2: Run `uv sync` to verify**

```bash
uv sync
```

Expected: Resolves and installs all dependencies into `.venv/`. No errors.

---

### Task 2: Create supporting files

**Files:**
- Create: `justfile`
- Create: `.gitignore`
- Create: `README.md`

**Step 1: Create `justfile`**

```makefile
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
```

**Step 2: Create `.gitignore`**

```
data/
__pycache__/
.venv/
*.pyc
*.egg-info/
dist/
.pytest_cache/
.ruff_cache/
```

**Step 3: Create `README.md`**

```markdown
# zoneto

Toronto building application data pipeline. Fetches building permit and Committee of Adjustment data from the City of Toronto Open Data API and stores it locally as Hive-partitioned Parquet files.

## Quickstart

```bash
uv sync
uv run zoneto sync        # fetch all sources
uv run zoneto status      # print row counts and last sync time
```

## Data sources

| Key | Dataset | Mode |
|-----|---------|------|
| `permits_active` | Building Permits — Active | CKAN datastore |
| `permits_cleared` | Building Permits — Cleared | Bulk CSV by year |
| `coa` | Committee of Adjustment | Bulk CSV by year |

Data is stored in `data/` as Hive-partitioned Parquet (`data/<source>/year=YYYY/`).

## Dev tasks

```bash
just test    # run pytest
just lint    # ruff + ty
just fmt     # ruff format
```
```

---

### Task 3: Create package skeleton and CLI stub

**Files:**
- Create: `src/zoneto/__init__.py` (empty)
- Create: `src/zoneto/cli.py`
- Create: `tests/__init__.py` (empty)

**Step 1: Create `src/zoneto/__init__.py`**

Create an empty file at `src/zoneto/__init__.py`. No content needed.

**Step 2: Create `src/zoneto/cli.py`**

```python
from __future__ import annotations

from typing import Annotated

import typer

app = typer.Typer(help="Toronto building application data pipeline.")


@app.command()
def sync(
    source: Annotated[
        str | None,
        typer.Option(help="Source name to sync (default: all)"),
    ] = None,
) -> None:
    """Fetch data from sources and write to Parquet."""
    typer.echo("sync: not yet implemented")


@app.command()
def status() -> None:
    """Print last sync time and row counts per source."""
    typer.echo("status: not yet implemented")
```

**Step 3: Create `tests/__init__.py`**

Create an empty file at `tests/__init__.py`. No content needed.

**Step 4: Verify CLI entry point**

```bash
uv run zoneto sync
```

Expected output:
```
sync: not yet implemented
```

```bash
uv run zoneto status
```

Expected output:
```
status: not yet implemented
```

**Step 5: Verify code quality**

```bash
uv run ruff check src/
```

Expected: No output (zero issues).

```bash
uv run ty check src/
```

Expected: No errors.

**Step 6: Commit**

```bash
git add pyproject.toml justfile .gitignore README.md src/ tests/
git commit -m "chore: scaffold zoneto package with CLI skeleton and dev tooling"
```

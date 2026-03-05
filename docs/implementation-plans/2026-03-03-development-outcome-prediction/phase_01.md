# Phase 1: Project Setup

<!-- Plan date: 2026-03-04 -->
<!-- Branch: development-outcome-prediction -->

## Goal

Add analytics dependencies, create the `analytics` subpackage skeleton, and
define the canonical feature column lists.

## Files Changed

| File | Action |
|---|---|
| `pyproject.toml` | Add 5 runtime dependencies |
| `.gitignore` | Add `models/` directory |
| `src/zoneto/analytics/__init__.py` | Create (empty) |
| `src/zoneto/analytics/features.py` | Create with feature column constants |
| `tests/analytics/__init__.py` | Create (empty) |
| `tests/analytics/test_features.py` | Create with smoke tests |

## Step 1 — Write failing test

Create `tests/analytics/__init__.py` (empty) and
`tests/analytics/test_features.py`:

```python
from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)


def test_dev_cat_cols() -> None:
    assert DEV_CAT_COLS == [
        "application_type",
        "ward_number",
        "zoning_class",
        "secondary_plan_name",
    ]


def test_dev_num_cols() -> None:
    assert DEV_NUM_COLS == [
        "year_submitted",
        "in_heritage_register",
        "in_heritage_district",
        "in_secondary_plan",
        "has_community_meeting",
    ]


def test_coa_cat_cols() -> None:
    assert COA_CAT_COLS == [
        "application_type",
        "sub_type",
        "ward_number",
        "zoning_designation",
    ]


def test_coa_num_cols() -> None:
    assert COA_NUM_COLS == ["year_submitted"]
```

Run `uv run pytest tests/analytics/test_features.py` — expect
`ModuleNotFoundError: No module named 'zoneto.analytics'`.

## Step 2 — Add dependencies to pyproject.toml

In the `dependencies` list under `[project]`, add (alphabetically):

```toml
dependencies = [
    "duckdb>=1.0",
    "httpx>=0.27",
    "joblib>=1.3",
    "polars>=1.0",
    "pyarrow>=17.0",
    "pydantic>=2.0",
    "pyproj>=3.6",
    "rich>=13.0",
    "scikit-learn>=1.4",
    "shapely>=2.0",
    "typer>=0.12",
]
```

Then run:

```bash
uv sync
```

## Step 3 — Add models/ to .gitignore

If `.gitignore` exists, append:

```
models/
```

If it does not exist, create it with that single line.

## Step 4 — Create analytics subpackage

Create `src/zoneto/analytics/__init__.py` (empty file).

## Step 5 — Create features.py

Create `src/zoneto/analytics/features.py`:

```python
"""Canonical feature column lists for analytics models."""

DEV_CAT_COLS: list[str] = [
    "application_type",
    "ward_number",
    "zoning_class",
    "secondary_plan_name",
]

DEV_NUM_COLS: list[str] = [
    "year_submitted",
    "in_heritage_register",
    "in_heritage_district",
    "in_secondary_plan",
    "has_community_meeting",
]

COA_CAT_COLS: list[str] = [
    "application_type",
    "sub_type",
    "ward_number",
    "zoning_designation",
]

COA_NUM_COLS: list[str] = ["year_submitted"]
```

## Step 6 — Verify tests pass

```bash
uv run pytest tests/analytics/test_features.py -v
```

All 4 tests should pass.

## Step 7 — Lint

```bash
uv run ruff check src/zoneto/analytics/
uv run ty check src/zoneto/analytics/
```

Fix any issues before proceeding.

## Verification

- `uv sync` completes without error
- `python -c "import duckdb, joblib, pyproj, sklearn, shapely"` succeeds
- `uv run pytest tests/analytics/test_features.py` → 4 passed
- `uv run pytest` → all existing tests still pass

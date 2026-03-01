# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Define the typed `Source` protocol and `CKANConfig` model that form the contract for all data sources.

**Architecture:** `typing.Protocol` with `@runtime_checkable` for the source abstraction. Pydantic `BaseModel` for `CKANConfig`. No business logic in this phase — only type definitions and validation.

**Tech Stack:** Python 3.13, polars, pydantic v2, pytest

**Scope:** Phase 2 of 7 (source protocol and config models)

**Codebase verified:** 2026-02-28 — greenfield; all files in this phase need to be created.

---

### Task 1: Source protocol

**Files:**
- Create: `src/zoneto/sources/__init__.py` (empty)
- Create: `src/zoneto/sources/base.py`

**Step 1: Create `src/zoneto/sources/__init__.py`**

Create an empty file at `src/zoneto/sources/__init__.py`. No content needed.

**Step 2: Create `src/zoneto/sources/base.py`**

```python
from __future__ import annotations

from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class Source(Protocol):
    """Protocol for all data sources.

    Any class with a `name` str attribute and a `fetch()` method that
    returns a polars DataFrame satisfies this protocol.
    """

    name: str

    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        ...
```

**Step 3: Verify ty passes**

```bash
uv run ty check src/zoneto/sources/
```

Expected: No errors.

---

### Task 2: CKANConfig model

**Files:**
- Create: `src/zoneto/models.py`

**Step 1: Create `src/zoneto/models.py`**

```python
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class CKANConfig(BaseModel):
    """Configuration for a CKAN-based data source."""

    dataset_id: str
    access_mode: Literal["datastore", "bulk_csv"]
    year_start: int = 2015
```

**Step 2: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

---

### Task 3: CKANConfig tests

**Files:**
- Create: `tests/test_models.py`

**Step 1: Write `tests/test_models.py`**

```python
from __future__ import annotations

import pytest
from pydantic import ValidationError

from zoneto.models import CKANConfig


def test_datastore_defaults() -> None:
    config = CKANConfig(dataset_id="my-dataset", access_mode="datastore")
    assert config.dataset_id == "my-dataset"
    assert config.access_mode == "datastore"
    assert config.year_start == 2015


def test_bulk_csv_custom_year() -> None:
    config = CKANConfig(dataset_id="my-dataset", access_mode="bulk_csv", year_start=2020)
    assert config.year_start == 2020


def test_invalid_access_mode_raises() -> None:
    with pytest.raises(ValidationError):
        CKANConfig(dataset_id="my-dataset", access_mode="invalid")  # type: ignore[arg-type]


def test_missing_access_mode_raises() -> None:
    with pytest.raises(ValidationError):
        CKANConfig(dataset_id="my-dataset")  # type: ignore[call-arg]
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_models.py -v
```

Expected: All 4 tests pass.

**Step 3: Commit**

```bash
git add src/zoneto/sources/ src/zoneto/models.py tests/test_models.py
git commit -m "feat: add Source protocol and CKANConfig model"
```

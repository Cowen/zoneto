# Development Applications Source — Phase 2: Register `dev_applications`

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add `dev_applications` to the `SOURCES` registry so the CLI can sync and report it.

**Architecture:** One new entry in `src/zoneto/sources/registry.py`. Uses the `CKANSource` class (no new classes needed) and the `year_column` field added in Phase 1. `year_start=2000` is intentionally below the earliest records (~2008) to fetch the full historical archive.

**Tech Stack:** Python 3.13, Pydantic v2, pytest-httpx

**Scope:** Phase 2 of 4. Requires Phase 1 complete (`year_column` field must exist on `CKANConfig`).

**Codebase verified:** 2026-03-01

---

## Task 1: Write the failing tests (TDD — RED)

**Files:**
- Modify: `tests/test_registry.py`

The existing `tests/test_registry.py` has a test `test_all_three_sources_present` that asserts the SOURCES dict has exactly the set `{"permits_active", "permits_cleared", "coa"}`. Adding a fourth source will break this test. Fix it alongside the new tests.

### Step 1: Update `tests/test_registry.py`

Replace the entire content of `tests/test_registry.py` with:

```python
from __future__ import annotations

from zoneto.sources.base import Source
from zoneto.sources.registry import SOURCES


def test_all_sources_present() -> None:
    assert set(SOURCES.keys()) == {"permits_active", "permits_cleared", "coa", "dev_applications"}


def test_source_names_match_dataset_ids() -> None:
    assert SOURCES["permits_active"].name == "building-permits-active-permits"
    assert SOURCES["permits_cleared"].name == "building-permits-cleared-permits"
    assert SOURCES["coa"].name == "committee-of-adjustment-applications"
    assert SOURCES["dev_applications"].name == "development-applications"


def test_all_sources_satisfy_protocol() -> None:
    """Each value is a runtime-checkable Source (has name attr and fetch method)."""
    for key, source in SOURCES.items():
        assert isinstance(source, Source), f"{key}: does not satisfy Source protocol"


def test_all_sources_have_callable_fetch() -> None:
    for key, source in SOURCES.items():
        assert callable(source.fetch), f"{key}: fetch is not callable"


def test_dev_applications_config() -> None:
    """dev_applications uses date_submitted as year column, fetches all years."""
    source = SOURCES["dev_applications"]
    cfg = source.config
    assert cfg.dataset_id == "development-applications"
    assert cfg.access_mode == "datastore"
    assert cfg.year_start == 2000
    assert cfg.year_column == "date_submitted"
```

### Step 2: Run the tests to confirm they fail

```bash
uv run pytest tests/test_registry.py -v
```

Expected failure — `dev_applications` is not yet in SOURCES:

```
FAILED tests/test_registry.py::test_all_sources_present - AssertionError: assert {'coa', 'permits_active', 'permits_cleared'} == {'coa', 'dev_applications', 'permits_active', 'permits_cleared'}
FAILED tests/test_registry.py::test_source_names_match_dataset_ids - KeyError: 'dev_applications'
FAILED tests/test_registry.py::test_dev_applications_config - KeyError: 'dev_applications'
```

If any of these tests pass before adding the registry entry, the test is wrong — fix it before proceeding.

---

## Task 2: Add `dev_applications` to the registry (GREEN)

**Files:**
- Modify: `src/zoneto/sources/registry.py`

### Step 3: Append the new entry to the `SOURCES` dict

In `src/zoneto/sources/registry.py`, add the `dev_applications` entry after the `coa` entry (before the closing `}`).

Current closing block (lines 22–29):
```python
    "coa": CKANSource(
        CKANConfig(
            dataset_id="committee-of-adjustment-applications",
            access_mode="bulk_csv",
            year_start=2020,
        )
    ),
}
```

New closing block:
```python
    "coa": CKANSource(
        CKANConfig(
            dataset_id="committee-of-adjustment-applications",
            access_mode="bulk_csv",
            year_start=2020,
        )
    ),
    "dev_applications": CKANSource(
        CKANConfig(
            dataset_id="development-applications",
            access_mode="datastore",
            year_start=2000,
            year_column="date_submitted",
        )
    ),
}
```

---

## Task 3: Verify GREEN

### Step 4: Run the registry tests

```bash
uv run pytest tests/test_registry.py -v
```

Expected: All 5 tests pass (including the new `test_dev_applications_config`).

### Step 5: Run the full test suite — no regressions

```bash
just test
```

Expected: All tests pass.

### Step 6: Verify `zoneto status` shows the new source

```bash
uv run zoneto status
```

Expected: A table with 4 rows. `dev_applications` shows `—` for rows and last-modified (no data fetched yet).

---

## Task 4: Commit

### Step 7: Stage and commit

```bash
git add tests/test_registry.py src/zoneto/sources/registry.py
git commit -m "feat: register dev_applications source in pipeline

Fetches all Toronto development applications (OZ/SA/CD/SB/PL types)
from the CKAN DataStore. year_start=2000 captures the full archive
(earliest records are from 2008). Uses date_submitted for year partitioning.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Optional: Manual live sync

This step requires network access and writes real data (~26k rows). Run only if you want to verify end-to-end behaviour against the live CKAN API:

```bash
uv run zoneto sync --source dev_applications
```

Expected: Writes `data/dev_applications/year=YYYY/` Parquet partitions covering 2008–present and exits without error. After completion, `uv run zoneto status` shows a non-zero row count for `dev_applications`.

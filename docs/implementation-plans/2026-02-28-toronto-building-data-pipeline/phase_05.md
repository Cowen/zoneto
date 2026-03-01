# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Create the source registry that maps the three Toronto dataset names to configured `CKANSource` instances.

**Architecture:** A module-level dict `SOURCES: dict[str, Source]` in `registry.py`. The CLI and storage layers import from this dict — they never instantiate sources directly. Adding a new source requires only one new entry here.

**Tech Stack:** Python 3.13, polars, pytest

**Scope:** Phase 5 of 7 (source registry)

**Codebase verified:** 2026-02-28 — `src/zoneto/sources/registry.py` and `tests/test_registry.py` do not exist yet.

---

### Task 1: Source registry

**Files:**
- Create: `src/zoneto/sources/registry.py`

**Step 1: Create `src/zoneto/sources/registry.py`**

```python
from __future__ import annotations

from zoneto.models import CKANConfig
from zoneto.sources.base import Source
from zoneto.sources.ckan import CKANSource

SOURCES: dict[str, Source] = {
    "permits_active": CKANSource(CKANConfig(
        dataset_id="building-permits-active-permits",
        access_mode="datastore",
    )),
    "permits_cleared": CKANSource(CKANConfig(
        dataset_id="building-permits-cleared-permits",
        access_mode="bulk_csv",
    )),
    "coa": CKANSource(CKANConfig(
        dataset_id="committee-of-adjustment-applications",
        access_mode="bulk_csv",
    )),
}
```

**Step 2: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

---

### Task 2: Registry tests

**Files:**
- Create: `tests/test_registry.py`

**Step 1: Write `tests/test_registry.py`**

```python
from __future__ import annotations

from zoneto.sources.base import Source
from zoneto.sources.registry import SOURCES


def test_all_three_sources_present() -> None:
    assert set(SOURCES.keys()) == {"permits_active", "permits_cleared", "coa"}


def test_source_names_match_dataset_ids() -> None:
    assert SOURCES["permits_active"].name == "building-permits-active-permits"
    assert SOURCES["permits_cleared"].name == "building-permits-cleared-permits"
    assert SOURCES["coa"].name == "committee-of-adjustment-applications"


def test_all_sources_satisfy_protocol() -> None:
    """Each value is a runtime-checkable Source (has name attr and fetch method)."""
    for key, source in SOURCES.items():
        assert isinstance(source, Source), f"{key}: does not satisfy Source protocol"


def test_all_sources_have_callable_fetch() -> None:
    for key, source in SOURCES.items():
        assert callable(source.fetch), f"{key}: fetch is not callable"
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_registry.py -v
```

Expected: All 4 tests pass.

**Step 3: Run full suite**

```bash
uv run pytest -v
```

Expected: All tests pass.

**Step 4: Commit**

```bash
git add src/zoneto/sources/registry.py tests/test_registry.py
git commit -m "feat: add source registry with three Toronto datasets"
```

# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Extend `CKANSource` with bulk CSV mode — discovers year-based CSV resources via `package_show` and downloads them.

**Architecture:** Add `_fetch_bulk_csv()` to `CKANSource` and update `fetch()` to dispatch to it when `access_mode == "bulk_csv"`. Year extraction uses `\b(20\d{2})\b` regex on resource `name` fields. Resources with no 4-digit year or with a year below `year_start` are skipped. All qualifying CSVs are concatenated with `how="diagonal"` (handles minor schema differences between years).

**Tech Stack:** Python 3.13, httpx, polars, io (stdlib), re (stdlib), pytest, pytest-httpx

**Scope:** Phase 4 of 7 (CKAN bulk CSV source)

**Codebase verified:** 2026-02-28 — `src/zoneto/sources/ckan.py` exists from Phase 3; `tests/test_ckan_bulk_csv.py` does not exist yet.

---

### Task 1: Add `_fetch_bulk_csv` and update dispatch

**Files:**
- Modify: `src/zoneto/sources/ckan.py`

**Step 1: Add `import io` at the top of `src/zoneto/sources/ckan.py`**

The current imports section is:
```python
from __future__ import annotations

import re

import httpx
import polars as pl
```

Change it to:
```python
from __future__ import annotations

import io
import re

import httpx
import polars as pl
```

**Step 2: Replace the `fetch()` method**

Current `fetch()`:
```python
    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        with httpx.Client(base_url=CKAN_BASE, timeout=120.0) as client:
            if self.config.access_mode == "datastore":
                df = self._fetch_datastore(client)
            else:
                raise NotImplementedError("bulk_csv mode not yet implemented")
        return self._normalize(df)
```

Replace with:
```python
    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        with httpx.Client(base_url=CKAN_BASE, timeout=120.0) as client:
            if self.config.access_mode == "datastore":
                df = self._fetch_datastore(client)
            else:
                df = self._fetch_bulk_csv(client)
        return self._normalize(df)
```

**Step 3: Add `_fetch_bulk_csv()` method to `CKANSource`**

Add this method after `_fetch_datastore()` and before `_normalize()`:

```python
    def _fetch_bulk_csv(self, client: httpx.Client) -> pl.DataFrame:
        """Download year-based CSV resources discovered via package_show."""
        resp = client.get(
            "/api/3/action/package_show",
            params={"id": self.config.dataset_id},
        )
        resp.raise_for_status()
        resources: list[dict] = resp.json()["result"]["resources"]

        dfs: list[pl.DataFrame] = []
        for resource in resources:
            match = re.search(r"\b(20\d{2})\b", resource.get("name", ""))
            if match is None:
                continue
            if int(match.group(1)) < self.config.year_start:
                continue
            csv_resp = client.get(resource["url"])
            csv_resp.raise_for_status()
            df = pl.read_csv(
                io.BytesIO(csv_resp.content),
                infer_schema_length=10000,
            )
            dfs.append(df)

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal")
```

**Step 4: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

---

### Task 2: Bulk CSV tests

**Files:**
- Create: `tests/test_ckan_bulk_csv.py`

**Step 1: Write `tests/test_ckan_bulk_csv.py`**

```python
from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from zoneto.models import CKANConfig
from zoneto.sources.ckan import CKANSource


@pytest.fixture
def source() -> CKANSource:
    return CKANSource(CKANConfig(
        dataset_id="building-permits-cleared-permits",
        access_mode="bulk_csv",
        year_start=2020,
    ))


def _package_show_response(resources: list[dict]) -> dict:
    """Build a package_show API response with the given resources."""
    return {"result": {"resources": resources}}


def test_downloads_qualifying_years_only(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Only resources with year >= year_start are downloaded."""
    httpx_mock.add_response(
        json=_package_show_response([
            {"name": "Cleared Permits 2019", "url": "https://example.com/2019.csv"},
            {"name": "Cleared Permits 2020", "url": "https://example.com/2020.csv"},
            {"name": "Cleared Permits 2021", "url": "https://example.com/2021.csv"},
        ]),
    )
    # Only 2020 and 2021 are downloaded (2019 is below year_start=2020)
    httpx_mock.add_response(content=b"Application Date,Permit No\n2020-01-01,B001\n")
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-06-15,B002\n")

    df = source.fetch()
    assert len(df) == 2


def test_all_qualifying_csvs_concatenated(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """All qualifying CSVs are concatenated into a single DataFrame."""
    httpx_mock.add_response(
        json=_package_show_response([
            {"name": "Cleared 2020", "url": "https://example.com/2020.csv"},
            {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
            {"name": "Cleared 2022", "url": "https://example.com/2022.csv"},
        ]),
    )
    httpx_mock.add_response(content=b"Application Date,Permit No\n2020-01-01,B001\n2020-02-01,B002\n")
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-03-01,B003\n")
    httpx_mock.add_response(content=b"Application Date,Permit No\n2022-04-01,B004\n2022-05-01,B005\n")

    df = source.fetch()
    assert len(df) == 5


def test_non_year_resources_are_skipped(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Resources whose names contain no 4-digit year are skipped."""
    httpx_mock.add_response(
        json=_package_show_response([
            {"name": "Active Permits", "url": "https://example.com/active.csv"},
            {"name": "Metadata file", "url": "https://example.com/meta.csv"},
            {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
        ]),
    )
    # Only the 2021 resource should trigger a download
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-01-01,B001\n")

    df = source.fetch()
    assert len(df) == 1


def test_no_qualifying_resources_returns_empty(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """When no resources qualify, fetch returns an empty DataFrame."""
    httpx_mock.add_response(
        json=_package_show_response([
            {"name": "Cleared 2015", "url": "https://example.com/2015.csv"},
            {"name": "Cleared 2019", "url": "https://example.com/2019.csv"},
        ]),
    )
    # No downloads should occur (both years are below year_start=2020)

    df = source.fetch()
    assert df.is_empty()


def test_year_column_set_from_application_date(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """year column is derived from the application_date column after normalization."""
    httpx_mock.add_response(
        json=_package_show_response([
            {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
        ]),
    )
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-06-01,B001\n")

    df = source.fetch()
    assert "year" in df.columns
    assert df["year"][0] == 2021
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_ckan_bulk_csv.py -v
```

Expected: All 5 tests pass.

**Step 3: Run full suite**

```bash
uv run pytest -v
```

Expected: All tests pass (test_models, test_ckan_datastore, test_ckan_bulk_csv).

**Step 4: Commit**

```bash
git add src/zoneto/sources/ckan.py tests/test_ckan_bulk_csv.py
git commit -m "feat: add CKANSource bulk_csv mode with year-based resource filtering"
```

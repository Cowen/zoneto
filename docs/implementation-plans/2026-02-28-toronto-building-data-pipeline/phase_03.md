# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Implement `CKANSource` in datastore mode — paginated `datastore_search` API calls with column normalization.

**Architecture:** `CKANSource` class lives in `src/zoneto/sources/ckan.py`. `fetch()` dispatches to `_fetch_datastore()` when `access_mode == "datastore"`. `_normalize()` handles column renaming, date parsing, year derivation, and source labelling. Bulk CSV mode is stubbed with `NotImplementedError` until Phase 4.

**Tech Stack:** Python 3.13, httpx, polars, pytest, pytest-httpx

**Scope:** Phase 3 of 7 (CKAN datastore source)

**Codebase verified:** 2026-02-28 — `src/zoneto/sources/ckan.py` and `tests/test_ckan_datastore.py` do not exist yet.

---

### Task 1: CKANSource — datastore fetch and normalization

**Files:**
- Create: `src/zoneto/sources/ckan.py`

**Step 1: Create `src/zoneto/sources/ckan.py`**

```python
from __future__ import annotations

import re

import httpx
import polars as pl

from zoneto.models import CKANConfig

CKAN_BASE = "https://ckan0.cf.opendata.inter.prod-toronto.ca"


class CKANSource:
    """Fetches data from a City of Toronto CKAN dataset."""

    def __init__(self, config: CKANConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config.dataset_id

    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        with httpx.Client(base_url=CKAN_BASE, timeout=120.0) as client:
            if self.config.access_mode == "datastore":
                df = self._fetch_datastore(client)
            else:
                raise NotImplementedError("bulk_csv mode not yet implemented")
        return self._normalize(df)

    def _fetch_datastore(self, client: httpx.Client) -> pl.DataFrame:
        """Paginate through datastore_search until the response has no records."""
        limit = 32000
        offset = 0
        records: list[dict] = []

        while True:
            resp = client.get(
                "/api/3/action/datastore_search",
                params={
                    "id": self.config.dataset_id,
                    "limit": limit,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            page_records: list[dict] = resp.json()["result"]["records"]
            if not page_records:
                break
            records.extend(page_records)
            offset += limit

        if not records:
            return pl.DataFrame()
        return pl.DataFrame(records)

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize column names, parse dates, derive year, add source_name."""
        if df.is_empty():
            return df

        # 1. Rename all columns to snake_case
        rename_map = {
            c: re.sub(r"[^a-z0-9]+", "_", c.lower()).strip("_")
            for c in df.columns
        }
        df = df.rename(rename_map)

        # 2. Parse all columns whose names contain "date" (best-effort, nulls allowed)
        date_cols = [c for c in df.columns if "date" in c]
        for col in date_cols:
            df = df.with_columns(
                pl.col(col).cast(pl.String).str.to_date(strict=False).alias(col)
            )

        # 3. Derive year from application_date (null dates → year 0)
        if "application_date" in df.columns:
            df = df.with_columns(
                pl.col("application_date")
                .dt.year()
                .fill_null(0)
                .cast(pl.Int32)
                .alias("year")
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int32).alias("year"))

        # 4. Label records with their source dataset ID
        df = df.with_columns(pl.lit(self.config.dataset_id).alias("source_name"))

        return df
```

**Step 2: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

---

### Task 2: Datastore tests

**Files:**
- Create: `tests/test_ckan_datastore.py`

**Step 1: Write `tests/test_ckan_datastore.py`**

```python
from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from zoneto.models import CKANConfig
from zoneto.sources.ckan import CKANSource


@pytest.fixture
def source() -> CKANSource:
    return CKANSource(CKANConfig(
        dataset_id="building-permits-active-permits",
        access_mode="datastore",
    ))


def test_single_page_returns_all_records(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """A single page with records followed by an empty page fetches all records."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-15", "Permit No": "A001"},
            {"Application Date": "2024-02-20", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert len(df) == 2


def test_multi_page_pagination(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Records from multiple pages are concatenated correctly."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-01", "Permit No": "A001"},
            {"Application Date": "2024-01-02", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-03", "Permit No": "A003"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert len(df) == 3


def test_empty_first_response_returns_empty_dataframe(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """An empty first response terminates immediately and returns an empty DataFrame."""
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert df.is_empty()


def test_normalization_snake_case_columns(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Column names are converted to snake_case."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-06-15", "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert "application_date" in df.columns
    assert "permit_no" in df.columns
    assert "Application Date" not in df.columns


def test_normalization_year_derived_from_date(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """year column is derived from application_date."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2023-03-10", "Permit No": "A001"},
            {"Application Date": "2024-07-22", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    years = df["year"].to_list()
    assert 2023 in years
    assert 2024 in years


def test_null_date_produces_year_zero(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Records with null application dates get year=0."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": None, "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert df["year"][0] == 0


def test_source_name_column_added(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """source_name column is set to the dataset_id."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-01", "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert "source_name" in df.columns
    assert df["source_name"][0] == "building-permits-active-permits"
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_ckan_datastore.py -v
```

Expected: All 7 tests pass.

**Step 3: Run full suite**

```bash
uv run pytest -v
```

Expected: All tests pass (test_models.py + test_ckan_datastore.py).

**Step 4: Commit**

```bash
git add src/zoneto/sources/ckan.py tests/test_ckan_datastore.py
git commit -m "feat: add CKANSource datastore mode with normalization"
```

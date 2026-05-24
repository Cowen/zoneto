# Product Strategy Pivot — Phase 5: AIC Scraper Expansion

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Expand the AIC scraper to fetch full application records (not just decision dates) from the ArcGIS FeatureServer, producing a live alternative to the retired CKAN dev_applications dataset.

**Architecture:** New `fetch_aic_applications()` in `aic.py` queries the same ArcGIS endpoint without a folderrsn filter. New `AICSource` class in `aic_source.py` implements the Source protocol and writes Hive-partitioned Parquet to `data/aic_applications/`. `registry.py` exposes it as `aic_applications`. `enrich_dev()` prefers AIC records over CKAN records for matching folderrsn values. The `zoneto aic --full` CLI flag triggers the full fetch.

**Tech Stack:** httpx (already installed), polars, pytest-httpx for mocking

**Scope:** Phase 5 of 8. Independent of the serving layer (Phases 1–4).

**Codebase verified:** 2026-03-17

---

## Task 1: Add fetch_aic_applications() to aic.py

**Files:**
- Modify: `src/zoneto/sources/aic.py`
- Create: `tests/sources/__init__.py` (if absent)
- Create: `tests/sources/test_aic_source.py`

**Step 1: Create `tests/sources/__init__.py` if it does not exist**

```python
```

Check with: `ls tests/sources/` — if `__init__.py` is missing, create it.

**Step 2: Write the failing test**

Create `tests/sources/test_aic_source.py`:

```python
"""Tests for AIC full application record scraper."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from pytest_httpx import HTTPXMock

from zoneto.sources.aic import fetch_aic_applications

_ARCGIS_BASE = (
    "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services"
    "/COTGEO_IBMS_AIC_POINT/FeatureServer/0"
)
_ARCGIS_QUERY_URL = _ARCGIS_BASE + "/query"
_ARCGIS_META_URL = _ARCGIS_BASE


def _epoch_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _meta_resp(fields: list[str]) -> str:
    """Fake ArcGIS layer metadata response."""
    return json.dumps(
        {
            "fields": [{"name": f, "type": "esriFieldTypeString"} for f in fields],
            "maxRecordCount": 1000,
        }
    )


def _count_resp(count: int) -> str:
    return json.dumps({"count": count})


def _features_resp(features: list[dict]) -> str:
    return json.dumps({"features": [{"attributes": f} for f in features]})


SAMPLE_FIELDS = [
    "FOLDERRSN",
    "FOLDERTYPE",
    "STATUS",
    "DATE_SUBMITTED",
    "LOCATION",
    "WARD",
    "DESCRIPTION",
    "LATITUDE",
    "LONGITUDE",
]

SAMPLE_FEATURE = {
    "FOLDERRSN": "12345",
    "FOLDERTYPE": "OZ",
    "STATUS": "Under Review",
    "DATE_SUBMITTED": _epoch_ms(date(2023, 6, 1)),
    "LOCATION": "100 King St W",
    "WARD": "10",
    "DESCRIPTION": "47-storey mixed-use tower",
    "LATITUDE": 43.6480,
    "LONGITUDE": -79.3813,
}


def test_fetch_aic_applications_writes_parquet(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """fetch_aic_applications() writes Hive-partitioned Parquet to data/aic_applications/."""
    # metadata → count → features page → empty page (signals end of pagination)
    httpx_mock.add_response(url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_count_resp(1))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([SAMPLE_FEATURE]))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([]))

    count = fetch_aic_applications(tmp_path)

    assert count == 1
    # Hive-partitioned output must exist
    aic_dir = tmp_path / "aic_applications"
    parquet_files = list(aic_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 1, "Expected at least one Parquet file"


def test_fetch_aic_applications_returns_expected_columns(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Output DataFrame includes year, source_name, folderrsn, application_type."""
    httpx_mock.add_response(url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_count_resp(1))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([SAMPLE_FEATURE]))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([]))

    fetch_aic_applications(tmp_path)

    aic_dir = tmp_path / "aic_applications"
    df = pl.read_parquet(aic_dir, hive_partitioning=True)
    assert "folderrsn" in df.columns
    assert "application_type" in df.columns
    assert "year" in df.columns
    assert "source_name" in df.columns
    assert df["source_name"][0] == "aic_applications"


def test_fetch_aic_applications_year_from_date_submitted(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """year column is derived from DATE_SUBMITTED (2023-06-01 → year=2023)."""
    httpx_mock.add_response(url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_count_resp(1))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([SAMPLE_FEATURE]))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([]))

    fetch_aic_applications(tmp_path)
    df = pl.read_parquet(tmp_path / "aic_applications", hive_partitioning=True)
    assert df["year"][0] == 2023


def test_fetch_aic_applications_empty_response(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Returns 0 when ArcGIS returns no features."""
    httpx_mock.add_response(url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_count_resp(0))
    httpx_mock.add_response(url=_ARCGIS_QUERY_URL, text=_features_resp([]))

    count = fetch_aic_applications(tmp_path)
    assert count == 0
```

**Step 3: Run tests to confirm failure**

```bash
uv run pytest tests/sources/test_aic_source.py -v
```

Expected: `ImportError: cannot import name 'fetch_aic_applications' from 'zoneto.sources.aic'`

**Step 4: Add `fetch_aic_applications()` to `src/zoneto/sources/aic.py`**

Add the following after the existing `_epoch_ms_to_date` helper function (after line 171 in the original file):

```python
# Canonical ArcGIS field → snake_case output column mapping.
# Derived from the COTGEO_IBMS_AIC_POINT feature layer schema.
# Fields not in this map are included using their raw name lowercased.
_AIC_FIELD_MAP: dict[str, str] = {
    "FOLDERRSN": "folderrsn",
    "FOLDERTYPE": "application_type",
    "STATUS": "status",
    "DATE_SUBMITTED": "date_submitted",
    "LOCATION": "street_address",
    "WARD": "ward_number",
    "DESCRIPTION": "description",
    "LATITUDE": "lat",
    "LONGITUDE": "lon",
    "LATEST_MILESTONE": "latest_milestone",
    "LATEST_MILESTONE_DATE": "latest_milestone_date",
    "COMPLETE_DATE": "complete_date",
}

_AIC_APP_BASE_URL = (
    "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services"
    "/COTGEO_IBMS_AIC_POINT/FeatureServer/0"
)


def _discover_aic_fields(client: httpx.Client) -> list[str]:
    """Query ArcGIS layer metadata to get all available field names."""
    resp = client.get(_AIC_APP_BASE_URL, params={"f": "json"})
    resp.raise_for_status()
    meta = resp.json()
    return [f["name"] for f in meta.get("fields", [])]


def fetch_aic_applications(
    data_dir: Path,
    *,
    batch_size: int = 200,
) -> int:
    """Fetch full application records from ArcGIS COTGEO_IBMS_AIC_POINT FeatureServer.

    Unlike fetch_aic_decisions_arcgis(), this fetches ALL records without
    filtering by folderrsn — producing a live replacement for the retired
    CKAN dev_applications dataset.

    Discovers available fields via the layer metadata endpoint, then paginates
    all records in batches of `batch_size`.

    Writes Hive-partitioned Parquet to data_dir/aic_applications/year=YYYY/.
    Returns count of fetched rows.
    """
    query_url = _AIC_APP_BASE_URL + "/query"
    today = date.today()

    with httpx.Client(timeout=30.0) as client:
        # Discover available fields
        available_fields = _discover_aic_fields(client)
        # Request all known fields that exist in this layer
        out_fields = [f for f in _AIC_FIELD_MAP if f in available_fields]
        if not out_fields:
            out_fields = available_fields
        out_fields_str = ",".join(out_fields)

        # Get total record count
        count_resp = client.post(
            query_url,
            data={
                "where": "1=1",
                "returnCountOnly": "true",
                "f": "json",
            },
        )
        count_resp.raise_for_status()
        total = count_resp.json().get("count", 0)

        if total == 0:
            logger.info("AIC applications: no records found")
            return 0

        logger.info("AIC applications: fetching %d total records", total)

        all_rows: list[dict] = []
        offset = 0
        n_batches = math.ceil(total / batch_size)

        for batch_num in range(n_batches):
            resp = client.post(
                query_url,
                data={
                    "where": "1=1",
                    "outFields": out_fields_str,
                    "resultOffset": str(offset),
                    "resultRecordCount": str(batch_size),
                    "f": "json",
                },
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])
            if not features:
                break

            for feat in features:
                attrs = feat["attributes"]
                row: dict = {}
                for raw_field, out_field in _AIC_FIELD_MAP.items():
                    if raw_field in attrs:
                        val = attrs[raw_field]
                        # Convert epoch-ms timestamps to date
                        if raw_field in (
                            "DATE_SUBMITTED",
                            "LATEST_MILESTONE_DATE",
                            "COMPLETE_DATE",
                        ):
                            val = _epoch_ms_to_date(val)
                        row[out_field] = val
                # Include any extra fields not in our map
                for raw_field in attrs:
                    if raw_field not in _AIC_FIELD_MAP:
                        row[raw_field.lower()] = attrs[raw_field]
                row["scraped_at"] = today
                row["source_name"] = "aic_applications"
                all_rows.append(row)

            offset += len(features)
            logger.info(
                "AIC applications: batch %d/%d done (%d rows so far)",
                batch_num + 1,
                n_batches,
                len(all_rows),
            )

    if not all_rows:
        return 0

    df = pl.DataFrame(all_rows)

    # Derive year from date_submitted (required by Source protocol + Hive partitioning)
    if "date_submitted" in df.columns:
        df = df.with_columns(
            pl.col("date_submitted")
            .cast(pl.Date, strict=False)
            .dt.year()
            .cast(pl.Int32)
            .fill_null(0)
            .alias("year")
        )
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int32).alias("year"))

    # Ensure folderrsn is String
    if "folderrsn" in df.columns:
        df = df.with_columns(pl.col("folderrsn").cast(pl.String))

    from zoneto.storage import write_source  # noqa: PLC0415

    written = write_source(df, "aic_applications", data_dir)
    logger.info("AIC applications: wrote %d rows to aic_applications/", written)
    return written
```

**Step 5: Run tests to confirm they pass**

```bash
uv run pytest tests/sources/test_aic_source.py -v
```

Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/zoneto/sources/aic.py tests/sources/test_aic_source.py
git commit -m "feat: add fetch_aic_applications() for full AIC record ingestion"
```

---

## Task 2: Create AICSource class and add to registry

**Files:**
- Create: `src/zoneto/sources/aic_source.py`
- Modify: `src/zoneto/sources/registry.py`

**Step 1: Write the failing test**

Add to `tests/sources/test_aic_source.py`:

```python
def test_aic_source_implements_protocol(tmp_path: Path) -> None:
    """AICSource satisfies the Source runtime-checkable protocol."""
    from zoneto.sources.aic_source import AICSource
    from zoneto.sources.base import Source

    source = AICSource(data_dir=tmp_path)
    assert isinstance(source, Source), "AICSource must satisfy Source protocol"
    assert source.name == "aic_applications"


def test_aic_source_in_registry() -> None:
    """aic_applications key exists in SOURCES dict."""
    from zoneto.sources.registry import SOURCES

    assert "aic_applications" in SOURCES, (
        "'aic_applications' must be registered in SOURCES"
    )
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/sources/test_aic_source.py::test_aic_source_implements_protocol tests/sources/test_aic_source.py::test_aic_source_in_registry -v
```

Expected: `ImportError: cannot import name 'AICSource'`

**Step 3: Create `src/zoneto/sources/aic_source.py`**

```python
"""AICSource: Source protocol implementation for AIC full application records."""
from __future__ import annotations

from pathlib import Path

import polars as pl


class AICSource:
    """Source that fetches full application records from the AIC ArcGIS FeatureServer.

    Unlike CKANSource, this source reads from the ArcGIS REST API directly —
    providing a live alternative to the retired CKAN dev_applications dataset.
    """

    name: str = "aic_applications"

    def __init__(
        self,
        data_dir: Path = Path("data"),
        *,
        batch_size: int = 200,
    ) -> None:
        self._data_dir = data_dir
        self._batch_size = batch_size

    def fetch(self) -> pl.DataFrame:
        """Fetch all AIC application records from ArcGIS and return as DataFrame.

        Writes Hive-partitioned Parquet to data_dir/aic_applications/ and
        returns the full DataFrame.
        """
        from zoneto.sources.aic import fetch_aic_applications  # noqa: PLC0415

        fetch_aic_applications(self._data_dir, batch_size=self._batch_size)
        return pl.read_parquet(
            self._data_dir / "aic_applications",
            hive_partitioning=True,
        )
```

**Step 4: Modify `src/zoneto/sources/registry.py`**

Add `AICSource` import and register it in `SOURCES`:

```python
from __future__ import annotations

from zoneto.models import CKANConfig
from zoneto.sources.aic_source import AICSource
from zoneto.sources.base import Source
from zoneto.sources.ckan import CKANSource

SOURCES: dict[str, Source] = {
    "permits_active": CKANSource(
        CKANConfig(
            dataset_id="building-permits-active-permits",
            access_mode="datastore",
            year_start=2020,
        )
    ),
    "permits_cleared": CKANSource(
        CKANConfig(
            dataset_id="building-permits-cleared-permits",
            access_mode="datastore",
            year_start=2020,
        )
    ),
    "coa": CKANSource(
        CKANConfig(
            dataset_id="committee-of-adjustment-applications",
            access_mode="bulk_csv",
            year_start=2018,
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
    "aic_applications": AICSource(),
}
```

**Step 5: Run tests**

```bash
uv run pytest tests/sources/test_aic_source.py -v
```

Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/zoneto/sources/aic_source.py src/zoneto/sources/registry.py
git commit -m "feat: add AICSource class and register aic_applications in SOURCES"
```

---

## Task 3: Add --full flag to the `aic` CLI command

**Files:**
- Modify: `src/zoneto/cli.py` (lines 84–99, `aic` command)

**Step 1: Find the existing `aic` command in `src/zoneto/cli.py`** (around line 84):

```python
@app.command()
def aic(
    delay: float = typer.Option(1.0, help="Delay between AIC requests (seconds)."),
) -> None:
    """Scrape AIC portal for OZ/SA decision milestone dates."""
```

**Step 2: Update the `aic` command to add `--full` flag**

Replace the existing `aic` command with:

```python
@app.command()
def aic(
    delay: float = typer.Option(1.0, help="Delay between AIC requests (seconds)."),
    full: bool = typer.Option(
        False,
        "--full/--no-full",
        help="Fetch full application records from ArcGIS (replaces CKAN dev_applications).",
    ),
) -> None:
    """Scrape AIC portal for OZ/SA decision milestone dates.

    With --full: also fetches complete application records from ArcGIS,
    writing to data/aic_applications/. This provides a live replacement
    for the retired CKAN dev_applications dataset.
    """
    from zoneto.sources.aic import fetch_aic_decisions_arcgis

    console.print("[bold]Scraping AIC portal...[/bold]")
    try:
        n = fetch_aic_decisions_arcgis(DATA_DIR, batch_size=200)
        console.print(f"[green]✓[/green] AIC decisions: {n} new rows")

        if full:
            from zoneto.sources.aic import fetch_aic_applications  # noqa: PLC0415

            console.print("[bold]Fetching full AIC application records...[/bold]")
            n_full = fetch_aic_applications(DATA_DIR)
            console.print(f"[green]✓[/green] AIC applications: {n_full} rows written")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)
```

**Step 3: Add `aic-full` task to justfile**

Add to `justfile`:

```makefile
# Fetch full AIC application records (replacement for CKAN dev_applications)
aic-full:
    uv run zoneto aic --full
```

**Step 4: Verify CLI**

```bash
uv run zoneto aic --help
```

Expected: `--full/--no-full` option appears in help.

**Step 5: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/zoneto/cli.py justfile
git commit -m "feat: add --full flag to 'aic' command and 'just aic-full' task"
```

---

## Task 4: Add AIC preference logic to enrich_dev()

**Files:**
- Modify: `src/zoneto/analytics/enrich.py` (beginning of `enrich_dev()`, after parquet read)
- Create: `tests/analytics/test_enrich_aic_preference.py`

**Step 1: Write the failing test**

Create `tests/analytics/test_enrich_aic_preference.py`:

```python
"""Tests verifying enrich_dev() prefers AIC records over CKAN for same folderrsn."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import enrich_dev


def _write_minimal_ckan_dev(tmp_path: Path) -> None:
    """Write a minimal CKAN dev_applications parquet."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003"],
            "application_type": ["OZ", "OZ", "SA"],
            "status": ["Closed", "Under Review", "Closed"],
            "date_submitted": ["2021-06-01", "2022-01-15", "2020-03-10"],
            "description": ["CKAN OZ desc", "CKAN OZ2 desc", "CKAN SA desc"],
            "ward_number": ["10", "11", "10"],
            "x": [636000.0, 636100.0, 636200.0],
            "y": [4836000.0, 4836100.0, 4836200.0],
            "year": pl.Series([2021, 2022, 2020], dtype=pl.Int32),
            "source_name": ["dev_applications"] * 3,
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def _write_minimal_aic_apps(tmp_path: Path) -> None:
    """Write AIC application records that overlap with CKAN on F001."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F004"],  # F001 overlaps, F004 is AIC-only
            "application_type": ["OZ", "OZ"],
            "status": ["Approved", "Under Review"],
            "date_submitted": pl.Series(
                ["2021-06-01", "2023-05-01"]
            ).str.to_date(),
            "description": ["AIC OZ desc (preferred)", "AIC-only application"],
            "ward_number": ["10", "12"],
            "year": pl.Series([2021, 2023], dtype=pl.Int32),
            "source_name": ["aic_applications"] * 2,
        }
    )
    out = tmp_path / "aic_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_uses_aic_over_ckan_for_matching_folderrsn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When AIC data exists, enrich_dev() uses AIC status/description for F001."""
    _write_minimal_ckan_dev(tmp_path)
    _write_minimal_aic_apps(tmp_path)

    # Stub out spatial join, reference fetch, and AIC decision scrape
    monkeypatch.setattr(
        "zoneto.analytics.enrich._spatial_join_dev",
        lambda df, data_dir: df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        ),
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_reference", lambda data_dir: None
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_aic_decisions",
        lambda data_dir, delay: 0,
    )

    enrich_dev(tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    # F001: AIC record should override CKAN
    f001 = df.filter(pl.col("folderrsn") == "F001")
    assert len(f001) == 1
    assert f001["description"][0] == "AIC OZ desc (preferred)"

    # F002 and F003: CKAN-only records still present
    assert len(df.filter(pl.col("folderrsn") == "F002")) == 1
    assert len(df.filter(pl.col("folderrsn") == "F003")) == 1


def test_enrich_dev_without_aic_falls_back_to_ckan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When no aic_applications parquet exists, enrich_dev() uses CKAN data unchanged."""
    _write_minimal_ckan_dev(tmp_path)
    # No AIC data written

    monkeypatch.setattr(
        "zoneto.analytics.enrich._spatial_join_dev",
        lambda df, data_dir: df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        ),
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_reference", lambda data_dir: None
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_aic_decisions",
        lambda data_dir, delay: 0,
    )

    enrich_dev(tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    f001 = df.filter(pl.col("folderrsn") == "F001")
    assert f001["description"][0] == "CKAN OZ desc"
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/analytics/test_enrich_aic_preference.py -v
```

Expected: `test_enrich_dev_uses_aic_over_ckan_for_matching_folderrsn` fails because the AIC preference logic is not yet implemented.

**Step 3: Add AIC preference logic to `enrich_dev()` in `src/zoneto/analytics/enrich.py`**

At the beginning of `enrich_dev()`, after the initial `df = pl.read_parquet(...)` line (around line 562), add:

```python
    # --- AIC application records: prefer over CKAN for matching folderrsn ---
    # If data/aic_applications/ exists (from 'zoneto aic --full'), merge it:
    # - AIC records override CKAN records for the same folderrsn
    # - AIC-only records (not in CKAN) are added to the dataset
    aic_apps_path = data_dir / "aic_applications"
    if aic_apps_path.exists() and any(aic_apps_path.rglob("*.parquet")):
        aic_df = pl.read_parquet(aic_apps_path, hive_partitioning=True)
        # Ensure folderrsn is String for join
        if "folderrsn" in aic_df.columns:
            aic_df = aic_df.with_columns(pl.col("folderrsn").cast(pl.String))
        if "folderrsn" in df.columns:
            df = df.with_columns(pl.col("folderrsn").cast(pl.String))
        # Align schemas: use CKAN schema as base, update matching rows from AIC
        aic_rsns = set(aic_df["folderrsn"].to_list()) if "folderrsn" in aic_df.columns else set()
        # Keep CKAN rows not in AIC; replace CKAN rows that AIC has; add AIC-only rows
        ckan_only = df.filter(~pl.col("folderrsn").is_in(list(aic_rsns)))
        # For AIC records, map column names to CKAN schema where possible
        shared_cols = [c for c in df.columns if c in aic_df.columns]
        aic_aligned = aic_df.select(shared_cols)
        # Add missing CKAN columns as nulls
        for col in df.columns:
            if col not in aic_aligned.columns:
                aic_aligned = aic_aligned.with_columns(
                    pl.lit(None).cast(df[col].dtype).alias(col)
                )
        aic_aligned = aic_aligned.select(df.columns)
        df = pl.concat([ckan_only, aic_aligned], how="diagonal")
        logger.info(
            "enrich_dev: merged AIC records — %d CKAN-only, %d from AIC (%d total)",
            len(ckan_only),
            len(aic_aligned),
            len(df),
        )
```

**Step 4: Run tests**

```bash
uv run pytest tests/analytics/test_enrich_aic_preference.py -v
```

Expected: All tests pass.

**Step 5: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 6: Run linter and type checker**

```bash
uv run ruff check src/zoneto/sources/ src/zoneto/analytics/enrich.py tests/sources/
uv run ty check src/zoneto/sources/ src/zoneto/analytics/enrich.py
```

Expected: No errors.

**Step 7: Commit**

```bash
git add src/zoneto/analytics/enrich.py tests/analytics/test_enrich_aic_preference.py
git commit -m "feat: prefer AIC application records over CKAN in enrich_dev() when available"
```

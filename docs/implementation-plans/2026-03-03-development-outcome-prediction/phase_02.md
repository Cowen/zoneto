# Phase 2: Enrichment Pipeline

<!-- Plan date: 2026-03-04 -->
<!-- Branch: development-outcome-prediction -->

## Goal

Build `src/zoneto/analytics/enrich.py` with three public functions:
`fetch_reference`, `enrich_coa`, and `enrich_dev`. Add `zoneto enrich` CLI
command. All reference data downloads are cached under `data/reference/`.

## Background: Reference Datasets

| Name | URL | Format | Use |
|---|---|---|---|
| Zoning | `https://ckan0.cf.opendata.inter.prod-toronto.ca/datastore/dump/76a2620f-a6b4-495d-8e41-c0ede1f8a928` | CSV with `ZN_ZONE` + `geometry` (GeoJSON string) | Join dev_applications by point-in-polygon |
| Heritage register | `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/e41da515-5ad1-4bc3-85ea-18ec9e55cd33/resource/108b1080-d048-439f-a9e8-e8d6cd81bddb/download/heritage_register_address_points_wgs84.zip` | ZIP → SHP (WGS84 points) | in_heritage_register flag |
| Heritage districts | `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/37a3c911-0813-4e87-90ed-3b9fa6156a63/resource/8e6b9347-63a8-4dac-91fb-a6491a8c1e5a/download/heritageconservationdistrict.zip` | ZIP → SHP | in_heritage_district flag |
| Secondary plans | `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/70a544e9-ee83-43a4-b0be-0dc973627ad7/resource/08099a8c-a598-4ca3-8395-e4159cc1ec1a/download/secondary-plans-data-2017-4326.geojson` | GeoJSON | secondary_plan_name, in_secondary_plan |

## Background: Status Label Mappings

### dev_applications — approved label (1 = approved, 0 = refused, null = unknown)

Approved (1) — case-insensitive after strip:
`{"closed", "noac issued", "council approved", "draft plan approved",
"final approval completed", "omb approved", "approved", "omb partially approved"}`

Refused (0):
`{"refused", "omb refused"}`

All other statuses → null (excluded from training).

### dev_applications — no_appeal label (1 = appealed, 0 = not appealed, null = unknown)

Appealed (1):
`{"omb appeal", "appeal received", "omb approved", "omb refused", "omb partially approved"}`

Not appealed (0): status is in approved set (see above) but not in appealed set.

Null: everything else (e.g. "under review", "application received").

### coa — approved label (1 = approved, 0 = refused/withdrawn, null = unknown)

Approved (1) — case-insensitive:
`{"approved", "conditional approval", "approved with conditions", "approved on condition"}`

Refused (0):
`{"refused", "withdrawn"}`

Null: `{"deferred", null}` and any other value.

### coa — days_to_approval (regression target)

`(finaldate - in_date).dt.total_days()` — only where both dates are non-null
and c_of_a_descision maps to approved=1.

## Files Changed

| File | Action |
|---|---|
| `src/zoneto/analytics/enrich.py` | Create |
| `src/zoneto/cli.py` | Add `enrich` command |
| `tests/analytics/test_enrich.py` | Create |

## Step 1 — Write failing tests

Create `tests/analytics/test_enrich.py`:

```python
"""Tests for enrich.py — all file I/O uses tmp_path."""
from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import enrich_coa, enrich_dev, fetch_reference


# ---------------------------------------------------------------------------
# fetch_reference
# ---------------------------------------------------------------------------

def test_fetch_reference_creates_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """fetch_reference should create reference subdirectories even if files already exist."""

    def fake_download(url: str, dest: Path) -> None:
        # Write a minimal ZIP for ZIP URLs, plain text for CSV/GeoJSON
        if url.endswith(".zip"):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("dummy.shp", b"")
        else:
            dest.write_bytes(b"id,geometry\n1,{}")

    monkeypatch.setattr("zoneto.analytics.enrich._download", fake_download)
    fetch_reference(data_dir=tmp_path)

    ref = tmp_path / "reference"
    assert (ref / "zoning.csv").exists()
    assert (ref / "heritage_register").is_dir()
    assert (ref / "heritage_districts").is_dir()
    assert (ref / "secondary_plans.geojson").exists()


# ---------------------------------------------------------------------------
# enrich_coa
# ---------------------------------------------------------------------------

def _make_coa_parquet(tmp_path: Path) -> None:
    """Write minimal COA parquet to tmp_path/coa/year=2022/part0.parquet."""
    df = pl.DataFrame(
        {
            "in_date": ["2022-01-15", "2022-03-01", "2022-06-10", "2022-09-01"],
            "finaldate": ["2022-04-20", "2022-05-15", None, "2022-11-30"],
            "c_of_a_descision": ["Approved", "Refused", "Deferred", "approved with conditions"],
            "ward": [5, 10, 15, 20],
            "application_type": ["Minor Variance", "Consent", "Minor Variance", "Consent"],
            "sub_type": ["A", "B", "A", "C"],
            "zoning_designation": ["RS", "RM", None, "CR"],
            "source_name": ["coa"] * 4,
            "year": [2022, 2022, 2022, 2022],
        }
    ).with_columns(
        pl.col("in_date").str.to_date(),
        pl.col("finaldate").str.to_date(),
    )
    out = tmp_path / "coa" / "year=2022"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_coa_creates_output(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    out = tmp_path / "enriched" / "coa.parquet"
    assert out.exists()


def test_enrich_coa_approved_label(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    # Row 0: Approved → 1
    assert df.filter(pl.col("ward_number") == 5)["coa_approved"][0] == 1
    # Row 1: Refused → 0
    assert df.filter(pl.col("ward_number") == 10)["coa_approved"][0] == 0
    # Row 2: Deferred → null
    assert df.filter(pl.col("ward_number") == 15)["coa_approved"][0] is None
    # Row 3: approved with conditions → 1
    assert df.filter(pl.col("ward_number") == 20)["coa_approved"][0] == 1


def test_enrich_coa_days_to_approval(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    # Row 0: 2022-01-15 to 2022-04-20 = 95 days, and is approved
    row = df.filter(pl.col("ward_number") == 5)
    assert row["coa_days_to_approval"][0] == 95
    # Row 1: Refused → days_to_approval is null
    row2 = df.filter(pl.col("ward_number") == 10)
    assert row2["coa_days_to_approval"][0] is None


def test_enrich_coa_ward_renamed(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    assert "ward_number" in df.columns
    assert "ward" not in df.columns


def test_enrich_coa_year_submitted(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    assert df["year_submitted"].dtype == pl.Int32
    assert df["year_submitted"][0] == 2022


# ---------------------------------------------------------------------------
# enrich_dev (unit — spatial join mocked)
# ---------------------------------------------------------------------------

def _make_dev_parquet(tmp_path: Path) -> None:
    """Write minimal dev_applications parquet."""
    df = pl.DataFrame(
        {
            "date_submitted": ["2021-06-01", "2021-09-15", "2022-01-10"],
            "status": ["Closed", "Refused", "Under Review"],
            "application_type": ["Rezoning", "Site Plan", "Rezoning"],
            "ward_number": ["Ward 1", "Ward 5", "Ward 10"],
            "community_meeting_date": ["2021-07-01", None, None],
            "x": ["630000.0", "631000.0", None],
            "y": ["4840000.0", "4841000.0", None],
            "source_name": ["dev_applications"] * 3,
            "year": [2021, 2021, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_creates_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _make_dev_parquet(tmp_path)
    # Stub spatial join to return empty enrichment columns
    def fake_spatial_join(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        )
    monkeypatch.setattr("zoneto.analytics.enrich._spatial_join_dev", fake_spatial_join)
    enrich_dev(data_dir=tmp_path)
    assert (tmp_path / "enriched" / "dev_applications.parquet").exists()


def test_enrich_dev_approved_label(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _make_dev_parquet(tmp_path)
    def fake_spatial_join(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        )
    monkeypatch.setattr("zoneto.analytics.enrich._spatial_join_dev", fake_spatial_join)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # "Closed" → approved = 1
    assert df.filter(pl.col("application_type") == "Rezoning").filter(
        pl.col("year_submitted") == 2021
    )["dev_approved"][0] == 1
    # "Refused" → approved = 0
    assert df.filter(pl.col("application_type") == "Site Plan")["dev_approved"][0] == 0
    # "Under Review" → null
    assert df.filter(pl.col("year_submitted") == 2022)["dev_approved"][0] is None


def test_enrich_dev_has_community_meeting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _make_dev_parquet(tmp_path)
    def fake_spatial_join(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        )
    monkeypatch.setattr("zoneto.analytics.enrich._spatial_join_dev", fake_spatial_join)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # Row 0 has community_meeting_date → 1
    row0 = df.filter(pl.col("year_submitted") == 2021).sort("has_community_meeting", descending=True)
    assert row0["has_community_meeting"][0] == 1
```

Run `uv run pytest tests/analytics/test_enrich.py` — expect
`ImportError: cannot import name 'enrich_coa' from 'zoneto.analytics.enrich'`.

## Step 2 — Implement `enrich.py`

Create `src/zoneto/analytics/enrich.py`. Full structure:

### 2a — Imports and constants

```python
"""Enrichment pipeline: fetch reference data, label outcomes, spatial join."""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import duckdb
import polars as pl
import pyproj

# ---------------------------------------------------------------------------
# Reference dataset URLs
# ---------------------------------------------------------------------------
_ZONING_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/datastore/dump/76a2620f-a6b4-495d-8e41-c0ede1f8a928"
)
_HERITAGE_REGISTER_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/e41da515-5ad1-4bc3-85ea-18ec9e55cd33"
    "/resource/108b1080-d048-439f-a9e8-e8d6cd81bddb"
    "/download/heritage_register_address_points_wgs84.zip"
)
_HERITAGE_DISTRICTS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/37a3c911-0813-4e87-90ed-3b9fa6156a63"
    "/resource/8e6b9347-63a8-4dac-91fb-a6491a8c1e5a"
    "/download/heritageconservationdistrict.zip"
)
_SECONDARY_PLANS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/70a544e9-ee83-43a4-b0be-0dc973627ad7"
    "/resource/08099a8c-a598-4ca3-8395-e4159cc1ec1a"
    "/download/secondary-plans-data-2017-4326.geojson"
)

# ---------------------------------------------------------------------------
# Status label sets (lowercase after strip)
# ---------------------------------------------------------------------------
_DEV_APPROVED_SET: frozenset[str] = frozenset({
    "closed", "noac issued", "council approved", "draft plan approved",
    "final approval completed", "omb approved", "approved", "omb partially approved",
})
_DEV_REFUSED_SET: frozenset[str] = frozenset({"refused", "omb refused"})
_DEV_APPEALED_SET: frozenset[str] = frozenset({
    "omb appeal", "appeal received", "omb approved", "omb refused", "omb partially approved",
})
_COA_APPROVED_SET: frozenset[str] = frozenset({
    "approved", "conditional approval", "approved with conditions", "approved on condition",
})
_COA_REFUSED_SET: frozenset[str] = frozenset({"refused", "withdrawn"})
```

### 2b — `_download` helper

```python
def _download(url: str, dest: Path) -> None:
    """Download *url* to *dest* (binary)."""
    import httpx
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        r = client.get(url)
        r.raise_for_status()
        dest.write_bytes(r.content)
```

### 2c — `fetch_reference`

```python
def fetch_reference(data_dir: Path = Path("data")) -> None:
    """Download all reference datasets to *data_dir*/reference/.

    Idempotent: skips files that already exist.
    """
    ref = data_dir / "reference"
    ref.mkdir(parents=True, exist_ok=True)

    # Zoning CSV
    zoning_csv = ref / "zoning.csv"
    if not zoning_csv.exists():
        _download(_ZONING_URL, zoning_csv)

    # Heritage register (ZIP → extract)
    hr_dir = ref / "heritage_register"
    if not hr_dir.exists():
        hr_zip = ref / "heritage_register.zip"
        _download(_HERITAGE_REGISTER_URL, hr_zip)
        hr_dir.mkdir()
        with zipfile.ZipFile(hr_zip) as zf:
            zf.extractall(hr_dir)
        hr_zip.unlink()

    # Heritage conservation districts (ZIP → extract)
    hd_dir = ref / "heritage_districts"
    if not hd_dir.exists():
        hd_zip = ref / "heritage_districts.zip"
        _download(_HERITAGE_DISTRICTS_URL, hd_zip)
        hd_dir.mkdir()
        with zipfile.ZipFile(hd_zip) as zf:
            zf.extractall(hd_dir)
        hd_zip.unlink()

    # Secondary plans GeoJSON
    sp_geojson = ref / "secondary_plans.geojson"
    if not sp_geojson.exists():
        _download(_SECONDARY_PLANS_URL, sp_geojson)
```

### 2d — `enrich_coa`

```python
def enrich_coa(data_dir: Path = Path("data")) -> int:
    """Enrich COA parquet with outcome labels; write data/enriched/coa.parquet.

    Returns row count written.
    """
    df = pl.read_parquet(data_dir / "coa", hive_partitioning=True)

    # Rename ward → ward_number (cast to str for consistency)
    df = df.rename({"ward": "ward_number"}).with_columns(
        pl.col("ward_number").cast(pl.Utf8)
    )

    # year_submitted from in_date
    df = df.with_columns(
        pl.col("in_date").dt.year().cast(pl.Int32).alias("year_submitted")
    )

    # coa_approved label
    def _coa_approved(val: str | None) -> int | None:
        if val is None:
            return None
        v = val.strip().lower()
        if v in _COA_APPROVED_SET:
            return 1
        if v in _COA_REFUSED_SET:
            return 0
        return None

    # Use polars map_elements for label derivation
    df = df.with_columns(
        pl.col("c_of_a_descision")
        .map_elements(_coa_approved, return_dtype=pl.Int8)
        .alias("coa_approved")
    )

    # coa_days_to_approval — only for approved rows with both dates present
    days = (pl.col("finaldate") - pl.col("in_date")).dt.total_days().cast(pl.Int32)
    df = df.with_columns(
        pl.when(pl.col("coa_approved") == 1)
        .then(days)
        .otherwise(None)
        .alias("coa_days_to_approval")
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "coa.parquet")
    return len(df)
```

### 2e — `_spatial_join_dev` (internal, monkeypatchable in tests)

```python
def _spatial_join_dev(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """Add zoning_class, secondary_plan_name, in_heritage_register,
    in_heritage_district, in_secondary_plan columns via DuckDB spatial join.

    Rows with null or garbage x/y get null/0 enrichment values.
    """
    ref = data_dir / "reference"

    # Reproject x/y from EPSG:26917 → EPSG:4326
    transformer = pyproj.Transformer.from_crs(
        "EPSG:26917", "EPSG:4326", always_xy=True
    )
    xs = df["x"].cast(pl.Float64, strict=False).to_list()
    ys = df["y"].cast(pl.Float64, strict=False).to_list()

    lons: list[float | None] = []
    lats: list[float | None] = []
    for x_val, y_val in zip(xs, ys):
        if x_val is None or y_val is None or y_val < 4_000_000:
            lons.append(None)
            lats.append(None)
        else:
            lon, lat = transformer.transform(x_val, y_val)
            lons.append(lon)
            lats.append(lat)

    df = df.with_columns(
        pl.Series("lon", lons, dtype=pl.Float64),
        pl.Series("lat", lats, dtype=pl.Float64),
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Register the polars df as a DuckDB table
    con.register("apps", df.to_arrow())

    hr_shp = str(next((ref / "heritage_register").glob("*.shp")))
    hd_shp = str(next((ref / "heritage_districts").glob("*.shp")))
    sp_geojson = str(ref / "secondary_plans.geojson")
    zoning_csv = str(ref / "zoning.csv")

    result = con.execute(f"""
        WITH pts AS (
            SELECT
                rowid AS _rid,
                lon, lat,
                CASE WHEN lon IS NOT NULL AND lat IS NOT NULL
                     THEN ST_Point(lon, lat) END AS geom
            FROM apps
        ),
        zoning_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                z.ZN_ZONE AS zoning_class
            FROM pts p
            LEFT JOIN read_csv('{zoning_csv}', AUTO_DETECT=TRUE) z
                ON ST_Within(p.geom, ST_GeomFromGeoJSON(z.geometry))
            WHERE p.geom IS NOT NULL
        ),
        hr_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                1::TINYINT AS in_heritage_register
            FROM pts p
            JOIN ST_Read('{hr_shp}') h
                ON ST_Intersects(ST_Buffer(p.geom, 0.0002), h.geom)
            WHERE p.geom IS NOT NULL
        ),
        hd_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                1::TINYINT AS in_heritage_district
            FROM pts p
            JOIN ST_Read('{hd_shp}') h
                ON ST_Within(p.geom, h.geom)
            WHERE p.geom IS NOT NULL
        ),
        sp_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                s.SECONDARY_PLAN_NAME AS secondary_plan_name
            FROM pts p
            JOIN ST_Read('{sp_geojson}') s
                ON ST_Within(p.geom, s.geom)
            WHERE p.geom IS NOT NULL
        )
        SELECT
            a.*,
            COALESCE(z.zoning_class, NULL)          AS zoning_class,
            COALESCE(h.in_heritage_register, 0)      AS in_heritage_register,
            COALESCE(d.in_heritage_district, 0)      AS in_heritage_district,
            COALESCE(sp.secondary_plan_name, NULL)   AS secondary_plan_name,
            CASE WHEN sp.secondary_plan_name IS NOT NULL THEN 1 ELSE 0 END AS in_secondary_plan
        FROM apps a
        LEFT JOIN zoning_join z ON a.rowid = z._rid
        LEFT JOIN hr_join h ON a.rowid = h._rid
        LEFT JOIN hd_join d ON a.rowid = d._rid
        LEFT JOIN sp_join sp ON a.rowid = sp._rid
    """).pl()

    con.close()
    return result
```

### 2f — `enrich_dev`

```python
def enrich_dev(data_dir: Path = Path("data")) -> int:
    """Enrich dev_applications parquet with spatial features and outcome labels.

    Writes data/enriched/dev_applications.parquet. Returns row count.
    """
    df = pl.read_parquet(data_dir / "dev_applications", hive_partitioning=True)

    # year_submitted from date_submitted
    df = df.with_columns(
        pl.col("date_submitted").dt.year().cast(pl.Int32).alias("year_submitted")
    )

    # has_community_meeting
    df = df.with_columns(
        pl.col("community_meeting_date")
        .is_not_null()
        .cast(pl.Int8)
        .alias("has_community_meeting")
    )

    # Spatial enrichment (monkeypatchable)
    df = _spatial_join_dev(df, data_dir)

    # dev_approved label
    def _dev_approved(val: str | None) -> int | None:
        if val is None:
            return None
        v = val.strip().lower()
        if v in _DEV_APPROVED_SET:
            return 1
        if v in _DEV_REFUSED_SET:
            return 0
        return None

    # dev_no_appeal label
    def _dev_no_appeal(val: str | None) -> int | None:
        if val is None:
            return None
        v = val.strip().lower()
        if v in _DEV_APPEALED_SET:
            return 1
        if v in _DEV_APPROVED_SET and v not in _DEV_APPEALED_SET:
            return 0
        return None

    df = df.with_columns(
        pl.col("status")
        .map_elements(_dev_approved, return_dtype=pl.Int8)
        .alias("dev_approved"),
        pl.col("status")
        .map_elements(_dev_no_appeal, return_dtype=pl.Int8)
        .alias("dev_no_appeal"),
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")
    return len(df)
```

## Step 3 — Add `enrich` CLI command

In `src/zoneto/cli.py`, add after the existing imports:

```python
from zoneto.analytics.enrich import enrich_coa, enrich_dev, fetch_reference
```

Add as a new command:

```python
@app.command()
def enrich(
    fetch_ref: Annotated[
        bool,
        typer.Option("--fetch-ref/--no-fetch-ref", help="Download reference datasets first."),
    ] = True,
) -> None:
    """Enrich raw Parquet with spatial features and outcome labels."""
    if fetch_ref:
        console.print("[bold]Fetching reference datasets...[/bold]")
        fetch_reference(DATA_DIR)
        console.print("  [green]✓[/green] Reference data ready")

    for label, fn in [("COA", enrich_coa), ("Dev applications", enrich_dev)]:
        console.print(f"[bold]Enriching {label}...[/bold]")
        try:
            count = fn(DATA_DIR)
            console.print(f"  [green]✓[/green] {count:,} rows written")
        except Exception as exc:
            console.print(f"  [red]✗ {exc}[/red]")
```

## Step 4 — Run tests

```bash
uv run pytest tests/analytics/test_enrich.py -v
```

All tests must pass. The spatial join tests use `monkeypatch` to stub
`_spatial_join_dev` so no reference files are needed.

## Step 5 — Lint

```bash
uv run ruff check src/zoneto/analytics/enrich.py src/zoneto/cli.py
uv run ty check src/zoneto/analytics/enrich.py src/zoneto/cli.py
```

## Verification

- `uv run pytest tests/analytics/test_enrich.py` → all pass
- `uv run pytest` → all existing tests still pass
- `uv run ruff check src/` → no issues
- `uv run ty check src/` → no issues

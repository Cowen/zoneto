"""Tests for enrich.py — all file I/O uses tmp_path."""

from __future__ import annotations

import json
import struct
import zipfile
from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import (
    _spatial_join_dev,
    enrich_coa,
    enrich_dev,
    enrich_permits,
    fetch_reference,
)


def _write_minimal_shp(shp_path: Path) -> None:
    """Write a valid empty Shapefile (null shape type, no features)."""
    # SHP/SHX header: 100 bytes — file code, unused×5, file length, version,
    # shape type, bounding box (8 doubles).
    header = (
        struct.pack(">iiiiii", 9994, 0, 0, 0, 0, 0)  # file code + 5 unused
        + struct.pack(">i", 50)  # file length in 16-bit words (100 bytes)
        + struct.pack("<i", 1000)  # version
        + struct.pack("<i", 0)  # shape type: null
        + struct.pack("<8d", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)  # bbox
    )
    shp_path.write_bytes(header)
    shp_path.with_suffix(".shx").write_bytes(header)
    # Minimal DBF: no field descriptors, no records.
    dbf = (
        bytes([3, 25, 3, 14])  # version, year, month, day
        + struct.pack("<i", 0)  # number of records
        + struct.pack("<hh", 33, 1)  # header size (32+terminator), record size
        + bytes(20)  # reserved
        + bytes([0x0D])  # field descriptor terminator
    )
    shp_path.with_suffix(".dbf").write_bytes(dbf)


def _fake_spatial_join(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
        pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
        pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
        pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
        pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
    )


@pytest.fixture()
def stub_spatial_join(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("zoneto.analytics.enrich._spatial_join_dev", _fake_spatial_join)


# ---------------------------------------------------------------------------
# fetch_reference
# ---------------------------------------------------------------------------


def test_fetch_reference_creates_dirs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fetch_reference should create reference subdirectories.

    Even if files already exist.
    """

    def fake_download(url: str, dest: Path) -> None:
        # Write a minimal ZIP for ZIP URLs, CSV for ward profiles, GeoJSON for others
        if url.endswith(".zip"):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("dummy.shp", b"")
        elif "ward" in dest.name:
            # Write minimal ward profiles CSV (transposed format)
            csv_content = (
                "Characteristic,Ward 1,Ward 2\n"
                "% Renter households,45.5,50.2\n"
                "Median total income of households in 2020 ($),75000,80000\n"
                "Population density per square kilometre,3500,4200\n"
                "% Single-detached house,25.5,20.1\n"
            )
            dest.write_text(csv_content)
        else:
            dest.write_bytes(b'{"type":"FeatureCollection","features":[]}')

    monkeypatch.setattr("zoneto.analytics.enrich._download", fake_download)
    fetch_reference(data_dir=tmp_path)

    ref = tmp_path / "reference"
    assert (ref / "zoning.geojson").exists()
    assert (ref / "heritage_register").is_dir()
    assert (ref / "heritage_districts").is_dir()
    assert (ref / "secondary_plans.geojson").exists()
    assert (ref / "ward_profiles.csv").exists()


# ---------------------------------------------------------------------------
# enrich_coa
# ---------------------------------------------------------------------------


def _make_coa_parquet(tmp_path: Path) -> None:
    """Write minimal COA parquet to tmp_path/coa/year=2022/part0.parquet."""
    df = pl.DataFrame(
        {
            "reference_file": ["REF-001", "REF-002", "REF-003", "REF-004"],
            "in_date": ["2022-01-15", "2022-03-01", "2022-06-10", "2022-09-01"],
            "finaldate": ["2022-04-20", "2022-05-15", None, "2022-11-30"],
            "hearing_date": ["2022-02-10", "2022-04-05", "2022-07-20", "2022-10-01"],
            "c_of_a_descision": [
                "Approved",
                "Refused",
                "Deferred",
                "approved with conditions",
            ],
            "ward": [5, 10, 15, 20],
            "application_type": [
                "Minor Variance",
                "Consent",
                "Minor Variance",
                "Consent",
            ],
            "sub_type": ["A", "B", "A", "C"],
            "zoning_designation": ["RS", "RM", None, "CR"],
            "planning_district": [
                "Toronto & East York",
                "North York",
                "Etobicoke York",
                "Scarborough",
            ],
            "source_name": ["coa"] * 4,
            "year": [2022, 2022, 2022, 2022],
        }
    ).with_columns(
        pl.col("in_date").str.to_date(),
        pl.col("finaldate").str.to_date(),
        pl.col("hearing_date").str.to_date(),
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
    assert df.filter(pl.col("ward_number") == "5")["coa_approved"][0] == 1
    # Row 1: Refused → 0
    assert df.filter(pl.col("ward_number") == "10")["coa_approved"][0] == 0
    # Row 2: Deferred → null
    assert df.filter(pl.col("ward_number") == "15")["coa_approved"][0] is None
    # Row 3: approved with conditions → 1
    assert df.filter(pl.col("ward_number") == "20")["coa_approved"][0] == 1


def test_enrich_coa_days_to_approval(tmp_path: Path) -> None:
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    # Row 0: 2022-01-15 to 2022-04-20 = 95 days, and is approved
    row = df.filter(pl.col("ward_number") == "5")
    assert row["coa_days_to_approval"][0] == 95
    # Row 1: Refused → days_to_approval is null
    row2 = df.filter(pl.col("ward_number") == "10")
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


def test_enrich_coa_ward_features(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """enrich_coa adds ward profile features (ward_pct_renters, etc.)."""
    _make_coa_parquet(tmp_path)

    # Mock _download to write ward profiles CSV when called for ward profiles
    def mock_download(url: str, dest: Path) -> None:
        if "ward" in dest.name:
            csv_content = (
                "Characteristic,Ward 1,Ward 5,Ward 10,Ward 15,Ward 20\n"
                "% Renter households,45.5,52.0,48.0,55.0,42.0\n"
                "Median total income of households in 2020 ($),75000,70000,80000,68000,85000\n"
                "Population density per square kilometre,3500,4200,3800,4500,3200\n"
                "% Single-detached house,25.5,20.0,30.0,18.5,35.0\n"
            )
            dest.write_text(csv_content)
        elif url.endswith(".zip"):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("dummy.shp", b"")
        else:
            dest.write_bytes(b'{"type":"FeatureCollection","features":[]}')

    monkeypatch.setattr("zoneto.analytics.enrich._download", mock_download)

    # Fetch reference to ensure ward profiles exist
    fetch_reference(data_dir=tmp_path)

    enrich_coa(data_dir=tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    # Verify ward profile columns are present
    ward_cols = ["ward_pct_renters", "ward_median_income", "ward_pop_density", "ward_pct_detached"]
    for col in ward_cols:
        assert col in df.columns, f"Missing column {col}"

    # Verify ward 5 has correct values (second row in CSV)
    ward_5_rows = df.filter(pl.col("ward_number") == "5")
    assert len(ward_5_rows) > 0
    # Check that ward profile values are actually populated
    assert ward_5_rows["ward_pct_renters"][0] is not None


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


def test_enrich_dev_creates_output(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    assert (tmp_path / "enriched" / "dev_applications.parquet").exists()


def test_enrich_dev_approved_label(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # "Closed" is no longer in approved set → null (ambiguous status)
    assert (
        df.filter(pl.col("application_type") == "Rezoning").filter(
            pl.col("year_submitted") == 2021
        )["dev_approved"][0]
        is None
    )
    # "Refused" → approved = 0
    assert df.filter(pl.col("application_type") == "Site Plan")["dev_approved"][0] == 0
    # "Under Review" → null
    assert df.filter(pl.col("year_submitted") == 2022)["dev_approved"][0] is None


def test_enrich_dev_appealed_label(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Test dev_appealed label logic (1 = appeal filed, 0 = approved without appeal)."""
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # "Closed" removed from approved set → not in any set → None
    assert (
        df.filter(pl.col("application_type") == "Rezoning").filter(
            pl.col("year_submitted") == 2021
        )["dev_appealed"][0]
        is None
    )
    # "Refused" is in refused set, not in approved or appealed → None
    assert (
        df.filter(pl.col("application_type") == "Site Plan")["dev_appealed"][0] is None
    )
    # "Under Review" not in any set → None
    assert df.filter(pl.col("year_submitted") == 2022)["dev_appealed"][0] is None


def test_enrich_dev_no_is_tlab_era(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """is_tlab_era should be removed — it was redundant with year_submitted."""
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    assert "is_tlab_era" not in df.columns


def test_enrich_dev_has_community_meeting(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # Row 0 has community_meeting_date → 1
    row0 = df.filter(pl.col("year_submitted") == 2021).sort(
        "has_community_meeting",
        descending=True,
    )
    assert row0["has_community_meeting"][0] == 1


def test_enrich_dev_ward_features(
    tmp_path: Path,
    stub_spatial_join: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """enrich_dev adds ward profile features."""
    _make_dev_parquet(tmp_path)

    # Mock _download to write ward profiles CSV when called for ward profiles
    def mock_download(url: str, dest: Path) -> None:
        if "ward" in dest.name:
            csv_content = (
                "Characteristic,Ward 1,Ward 5,Ward 10\n"
                "% Renter households,45.5,52.0,48.0\n"
                "Median total income of households in 2020 ($),75000,70000,80000\n"
                "Population density per square kilometre,3500,4200,3800\n"
                "% Single-detached house,25.5,20.0,30.0\n"
            )
            dest.write_text(csv_content)
        elif url.endswith(".zip"):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("dummy.shp", b"")
        else:
            dest.write_bytes(b'{"type":"FeatureCollection","features":[]}')

    monkeypatch.setattr("zoneto.analytics.enrich._download", mock_download)

    # Fetch reference to ensure ward profiles exist
    fetch_reference(data_dir=tmp_path)

    enrich_dev(data_dir=tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # Verify ward profile columns are present
    ward_cols = ["ward_pct_renters", "ward_median_income", "ward_pop_density", "ward_pct_detached"]
    for col in ward_cols:
        assert col in df.columns, f"Missing column {col}"

    # Verify that dev app in Ward 1 has the correct values
    ward_1_rows = df.filter(pl.col("ward_number") == "Ward 1")
    if len(ward_1_rows) > 0:
        assert ward_1_rows["ward_pct_renters"][0] is not None


# ---------------------------------------------------------------------------
# enrich_coa — new feature tests
# ---------------------------------------------------------------------------


def test_enrich_coa_planning_district(tmp_path: Path) -> None:
    """planning_district should be preserved as a feature column."""
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    assert "planning_district" in df.columns
    districts = df["planning_district"].to_list()
    assert "Toronto & East York" in districts
    assert "North York" in districts


def test_enrich_coa_deduplicates_on_reference_file(tmp_path: Path) -> None:
    """Rows sharing reference_file should be deduplicated (consolidated CSV overlap)."""
    df = pl.DataFrame(
        {
            "reference_file": ["REF-001", "REF-001", "REF-002"],
            "in_date": ["2022-01-15", "2022-01-15", "2022-03-01"],
            "finaldate": ["2022-04-20", "2022-04-20", "2022-05-15"],
            "hearing_date": ["2022-02-10", "2022-02-10", "2022-04-05"],
            "c_of_a_descision": ["Approved", "Approved", "Refused"],
            "ward": [5, 5, 10],
            "application_type": ["Minor Variance", "Minor Variance", "Consent"],
            "sub_type": ["A", "A", "B"],
            "zoning_designation": ["RS", "RS", "RM"],
            "planning_district": [
                "Toronto & East York",
                "Toronto & East York",
                "North York",
            ],
            "source_name": ["coa"] * 3,
            "year": [2022, 2022, 2022],
        }
    ).with_columns(
        pl.col("in_date").str.to_date(),
        pl.col("finaldate").str.to_date(),
        pl.col("hearing_date").str.to_date(),
    )
    out = tmp_path / "coa" / "year=2022"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    enrich_coa(data_dir=tmp_path)
    result = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    assert len(result) == 2  # REF-001 deduplicated to 1 row, REF-002 kept


# ---------------------------------------------------------------------------
# _spatial_join_dev integration (real DuckDB spatial, minimal reference files)
# ---------------------------------------------------------------------------


def _setup_spatial_ref(ref: Path, zoning_zone: str = "CR3") -> None:
    """Create minimal reference files for _spatial_join_dev integration tests.

    Zoning polygon covers Toronto WGS84 (-80,-79) × (43,44).
    Secondary plans polygon is placed at (0,0)-(1,1) so it never matches.
    Heritage SHPs are empty (no features → default 0 enrichment).
    """
    hr_dir = ref / "heritage_register"
    hd_dir = ref / "heritage_districts"
    hr_dir.mkdir(parents=True)
    hd_dir.mkdir(parents=True)

    zoning = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"ZN_ZONE": zoning_zone},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-80.0, 43.0], [-79.0, 43.0],
                    [-79.0, 44.0], [-80.0, 44.0], [-80.0, 43.0],
                ]],
            },
        }],
    }
    (ref / "zoning.geojson").write_text(json.dumps(zoning))
    _write_minimal_shp(hr_dir / "register.shp")
    _write_minimal_shp(hd_dir / "districts.shp")
    (ref / "secondary_plans.geojson").write_text(json.dumps({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"SECONDARY_PLAN_NAME": "Nowhere Plan"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]
                ],
            },
        }],
    }))


def test_spatial_join_dev_uses_mtm_zone10_crs(tmp_path: Path) -> None:
    """_spatial_join_dev must reproject from EPSG:2952 (MTM Zone 10), not EPSG:26917.

    99.8% of dev application coordinates have x~302k-326k, which is MTM Zone 10
    (false easting 304,800). Treating these as UTM Zone 17N (false easting 500,000)
    places every Toronto parcel in Michigan (~-83° lon), outside any zoning polygon.

    x=313,000, y=4,834,000 in EPSG:2952 → ~(-79.4°, 43.65°) WGS84 (downtown Toronto).
    x=313,000, y=4,834,000 in EPSG:26917 → ~(-83.3°, 43.65°) WGS84 (Michigan).
    """
    ref = tmp_path / "reference"
    _setup_spatial_ref(ref)

    # MTM Zone 10 coordinates for a downtown Toronto parcel
    df = pl.DataFrame({"x": ["313000.0", None], "y": ["4834000.0", None]})
    result = _spatial_join_dev(df, tmp_path)

    # With correct CRS (EPSG:2952), point lands in Toronto → zoning_class assigned
    assert result["zoning_class"][0] == "CR3", (
        "zoning_class is null — CRS may be wrong (EPSG:26917 maps x=313k to Michigan)"
    )
    assert result["zoning_class"][1] is None  # null coords → null zoning


def test_spatial_join_dev_reads_zoning_geojson(tmp_path: Path) -> None:
    """_spatial_join_dev reads zoning from zoning.geojson via DuckDB ST_Read.

    Uses x=313000, y=4834000 (EPSG:2952 MTM Zone 10) which projects to
    ~(-79.4°, 43.65°) WGS84, inside the test polygon covering (-80,-79) × (43,44).
    """
    ref = tmp_path / "reference"
    _setup_spatial_ref(ref)

    df = pl.DataFrame({"x": ["313000.0", None], "y": ["4834000.0", None]})
    result = _spatial_join_dev(df, tmp_path)

    # Point inside polygon → zoning_class assigned
    assert result["zoning_class"][0] == "CR3"
    # Null coordinates → null zoning_class
    assert result["zoning_class"][1] is None


# ---------------------------------------------------------------------------
# enrich_permits
# ---------------------------------------------------------------------------


def _make_permits_parquet(tmp_path: Path) -> None:
    """Write minimal permits_cleared parquet."""
    df = pl.DataFrame(
        {
            "application_date": [
                "2022-01-10",
                "2022-03-15",
                "2022-06-01",
                "2022-08-20",
            ],
            "issued_date": ["2022-04-20", "2022-07-01", None, "2022-10-05"],
            "permit_type": [
                "New Houses",
                "Small Residential Projects",
                "New Houses",
                "Commercial",
            ],
            "structure_type": [
                "Detached House",
                "Semi-Detached",
                "Row House",
                "Office",
            ],
            "ward_grid": ["W01", "W02", "W03", "W04"],
            "est_const_cost": [500000.0, 150000.0, 800000.0, 2000000.0],
            "dwelling_units_created": [1, 0, 2, 0],
            "dwelling_units_lost": [0, 0, 0, 0],
            "residential": [1, 1, 1, 0],
            "commercial": [0, 0, 0, 1],
            "industrial": [0, 0, 0, 0],
            "institutional": [0, 0, 0, 0],
            "source_name": ["permits_cleared"] * 4,
            "year": [2022, 2022, 2022, 2022],
        }
    ).with_columns(
        pl.col("application_date").str.to_date(),
        pl.col("issued_date").str.to_date(),
    )
    out = tmp_path / "permits_cleared" / "year=2022"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_permits_creates_output(tmp_path: Path) -> None:
    _make_permits_parquet(tmp_path)
    enrich_permits(data_dir=tmp_path)
    assert (tmp_path / "enriched" / "permits_cleared.parquet").exists()


def test_enrich_permits_issuance_days(tmp_path: Path) -> None:
    """permit_issuance_days = issued_date - application_date in calendar days."""
    _make_permits_parquet(tmp_path)
    enrich_permits(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "permits_cleared.parquet")
    assert "permit_issuance_days" in df.columns
    # Row 0: 2022-01-10 → 2022-04-20 = 100 days
    row0 = df.filter(pl.col("permit_type") == "New Houses").filter(
        pl.col("ward_grid") == "W01"
    )
    assert row0["permit_issuance_days"][0] == 100


def test_enrich_permits_null_issued_date(tmp_path: Path) -> None:
    """Rows with null issued_date should have null permit_issuance_days."""
    _make_permits_parquet(tmp_path)
    enrich_permits(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "permits_cleared.parquet")
    row = df.filter(pl.col("ward_grid") == "W03")
    assert row["permit_issuance_days"][0] is None


def test_enrich_permits_row_count(tmp_path: Path) -> None:
    """enrich_permits returns the count of rows written."""
    _make_permits_parquet(tmp_path)
    count = enrich_permits(data_dir=tmp_path)
    assert count == 4  # all 4 rows preserved (null issuance_days kept)


def test_enrich_permits_application_year(tmp_path: Path) -> None:
    """enrich_permits derives application_year (Int32) from application_date."""
    _make_permits_parquet(tmp_path)
    enrich_permits(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "permits_cleared.parquet")
    assert "application_year" in df.columns
    assert df["application_year"].dtype == pl.Int32
    assert df["application_year"][0] == 2022

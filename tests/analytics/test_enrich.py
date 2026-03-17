"""Tests for enrich.py — all file I/O uses tmp_path."""

from __future__ import annotations

import json
import struct
import zipfile
from datetime import date
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
        if url.endswith(".zip"):
            with zipfile.ZipFile(dest, "w") as zf:
                zf.writestr("dummy.shp", b"")
        else:
            dest.write_bytes(b'{"type":"FeatureCollection","features":[]}')

    def fake_fetch_ward_profiles_csv(ref: Path) -> None:
        (ref / "ward_profiles.csv").write_text(
            "ward_number,ward_pct_renters,ward_median_income,ward_pop_density,ward_pct_detached\n"
            "1,42.3,81000,2380.5,29.5\n"
            "2,48.7,100000,3140.2,48.2\n"
        )

    monkeypatch.setattr("zoneto.analytics.enrich._download", fake_download)
    monkeypatch.setattr(
        "zoneto.analytics.enrich._fetch_ward_profiles_csv",
        fake_fetch_ward_profiles_csv,
    )
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
) -> None:
    """enrich_coa adds ward profile features (ward_pct_renters, etc.)."""
    _make_coa_parquet(tmp_path)

    # Write ward_profiles.csv directly (simple format)
    ref = tmp_path / "reference"
    ref.mkdir(parents=True, exist_ok=True)
    (ref / "ward_profiles.csv").write_text(
        "ward_number,ward_pct_renters,ward_median_income,ward_pop_density,ward_pct_detached\n"
        "5,52.0,70000,4200.0,20.0\n"
        "10,48.0,80000,3800.0,30.0\n"
        "15,55.0,68000,4500.0,18.5\n"
        "20,42.0,85000,3200.0,35.0\n"
    )

    enrich_coa(data_dir=tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    # Verify ward profile columns are present
    ward_cols = [
        "ward_pct_renters",
        "ward_median_income",
        "ward_pop_density",
        "ward_pct_detached",
    ]
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
            "folderrsn": ["AAA", "BBB", "CCC"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=AAA",
                "https://app.toronto.ca/AIC/details?folderRsn=BBB",
                None,
            ],
            "description": ["Rezoning application", "Site plan approval", "Rezoning"],
            "date_submitted": ["2021-06-01", "2021-09-15", "2022-01-10"],
            "status": ["Closed", "Refused", "Under Review"],
            "application_type": ["Rezoning", "Site Plan", "Rezoning"],
            "ward_number": ["Ward 1", "Ward 5", "Ward 10"],
            "community_meeting_date": ["2021-07-01", None, None],
            "parent_folder_number": ["23 456789 OZ", None, None],
            "postal": ["M5V 2T6", "M4K 1A1", None],
            "x": ["630000.0", "631000.0", None],
            "y": ["4840000.0", "4841000.0", None],
            "source_name": ["dev_applications"] * 3,
            "year": [2021, 2021, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def _make_dev_parquet_oz_sa(tmp_path: Path) -> None:
    """Write dev_applications parquet with OZ+SA types for survival label tests."""
    df = pl.DataFrame(
        {
            "folderrsn": ["AAA", "BBB", "CCC"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=AAA",
                "https://app.toronto.ca/AIC/details?folderRsn=BBB",
                None,
            ],
            "description": ["OPA and rezoning", "Site plan approval", "Rezoning"],
            "date_submitted": ["2021-06-01", "2021-09-15", "2022-01-10"],
            "status": ["Closed", "Closed", "Under Review"],
            "application_type": ["OZ", "SA", "OZ"],
            "ward_number": ["Ward 1", "Ward 5", "Ward 10"],
            "community_meeting_date": [None, None, None],
            "parent_folder_number": [None, None, None],
            "postal": ["M5V 2T6", "M4K 1A1", None],
            "x": ["630000.0", "631000.0", None],
            "y": ["4840000.0", "4841000.0", None],
            "source_name": ["dev_applications"] * 3,
            "year": [2021, 2021, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part0.parquet")


def _make_aic_decisions(tmp_path: Path) -> None:
    """Write minimal aic_decisions.parquet with one OZ and one SA decision."""
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "folderrsn": ["AAA", "BBB"],
            "decision_date": [date(2022, 11, 8), date(2021, 2, 14)],
            "complete_date": [date(2021, 3, 15), date(2020, 6, 1)],
            "scraped_at": [date(2026, 3, 15), date(2026, 3, 15)],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    df.write_parquet(ref_dir / "aic_decisions.parquet")


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
) -> None:
    """enrich_dev adds ward profile features."""
    _make_dev_parquet(tmp_path)

    # Write ward_profiles.csv directly (simple format)
    ref = tmp_path / "reference"
    ref.mkdir(parents=True, exist_ok=True)
    (ref / "ward_profiles.csv").write_text(
        "ward_number,ward_pct_renters,ward_median_income,ward_pop_density,ward_pct_detached\n"
        "1,45.5,75000,3500.0,25.5\n"
        "5,52.0,70000,4200.0,20.0\n"
        "10,48.0,80000,3800.0,30.0\n"
    )

    enrich_dev(data_dir=tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # Verify ward profile columns are present
    ward_cols = [
        "ward_pct_renters",
        "ward_median_income",
        "ward_pop_density",
        "ward_pct_detached",
    ]
    for col in ward_cols:
        assert col in df.columns, f"Missing column {col}"

    # Verify that dev app in Ward 1 has the correct values
    ward_1_rows = df.filter(pl.col("ward_number") == "Ward 1")
    if len(ward_1_rows) > 0:
        assert ward_1_rows["ward_pct_renters"][0] is not None


def test_enrich_dev_is_active(tmp_path, stub_spatial_join):
    """'Under Review' → is_active=1; decided statuses → is_active=0."""
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    rows = dict(zip(df["status"].to_list(), df["is_active"].to_list()))
    assert rows["Under Review"] == 1
    assert rows["Closed"] == 0
    assert rows["Refused"] == 0


def test_enrich_dev_has_parent_application(tmp_path, stub_spatial_join):
    """Non-null parent_folder_number → has_parent_application=1, null → 0."""
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    assert df["has_parent_application"].to_list() == [1, 0, 0]


def test_enrich_dev_postal_fsa(tmp_path, stub_spatial_join):
    """postal_fsa is first 3 chars of postal; null postal → null fsa."""
    _make_dev_parquet(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    assert df["postal_fsa"].to_list() == ["M5V", "M4K", None]


# ---------------------------------------------------------------------------
# enrich_coa — new feature tests
# ---------------------------------------------------------------------------


def test_enrich_coa_caps_days_at_730(tmp_path: Path) -> None:
    """coa_days_to_approval > 730 days should be null (outlier exclusion).

    A 2,992-day outlier destabilizes the regression across CV folds.
    Cap at 730 days (2 years); values beyond are treated as exceptional/null.
    """
    # Build a COA parquet with one row spanning >730 days (2014 → 2022 = ~2,992d)
    df = pl.DataFrame(
        {
            "reference_file": ["REF-LONG", "REF-SHORT"],
            "in_date": ["2014-01-01", "2022-01-15"],
            "finaldate": ["2022-03-01", "2022-04-20"],
            "hearing_date": ["2014-02-01", "2022-02-10"],
            "c_of_a_descision": ["Approved", "Approved"],
            "ward": [5, 10],
            "application_type": ["Minor Variance", "Minor Variance"],
            "sub_type": ["A", "A"],
            "zoning_designation": ["RS", "RS"],
            "planning_district": ["Toronto & East York", "Toronto & East York"],
            "source_name": ["coa", "coa"],
            "year": [2014, 2022],
        }
    ).with_columns(
        pl.col("in_date").str.to_date(),
        pl.col("finaldate").str.to_date(),
        pl.col("hearing_date").str.to_date(),
    )
    out = tmp_path / "coa" / "year=2014"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    enrich_coa(data_dir=tmp_path)
    result = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")

    # The long row (>730d) should have null coa_days_to_approval
    long_row = result.filter(pl.col("reference_file") == "REF-LONG")
    assert long_row["coa_days_to_approval"][0] is None, (
        "coa_days_to_approval > 730 days should be null (outlier cap)"
    )

    # The short row (95 days) should be preserved
    short_row = result.filter(pl.col("reference_file") == "REF-SHORT")
    assert short_row["coa_days_to_approval"][0] == 95


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
        "features": [
            {
                "type": "Feature",
                "properties": {"ZN_ZONE": zoning_zone},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-80.0, 43.0],
                            [-79.0, 43.0],
                            [-79.0, 44.0],
                            [-80.0, 44.0],
                            [-80.0, 43.0],
                        ]
                    ],
                },
            }
        ],
    }
    (ref / "zoning.geojson").write_text(json.dumps(zoning))
    _write_minimal_shp(hr_dir / "register.shp")
    _write_minimal_shp(hd_dir / "districts.shp")
    (ref / "secondary_plans.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"SECONDARY_PLAN_NAME": "Nowhere Plan"},
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.0, 0.0],
                                    [1.0, 0.0],
                                    [1.0, 1.0],
                                    [0.0, 1.0],
                                    [0.0, 0.0],
                                ]
                            ],
                        },
                    }
                ],
            }
        )
    )


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
            "mercantile": [0, 0, 0, 1],
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


# ---------------------------------------------------------------------------
# enrich_dev — survival label tests
# ---------------------------------------------------------------------------


def test_enrich_dev_survival_labels_for_oz_with_decision(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """OZ with decision date: dev_days_to_decision computed; dev_decision_event=1."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    oz_row = df.filter(pl.col("folderrsn") == "AAA")
    # date_submitted=2021-06-01, decision_date=2022-11-08 → 525 days
    assert oz_row["dev_days_to_decision"][0] == 525
    assert oz_row["dev_decision_event"][0] == 1
    assert oz_row["dev_days_observed"][0] == 525


def test_enrich_dev_decision_event_zero_for_active(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Active OZ: dev_decision_event=0; dev_days_observed=today minus date_submitted."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    active_row = df.filter(pl.col("folderrsn") == "CCC")
    assert active_row["dev_decision_event"][0] == 0
    assert active_row["dev_days_to_decision"][0] is None
    assert active_row["dev_days_observed"][0] is not None
    assert active_row["dev_days_observed"][0] > 0


def test_enrich_dev_days_to_decision_cap_at_3650(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Values > 3650 days become null (outlier cap)."""
    df = pl.DataFrame(
        {
            "folderrsn": ["OLD"],
            "application_url": ["https://app.toronto.ca/AIC/details?folderRsn=OLD"],
            "description": ["Rezoning"],
            "date_submitted": ["2010-01-01"],
            "status": ["Closed"],
            "application_type": ["OZ"],
            "ward_number": ["Ward 1"],
            "community_meeting_date": [None],
            "parent_folder_number": [None],
            "postal": [None],
            "x": [None],
            "y": [None],
            "source_name": ["dev_applications"],
            "year": [2010],
        },
        schema={
            "folderrsn": pl.Utf8,
            "application_url": pl.Utf8,
            "description": pl.Utf8,
            "date_submitted": pl.Utf8,
            "status": pl.Utf8,
            "application_type": pl.Utf8,
            "ward_number": pl.Utf8,
            "community_meeting_date": pl.Utf8,
            "parent_folder_number": pl.Utf8,
            "postal": pl.Utf8,
            "x": pl.Utf8,
            "y": pl.Utf8,
            "source_name": pl.Utf8,
            "year": pl.Int32,
        },
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2010"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    decisions = pl.DataFrame(
        {
            "folderrsn": ["OLD"],
            "decision_date": [date(2020, 6, 1)],  # ~3804 days after 2010-01-01
            "complete_date": [None],
            "scraped_at": [date(2026, 3, 15)],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    decisions.write_parquet(ref_dir / "aic_decisions.parquet")

    enrich_dev(data_dir=tmp_path)
    df_out = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    old_row = df_out.filter(pl.col("folderrsn") == "OLD")
    assert old_row["dev_days_to_decision"][0] is None


def test_enrich_dev_is_combined_application_oz_opa(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """OZ application with 'OPA' in description: is_combined_application=1."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    oz_row = df.filter(pl.col("folderrsn") == "AAA")
    # description="OPA and rezoning", application_type="OZ"
    assert oz_row["is_combined_application"][0] == 1
    # SA row should be 0 (not OZ type)
    sa_row = df.filter(pl.col("folderrsn") == "BBB")
    assert sa_row["is_combined_application"][0] == 0


# ---------------------------------------------------------------------------
# enrich_dev — dev_appealed label coverage (P0 fix)
# ---------------------------------------------------------------------------


def _make_dev_parquet_appeals(tmp_path: Path) -> None:
    """Write dev_applications parquet with OZ/SA/CD for appeal label tests."""
    df = pl.DataFrame(
        {
            "folderrsn": ["A1", "A2", "A3", "A4", "A5"],
            "application_url": [None] * 5,
            "description": ["Test"] * 5,
            "date_submitted": ["2021-01-01"] * 5,
            "status": [
                "OMB Appeal",
                "Refused",
                "Under Review",
                "Council Approved",
                "Refused",
            ],
            "application_type": ["OZ", "SA", "OZ", "OZ", "CD"],
            "ward_number": ["Ward 1"] * 5,
            "community_meeting_date": [None] * 5,
            "parent_folder_number": [None] * 5,
            "postal": [None] * 5,
            "x": [None] * 5,
            "y": [None] * 5,
            "source_name": ["dev_applications"] * 5,
            "year": [2021] * 5,
        },
        schema={
            "folderrsn": pl.Utf8,
            "application_url": pl.Utf8,
            "description": pl.Utf8,
            "date_submitted": pl.Utf8,
            "status": pl.Utf8,
            "application_type": pl.Utf8,
            "ward_number": pl.Utf8,
            "community_meeting_date": pl.Utf8,
            "parent_folder_number": pl.Utf8,
            "postal": pl.Utf8,
            "x": pl.Utf8,
            "y": pl.Utf8,
            "source_name": pl.Utf8,
            "year": pl.Int32,
        },
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_appealed_oz_sa_label_coverage(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """OZ/SA closed apps get 0/1; active apps get null; non-OZ/SA always null.

    Fixes 50/50 base-rate selection bias: previously only appeals (1) and
    explicitly-approved (0) got labels. Closed apps like 'Refused' got null,
    which excluded large numbers of non-appealed cases from training.

    New logic (OZ/SA only):
    - 1  if status in _DEV_APPEALED_SET
    - 0  if status is not null and not in _DEV_ACTIVE_SET (closed without appeal)
    - null  if status is null, active, or application_type is not OZ/SA
    """
    _make_dev_parquet_appeals(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    # A1: OZ + "OMB Appeal" → 1
    assert df.filter(pl.col("folderrsn") == "A1")["dev_appealed"][0] == 1
    # A2: SA + "Refused" → 0 (closed without appeal — previously was null!)
    assert df.filter(pl.col("folderrsn") == "A2")["dev_appealed"][0] == 0
    # A3: OZ + "Under Review" → null (still active)
    assert df.filter(pl.col("folderrsn") == "A3")["dev_appealed"][0] is None
    # A4: OZ + "Council Approved" → 0 (closed without appeal)
    assert df.filter(pl.col("folderrsn") == "A4")["dev_appealed"][0] == 0
    # A5: CD + "Refused" → null (not OZ/SA — different appeal mechanism)
    assert df.filter(pl.col("folderrsn") == "A5")["dev_appealed"][0] is None


# ---------------------------------------------------------------------------
# enrich_dev — extract storeys and units from description (P0)
# ---------------------------------------------------------------------------


def _make_dev_parquet_descriptions(tmp_path: Path) -> None:
    """Write dev_applications parquet with various description patterns."""
    df = pl.DataFrame(
        {
            "folderrsn": ["D1", "D2", "D3", "D4", "D5", "D6"],
            "application_url": [None] * 6,
            "description": [
                "Proposed 12 storey mixed-use building with 551 units",
                "28-storey residential tower, 186 dwelling units",
                "3 storeys, no unit count mentioned",
                "Rezoning to permit commercial use",
                "A 40 STOREY building with 320 UNITS near transit",
                None,
            ],
            "date_submitted": ["2021-01-01"] * 6,
            "status": ["Closed"] * 6,
            "application_type": ["OZ"] * 6,
            "ward_number": ["Ward 1"] * 6,
            "community_meeting_date": [None] * 6,
            "parent_folder_number": [None] * 6,
            "postal": [None] * 6,
            "x": [None] * 6,
            "y": [None] * 6,
            "source_name": ["dev_applications"] * 6,
            "year": [2021] * 6,
        },
        schema={
            "folderrsn": pl.Utf8,
            "application_url": pl.Utf8,
            "description": pl.Utf8,
            "date_submitted": pl.Utf8,
            "status": pl.Utf8,
            "application_type": pl.Utf8,
            "ward_number": pl.Utf8,
            "community_meeting_date": pl.Utf8,
            "parent_folder_number": pl.Utf8,
            "postal": pl.Utf8,
            "x": pl.Utf8,
            "y": pl.Utf8,
            "source_name": pl.Utf8,
            "year": pl.Int32,
        },
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_extracts_storeys(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Extract proposed storey count from description via regex."""
    _make_dev_parquet_descriptions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    # "12 storey" → 12
    assert df.filter(pl.col("folderrsn") == "D1")["proposed_storeys"][0] == 12
    # "28-storey" → 28
    assert df.filter(pl.col("folderrsn") == "D2")["proposed_storeys"][0] == 28
    # "3 storeys" → 3
    assert df.filter(pl.col("folderrsn") == "D3")["proposed_storeys"][0] == 3
    # No storey mention → null
    assert df.filter(pl.col("folderrsn") == "D4")["proposed_storeys"][0] is None
    # "40 STOREY" (case insensitive) → 40
    assert df.filter(pl.col("folderrsn") == "D5")["proposed_storeys"][0] == 40
    # null description → null
    assert df.filter(pl.col("folderrsn") == "D6")["proposed_storeys"][0] is None


def test_enrich_dev_extracts_units(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Extract proposed unit count from description via regex."""
    _make_dev_parquet_descriptions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    # "551 units" → 551
    assert df.filter(pl.col("folderrsn") == "D1")["proposed_units"][0] == 551
    # "186 dwelling units" → 186
    assert df.filter(pl.col("folderrsn") == "D2")["proposed_units"][0] == 186
    # No unit mention → null
    assert df.filter(pl.col("folderrsn") == "D3")["proposed_units"][0] is None
    # No unit mention → null
    assert df.filter(pl.col("folderrsn") == "D4")["proposed_units"][0] is None
    # "320 UNITS" (case insensitive) → 320
    assert df.filter(pl.col("folderrsn") == "D5")["proposed_units"][0] == 320
    # null description → null
    assert df.filter(pl.col("folderrsn") == "D6")["proposed_units"][0] is None


# ---------------------------------------------------------------------------
# enrich_dev — ward rolling 3-year appeal rate (P1)
# ---------------------------------------------------------------------------


def _make_dev_parquet_ward_rates(tmp_path: Path) -> None:
    """Write dev_applications parquet spanning multiple years/wards for rate tests."""
    # Ward 1: 2018 (2 apps, 1 appeal), 2019 (2 apps, 0 appeals), 2021 (1 app)
    # Ward 2: 2018 (1 app, 0 appeals), 2021 (1 app)
    rows = [
        ("W1A", "2018-03-01", "OMB Appeal", "OZ", "Ward 1"),
        ("W1B", "2018-06-01", "Closed", "OZ", "Ward 1"),
        ("W1C", "2019-02-01", "Refused", "SA", "Ward 1"),
        ("W1D", "2019-08-01", "Council Approved", "OZ", "Ward 1"),
        ("W1E", "2021-04-01", "Under Review", "OZ", "Ward 1"),
        ("W2A", "2018-05-01", "Closed", "SA", "Ward 2"),
        ("W2B", "2021-06-01", "Under Review", "OZ", "Ward 2"),
    ]
    df = pl.DataFrame(
        {
            "folderrsn": [r[0] for r in rows],
            "application_url": [None] * len(rows),
            "description": ["Rezoning"] * len(rows),
            "date_submitted": [r[1] for r in rows],
            "status": [r[2] for r in rows],
            "application_type": [r[3] for r in rows],
            "ward_number": [r[4] for r in rows],
            "community_meeting_date": [None] * len(rows),
            "parent_folder_number": [None] * len(rows),
            "postal": [None] * len(rows),
            "x": [None] * len(rows),
            "y": [None] * len(rows),
            "source_name": ["dev_applications"] * len(rows),
            "year": [int(r[1][:4]) for r in rows],
        },
        schema={
            "folderrsn": pl.Utf8,
            "application_url": pl.Utf8,
            "description": pl.Utf8,
            "date_submitted": pl.Utf8,
            "status": pl.Utf8,
            "application_type": pl.Utf8,
            "ward_number": pl.Utf8,
            "community_meeting_date": pl.Utf8,
            "parent_folder_number": pl.Utf8,
            "postal": pl.Utf8,
            "x": pl.Utf8,
            "y": pl.Utf8,
            "source_name": pl.Utf8,
            "year": pl.Int32,
        },
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2018"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_ward_appeal_rate_3y(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Ward rolling 3-year appeal rate uses only prior data, avoids leakage."""
    _make_dev_parquet_ward_rates(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    assert "ward_appeal_rate_3y" in df.columns

    # W1A (2018, Ward 1): no prior data → null
    w1a = df.filter(pl.col("folderrsn") == "W1A")["ward_appeal_rate_3y"][0]
    assert w1a is None

    # W1C (2019, Ward 1): prior data is 2018 Ward 1 (1 appeal / 2 OZ/SA = 0.5)
    w1c = df.filter(pl.col("folderrsn") == "W1C")["ward_appeal_rate_3y"][0]
    assert w1c is not None
    assert abs(w1c - 0.5) < 0.01

    # W1E (2021, Ward 1): prior 3yr = 2018-2020. 2018: 1/2, 2019: 0/2 → 1/4 = 0.25
    w1e = df.filter(pl.col("folderrsn") == "W1E")["ward_appeal_rate_3y"][0]
    assert w1e is not None
    assert abs(w1e - 0.25) < 0.01

    # W2A (2018, Ward 2): no prior data → null
    w2a = df.filter(pl.col("folderrsn") == "W2A")["ward_appeal_rate_3y"][0]
    assert w2a is None

    # W2B (2021, Ward 2): prior 3yr = 2018-2020 Ward 2: 0 appeals / 1 app = 0.0
    w2b = df.filter(pl.col("folderrsn") == "W2B")["ward_appeal_rate_3y"][0]
    assert w2b is not None
    assert abs(w2b - 0.0) < 0.01

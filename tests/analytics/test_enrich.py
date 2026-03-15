"""Tests for enrich.py — all file I/O uses tmp_path."""

from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import (
    enrich_coa,
    enrich_dev,
    enrich_permits,
    fetch_reference,
)


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


def test_enrich_coa_hearing_month(tmp_path: Path) -> None:
    """hearing_month should be extracted from hearing_date (1-12)."""
    _make_coa_parquet(tmp_path)
    enrich_coa(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "coa.parquet")
    assert "hearing_month" in df.columns
    # hearing_dates in fixture: 2022-02-10, 2022-04-05, 2022-07-20, 2022-10-01
    months = df.sort("in_date")["hearing_month"].to_list()
    assert months == [2, 4, 7, 10]


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

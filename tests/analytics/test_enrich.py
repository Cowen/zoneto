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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_enrich_dev_approved_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_enrich_dev_no_appeal_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dev_no_appeal label logic."""
    _make_dev_parquet(tmp_path)

    def fake_spatial_join(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
        return df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
        )

    monkeypatch.setattr("zoneto.analytics.enrich._spatial_join_dev",
                        fake_spatial_join)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    # "Closed" is in approved set, not in appealed → 0
    assert df.filter(pl.col("application_type") == "Rezoning").filter(
        pl.col("year_submitted") == 2021
    )["dev_no_appeal"][0] == 0
    # "Refused" is in refused set, neither in approved nor appealed → None
    assert df.filter(pl.col("application_type") == "Site Plan")[
        "dev_no_appeal"
    ][0] is None
    # "Under Review" not in any set → None
    assert df.filter(pl.col("year_submitted") == 2022)["dev_no_appeal"][0] is None


def test_enrich_dev_has_community_meeting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    row0 = df.filter(pl.col("year_submitted") == 2021).sort(
        "has_community_meeting",
        descending=True,
    )
    assert row0["has_community_meeting"][0] == 1

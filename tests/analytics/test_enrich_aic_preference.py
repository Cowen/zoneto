"""Tests verifying enrich_dev() enriches CKAN records with AIC-only fields."""

from __future__ import annotations

from datetime import date
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
            "community_meeting_date": [None, None, None],
            "postal": ["M5V", "M5W", "M5X"],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def _write_minimal_aic_apps(tmp_path: Path) -> None:
    """Write AIC application records with AIC-only spatial/milestone fields."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F004"],  # F001 overlaps CKAN, F004 is AIC-only
            "application_type": ["OZ", "OZ"],
            "lat": [43.67, 43.68],
            "lon": [-79.45, -79.46],
            "latest_milestone": ["Hearing Scheduled", "Application Complete"],
            "latest_milestone_date": pl.Series(
                [date(2022, 3, 1), date(2023, 6, 15)], dtype=pl.Date
            ),
            "complete_date": pl.Series([None, None], dtype=pl.Date),
            "scraped_at": pl.Series(
                [date(2024, 1, 1), date(2024, 1, 1)], dtype=pl.Date
            ),
            "year": pl.Series([2021, 2023], dtype=pl.Int32),
            "source_name": ["aic_applications"] * 2,
        }
    )
    out = tmp_path / "aic_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


def test_enrich_dev_enriches_ckan_with_aic_spatial_fields(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AIC adds lat/lon/milestone to matching CKAN rows; CKAN fields are preserved."""
    _write_minimal_ckan_dev(tmp_path)
    _write_minimal_aic_apps(tmp_path)

    monkeypatch.setattr(
        "zoneto.analytics.enrich.spatial_join_dev",
        lambda df, data_dir: df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
            pl.lit(None, dtype=pl.Int32).alias("zoning_max_units"),
            pl.lit(None, dtype=pl.Float64).alias("zoning_max_density"),
            pl.lit(None, dtype=pl.Int32).alias("zoning_max_storeys"),
        ),
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_reference", lambda data_dir: None
    )

    enrich_dev(tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")

    # F001: CKAN fields are preserved; AIC-only lat/lon/milestone fields are added
    f001 = df.filter(pl.col("folderrsn") == "F001")
    assert len(f001) == 1
    assert f001["description"][0] == "CKAN OZ desc", "CKAN description preserved"
    assert f001["status"][0] == "Closed", "CKAN status must be preserved"
    assert f001["lat"][0] == pytest.approx(43.67), "AIC lat must be added"
    assert f001["lon"][0] == pytest.approx(-79.45), "AIC lon must be added"
    assert f001["latest_milestone"][0] == "Hearing Scheduled"

    # F002 and F003: CKAN-only records still present, lat/lon null
    f002 = df.filter(pl.col("folderrsn") == "F002")
    assert len(f002) == 1
    assert f002["lat"][0] is None, "CKAN-only row has no AIC lat"

    assert len(df.filter(pl.col("folderrsn") == "F003")) == 1


def test_enrich_dev_without_aic_falls_back_to_ckan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fall back to CKAN when no aic_applications parquet exists."""
    _write_minimal_ckan_dev(tmp_path)
    # No AIC data written

    monkeypatch.setattr(
        "zoneto.analytics.enrich.spatial_join_dev",
        lambda df, data_dir: df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("zoning_class"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_register"),
            pl.lit(0, dtype=pl.Int8).alias("in_heritage_district"),
            pl.lit(None, dtype=pl.Utf8).alias("secondary_plan_name"),
            pl.lit(0, dtype=pl.Int8).alias("in_secondary_plan"),
            pl.lit(None, dtype=pl.Int32).alias("zoning_max_units"),
            pl.lit(None, dtype=pl.Float64).alias("zoning_max_density"),
            pl.lit(None, dtype=pl.Int32).alias("zoning_max_storeys"),
        ),
    )
    monkeypatch.setattr(
        "zoneto.analytics.enrich.fetch_reference", lambda data_dir: None
    )

    enrich_dev(tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    f001 = df.filter(pl.col("folderrsn") == "F001")
    assert f001["description"][0] == "CKAN OZ desc"

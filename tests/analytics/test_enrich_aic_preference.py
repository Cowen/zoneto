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
            "community_meeting_date": [None, None, None],
            "postal": ["M5V", "M5W", "M5X"],
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
            "date_submitted": pl.Series(["2021-06-01", "2023-05-01"]).str.to_date(),
            "description": ["AIC OZ desc (preferred)", "AIC-only application"],
            "ward_number": ["10", "12"],
            "year": pl.Series([2021, 2023], dtype=pl.Int32),
            "source_name": ["aic_applications"] * 2,
            "community_meeting_date": [None, None],
            "postal": ["M5V", "M5Z"],
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

    # Stub out spatial join and reference fetch
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
    """Fall back to CKAN when no aic_applications parquet exists."""
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

    enrich_dev(tmp_path)

    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    f001 = df.filter(pl.col("folderrsn") == "F001")
    assert f001["description"][0] == "CKAN OZ desc"

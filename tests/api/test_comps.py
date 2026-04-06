"""Tests for DuckDB comps query builder."""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest

from zoneto.api.comps import query_comps


@pytest.fixture
def enriched_parquet(tmp_path: Path) -> Path:
    """Minimal enriched dev_applications parquet with known spatial distribution."""
    current_year = datetime.date.today().year
    path = tmp_path / "enriched" / "dev_applications.parquet"
    path.parent.mkdir(parents=True)

    # F001: OZ, ward 10, near (43.65, -79.38), recent
    # F002: OZ, ward 10, near (43.65, -79.38), recent, was appealed
    # F003: SA, ward 11, near, recent
    # F004: OZ, ward 10, far away (Toronto north), recent
    # F005: OZ, ward 10, near, very old (outside 5yr window)
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003", "F004", "F005"],
            "application_type": ["OZ", "OZ", "SA", "OZ", "OZ"],
            "ward_number": ["10", "10", "11", "10", "10"],
            "zoning_class": ["RA1", "RA2", "RM", "RA1", "RA1"],
            "status": ["Approved", "Appealed", "Approved", "Approved", "Approved"],
            "year_submitted": pl.Series(
                [
                    current_year - 1,
                    current_year - 2,
                    current_year - 3,
                    current_year - 1,
                    current_year - 10,
                ],
                dtype=pl.Int32,
            ),
            "lat": [43.650, 43.651, 43.652, 43.700, 43.650],
            "lon": [-79.380, -79.381, -79.382, -79.450, -79.380],
            "dev_approved": pl.Series([1, 1, 1, 1, 1], dtype=pl.Int8),
            "dev_appealed": pl.Series([0, 1, 0, 0, 0], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series(
                [365, 730, 400, 300, 500], dtype=pl.Int32
            ),
            "proposed_storeys": pl.Series([10, 20, None, 5, 8], dtype=pl.Int32),
            "proposed_units": pl.Series([100, 200, None, 50, 80], dtype=pl.Int32),
            "zoning_max_units": pl.Series([50, 100, None, 100, 50], dtype=pl.Int32),
            "zoning_max_density": pl.Series(
                [3.5, 2.0, None, 1.5, 3.5], dtype=pl.Float64
            ),
            "unit_excess_ratio": pl.Series(
                [2.0, 2.0, None, 0.5, 1.6], dtype=pl.Float64
            ),
            "zoning_max_storeys": pl.Series(
                [10, 5, None, 20, 10], dtype=pl.Int32
            ),
            "storey_excess_ratio": pl.Series(
                [1.0, 4.0, None, 0.25, 0.8], dtype=pl.Float64
            ),
            "description": ["OZ application"] * 5,
            "street_num": ["100", "200", "300", "400", "500"],
            "street_name": ["King St", "Queen St", "Bloor St", "Yonge St", "Bay St"],
        }
    )
    df.write_parquet(path)
    return path


def test_query_comps_no_filters_respects_years(enriched_parquet: Path) -> None:
    """Default years=5 excludes applications older than 5 years."""
    results = query_comps(enriched_parquet, years=5)
    folderrsns = {r["folderrsn"] for r in results}
    assert "F005" not in folderrsns  # submitted 10 years ago
    assert "F001" in folderrsns


def test_query_comps_by_application_type(enriched_parquet: Path) -> None:
    """Filters by application_type."""
    results = query_comps(enriched_parquet, application_type="SA", years=5)
    assert len(results) == 1
    assert results[0]["application_type"] == "SA"
    assert results[0]["folderrsn"] == "F003"


def test_query_comps_by_ward(enriched_parquet: Path) -> None:
    """Filters by ward_number."""
    results = query_comps(enriched_parquet, ward_number="11", years=5)
    assert len(results) == 1
    assert results[0]["ward_number"] == "11"


def test_query_comps_spatial_excludes_distant(enriched_parquet: Path) -> None:
    """Spatial filter excludes F004 which is ~5 km north."""
    results = query_comps(
        enriched_parquet,
        lat=43.650,
        lon=-79.380,
        radius_m=500,
        years=5,
    )
    folderrsns = {r["folderrsn"] for r in results}
    assert "F004" not in folderrsns
    assert "F001" in folderrsns
    assert "F002" in folderrsns


def test_query_comps_spatial_sorted_by_proximity(enriched_parquet: Path) -> None:
    """When lat/lon provided, results sorted closest-first."""
    results = query_comps(
        enriched_parquet,
        lat=43.650,
        lon=-79.380,
        radius_m=500,
        years=5,
    )
    assert results[0]["folderrsn"] == "F001"  # closest to query point


def test_query_comps_limit(enriched_parquet: Path) -> None:
    """Limit caps result count."""
    results = query_comps(enriched_parquet, years=10, limit=2)
    assert len(results) <= 2


def test_query_comps_empty_when_no_match(enriched_parquet: Path) -> None:
    """Returns empty list when no applications match."""
    results = query_comps(enriched_parquet, application_type="NONEXISTENT", years=5)
    assert results == []


def test_query_comps_result_shape(enriched_parquet: Path) -> None:
    """Each result dict contains expected keys."""
    results = query_comps(enriched_parquet, years=5, limit=1)
    assert len(results) == 1
    rec = results[0]
    for key in (
        "folderrsn",
        "application_type",
        "ward_number",
        "zoning_class",
        "status",
        "year_submitted",
        "lat",
        "lon",
        "dev_approved",
        "dev_appealed",
        "dev_days_to_decision",
        "street_address",
    ):
        assert key in rec, f"Missing key: {key}"

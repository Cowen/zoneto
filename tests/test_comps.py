"""Tests for the comps query builder."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def _write_enriched(tmp_path: Path, rows: list[dict]) -> Path:
    df = pl.DataFrame(rows)
    path = tmp_path / "dev_applications.parquet"
    df.write_parquet(path)
    return path


def test_query_comps_includes_application_url(tmp_path: Path) -> None:
    """application_url must be returned by query_comps when present in data."""
    from zoneto.api.comps import query_comps

    enriched = _write_enriched(
        tmp_path,
        [
            {
                "folderrsn": "5148123",
                "application_type": "OZ",
                "ward_number": "10",
                "zoning_class": "CRE",
                "status": "Closed",
                "year_submitted": 2022,
                "lat": 43.645,
                "lon": -79.396,
                "dev_approved": 1,
                "dev_appealed": 0,
                "dev_days_to_decision": 365,
                "proposed_storeys": 20,
                "proposed_units": 200,
                "description": "Mixed use tower",
                "street_num": "441",
                "street_name": "King St W",
                "application_url": "http://app.toronto.ca/AIC/index.do?folderRsn=abc123",
            }
        ],
    )

    results = query_comps(enriched)
    assert len(results) == 1
    assert (
        results[0]["application_url"]
        == "http://app.toronto.ca/AIC/index.do?folderRsn=abc123"
    )


def test_query_comps_deduplicates_by_folderrsn(tmp_path: Path) -> None:
    """Multiple rows with the same folderrsn must be collapsed to one result."""
    from zoneto.api.comps import query_comps

    enriched = _write_enriched(
        tmp_path,
        [
            {
                "folderrsn": "1111111",
                "application_type": "OZ",
                "ward_number": "10",
                "zoning_class": "CRE",
                "status": "Closed",
                "year_submitted": 2022,
                "lat": 43.645,
                "lon": -79.396,
                "dev_approved": 1,
                "dev_appealed": 0,
                "dev_days_to_decision": 365,
                "proposed_storeys": 20,
                "proposed_units": 200,
                "description": "Tower A",
                "street_num": "100",
                "street_name": "King St W",
                "application_url": "http://app.toronto.ca/AIC/index.do?folderRsn=abc",
            },
            {
                "folderrsn": "1111111",  # duplicate
                "application_type": "OZ",
                "ward_number": "10",
                "zoning_class": "CRE",
                "status": "Closed",
                "year_submitted": 2022,
                "lat": 43.645,
                "lon": -79.396,
                "dev_approved": 1,
                "dev_appealed": 0,
                "dev_days_to_decision": 365,
                "proposed_storeys": 20,
                "proposed_units": 200,
                "description": "Tower A",
                "street_num": "100",
                "street_name": "King St W",
                "application_url": "http://app.toronto.ca/AIC/index.do?folderRsn=abc",
            },
            {
                "folderrsn": "2222222",
                "application_type": "SA",
                "ward_number": "10",
                "zoning_class": "CRE",
                "status": "Active",
                "year_submitted": 2023,
                "lat": 43.646,
                "lon": -79.397,
                "dev_approved": None,
                "dev_appealed": None,
                "dev_days_to_decision": None,
                "proposed_storeys": None,
                "proposed_units": None,
                "description": "Tower B",
                "street_num": "200",
                "street_name": "Queen St W",
                "application_url": None,
            },
        ],
    )

    results = query_comps(enriched)
    assert len(results) == 2
    rsns = {r["folderrsn"] for r in results}
    assert rsns == {"1111111", "2222222"}


def test_query_comps_application_url_null_when_absent(tmp_path: Path) -> None:
    """application_url is None when not present in data."""
    from zoneto.api.comps import query_comps

    # Create parquet without application_url column
    df = pl.DataFrame(
        {
            "folderrsn": ["9999999"],
            "application_type": ["OZ"],
            "ward_number": ["5"],
            "zoning_class": ["R"],
            "status": ["Active"],
            "year_submitted": [2023],
            "lat": [43.7],
            "lon": [-79.4],
            "dev_approved": [None],
            "dev_appealed": [None],
            "dev_days_to_decision": [None],
            "proposed_storeys": [None],
            "proposed_units": [None],
            "description": [""],
            "street_num": ["1"],
            "street_name": ["Queen St"],
        },
        schema={
            "folderrsn": pl.String,
            "application_type": pl.String,
            "ward_number": pl.String,
            "zoning_class": pl.String,
            "status": pl.String,
            "year_submitted": pl.Int32,
            "lat": pl.Float64,
            "lon": pl.Float64,
            "dev_approved": pl.Int8,
            "dev_appealed": pl.Int8,
            "dev_days_to_decision": pl.Int32,
            "proposed_storeys": pl.Int32,
            "proposed_units": pl.Int32,
            "description": pl.String,
            "street_num": pl.String,
            "street_name": pl.String,
        },
    )
    path = tmp_path / "dev_applications.parquet"
    df.write_parquet(path)

    results = query_comps(path)
    assert len(results) == 1
    assert results[0].get("application_url") is None

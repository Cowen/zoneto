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


def test_query_comps_includes_spatial_features(tmp_path: Path) -> None:
    """Spatial and demographic features must be returned when present in parquet."""
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
                "application_url": None,
                "in_heritage_register": 0,
                "in_heritage_district": 0,
                "in_secondary_plan": 1,
                "secondary_plan_name": "King-Spadina",
                "in_mtsa": 1,
                "ward_pct_renters": 0.52,
                "ward_median_income": 65000.0,
                "ward_pop_density": 12000.0,
                "ward_pct_detached": 0.08,
                "ward_appeal_rate_3y": 0.18,
                "has_community_meeting": 0,
            }
        ],
    )

    results = query_comps(enriched)
    assert len(results) == 1
    r = results[0]
    assert r["in_heritage_register"] == 0
    assert r["in_heritage_district"] == 0
    assert r["in_secondary_plan"] == 1
    assert r["secondary_plan_name"] == "King-Spadina"
    assert r["in_mtsa"] == 1
    assert abs(r["ward_pct_renters"] - 0.52) < 1e-6
    assert abs(r["ward_median_income"] - 65000.0) < 1e-6
    assert abs(r["ward_pop_density"] - 12000.0) < 1e-6
    assert abs(r["ward_pct_detached"] - 0.08) < 1e-6
    assert abs(r["ward_appeal_rate_3y"] - 0.18) < 1e-6
    assert r["has_community_meeting"] == 0


def test_query_comps_spatial_features_null_when_absent(tmp_path: Path) -> None:
    """Spatial features return None when columns are absent from parquet."""
    import polars as pl

    from zoneto.api.comps import query_comps

    df = pl.DataFrame(
        {
            "folderrsn": ["9999998"],
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
    path = tmp_path / "spatial_absent.parquet"
    df.write_parquet(path)

    results = query_comps(path)
    assert len(results) == 1
    r = results[0]
    assert r.get("in_heritage_register") is None
    assert r.get("in_heritage_district") is None
    assert r.get("in_secondary_plan") is None
    assert r.get("secondary_plan_name") is None
    assert r.get("in_mtsa") is None
    assert r.get("ward_pct_renters") is None
    assert r.get("has_community_meeting") is None


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

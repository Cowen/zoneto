"""Tests for AIC full application record scraper."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from pytest_httpx import HTTPXMock

from zoneto.sources.aic import fetch_aic_applications

_ARCGIS_BASE = (
    "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services"
    "/COTGEO_IBMS_AIC_POINT/FeatureServer/0"
)
_ARCGIS_QUERY_URL = _ARCGIS_BASE + "/query"
_ARCGIS_META_URL = _ARCGIS_BASE


def _epoch_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _meta_resp(fields: list[str]) -> str:
    """Fake ArcGIS layer metadata response."""
    return json.dumps(
        {
            "fields": [{"name": f, "type": "esriFieldTypeString"} for f in fields],
            "maxRecordCount": 1000,
        }
    )


def _count_resp(count: int) -> str:
    return json.dumps({"count": count})


def _features_resp(features: list[dict]) -> str:
    return json.dumps({"features": [{"attributes": f} for f in features]})


SAMPLE_FIELDS = [
    "FOLDERRSN",
    "FOLDERTYPE",
    "STATUS",
    "DATE_SUBMITTED",
    "LOCATION",
    "WARD",
    "DESCRIPTION",
    "LATITUDE",
    "LONGITUDE",
]

SAMPLE_FEATURE = {
    "FOLDERRSN": "12345",
    "FOLDERTYPE": "OZ",
    "STATUS": "Under Review",
    "DATE_SUBMITTED": _epoch_ms(date(2023, 6, 1)),
    "LOCATION": "100 King St W",
    "WARD": "10",
    "DESCRIPTION": "47-storey mixed-use tower",
    "LATITUDE": 43.6480,
    "LONGITUDE": -79.3813,
}


def test_fetch_aic_applications_writes_parquet(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Write Hive-partitioned Parquet to data/aic_applications/."""
    # metadata → count → features page (1 record fits in 1 page of 200)
    httpx_mock.add_response(
        url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS)
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_count_resp(1),
    )
    # First (and only) features fetch returns data
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_features_resp([SAMPLE_FEATURE]),
    )

    count = fetch_aic_applications(tmp_path)

    assert count == 1
    # Hive-partitioned output must exist
    aic_dir = tmp_path / "aic_applications"
    parquet_files = list(aic_dir.rglob("*.parquet"))
    assert len(parquet_files) >= 1, "Expected at least one Parquet file"


def test_fetch_aic_applications_returns_expected_columns(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Output DataFrame includes year, source_name, folderrsn, application_type."""
    httpx_mock.add_response(
        url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS)
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_count_resp(1),
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_features_resp([SAMPLE_FEATURE]),
    )

    fetch_aic_applications(tmp_path)

    aic_dir = tmp_path / "aic_applications"
    df = pl.read_parquet(aic_dir, hive_partitioning=True)
    assert "folderrsn" in df.columns
    assert "application_type" in df.columns
    assert "year" in df.columns
    assert "source_name" in df.columns
    assert df["source_name"][0] == "aic_applications"


def test_fetch_aic_applications_year_from_date_submitted(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """year column is derived from DATE_SUBMITTED (2023-06-01 → year=2023)."""
    httpx_mock.add_response(
        url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS)
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_count_resp(1),
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_features_resp([SAMPLE_FEATURE]),
    )

    fetch_aic_applications(tmp_path)
    df = pl.read_parquet(tmp_path / "aic_applications", hive_partitioning=True)
    assert df["year"][0] == 2023


def test_fetch_aic_applications_empty_response(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Returns 0 when ArcGIS returns no features."""
    httpx_mock.add_response(
        url=_ARCGIS_META_URL + "?f=json", text=_meta_resp(SAMPLE_FIELDS)
    )
    httpx_mock.add_response(
        url=_ARCGIS_QUERY_URL,
        method="POST",
        text=_count_resp(0),
    )

    count = fetch_aic_applications(tmp_path)
    assert count == 0

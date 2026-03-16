"""Tests for fetch_aic_decisions_arcgis (ArcGIS FeatureServer approach)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
from pytest_httpx import HTTPXMock

from zoneto.sources.aic import fetch_aic_decisions_arcgis

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ARCGIS_URL = (
    "https://services3.arcgis.com/b9WvedVPoizGfvfD/ArcGIS/rest/services"
    "/COTGEO_IBMS_AIC_POINT/FeatureServer/0/query"
)


def _epoch_ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _arcgis_resp(features: list[dict]) -> str:
    return json.dumps({"features": [{"attributes": f} for f in features]})


def _make_dev_parquet(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "folderrsn": ["111", "222", "333"],
            "application_type": ["OZ", "SA", "OZ"],
            "application_url": [
                "http://app.toronto.ca/AIC/index.do?folderRsn=aaa",
                "http://app.toronto.ca/AIC/index.do?folderRsn=bbb",
                "http://app.toronto.ca/AIC/index.do?folderRsn=ccc",
            ],
            "status": ["Closed", "Closed", "Under Review"],
            "date_submitted": ["2021-01-01", "2020-01-01", "2022-01-01"],
            "year": [2021, 2020, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_oz_decision_date_extracted(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    """OZ application: LATEST_MILESTONE='City Council Decision Made' → decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(
        text=_arcgis_resp(
            [
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": _epoch_ms(date(2021, 3, 15)),
                },
                {
                    "FOLDERRSN": 222,
                    "FOLDERTYPE": "SA",
                    "LATEST_MILESTONE": "Statement of Approval Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2021, 2, 14)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 333,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "Notice of Complete Application Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 1, 10)),
                    "COMPLETE_DATE": _epoch_ms(date(2022, 1, 10)),
                },
            ]
        ),
    )

    count = fetch_aic_decisions_arcgis(tmp_path)

    out = tmp_path / "reference" / "aic_decisions.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    oz_row = df.filter(pl.col("folderrsn") == "111")
    assert oz_row["decision_date"][0] == date(2022, 11, 8)
    assert oz_row["complete_date"][0] == date(2021, 3, 15)
    assert count == 3


def test_sa_decision_date_extracted(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    """SA application: LATEST_MILESTONE='Statement of Approval Issued' → decision_date."""  # noqa: E501
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(
        text=_arcgis_resp(
            [
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 222,
                    "FOLDERTYPE": "SA",
                    "LATEST_MILESTONE": "Statement of Approval Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2021, 2, 14)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 333,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "Application Circulated",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 1, 10)),
                    "COMPLETE_DATE": None,
                },
            ]
        ),
    )

    fetch_aic_decisions_arcgis(tmp_path)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    sa_row = df.filter(pl.col("folderrsn") == "222")
    assert sa_row["decision_date"][0] == date(2021, 2, 14)


def test_non_decision_milestone_produces_null(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Application whose latest milestone is not a decision → null decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(
        text=_arcgis_resp(
            [
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 222,
                    "FOLDERTYPE": "SA",
                    "LATEST_MILESTONE": "Statement of Approval Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2021, 2, 14)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 333,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "Application Circulated",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 1, 10)),
                    "COMPLETE_DATE": None,
                },
            ]
        ),
    )

    fetch_aic_decisions_arcgis(tmp_path)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    no_decision_row = df.filter(pl.col("folderrsn") == "333")
    assert no_decision_row["decision_date"][0] is None


def test_already_cached_rows_skipped(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    """Rows already in aic_decisions.parquet are not re-fetched."""
    _make_dev_parquet(tmp_path)
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    existing = pl.DataFrame(
        {
            "folderrsn": ["111"],
            "decision_date": [date(2022, 11, 8)],
            "complete_date": [date(2021, 3, 15)],
            "scraped_at": [date.today()],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    existing.write_parquet(ref_dir / "aic_decisions.parquet")

    httpx_mock.add_response(
        text=_arcgis_resp(
            [
                {
                    "FOLDERRSN": 222,
                    "FOLDERTYPE": "SA",
                    "LATEST_MILESTONE": "Statement of Approval Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2021, 2, 14)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 333,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "Application Circulated",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 1, 10)),
                    "COMPLETE_DATE": None,
                },
            ]
        ),
    )

    count = fetch_aic_decisions_arcgis(tmp_path)
    assert count == 2

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(df) == 3  # 1 pre-existing + 2 new


def test_duplicate_folderrsn_in_arcgis_response_deduplicated(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Same FOLDERRSN returned multiple times (multi-address apps) → one row output."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(
        text=_arcgis_resp(
            [
                # folderrsn 111 appears 3 times (different addresses)
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": _epoch_ms(date(2021, 3, 15)),
                },
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": _epoch_ms(date(2021, 3, 15)),
                },
                {
                    "FOLDERRSN": 111,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "City Council Decision Made",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 11, 8)),
                    "COMPLETE_DATE": _epoch_ms(date(2021, 3, 15)),
                },
                {
                    "FOLDERRSN": 222,
                    "FOLDERTYPE": "SA",
                    "LATEST_MILESTONE": "Statement of Approval Issued",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2021, 2, 14)),
                    "COMPLETE_DATE": None,
                },
                {
                    "FOLDERRSN": 333,
                    "FOLDERTYPE": "OZ",
                    "LATEST_MILESTONE": "Application Circulated",
                    "LATEST_MILESTONE_DATE": _epoch_ms(date(2022, 1, 10)),
                    "COMPLETE_DATE": None,
                },
            ]
        ),
    )

    count = fetch_aic_decisions_arcgis(tmp_path)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(df) == 3
    assert count == 3


def test_batching_issues_multiple_requests(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """When folderrsn count exceeds batch_size, multiple API requests are issued."""
    # Create 5 rows, batch_size=2 → 3 requests
    df = pl.DataFrame(
        {
            "folderrsn": ["1", "2", "3", "4", "5"],
            "application_type": ["OZ", "SA", "OZ", "SA", "OZ"],
            "application_url": ["http://x"] * 5,
            "status": ["Closed"] * 5,
            "date_submitted": ["2021-01-01"] * 5,
            "year": [2021] * 5,
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    # Add 3 responses (ceil(5/2) = 3 batches)
    for _ in range(3):
        httpx_mock.add_response(text=_arcgis_resp([]))

    count = fetch_aic_decisions_arcgis(tmp_path, batch_size=2)
    # 3 batches issued; ArcGIS returned no features, but 5 null-decision rows
    # are still written so those apps aren't re-queried on the next run
    assert count == 5

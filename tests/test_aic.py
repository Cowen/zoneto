"""Tests for the AIC scraper (fetch_aic_decisions)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
from pytest_httpx import HTTPXMock

from zoneto.sources.aic import fetch_aic_decisions

# ---------------------------------------------------------------------------
# Synthetic AIC HTML fixtures
# ---------------------------------------------------------------------------

_OZ_MILESTONES_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2021-03-15</td></tr>
  <tr><td>Community Consultation Meeting</td><td>2021-05-20</td></tr>
  <tr><td>City Council Decision Made</td><td>2022-11-08</td></tr>
</table>
</body></html>
"""

_SA_MILESTONES_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2020-06-01</td></tr>
  <tr><td>Statement of Approval Issued</td><td>2021-02-14</td></tr>
</table>
</body></html>
"""

_NO_DECISION_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2022-01-10</td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Fixtures: minimal dev_applications parquet
# ---------------------------------------------------------------------------


def _make_dev_parquet(tmp_path: Path) -> None:
    """Write minimal dev_applications parquet with OZ and SA rows."""
    df = pl.DataFrame(
        {
            "folderrsn": ["111", "222", "333"],
            "application_type": ["OZ", "SA", "OZ"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=111",
                "https://app.toronto.ca/AIC/details?folderRsn=222",
                "https://app.toronto.ca/AIC/details?folderRsn=333",
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


def test_oz_application_extracts_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """OZ application: extracts 'City Council Decision Made' as decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    out = tmp_path / "reference" / "aic_decisions.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    oz_row = df.filter(pl.col("folderrsn") == "111")
    assert oz_row["decision_date"][0] == date(2022, 11, 8)
    assert oz_row["complete_date"][0] == date(2021, 3, 15)
    assert count == 3


def test_sa_application_extracts_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """SA application: extracts 'Statement of Approval Issued' as decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    sa_row = df.filter(pl.col("folderrsn") == "222")
    assert sa_row["decision_date"][0] == date(2021, 2, 14)


def test_missing_milestone_produces_null_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Application with no decision milestone gets null decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    no_decision_row = df.filter(pl.col("folderrsn") == "333")
    assert no_decision_row["decision_date"][0] is None


def test_already_scraped_rows_are_skipped(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Rows already in aic_decisions.parquet are not re-fetched (idempotency)."""
    _make_dev_parquet(tmp_path)

    # Pre-populate cache with folderrsn "111"
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

    # Only 2 new rows should be fetched (222 and 333)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)
    assert count == 2

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(df) == 3  # 1 pre-existing + 2 newly scraped


def test_http_error_skipped_without_crash(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """HTTP 404 for one URL is logged and skipped; other rows still processed."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(status_code=404)  # folderrsn=111 fails
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)  # folderrsn=222 succeeds
    httpx_mock.add_response(text=_NO_DECISION_HTML)  # folderrsn=333 succeeds

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    # Only 2 rows written (111 skipped due to HTTP error)
    assert len(df) == 2
    assert count == 2

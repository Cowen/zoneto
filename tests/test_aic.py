"""Tests for the AIC scraper (fetch_aic_decisions)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from pytest_httpx import HTTPXMock

import zoneto.sources.aic as _aic_module
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


def test_http_redirect_to_https_is_followed(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """http:// URLs that 302-redirect to https:// are followed successfully."""
    df = pl.DataFrame(
        {
            "folderrsn": ["111"],
            "application_type": ["OZ"],
            "application_url": ["http://app.toronto.ca/AIC/details?folderRsn=111"],
            "status": ["Closed"],
            "date_submitted": ["2021-01-01"],
            "year": [2021],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    https_url = "https://app.toronto.ca/AIC/details?folderRsn=111"
    httpx_mock.add_response(
        url="http://app.toronto.ca/AIC/details?folderRsn=111",
        status_code=302,
        headers={"Location": https_url},
    )
    httpx_mock.add_response(url=https_url, text=_OZ_MILESTONES_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    assert count == 1
    result = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert result["decision_date"][0] == date(2022, 11, 8)


@pytest.mark.httpx_mock(assert_all_responses_were_requested=False)
def test_duplicate_folderrsn_scraped_only_once(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Same folderrsn appearing in multiple year partitions is scraped exactly once."""
    for year in (2021, 2022):
        df = pl.DataFrame(
            {
                "folderrsn": ["111"],
                "application_type": ["OZ"],
                "application_url": [
                    "https://app.toronto.ca/AIC/details?folderRsn=111"
                ],
                "status": ["Closed"],
                "date_submitted": [f"{year}-01-01"],
                "year": [year],
            }
        ).with_columns(pl.col("date_submitted").str.to_date())
        p = tmp_path / "dev_applications" / f"year={year}"
        p.mkdir(parents=True)
        df.write_parquet(p / "part0.parquet")

    # Two responses queued — second would be consumed if scraper hits the URL twice
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    assert count == 1
    result = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(result) == 1


def test_progress_logged_at_start_and_each_chunk(
    tmp_path: Path, httpx_mock: HTTPXMock, caplog: pytest.LogCaptureFixture
) -> None:
    """fetch_aic_decisions logs a start message and a progress line per chunk."""
    import logging

    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    with caplog.at_level(logging.INFO, logger="zoneto.sources.aic"):
        fetch_aic_decisions(tmp_path, delay=0.0, chunk_size=2)

    messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
    # Start message mentions total to scrape
    assert any("3" in m for m in messages), "Expected total count in start message"
    # Progress message after each flush mentions scraped so far
    assert any("scraped" in m.lower() for m in messages)


def test_chunk_size_writes_all_rows(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    """chunk_size smaller than total rows still writes all rows."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0, chunk_size=2)

    assert count == 3
    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(df) == 3


def test_chunk_data_preserved_on_interrupt(
    tmp_path: Path, httpx_mock: HTTPXMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rows in completed chunks survive if a later row causes an unexpected crash."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    original_parse = _aic_module._parse_milestones
    parse_calls = 0

    def crashing_parse(html: str, app_type: str) -> tuple:
        nonlocal parse_calls
        parse_calls += 1
        if parse_calls >= 3:
            raise RuntimeError("Simulated crash")
        return original_parse(html, app_type)

    monkeypatch.setattr(_aic_module, "_parse_milestones", crashing_parse)

    with pytest.raises(RuntimeError, match="Simulated crash"):
        fetch_aic_decisions(tmp_path, delay=0.0, chunk_size=2)

    out = tmp_path / "reference" / "aic_decisions.parquet"
    assert out.exists(), "Cache file should exist even after crash"
    df = pl.read_parquet(out)
    assert len(df) == 2, "First chunk (2 rows) should be saved before the crash"

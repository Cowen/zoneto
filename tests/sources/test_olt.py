"""Tests for OLT decision scraper."""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl
from pytest_httpx import HTTPXMock

from zoneto.sources.olt import fetch_olt_decisions

_OLT_BASE = "https://olt.gov.on.ca"
_OLT_SEARCH_URL = _OLT_BASE + "/decisions/"


def _make_search_html(cases: list[dict]) -> str:
    """Minimal HTML mimicking OLT search results table."""
    rows = ""
    for c in cases:
        rows += f"""
        <tr>
            <td><a href="/decisions/{c["case_number"]}">{c["case_number"]}</a></td>
            <td>{c.get("municipality", "Toronto")}</td>
            <td>{c.get("hearing_date", "2023-01-15")}</td>
            <td>{c.get("decision_date", "2023-03-10")}</td>
            <td>{c.get("outcome", "Dismissed")}</td>
            <td>{c.get("address", "100 King St W, Toronto")}</td>
        </tr>"""
    return f"""<!DOCTYPE html><html><body>
    <table id="decisions-table">
    <thead><tr>
      <th>Case Number</th><th>Municipality</th>
      <th>Hearing Date</th><th>Decision Date</th>
      <th>Outcome</th><th>Address</th>
    </tr></thead>
    <tbody>{rows}</tbody>
    </table>
    </body></html>"""


def _make_empty_page_html() -> str:
    return """<!DOCTYPE html><html><body>
    <table id="decisions-table"><thead></thead><tbody></tbody></table>
    </body></html>"""


def test_fetch_olt_decisions_writes_parquet(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """fetch_olt_decisions() writes olt_decisions.parquet to data/reference/."""
    cases = [
        {
            "case_number": "PL220001",
            "municipality": "Toronto",
            "hearing_date": "2022-09-12",
            "decision_date": "2022-11-30",
            "outcome": "Dismissed",
            "address": "500 Queen St W, Toronto",
        }
    ]
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_search_html(cases),
    )
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_empty_page_html(),
    )

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 1

    out_path = tmp_path / "reference" / "olt_decisions.parquet"
    assert out_path.exists()
    df = pl.read_parquet(out_path)
    assert "case_number" in df.columns
    assert "outcome" in df.columns
    assert "decision_date" in df.columns
    assert "address" in df.columns
    assert df["decision_date"].dtype == pl.Date
    assert df["hearing_date"].dtype == pl.Date


def test_fetch_olt_decisions_parses_case_fields(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Parsed fields match the source HTML values."""
    cases = [
        {
            "case_number": "OLT-22-000123",
            "municipality": "Toronto",
            "hearing_date": "2022-09-12",
            "decision_date": "2022-11-30",
            "outcome": "Allowed",
            "address": "200 Front St W, Toronto",
        }
    ]
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_search_html(cases),
    )
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_empty_page_html(),
    )

    fetch_olt_decisions(tmp_path, delay=0.0)
    df = pl.read_parquet(tmp_path / "reference" / "olt_decisions.parquet")

    assert df["case_number"][0] == "OLT-22-000123"
    assert df["outcome"][0] == "Allowed"
    assert df["address"][0] == "200 Front St W, Toronto"
    assert df["decision_date"].dtype == pl.Date
    assert df["hearing_date"].dtype == pl.Date


def test_fetch_olt_decisions_empty_results(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Returns 0 and does not write file when no decisions found."""
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_empty_page_html(),
    )

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 0
    assert not (tmp_path / "reference" / "olt_decisions.parquet").exists()


def test_fetch_olt_decisions_paginates(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    """Fetches multiple pages until an empty page is returned."""
    page1 = [
        {"case_number": f"PL2200{i:02d}", "address": f"{i} King St"} for i in range(3)
    ]
    page2 = [
        {"case_number": f"PL2200{i:02d}", "address": f"{i} Queen St"}
        for i in range(3, 5)
    ]

    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_search_html(page1),
    )
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_search_html(page2),
    )
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_empty_page_html(),
    )

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 5


def test_fetch_olt_decisions_handles_http_error(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Stops pagination on HTTP error and returns collected data."""
    page1 = [
        {"case_number": "PL220001", "address": "100 King St"}
    ]
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        text=_make_search_html(page1),
    )
    httpx_mock.add_response(
        url=re.compile(r"https://olt\.gov\.on\.ca/decisions/"),
        status_code=503,
    )

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    # Should return the count from page 1 since pagination stopped at page 2 error
    assert count == 1

    out_path = tmp_path / "reference" / "olt_decisions.parquet"
    assert out_path.exists()
    df = pl.read_parquet(out_path)
    assert len(df) == 1
    assert df["case_number"][0] == "PL220001"

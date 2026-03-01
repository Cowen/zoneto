from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from zoneto.models import CKANConfig
from zoneto.sources.ckan import CKANSource


@pytest.fixture
def source() -> CKANSource:
    return CKANSource(CKANConfig(
        dataset_id="building-permits-active-permits",
        access_mode="datastore",
    ))


def test_single_page_returns_all_records(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """A single page with records followed by an empty page fetches all records."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-15", "Permit No": "A001"},
            {"Application Date": "2024-02-20", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert len(df) == 2


def test_multi_page_pagination(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Records from multiple pages are concatenated correctly."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-01", "Permit No": "A001"},
            {"Application Date": "2024-01-02", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-03", "Permit No": "A003"},
        ]}},
    )
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert len(df) == 3


def test_empty_first_response_returns_empty_dataframe(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """An empty first response terminates immediately and returns an empty DataFrame."""
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert df.is_empty()


def test_normalization_snake_case_columns(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Column names are converted to snake_case."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-06-15", "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert "application_date" in df.columns
    assert "permit_no" in df.columns
    assert "Application Date" not in df.columns


def test_normalization_year_derived_from_date(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """year column is derived from application_date."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2023-03-10", "Permit No": "A001"},
            {"Application Date": "2024-07-22", "Permit No": "A002"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    years = df["year"].to_list()
    assert 2023 in years
    assert 2024 in years


def test_null_date_produces_year_zero(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Records with null application dates get year=0."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": None, "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert df["year"][0] == 0


def test_source_name_column_added(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """source_name column is set to the dataset_id."""
    httpx_mock.add_response(
        json={"result": {"records": [
            {"Application Date": "2024-01-01", "Permit No": "A001"},
        ]}},
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert "source_name" in df.columns
    assert df["source_name"][0] == "building-permits-active-permits"

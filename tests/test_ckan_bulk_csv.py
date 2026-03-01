from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from zoneto.models import CKANConfig
from zoneto.sources.ckan import CKANSource


@pytest.fixture
def source() -> CKANSource:
    return CKANSource(
        CKANConfig(
            dataset_id="building-permits-cleared-permits",
            access_mode="bulk_csv",
            year_start=2020,
        )
    )


def _package_show_response(resources: list[dict]) -> dict:
    """Build a package_show API response with the given resources."""
    return {"result": {"resources": resources}}


def test_downloads_qualifying_years_only(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Only resources with year >= year_start are downloaded."""
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {"name": "Cleared Permits 2019", "url": "https://example.com/2019.csv"},
                {"name": "Cleared Permits 2020", "url": "https://example.com/2020.csv"},
                {"name": "Cleared Permits 2021", "url": "https://example.com/2021.csv"},
            ]
        ),
    )
    # Only 2020 and 2021 are downloaded (2019 is below year_start=2020)
    httpx_mock.add_response(content=b"Application Date,Permit No\n2020-01-01,B001\n")
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-06-15,B002\n")

    df = source.fetch()
    assert len(df) == 2


def test_all_qualifying_csvs_concatenated(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """All qualifying CSVs are concatenated into a single DataFrame."""
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {"name": "Cleared 2020", "url": "https://example.com/2020.csv"},
                {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
                {"name": "Cleared 2022", "url": "https://example.com/2022.csv"},
            ]
        ),
    )
    httpx_mock.add_response(
        content=b"Application Date,Permit No\n2020-01-01,B001\n2020-02-01,B002\n"
    )
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-03-01,B003\n")
    httpx_mock.add_response(
        content=b"Application Date,Permit No\n2022-04-01,B004\n2022-05-01,B005\n"
    )

    df = source.fetch()
    assert len(df) == 5


def test_non_year_resources_are_skipped(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Resources whose names contain no 4-digit year are skipped."""
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {"name": "Active Permits", "url": "https://example.com/active.csv"},
                {"name": "Metadata file", "url": "https://example.com/meta.csv"},
                {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
            ]
        ),
    )
    # Only the 2021 resource should trigger a download
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-01-01,B001\n")

    df = source.fetch()
    assert len(df) == 1


def test_no_qualifying_resources_returns_empty(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """When no resources qualify, fetch returns an empty DataFrame."""
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {"name": "Cleared 2015", "url": "https://example.com/2015.csv"},
                {"name": "Cleared 2019", "url": "https://example.com/2019.csv"},
            ]
        ),
    )
    # No downloads should occur (both years are below year_start=2020)

    df = source.fetch()
    assert df.is_empty()


def test_year_column_set_from_application_date(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """year column is derived from the application_date column after normalization."""
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {"name": "Cleared 2021", "url": "https://example.com/2021.csv"},
            ]
        ),
    )
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-06-01,B001\n")

    df = source.fetch()
    assert "year" in df.columns
    assert df["year"][0] == 2021

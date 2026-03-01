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


def _csv_resource(name: str, url: str) -> dict:
    """Build a CSV resource dict with format='CSV' (the default for bulk download)."""
    return {"name": name, "url": url, "format": "CSV"}


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
                _csv_resource("Cleared Permits 2019", "https://example.com/2019.csv"),
                _csv_resource("Cleared Permits 2020", "https://example.com/2020.csv"),
                _csv_resource("Cleared Permits 2021", "https://example.com/2021.csv"),
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
                _csv_resource("Cleared 2020", "https://example.com/2020.csv"),
                _csv_resource("Cleared 2021", "https://example.com/2021.csv"),
                _csv_resource("Cleared 2022", "https://example.com/2022.csv"),
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
                _csv_resource("Active Permits", "https://example.com/active.csv"),
                _csv_resource("Metadata file", "https://example.com/meta.csv"),
                _csv_resource("Cleared 2021", "https://example.com/2021.csv"),
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
                _csv_resource("Cleared 2015", "https://example.com/2015.csv"),
                _csv_resource("Cleared 2019", "https://example.com/2019.csv"),
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
            [_csv_resource("Cleared 2021", "https://example.com/2021.csv")]
        ),
    )
    httpx_mock.add_response(content=b"Application Date,Permit No\n2021-06-01,B001\n")

    df = source.fetch()
    assert "year" in df.columns
    assert df["year"][0] == 2021


def test_mixed_numeric_column_parsed_without_error(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """A CSV with float-formatted values like '0.' in an otherwise-integer column
    is parsed without error.

    Regression test for infer_schema_length=None: with a smaller inference window,
    polars infers the column as i64 from early rows and then fails to parse '0.'.
    """
    httpx_mock.add_response(
        json=_package_show_response(
            [_csv_resource("Cleared 2021", "https://example.com/2021.csv")]
        ),
    )
    # 'Units' column: first row has a plain integer, second has '0.' (float notation)
    csv_content = (
        b"Application Date,Permit No,Units\n"
        b"2021-01-01,B001,5\n"
        b"2021-02-01,B002,0.\n"
    )
    httpx_mock.add_response(content=csv_content)

    df = source.fetch()
    assert len(df) == 2


def test_only_csv_format_resources_are_downloaded(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Only resources with format='CSV' are downloaded; XML and JSON duplicates
    that share the same year in their name are skipped.

    Regression test: package_show returns CSV, XML, and JSON variants for each
    year. Downloading all three and feeding XML/JSON to pl.read_csv creates
    garbage columns (observed: 139,810 columns on the COA dataset).
    """
    httpx_mock.add_response(
        json=_package_show_response(
            [
                {
                    "name": "Closed Applications 2021",
                    "url": "https://example.com/2021.csv",
                    "format": "CSV",
                },
                {
                    "name": "Closed Applications 2021.xml",
                    "url": "https://example.com/2021.xml",
                    "format": "XML",
                },
                {
                    "name": "Closed Applications 2021.json",
                    "url": "https://example.com/2021.json",
                    "format": "JSON",
                },
            ]
        ),
    )
    # Only one HTTP call for the CSV; XML and JSON should never be requested
    httpx_mock.add_response(
        content=b"Application Date,Permit No\n2021-06-01,C001\n"
    )

    df = source.fetch()
    assert len(df) == 1


def test_unrecognized_date_format_does_not_raise(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """A date column whose format polars cannot auto-detect is left as-is without
    raising, and the row is still returned with year=0.

    Regression test: COA CSVs use MM/DD/YYYY dates which polars cannot infer,
    causing 'could not find an appropriate format to parse dates'.
    """
    httpx_mock.add_response(
        json=_package_show_response(
            [_csv_resource("COA 2021", "https://example.com/2021.csv")]
        ),
    )
    csv_content = b"Application Date,File Number\n01/15/2021,A21-001\n"
    httpx_mock.add_response(content=csv_content)

    df = source.fetch()
    assert len(df) == 1
    assert df["year"][0] == 0  # date unparseable -> year defaults to 0


def test_duplicate_snake_case_column_names_are_deduplicated(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Two columns that normalize to the same snake_case name do not raise.

    Regression test: COA CSVs have columns like 'Street Direction' and
    'STREET_DIRECTION' that both map to 'street_direction', causing a duplicate
    column error inside df.rename().
    """
    httpx_mock.add_response(
        json=_package_show_response(
            [_csv_resource("COA 2021", "https://example.com/2021.csv")]
        ),
    )
    csv_content = (
        b"Application Date,Street Direction,STREET_DIRECTION\n"
        b"2021-01-01,N,North\n"
    )
    httpx_mock.add_response(content=csv_content)

    df = source.fetch()
    assert len(df) == 1
    assert "street_direction" in df.columns


def test_ragged_csv_rows_are_truncated(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Rows with more fields than the header are silently truncated.

    Regression test for truncate_ragged_lines=True: without it, polars raises
    'found more fields than defined in Schema' for rows with extra trailing fields.
    """
    httpx_mock.add_response(
        json=_package_show_response(
            [_csv_resource("Cleared 2021", "https://example.com/2021.csv")]
        ),
    )
    csv_content = (
        b"Application Date,Permit No\n"
        b"2021-01-01,B001\n"
        b"2021-02-01,B002,extra_field\n"
    )
    httpx_mock.add_response(content=csv_content)

    df = source.fetch()
    assert len(df) == 2

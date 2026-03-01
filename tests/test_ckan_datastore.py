from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from zoneto.models import CKANConfig
from zoneto.sources.ckan import CKANSource

_RESOURCE_ID = "test-resource-id"

_PACKAGE_SHOW = {
    "result": {"resources": [{"id": _RESOURCE_ID, "datastore_active": True}]}
}


@pytest.fixture
def source() -> CKANSource:
    return CKANSource(
        CKANConfig(
            dataset_id="building-permits-active-permits",
            access_mode="datastore",
        )
    )


def test_single_page_returns_all_records(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """A single page with records followed by an empty page fetches all records."""
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2024-01-15", "Permit No": "A001"},
                    {"Application Date": "2024-02-20", "Permit No": "A002"},
                ]
            }
        },
    )
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert len(df) == 2


def test_multi_page_pagination(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """Records from multiple pages are concatenated correctly."""
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2024-01-01", "Permit No": "A001"},
                    {"Application Date": "2024-01-02", "Permit No": "A002"},
                ]
            }
        },
    )
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2024-01-03", "Permit No": "A003"},
                ]
            }
        },
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
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={"result": {"records": []}},
    )

    df = source.fetch()
    assert df.is_empty()


def test_normalization_snake_case_columns(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """Column names are converted to snake_case."""
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2024-06-15", "Permit No": "A001"},
                ]
            }
        },
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
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2023-03-10", "Permit No": "A001"},
                    {"Application Date": "2024-07-22", "Permit No": "A002"},
                ]
            }
        },
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
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": None, "Permit No": "A001"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert df["year"][0] == 0


def test_year_filter_excludes_records_before_year_start(
    httpx_mock: HTTPXMock,
) -> None:
    """Records with year < year_start are excluded from the returned DataFrame."""
    filtered_source = CKANSource(
        CKANConfig(
            dataset_id="building-permits-active-permits",
            access_mode="datastore",
            year_start=2020,
        )
    )
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2019-06-01", "Permit No": "OLD001"},
                    {"Application Date": "2021-03-15", "Permit No": "NEW001"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = filtered_source.fetch()
    assert len(df) == 1
    assert df["permit_no"][0] == "NEW001"


def test_null_date_records_preserved_despite_year_filter(
    httpx_mock: HTTPXMock,
) -> None:
    """Records with null application dates (year=0) are kept regardless of year_start."""
    filtered_source = CKANSource(
        CKANConfig(
            dataset_id="building-permits-active-permits",
            access_mode="datastore",
            year_start=2020,
        )
    )
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": None, "Permit No": "NULL001"},
                    {"Application Date": "2021-03-15", "Permit No": "NEW001"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = filtered_source.fetch()
    assert len(df) == 2


def test_column_null_in_first_100_records_then_string_does_not_raise(
    httpx_mock: HTTPXMock, source: CKANSource
) -> None:
    """A column that is null for the first 100 records then holds a string value
    is handled without a schema mismatch error.

    Regression test: pl.DataFrame(list_of_dicts) defaults to
    infer_schema_length=100, so a column that is null in the first 100 rows is
    inferred as Null type; appending a string value (e.g. 'Sfd Detached') in
    row 101 then raises 'could not append value of type str to the builder'.
    """
    null_records = [
        {"Application Date": "2021-01-01", "Permit No": f"A{i:03d}", "Work Type": None}
        for i in range(100)
    ]
    string_record = {
        "Application Date": "2021-01-02",
        "Permit No": "A100",
        "Work Type": "Sfd Detached",
    }
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={"result": {"records": [*null_records, string_record]}}
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert len(df) == 101


def test_source_name_column_added(httpx_mock: HTTPXMock, source: CKANSource) -> None:
    """source_name column is set to the dataset_id."""
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Application Date": "2024-01-01", "Permit No": "A001"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    assert "source_name" in df.columns
    assert df["source_name"][0] == "building-permits-active-permits"


def test_custom_year_column_derives_year(httpx_mock: HTTPXMock) -> None:
    """year is derived from the configured year_column, not the hardcoded application_date.

    A source configured with year_column='date_submitted' must extract year
    from that column after snake_case normalization ('Date Submitted' -> 'date_submitted').
    """
    source = CKANSource(
        CKANConfig(
            dataset_id="development-applications",
            access_mode="datastore",
            year_column="date_submitted",
        )
    )
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Date Submitted": "2023-05-15", "APPLICATION#": "OZ-01"},
                    {"Date Submitted": "2021-11-30", "APPLICATION#": "SA-02"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    years = df["year"].to_list()
    assert 2023 in years
    assert 2021 in years

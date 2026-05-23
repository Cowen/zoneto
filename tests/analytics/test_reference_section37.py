"""Tests for Section 37 reference dataset ingestion."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from zoneto.analytics.reference import _fetch_section37


@pytest.fixture()
def raw_csv_content() -> bytes:
    return (
        b"_id,Ward,Council/OMB Date,Monetary Value,ByLaw,Location,Community Benefits\n"
        b'1,5,2022-06-15,500000.00,123-2022,"100 King St W","Cash contribution"\n'
        b'2,10,2021-03-01,,"456-2021","200 Bloor St","Parkland dedication"\n'
        b'3,5,2023-09-20,1200000.00,789-2023,"50 Bay St","Cash, Affordable Housing"\n'
    )


class TestFetchSection37:
    def test_writes_parquet_with_expected_schema(
        self, tmp_path: Path, raw_csv_content: bytes
    ) -> None:
        """Given: Valid Section 37 CSV response from CKAN.
        When: _fetch_section37 is called.
        Then: section37.parquet is written with normalized column names."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = mock_response
        mock_response.content = raw_csv_content

        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response

        with patch("zoneto.analytics.reference.httpx.Client", return_value=mock_client):
            _fetch_section37(tmp_path)

        out = tmp_path / "section37.parquet"
        assert out.exists()
        df = pl.read_parquet(out)
        assert "ward" in df.columns
        assert "monetary_value" in df.columns
        assert "location" in df.columns
        assert "community_benefits" in df.columns
        assert "council_date" in df.columns

    def test_monetary_value_parsed_as_float(
        self, tmp_path: Path, raw_csv_content: bytes
    ) -> None:
        """Given: CSV with numeric and null monetary values.
        When: _fetch_section37 is called.
        Then: monetary_value column is Float64 with nulls for empty cells."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = mock_response
        mock_response.content = raw_csv_content
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response

        with patch("zoneto.analytics.reference.httpx.Client", return_value=mock_client):
            _fetch_section37(tmp_path)

        df = pl.read_parquet(tmp_path / "section37.parquet")
        assert df["monetary_value"].dtype == pl.Float64
        assert df["monetary_value"][0] == 500000.0
        assert df["monetary_value"][1] is None

    def test_row_count_matches_csv(
        self, tmp_path: Path, raw_csv_content: bytes
    ) -> None:
        """Given: CSV with 3 data rows.
        When: _fetch_section37 is called.
        Then: parquet has exactly 3 rows."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = mock_response
        mock_response.content = raw_csv_content
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.get.return_value = mock_response

        with patch("zoneto.analytics.reference.httpx.Client", return_value=mock_client):
            _fetch_section37(tmp_path)

        df = pl.read_parquet(tmp_path / "section37.parquet")
        assert len(df) == 3

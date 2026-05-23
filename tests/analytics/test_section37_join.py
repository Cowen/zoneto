"""Tests for Section 37 enrichment join."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from zoneto.analytics.labels import _add_section37_features


def _write_section37(path: Path) -> None:
    pl.DataFrame(
        {
            "location": ["100 King St W", "200 Bloor St", "100 King St W"],
            "monetary_value": [500000.0, None, 750000.0],
            "community_benefits": ["Cash contribution", "Parkland dedication", "Affordable Housing"],
            "ward": ["5", "10", "5"],
            "council_date": ["2022-06-15", "2021-03-01", "2023-09-20"],
        }
    ).write_parquet(path)


class TestAddSection37Features:
    def test_address_match_sums_monetary_value(self, tmp_path: Path) -> None:
        """Given: Dev application address matches two Section 37 records.
        When: _add_section37_features called.
        Then: s37_monetary_value is the sum of matched records."""
        ref = tmp_path / "reference"
        ref.mkdir()
        _write_section37(ref / "section37.parquet")

        df = pl.DataFrame(
            {
                "address": ["100 King St W", "999 Unknown Ave"],
                "folderrsn": ["F001", "F002"],
            }
        )
        result = _add_section37_features(df, tmp_path)

        assert "s37_monetary_value" in result.columns
        assert result["s37_monetary_value"][0] == 1250000.0  # 500000 + 750000
        assert result["s37_monetary_value"][1] is None

    def test_no_match_returns_null(self, tmp_path: Path) -> None:
        """Given: Dev application address has no Section 37 match.
        When: _add_section37_features called.
        Then: s37_monetary_value and s37_benefit_text are both null."""
        ref = tmp_path / "reference"
        ref.mkdir()
        _write_section37(ref / "section37.parquet")

        df = pl.DataFrame(
            {"address": ["999 Unknown Ave"], "folderrsn": ["F003"]}
        )
        result = _add_section37_features(df, tmp_path)
        assert result["s37_monetary_value"][0] is None
        assert result["s37_benefit_text"][0] is None

    def test_benefit_text_concatenated(self, tmp_path: Path) -> None:
        """Given: Dev application matches multiple Section 37 records.
        When: _add_section37_features called.
        Then: s37_benefit_text contains all benefit types joined."""
        ref = tmp_path / "reference"
        ref.mkdir()
        _write_section37(ref / "section37.parquet")

        df = pl.DataFrame(
            {"address": ["100 King St W"], "folderrsn": ["F001"]}
        )
        result = _add_section37_features(df, tmp_path)
        benefit_text = result["s37_benefit_text"][0]
        assert benefit_text is not None
        assert "Cash contribution" in benefit_text
        assert "Affordable Housing" in benefit_text

    def test_missing_section37_file_returns_null_columns(self, tmp_path: Path) -> None:
        """Given: section37.parquet does not exist.
        When: _add_section37_features called.
        Then: Both S.37 columns are null for all rows."""
        df = pl.DataFrame(
            {"address": ["100 King St W"], "folderrsn": ["F001"]}
        )
        result = _add_section37_features(df, tmp_path)
        assert "s37_monetary_value" in result.columns
        assert "s37_benefit_text" in result.columns
        assert result["s37_monetary_value"][0] is None
        assert result["s37_benefit_text"][0] is None

    def test_missing_address_column_returns_null_columns(self, tmp_path: Path) -> None:
        """Given: Dev DataFrame has no address column.
        When: _add_section37_features called.
        Then: S.37 columns are null for all rows."""
        ref = tmp_path / "reference"
        ref.mkdir()
        _write_section37(ref / "section37.parquet")

        df = pl.DataFrame({"folderrsn": ["F001", "F002"]})
        result = _add_section37_features(df, tmp_path)
        assert result["s37_monetary_value"].is_null().all()
        assert result["s37_benefit_text"].is_null().all()

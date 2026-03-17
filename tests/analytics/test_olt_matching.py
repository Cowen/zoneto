"""Tests for OLT-to-dev_applications fuzzy address matching."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import match_olt_to_dev


@pytest.fixture
def olt_parquet(tmp_path: Path) -> Path:
    """Minimal olt_decisions.parquet with known cases."""
    df = pl.DataFrame(
        {
            "case_number": ["OLT-22-001", "OLT-22-002", "OLT-23-003"],
            "outcome": ["Dismissed", "Allowed", "Dismissed"],
            "decision_date": ["2022-11-30", "2023-02-14", "2023-06-01"],
            "address": [
                "100 King St W, Toronto",
                "200 Queen St W, Toronto",
                "999 Remote Ave, Toronto",
            ],
        }
    ).cast({"decision_date": pl.Date})
    path = tmp_path / "reference" / "olt_decisions.parquet"
    path.parent.mkdir(parents=True)
    df.write_parquet(path)
    return path


@pytest.fixture
def dev_parquet(tmp_path: Path) -> Path:
    """Minimal dev_applications parquet with addresses matching OLT cases."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003"],
            "street_num": ["100", "200", "300"],
            "street_name": ["King St W", "Queen St W", "Bay St"],
            "year_submitted": pl.Series([2021, 2022, 2021], dtype=pl.Int32),
            "application_type": ["OZ", "OZ", "SA"],
        }
    )
    path = tmp_path / "enriched" / "dev_applications_staging.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def test_match_olt_high_confidence_case(olt_parquet: Path, dev_parquet: Path) -> None:
    """F001 address '100 King St W' matches OLT case '100 King St W, Toronto'."""
    dev_df = pl.read_parquet(dev_parquet)
    result = match_olt_to_dev(dev_df, olt_parquet.parent.parent)

    f001 = result.filter(pl.col("folderrsn") == "F001")
    assert f001["olt_case_number"][0] == "OLT-22-001"
    assert f001["olt_outcome"][0] == "Dismissed"


def test_match_olt_no_match_returns_null(olt_parquet: Path, dev_parquet: Path) -> None:
    """F003 '300 Bay St' has no close OLT match — OLT columns are null."""
    dev_df = pl.read_parquet(dev_parquet)
    result = match_olt_to_dev(dev_df, olt_parquet.parent.parent)

    f003 = result.filter(pl.col("folderrsn") == "F003")
    assert f003["olt_case_number"][0] is None


def test_match_olt_columns_present_when_no_olt_data(tmp_path: Path) -> None:
    """When olt_decisions.parquet is absent, columns are added as all-null."""
    dev_df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "street_num": ["100"],
            "street_name": ["King St W"],
            "year_submitted": pl.Series([2021], dtype=pl.Int32),
        }
    )
    result = match_olt_to_dev(dev_df, tmp_path)

    assert "olt_case_number" in result.columns
    assert "olt_outcome" in result.columns
    assert "olt_decision_date" in result.columns
    assert result["olt_case_number"][0] is None


def test_match_olt_confidence_threshold_filters_weak_matches(
    tmp_path: Path,
) -> None:
    """Addresses with similarity below threshold produce null OLT columns."""
    olt_df = pl.DataFrame(
        {
            "case_number": ["OLT-22-999"],
            "outcome": ["Allowed"],
            "decision_date": ["2022-11-30"],
            "address": ["9999 Completely Different Rd, Toronto"],
        }
    ).cast({"decision_date": pl.Date})
    olt_path = tmp_path / "reference" / "olt_decisions.parquet"
    olt_path.parent.mkdir(parents=True)
    olt_df.write_parquet(olt_path)

    dev_df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "street_num": ["100"],
            "street_name": ["King St W"],
            "year_submitted": pl.Series([2021], dtype=pl.Int32),
        }
    )
    result = match_olt_to_dev(dev_df, tmp_path)
    assert result["olt_case_number"][0] is None


def test_match_olt_uses_street_number_index_for_performance(
    tmp_path: Path,
) -> None:
    """Matching is accelerated via street-number indexing."""
    # Create OLT data with many addresses, most with street number 999
    olt_rows = [
        {
            "case_number": f"OLT-22-{i:03d}",
            "outcome": "Dismissed",
            "decision_date": "2022-11-30",
            "address": f"999 Oak Ave {i}, Toronto",
        }
        for i in range(100)
    ]
    # Add a few with street number 100
    olt_rows.extend(
        [
            {
                "case_number": "OLT-22-100A",
                "outcome": "Allowed",
                "decision_date": "2023-02-14",
                "address": "100 King St W, Toronto",
            }
        ]
    )

    olt_df = pl.DataFrame(olt_rows).cast({"decision_date": pl.Date})
    olt_path = tmp_path / "reference" / "olt_decisions.parquet"
    olt_path.parent.mkdir(parents=True)
    olt_df.write_parquet(olt_path)

    # Create a dev application that should match the 100 King St OLT case
    dev_df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "street_num": ["100"],
            "street_name": ["King St W"],
            "year_submitted": pl.Series([2021], dtype=pl.Int32),
        }
    )

    result = match_olt_to_dev(dev_df, tmp_path)
    # Should match the 100 King St case, not any 999 Oak Ave case
    assert result["olt_case_number"][0] == "OLT-22-100A"
    assert result["olt_outcome"][0] == "Allowed"

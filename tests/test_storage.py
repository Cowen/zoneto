from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from zoneto.storage import last_modified, source_row_counts, write_source


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "year": pl.Series([2023, 2023, 2024], dtype=pl.Int32),
            "permit_no": ["A001", "A002", "B001"],
            "source_name": ["test", "test", "test"],
        }
    )


def test_write_creates_hive_directory_structure(
    tmp_path: Path, sample_df: pl.DataFrame
) -> None:
    """write_source creates year=YYYY subdirectories."""
    write_source(sample_df, "permits_active", tmp_path)

    assert (tmp_path / "permits_active" / "year=2023").exists()
    assert (tmp_path / "permits_active" / "year=2024").exists()


def test_write_returns_row_count(tmp_path: Path, sample_df: pl.DataFrame) -> None:
    """write_source returns the number of rows written."""
    count = write_source(sample_df, "permits_active", tmp_path)
    assert count == 3


def test_second_write_removes_stale_partitions(tmp_path: Path) -> None:
    """A second write overwrites the previous data with no leftover partitions."""
    df1 = pl.DataFrame(
        {
            "year": pl.Series([2023], dtype=pl.Int32),
            "permit_no": ["A001"],
            "source_name": ["test"],
        }
    )
    df2 = pl.DataFrame(
        {
            "year": pl.Series([2024], dtype=pl.Int32),
            "permit_no": ["B001"],
            "source_name": ["test"],
        }
    )

    write_source(df1, "permits_active", tmp_path)
    write_source(df2, "permits_active", tmp_path)

    assert not (tmp_path / "permits_active" / "year=2023").exists()
    assert (tmp_path / "permits_active" / "year=2024").exists()


def test_source_row_counts_returns_total(
    tmp_path: Path, sample_df: pl.DataFrame
) -> None:
    """source_row_counts returns the total rows across all partitions."""
    write_source(sample_df, "permits_active", tmp_path)
    assert source_row_counts("permits_active", tmp_path) == 3


def test_source_row_counts_missing_returns_none(tmp_path: Path) -> None:
    """source_row_counts returns None when the source directory does not exist."""
    assert source_row_counts("nonexistent", tmp_path) is None


def test_last_modified_returns_datetime(
    tmp_path: Path, sample_df: pl.DataFrame
) -> None:
    """last_modified returns a datetime when data exists."""
    write_source(sample_df, "permits_active", tmp_path)
    result = last_modified("permits_active", tmp_path)
    assert isinstance(result, datetime)


def test_last_modified_missing_returns_none(tmp_path: Path) -> None:
    """last_modified returns None when the source directory does not exist."""
    assert last_modified("nonexistent", tmp_path) is None

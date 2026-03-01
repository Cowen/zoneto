from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from typing import cast

import polars as pl


def write_source(df: pl.DataFrame, name: str, data_dir: Path) -> int:
    """Write DataFrame to Hive-partitioned Parquet, replacing any previous data.

    Deletes the source subdirectory first so no stale year-partitions remain.
    Returns the number of rows written.
    """
    source_dir = data_dir / name
    if source_dir.exists():
        shutil.rmtree(source_dir)
    # use_pyarrow omitted: polars 1.38+ native engine creates correct year=YYYY/ Hive
    # directories; use_pyarrow=True in that version creates a single flat file instead.
    df.write_parquet(source_dir, partition_by=["year"])
    return len(df)


def source_row_counts(name: str, data_dir: Path) -> int | None:
    """Return the total row count for a source, or None if no data exists."""
    source_dir = data_dir / name
    if not source_dir.exists():
        return None
    df = cast(
        pl.DataFrame,
        pl.scan_parquet(str(source_dir / "**/*.parquet")).select(pl.len()).collect(),
    )
    return df.item()


def last_modified(name: str, data_dir: Path) -> datetime | None:
    """Return the most recent mtime across all Parquet files for a source.

    Returns None if the source directory does not exist or contains no files.
    """
    source_dir = data_dir / name
    if not source_dir.exists():
        return None
    parquet_files = list(source_dir.rglob("*.parquet"))
    if not parquet_files:
        return None
    return datetime.fromtimestamp(max(f.stat().st_mtime for f in parquet_files))

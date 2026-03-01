# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Implement the storage layer — write normalized DataFrames to Hive-partitioned Parquet and read back row counts and modification times.

**Architecture:** Three pure functions in `src/zoneto/storage.py`. `write_source` deletes the source directory first (ensuring no stale partitions) then writes partitioned Parquet. `source_row_counts` uses lazy scanning for efficiency. `last_modified` walks partition files for the most recent mtime.

**Tech Stack:** Python 3.13, polars (with pyarrow backend for partitioned writes), shutil (stdlib), pytest (with tmp_path fixture)

**Scope:** Phase 6 of 7 (storage layer)

**Codebase verified:** 2026-02-28 — `src/zoneto/storage.py` and `tests/test_storage.py` do not exist yet.

---

### Task 1: Storage functions

**Files:**
- Create: `src/zoneto/storage.py`

**Step 1: Create `src/zoneto/storage.py`**

```python
from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import polars as pl


def write_source(df: pl.DataFrame, name: str, data_dir: Path) -> int:
    """Write DataFrame to Hive-partitioned Parquet, replacing any previous data.

    Deletes the source subdirectory first so no stale year-partitions remain.
    Returns the number of rows written.
    """
    source_dir = data_dir / name
    if source_dir.exists():
        shutil.rmtree(source_dir)
    df.write_parquet(source_dir, partition_by=["year"], use_pyarrow=True)
    return len(df)


def source_row_counts(name: str, data_dir: Path) -> int | None:
    """Return the total row count for a source, or None if no data exists."""
    source_dir = data_dir / name
    if not source_dir.exists():
        return None
    return pl.scan_parquet(str(source_dir / "**/*.parquet")).select(pl.len()).collect()[0, 0]


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
```

**Step 2: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

---

### Task 2: Storage tests

**Files:**
- Create: `tests/test_storage.py`

**Step 1: Write `tests/test_storage.py`**

```python
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from zoneto.storage import last_modified, source_row_counts, write_source


@pytest.fixture
def sample_df() -> pl.DataFrame:
    return pl.DataFrame({
        "year": pl.Series([2023, 2023, 2024], dtype=pl.Int32),
        "permit_no": ["A001", "A002", "B001"],
        "source_name": ["test", "test", "test"],
    })


def test_write_creates_hive_directory_structure(tmp_path: Path, sample_df: pl.DataFrame) -> None:
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
    df1 = pl.DataFrame({
        "year": pl.Series([2023], dtype=pl.Int32),
        "permit_no": ["A001"],
        "source_name": ["test"],
    })
    df2 = pl.DataFrame({
        "year": pl.Series([2024], dtype=pl.Int32),
        "permit_no": ["B001"],
        "source_name": ["test"],
    })

    write_source(df1, "permits_active", tmp_path)
    write_source(df2, "permits_active", tmp_path)

    assert not (tmp_path / "permits_active" / "year=2023").exists()
    assert (tmp_path / "permits_active" / "year=2024").exists()


def test_source_row_counts_returns_total(tmp_path: Path, sample_df: pl.DataFrame) -> None:
    """source_row_counts returns the total rows across all partitions."""
    write_source(sample_df, "permits_active", tmp_path)
    assert source_row_counts("permits_active", tmp_path) == 3


def test_source_row_counts_missing_returns_none(tmp_path: Path) -> None:
    """source_row_counts returns None when the source directory does not exist."""
    assert source_row_counts("nonexistent", tmp_path) is None


def test_last_modified_returns_datetime(tmp_path: Path, sample_df: pl.DataFrame) -> None:
    """last_modified returns a datetime when data exists."""
    write_source(sample_df, "permits_active", tmp_path)
    result = last_modified("permits_active", tmp_path)
    assert isinstance(result, datetime)


def test_last_modified_missing_returns_none(tmp_path: Path) -> None:
    """last_modified returns None when the source directory does not exist."""
    assert last_modified("nonexistent", tmp_path) is None
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_storage.py -v
```

Expected: All 7 tests pass.

**Step 3: Run full suite**

```bash
uv run pytest -v
```

Expected: All tests pass.

**Step 4: Commit**

```bash
git add src/zoneto/storage.py tests/test_storage.py
git commit -m "feat: add Hive-partitioned Parquet storage layer"
```

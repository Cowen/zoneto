# Toronto Building Data Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Wire the `sync` and `status` CLI commands to the source registry and storage layer with Rich-colored output.

**Architecture:** Rewrite `src/zoneto/cli.py` to import from `SOURCES` and call `write_source`. `sync` iterates all sources (or one if `--source` given), fetches, writes, and prints per-source results with Rich. Exceptions on one source are caught and printed without aborting the others. `status` prints a Rich table with row count and last-modified time per source.

**Tech Stack:** Python 3.13, typer, rich, polars, pytest

**Scope:** Phase 7 of 7 (CLI integration)

**Codebase verified:** 2026-02-28 — `src/zoneto/cli.py` exists from Phase 1 (stubbed); all other dependencies (SOURCES, storage functions) exist from prior phases.

---

### Task 1: Rewrite cli.py

**Files:**
- Modify: `src/zoneto/cli.py` (full rewrite)

**Step 1: Replace the entire contents of `src/zoneto/cli.py`**

```python
from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from zoneto.sources.registry import SOURCES
from zoneto.storage import last_modified, source_row_counts, write_source

app = typer.Typer(help="Toronto building application data pipeline.")
console = Console()

DATA_DIR = Path("data")


@app.command()
def sync(
    source: Annotated[
        str | None,
        typer.Option(help="Source name to sync (default: all). One of: permits_active, permits_cleared, coa."),
    ] = None,
) -> None:
    """Fetch data from sources and write to Parquet."""
    if source is not None and source not in SOURCES:
        console.print(f"[red]Unknown source: {source!r}[/red]")
        console.print(f"Available sources: {', '.join(SOURCES)}")
        raise typer.Exit(code=1)

    sources_to_sync = {source: SOURCES[source]} if source is not None else dict(SOURCES)

    for name, src in sources_to_sync.items():
        console.print(f"[bold]Syncing {name}...[/bold]")
        try:
            df = src.fetch()
            count = write_source(df, name, DATA_DIR)
            console.print(f"  [green]✓[/green] {count:,} rows written")
        except Exception as exc:
            console.print(f"  [red]✗ {exc}[/red]")


@app.command()
def status() -> None:
    """Print last sync time and row counts per source."""
    table = Table(title="Zoneto Source Status")
    table.add_column("Source", style="bold")
    table.add_column("Rows", justify="right")
    table.add_column("Last Modified")

    for name in SOURCES:
        count = source_row_counts(name, DATA_DIR)
        modified = last_modified(name, DATA_DIR)

        rows_str = f"{count:,}" if count is not None else "[dim]no data[/dim]"
        modified_str = modified.strftime("%Y-%m-%d %H:%M") if modified is not None else "[dim]no data[/dim]"

        table.add_row(name, rows_str, modified_str)

    console.print(table)
```

**Step 2: Verify ty passes**

```bash
uv run ty check src/
```

Expected: No errors.

**Step 3: Run full test suite**

```bash
uv run pytest -v
```

Expected: All tests pass (test_models, test_ckan_datastore, test_ckan_bulk_csv, test_registry, test_storage).

---

### Task 2: CLI unit tests

**Files:**
- Create: `tests/test_cli.py`

**Step 1: Write `tests/test_cli.py`**

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest
from typer.testing import CliRunner

from zoneto.cli import app

runner = CliRunner()


def test_sync_unknown_source_exits_with_code_1() -> None:
    """Providing an unknown --source name exits with code 1."""
    result = runner.invoke(app, ["sync", "--source", "nonexistent"])
    assert result.exit_code == 1
    assert "Unknown source" in result.output


def test_status_shows_no_data_before_any_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """status prints a table with 'no data' for all sources before sync is run."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "no data" in result.output


def test_sync_writes_data_to_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """sync calls fetch, writes Parquet data, and exits 0."""
    fake_df = pl.DataFrame({
        "year": pl.Series([2024], dtype=pl.Int32),
        "permit_no": ["A001"],
        "source_name": ["fake"],
    })
    mock_source = MagicMock()
    mock_source.fetch.return_value = fake_df

    monkeypatch.setattr("zoneto.cli.SOURCES", {"fake": mock_source})
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)

    result = runner.invoke(app, ["sync"])
    assert result.exit_code == 0
    assert (tmp_path / "fake").exists()


def test_sync_continues_after_source_exception(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An exception from one source is printed but other sources still run."""
    good_df = pl.DataFrame({
        "year": pl.Series([2024], dtype=pl.Int32),
        "permit_no": ["A001"],
        "source_name": ["good"],
    })
    bad_source = MagicMock()
    bad_source.fetch.side_effect = RuntimeError("network failure")
    good_source = MagicMock()
    good_source.fetch.return_value = good_df

    monkeypatch.setattr("zoneto.cli.SOURCES", {"bad": bad_source, "good": good_source})
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)

    result = runner.invoke(app, ["sync"])
    assert result.exit_code == 0          # does not abort on error
    assert good_source.fetch.called       # good source was still attempted
```

**Step 2: Run tests**

```bash
uv run pytest tests/test_cli.py -v
```

Expected: All 4 tests pass.

**Step 3: Run full test suite**

```bash
uv run pytest -v
```

Expected: All tests pass (test_models, test_ckan_datastore, test_ckan_bulk_csv, test_registry, test_storage, test_cli).

**Step 4: Verify status command works before any data exists**

```bash
uv run zoneto status
```

Expected: A table is printed with all three sources showing "no data" for rows and last modified.

**Step 5: Smoke test against live CKAN API**

This step makes a real network request. It is a manual verification step, not part of the pytest suite.

```bash
uv run zoneto sync --source permits_active
```

Expected:
- Prints "Syncing permits_active..."
- Downloads all pages from the CKAN datastore_search endpoint (may take 10–60 seconds)
- Prints "✓ X,XXX rows written" (exact count varies)
- Creates `data/permits_active/year=YYYY/` directories

```bash
uv run zoneto status
```

Expected: Table shows a non-zero row count and a recent timestamp for `permits_active`.

**Step 6: Commit**

```bash
git add src/zoneto/cli.py tests/test_cli.py
git commit -m "feat: wire sync and status commands to registry and storage"
```

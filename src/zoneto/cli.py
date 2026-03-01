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
        typer.Option(
            help=(
                "Source name to sync (default: all)."
                " One of: permits_active, permits_cleared, coa."
            )
        ),
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
        modified_str = (
            modified.strftime("%Y-%m-%d %H:%M")
            if modified is not None
            else "[dim]no data[/dim]"
        )

        table.add_row(name, rows_str, modified_str)

    console.print(table)

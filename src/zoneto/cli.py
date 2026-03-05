from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from zoneto.analytics.enrich import enrich_coa, enrich_dev, fetch_reference
from zoneto.analytics.train import train_all
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


@app.command()
def enrich(
    fetch_ref: Annotated[
        bool,
        typer.Option(
            "--fetch-ref/--no-fetch-ref",
            help="Download reference datasets first.",
        ),
    ] = True,
) -> None:
    """Enrich raw Parquet with spatial features and outcome labels."""
    if fetch_ref:
        console.print("[bold]Fetching reference datasets...[/bold]")
        fetch_reference(DATA_DIR)
        console.print("  [green]✓[/green] Reference data ready")

    for label, fn in [("COA", enrich_coa), ("Dev applications", enrich_dev)]:
        console.print(f"[bold]Enriching {label}...[/bold]")
        try:
            count = fn(DATA_DIR)
            console.print(f"  [green]✓[/green] {count:,} rows written")
        except Exception as exc:
            console.print(f"  [red]✗ {exc}[/red]")


@app.command()
def train(
    model_dir: Annotated[
        Path,
        typer.Option(help="Directory to write .joblib model files."),
    ] = Path("models"),
) -> None:
    """Train all outcome-prediction models from enriched Parquet."""
    console.print("[bold]Training models...[/bold]")
    try:
        results = train_all(data_dir=DATA_DIR, model_dir=model_dir)
        for name, count in results.items():
            console.print(f"  [green]✓[/green] {name}: {count:,} training rows")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)

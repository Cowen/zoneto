from __future__ import annotations

from typing import Annotated

import typer

app = typer.Typer(help="Toronto building application data pipeline.")


@app.command()
def sync(
    source: Annotated[
        str | None,
        typer.Option(help="Source name to sync (default: all)"),
    ] = None,
) -> None:
    """Fetch data from sources and write to Parquet."""
    typer.echo("sync: not yet implemented")


@app.command()
def status() -> None:
    """Print last sync time and row counts per source."""
    typer.echo("status: not yet implemented")

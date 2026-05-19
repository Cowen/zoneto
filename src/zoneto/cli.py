from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from zoneto.analytics.bylaw_index import BylawIndex
from zoneto.analytics.enrich import (
    enrich_coa,
    enrich_dev,
    enrich_permits,
    fetch_reference,
)
from zoneto.analytics.importance import feature_importance
from zoneto.analytics.score import score_all
from zoneto.analytics.train import train_all
from zoneto.sources.aic import fetch_aic_decisions_arcgis
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
def aic(
    delay: Annotated[
        float,
        typer.Option(help="Seconds to sleep between AIC requests."),
    ] = 1.0,
    full: Annotated[
        bool,
        typer.Option(
            "--full/--no-full",
            help="Fetch full application records from ArcGIS (replaces CKAN).",
        ),
    ] = False,
) -> None:
    """Scrape AIC portal for OZ/SA decision milestone dates.

    With --full: also fetches complete application records from ArcGIS,
    writing to data/aic_applications/. This provides a live replacement
    for the retired CKAN dev_applications dataset.
    """
    logging.basicConfig(format="%(message)s", level=logging.INFO)
    console.print("[bold]Scraping AIC portal...[/bold]")
    try:
        n = fetch_aic_decisions_arcgis(DATA_DIR)
        console.print(f"[green]✓[/green] AIC decisions: {n} new rows")

        if full:
            from zoneto.sources.aic import fetch_aic_applications  # noqa: PLC0415

            console.print("[bold]Fetching full AIC application records...[/bold]")
            n_full = fetch_aic_applications(DATA_DIR)
            console.print(f"[green]✓[/green] AIC applications: {n_full} rows written")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)


@app.command()
def olt(
    delay: Annotated[
        float,
        typer.Option(
            help=(
                "Delay between OLT page requests (seconds). "
                "Respect the government site."
            )
        ),
    ] = 2.0,
) -> None:
    """Scrape Ontario Land Tribunal decisions for Toronto applications.

    Writes data/reference/olt_decisions.parquet. Use 'zoneto enrich --fetch-olt'
    to join OLT decisions to dev_applications after scraping.
    """
    from zoneto.sources.olt import fetch_olt_decisions  # noqa: PLC0415

    console.print("[bold]Scraping OLT decisions...[/bold]")
    try:
        n = fetch_olt_decisions(DATA_DIR, delay=delay)
        console.print(f"[green]✓[/green] OLT decisions: {n} records written")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)


@app.command()
def bylaw_index(
    bylaw_dir: Annotated[
        Path,
        typer.Option(help="Directory containing bylaw .txt files."),
    ] = Path("docs/bylaw"),
    index_dir: Annotated[
        Path,
        typer.Option(help="Directory to write chunks.parquet and embeddings.npy."),
    ] = Path("data/bylaw_index"),
) -> None:
    """Build or rebuild the by-law section embedding index."""
    bylaw_dir = Path(bylaw_dir)
    index_dir = Path(index_dir)

    if not bylaw_dir.exists():
        console.print(f"[red]Bylaw directory not found: {bylaw_dir}[/red]")
        raise typer.Exit(code=1)

    bylaw_files = list(bylaw_dir.glob("*.txt"))
    if not bylaw_files:
        console.print(f"[red]No .txt files found in {bylaw_dir}[/red]")
        raise typer.Exit(code=1)

    console.print(f"[bold]Building bylaw index from {len(bylaw_files)} files...[/bold]")
    try:
        index = BylawIndex.build(bylaw_dir, index_dir, progress=console.print)

        # Load and report statistics
        import polars as pl  # noqa: PLC0415

        chunks_df = pl.read_parquet(index_dir / "chunks.parquet")
        n_chunks = len(chunks_df)

        console.print("[green]✓[/green] Index built successfully")
        console.print(f"  Chunks: {n_chunks:,}")
        console.print(f"  Embeddings: {index.embeddings.shape}")
        console.print("  Files: chunks.parquet, embeddings.npy")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)


@app.command()
def enrich(
    fetch_ref: Annotated[
        bool,
        typer.Option(
            "--fetch-ref/--no-fetch-ref",
            help="Download reference datasets first.",
        ),
    ] = True,
    fetch_aic: Annotated[
        bool,
        typer.Option(
            "--fetch-aic/--no-fetch-aic",
            help="Scrape AIC portal for decision dates before enriching.",
        ),
    ] = True,
    fetch_olt: Annotated[
        bool,
        typer.Option(
            "--fetch-olt/--no-fetch-olt",
            help=(
                "Match OLT decisions to dev_applications "
                "(requires prior 'zoneto olt' run)."
            ),
        ),
    ] = False,
) -> None:
    """Enrich raw Parquet with spatial features and outcome labels."""
    if fetch_ref:
        console.print("[bold]Fetching reference datasets...[/bold]")
        fetch_reference(DATA_DIR)
        console.print("  [green]✓[/green] Reference data ready")

    if fetch_aic:
        console.print("[bold]Scraping AIC decision dates...[/bold]")
        try:
            count = fetch_aic_decisions_arcgis(DATA_DIR)
            console.print(f"  [green]✓[/green] {count:,} applications scraped")
        except Exception as exc:
            console.print(f"  [red]✗ AIC scrape failed: {exc}[/red]")
            # Non-fatal: enrich can proceed without AIC data

    for label, fn in [
        ("COA", enrich_coa),
        ("Dev applications", enrich_dev),
        ("Permits", enrich_permits),
    ]:
        console.print(f"[bold]Enriching {label}...[/bold]")
        try:
            count = fn(DATA_DIR)
            console.print(f"  [green]✓[/green] {count:,} rows written")
        except Exception as exc:
            console.print(f"  [red]✗ {exc}[/red]")

    if fetch_olt:
        import polars as pl  # noqa: PLC0415

        from zoneto.analytics.enrich import match_olt_to_dev  # noqa: PLC0415

        enriched_dev_path = DATA_DIR / "enriched" / "dev_applications.parquet"
        if enriched_dev_path.exists():
            dev_df = pl.read_parquet(enriched_dev_path)
            dev_df = match_olt_to_dev(dev_df, DATA_DIR)
            dev_df.write_parquet(enriched_dev_path)
            console.print("[green]✓[/green] OLT decisions matched to dev_applications")
        else:
            console.print(
                "[yellow]⚠[/yellow] enriched dev_applications not found — "
                "run enrich first"
            )


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
        counts, metrics = train_all(data_dir=DATA_DIR, model_dir=model_dir)

        # Build and display metrics table
        table = Table(title="Model Training Results")
        table.add_column("Model", style="bold")
        table.add_column("N rows", justify="right")
        table.add_column("Primary metric", justify="right")
        table.add_column("Secondary metric", justify="right")
        table.add_column("Status", justify="center")

        _TRACKING_ONLY = {"coa_days_to_approval"}

        for name, count in counts.items():
            metric = metrics[name]
            if "roc_auc_mean" in metric:
                primary = (
                    f"AUC {metric['roc_auc_mean']:.3f}±{metric['roc_auc_std']:.3f}"
                )
                secondary = f"Brier {metric['brier_score_mean']:.3f}"
            elif "concordance_index_mean" in metric:
                primary = (
                    f"C-index {metric['concordance_index_mean']:.3f}"
                    f"±{metric['concordance_index_std']:.3f}"
                )
                secondary = ""
            else:
                primary = f"R² {metric['r2_mean']:.3f}±{metric['r2_std']:.3f}"
                secondary = f"MAE {metric['mae_mean']:.0f}d"

            if name in _TRACKING_ONLY:
                status = "[yellow]tracking only[/yellow]"
            elif metric.get("production_ready"):
                status = "[green]production[/green]"
            else:
                status = "[red]not ready[/red]"

            table.add_row(name, f"{count:,}", primary, secondary, status)

        console.print(table)
        console.print(f"[green]✓[/green] Metrics saved to {model_dir / 'metrics.json'}")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)


@app.command()
def importance(
    model: Annotated[
        str,
        typer.Argument(
            help=(
                "Model name. One of: dev_applications_approved,"
                " dev_applications_appealed, coa_approved, coa_days_to_approval."
            )
        ),
    ],
    model_dir: Annotated[
        Path,
        typer.Option(help="Directory containing .joblib model files."),
    ] = Path("models"),
    builtin: Annotated[
        bool,
        typer.Option(
            "--builtin/--permutation",
            help="Use built-in impurity importance (fast) instead of permutation.",
        ),
    ] = False,
) -> None:
    """Rank input features by their contribution to a model's predictions."""
    try:
        df = feature_importance(
            model,
            data_dir=DATA_DIR,
            model_dir=model_dir,
            builtin=builtin,
        )
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(code=1)

    method = "built-in impurity" if builtin else "permutation (roc_auc/r2)"
    console.print(f"[bold]Feature importance — {model}[/bold] ({method})\n")

    table = Table()
    table.add_column("Rank", justify="right", style="dim")
    table.add_column("Feature", style="bold")
    table.add_column("Importance", justify="right")
    table.add_column("± Std", justify="right", style="dim")

    for i, row in enumerate(df.iter_rows(named=True), 1):
        table.add_row(
            str(i),
            row["feature"],
            f"{row['importance_mean']:.4f}",
            f"{row['importance_std']:.4f}",
        )

    console.print(table)


@app.command()
def summary() -> None:
    """Print percentile distributions for all scored prediction columns."""
    scores_dir = DATA_DIR / "scores"
    if not scores_dir.exists():
        console.print("[dim]No scored data found.[/dim]")
        return

    import polars as pl

    parquet_files = sorted(scores_dir.glob("*.parquet"))
    if not parquet_files:
        console.print("[dim]No scored data found.[/dim]")
        return

    percentiles = [0.05, 0.25, 0.50, 0.75, 0.95]
    pct_labels = ["p5", "p25", "p50", "p75", "p95"]

    for path in parquet_files:
        source = path.stem
        df = pl.read_parquet(path)
        score_cols = sorted(
            c for c in df.columns if c.startswith("pred_") or c.startswith("prob_")
        )
        if not score_cols:
            continue

        table = Table(title=f"{source} ({len(df):,} rows)")
        table.add_column("Column", style="bold")
        table.add_column("Mean", justify="right")
        for label in pct_labels:
            table.add_column(label, justify="right")

        for col in score_cols:
            series = df[col].drop_nulls()
            if series.len() == 0:
                continue
            mean = series.mean()
            pcts = [series.quantile(p) for p in percentiles]
            fmt = ".3f" if (mean is not None and abs(mean) < 10) else ".0f"  # type: ignore[arg-type]
            table.add_row(
                col,
                f"{mean:{fmt}}",
                *[f"{v:{fmt}}" for v in pcts],
            )

        console.print(table)
        console.print()


@app.command()
def score(
    model_dir: Annotated[
        Path,
        typer.Option(help="Directory containing .joblib model files."),
    ] = Path("models"),
) -> None:
    """Run batch inference on enriched Parquet; write data/scores/."""
    console.print("[bold]Scoring...[/bold]")
    try:
        score_all(data_dir=DATA_DIR, model_dir=model_dir)
        console.print("  [green]✓[/green] Scores written to data/scores/")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)


@app.command()
def serve(
    port: Annotated[
        int,
        typer.Option(help="Port to listen on."),
    ] = 8000,
    host: Annotated[
        str,
        typer.Option(help="Host to bind to."),
    ] = "0.0.0.0",
    data_dir: Annotated[
        Path,
        typer.Option(help="Data directory."),
    ] = DATA_DIR,
    model_dir: Annotated[
        Path,
        typer.Option(help="Model directory."),
    ] = Path("models"),
    static_dir: Annotated[
        Path,
        typer.Option(help="Static files directory."),
    ] = Path("static"),
    reload: Annotated[
        bool,
        typer.Option(help="Auto-reload on code changes (development only)."),
    ] = False,
) -> None:
    """Start the FastAPI serving layer."""
    import uvicorn

    from zoneto.api.app import create_app

    application = create_app(
        data_dir=data_dir, model_dir=model_dir, static_dir=static_dir
    )
    uvicorn.run(application, host=host, port=port, reload=reload)

# Phase 5: CLI Integration and Documentation

**Design phase:** Phase 6

**Goal:** Add `zoneto aic` CLI command, `--fetch-aic/--no-fetch-aic` flag on `zoneto enrich`, update justfile and CLAUDE.md, and add CLI tests.

---

### Task 1: Write failing CLI tests

**Files:**
- Modify: `tests/test_cli.py`

**Step 1: Add CLI tests for AIC command and enrich flag**

Add at the end of `tests/test_cli.py`:

```python
def test_aic_command_calls_fetch_aic_decisions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto aic calls fetch_aic_decisions and exits 0."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch = MagicMock(return_value=42)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions", mock_fetch)

    result = runner.invoke(app, ["aic"])

    assert result.exit_code == 0
    assert mock_fetch.called
    assert "42" in result.output


def test_enrich_no_fetch_aic_skips_aic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto enrich --no-fetch-aic skips AIC fetch."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch_aic = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions", mock_fetch_aic)
    mock_enrich_dev = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_dev", mock_enrich_dev)
    mock_enrich_coa = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_coa", mock_enrich_coa)
    mock_enrich_permits = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_permits", mock_enrich_permits)
    monkeypatch.setattr("zoneto.cli.fetch_reference", MagicMock(return_value=None))

    result = runner.invoke(app, ["enrich", "--no-fetch-aic"])

    assert result.exit_code == 0
    assert not mock_fetch_aic.called


def test_enrich_fetch_aic_default_calls_aic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zoneto enrich (default) calls fetch_aic_decisions."""
    monkeypatch.setattr("zoneto.cli.DATA_DIR", tmp_path)
    mock_fetch_aic = MagicMock(return_value=5)
    monkeypatch.setattr("zoneto.cli.fetch_aic_decisions", mock_fetch_aic)
    mock_enrich_dev = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_dev", mock_enrich_dev)
    mock_enrich_coa = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_coa", mock_enrich_coa)
    mock_enrich_permits = MagicMock(return_value=0)
    monkeypatch.setattr("zoneto.cli.enrich_permits", mock_enrich_permits)
    monkeypatch.setattr("zoneto.cli.fetch_reference", MagicMock(return_value=None))

    result = runner.invoke(app, ["enrich"])

    assert result.exit_code == 0
    assert mock_fetch_aic.called
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_cli.py -k "aic" -v
```

Expected: failures — `zoneto aic` command doesn't exist yet; `--fetch-aic` flag doesn't exist.

---

### Task 2: Implement `zoneto aic` command and `--fetch-aic` flag

**Files:**
- Modify: `src/zoneto/cli.py`

**Step 1: Add `fetch_aic_decisions` import**

In `src/zoneto/cli.py`, update the import from `analytics/enrich`:

```python
from zoneto.analytics.enrich import (
    enrich_coa,
    enrich_dev,
    enrich_permits,
    fetch_reference,
)
from zoneto.sources.aic import fetch_aic_decisions
```

**Step 2: Add `aic` command**

Add after the `status` command (around line 80), before the `enrich` command:

```python
@app.command()
def aic(
    delay: Annotated[
        float,
        typer.Option(help="Seconds to sleep between AIC requests."),
    ] = 1.0,
) -> None:
    """Scrape AIC portal for OZ/SA decision milestone dates."""
    console.print("[bold]Scraping AIC decision dates...[/bold]")
    try:
        count = fetch_aic_decisions(DATA_DIR, delay=delay)
        console.print(f"  [green]✓[/green] {count:,} applications scraped")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)
```

**Step 3: Add `--fetch-aic/--no-fetch-aic` flag to `enrich` command**

Find the `enrich` command definition (line ~82). Update it:

```python
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
) -> None:
    """Enrich raw Parquet with spatial features and outcome labels."""
    if fetch_ref:
        console.print("[bold]Fetching reference datasets...[/bold]")
        fetch_reference(DATA_DIR)
        console.print("  [green]✓[/green] Reference data ready")

    if fetch_aic:
        console.print("[bold]Scraping AIC decision dates...[/bold]")
        try:
            count = fetch_aic_decisions(DATA_DIR)
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
```

**Step 4: Fix `train` command metrics display for survival model**

The existing `train` CLI command's metrics table (lines ~132-139 of `cli.py`) has two branches: `roc_auc_mean` for classifiers and `r2_mean`/`mae_mean` for regressors. The survival model returns `concordance_index_mean` — neither branch handles it, causing a `KeyError`.

Find this block in the `train` command:

```python
            if "roc_auc_mean" in metric:
                primary = (
                    f"AUC {metric['roc_auc_mean']:.3f}±{metric['roc_auc_std']:.3f}"
                )
                secondary = f"Brier {metric['brier_score_mean']:.3f}"
            else:
                primary = f"R² {metric['r2_mean']:.3f}±{metric['r2_std']:.3f}"
                secondary = f"MAE {metric['mae_mean']:.0f}d"
```

Replace with:

```python
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
```

**Step 5: Run CLI tests**

```bash
uv run pytest tests/test_cli.py -v
```

Expected: all tests pass.

**Step 6: Run full test suite**

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
```

Expected: all tests pass, lint clean.

**Step 7: Commit CLI changes**

```bash
git add src/zoneto/cli.py tests/test_cli.py
git commit -m "feat: add zoneto aic command, --fetch-aic flag, fix train metrics for survival model"
```

---

### Task 3: Update justfile

**Files:**
- Modify: `justfile`

**Step 1: Add `aic` task and update `pipeline` comment**

In `justfile`, add the `aic` task after the `enrich` task:

```makefile
# Scrape AIC portal for decision dates
aic:
    uv run zoneto aic
```

The `pipeline` task already calls `just enrich` which now includes AIC scraping by default. Add a comment:

```makefile
# Run the full analytics pipeline: enrich (includes AIC scrape) → train → score
pipeline:
    just enrich
    just train
    just score
```

**Step 2: Verify justfile syntax**

```bash
just --list
```

Expected: `aic` appears in the task list.

**Step 3: Commit justfile**

```bash
git add justfile
git commit -m "chore: add aic justfile task"
```

---

### Task 4: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Update the CLI section**

In `CLAUDE.md`, find the `### CLI` section and update the `zoneto enrich` entry:

```markdown
- `zoneto enrich [--fetch-ref/--no-fetch-ref] [--fetch-aic/--no-fetch-aic]` -- enriches raw parquet with outcome
  labels and spatial features. Downloads reference datasets to `data/reference/` if
  `--fetch-ref` (default). Scrapes AIC portal for decision dates if `--fetch-aic` (default).
  Enriches COA, dev_applications, and permits_cleared.
  Writes enriched parquet to `data/enriched/`.
- `zoneto aic [--delay FLOAT]` -- scrapes AIC portal for OZ/SA decision milestone dates.
  Caches results to `data/reference/aic_decisions.parquet`. Default delay: 1.0s/request.
```

**Step 2: Update the Enrichment section**

In the `### Enrichment` section, add to the reference datasets list:

```markdown
- AIC decisions (scraped via `fetch_aic_decisions()` — `data/reference/aic_decisions.parquet`)
  Schema: `folderrsn (String), decision_date (Date|null), complete_date (Date|null), scraped_at (Date)`
  OZ: "City Council Decision Made"; SA: "Statement of Approval Issued"
```

Add to the enrichment functions list:

```markdown
- `fetch_aic_decisions(data_dir, *, delay=1.0)` -- scrapes AIC portal for OZ+SA milestone dates.
  Idempotent: skips already-scraped `folderrsn` values. Returns count of newly scraped rows.
```

In the `enrich_dev()` description, add the new columns:

```markdown
  New survival columns (requires AIC scrape): `dev_days_to_decision` (Int32|null, capped at 3,650 days),
  `dev_decision_event` (Int8|null, 1=closed/0=active/null=non-OZ/SA),
  `dev_days_observed` (Int32|null, days_to_decision for events; today-submitted for censored).
  New feature: `is_combined_application` (Int8, 1 if OZ with OPA in description).
```

**Step 3: Update the Training section**

In the models table, add:

```markdown
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | enriched dev_applications | OZ+SA only (null event = excluded); trained only if AIC scraped |
```

Update `train_all` description to note:

```markdown
  Optional survival model (trained if `dev_days_observed` present in enriched dev parquet):
  dev_days_to_decision. Survival model uses c-index threshold (>= 0.65) for `production_ready`.
```

**Step 4: Update the Scoring section**

In the output columns table, add:

```markdown
| dev_applications | `pred_dev_days_to_decision` | float | predicted median days to decision (survival model) |
```

**Step 5: Update the Dependencies table**

Add:

```markdown
| beautifulsoup4 | HTML parsing for AIC scraper |
| scikit-survival | Survival analysis (GradientBoostingSurvivalAnalysis, concordance_index_censored) |
```

**Step 6: Update the Features section**

In `DEV_NUM_COLS`, note that `is_combined_application` was added.

**Step 7: Commit documentation**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md for AIC scraper, survival model, and CLI changes"
```

---

### Final: Run complete verification

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
just --list
```

Expected: all tests pass, lint clean, `aic` visible in just task list.

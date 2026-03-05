# Phase 3: Training Pipeline

<!-- Plan date: 2026-03-04 -->
<!-- Branch: development-outcome-prediction -->

## Goal

Build `src/zoneto/analytics/train.py` with sklearn pipelines, train all 4
models, serialize to `models/`, and add `zoneto train` CLI command.

## Background: Models

| File | Estimator | Target col | Feature source | Label filter |
|---|---|---|---|---|
| `dev_applications_approved.joblib` | HistGradientBoostingClassifier | `dev_approved` | `data/enriched/dev_applications.parquet` | drop rows where label is null |
| `dev_applications_no_appeal.joblib` | HistGradientBoostingClassifier | `dev_no_appeal` | `data/enriched/dev_applications.parquet` | drop rows where label is null |
| `coa_approved.joblib` | HistGradientBoostingClassifier | `coa_approved` | `data/enriched/coa.parquet` | drop rows where label is null |
| `coa_days_to_approval.joblib` | HistGradientBoostingRegressor | `coa_days_to_approval` | `data/enriched/coa.parquet` | drop rows where target is null |

## Background: sklearn Pipeline

```python
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OrdinalEncoder

preprocessor = ColumnTransformer([
    ("cat", Pipeline([
        ("impute", SimpleImputer(strategy="constant", fill_value="__missing__")),
        ("encode", OrdinalEncoder(
            handle_unknown="use_encoded_value",
            unknown_value=-1,
        )),
    ]), cat_cols),
    ("num", "passthrough", num_cols),
])
```

HistGradientBoosting handles NaN natively in numeric columns — no numeric
imputation step is needed.

## Files Changed

| File | Action |
|---|---|
| `src/zoneto/analytics/train.py` | Create |
| `src/zoneto/cli.py` | Add `train` command |
| `tests/analytics/test_train.py` | Create |

## Step 1 — Write failing tests

Create `tests/analytics/test_train.py`:

```python
"""Tests for train.py — synthetic data, no actual parquet required."""
from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl
import pytest
from sklearn.pipeline import Pipeline

from zoneto.analytics.train import build_pipeline, train_all, train_source


def _make_dev_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched dev_applications.parquet."""
    df = pl.DataFrame(
        {
            "application_type": ["Rezoning", "Site Plan", "Rezoning", "OPA", "Site Plan"],
            "ward_number": ["Ward 1", "Ward 2", "Ward 3", "Ward 1", "Ward 4"],
            "zoning_class": ["RS", "RM", None, "CR", "RS"],
            "secondary_plan_name": [None, "Midtown", None, "Midtown", None],
            "year_submitted": [2018, 2019, 2020, 2021, 2022],
            "in_heritage_register": [0, 1, 0, 0, 1],
            "in_heritage_district": [0, 0, 0, 1, 0],
            "in_secondary_plan": [0, 1, 0, 1, 0],
            "has_community_meeting": [1, 0, 0, 1, 0],
            "dev_approved": [1, 0, 1, 1, 0],
            "dev_no_appeal": [0, None, 0, 1, 0],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def _make_coa_enriched(tmp_path: Path) -> Path:
    """Write a minimal enriched coa.parquet."""
    df = pl.DataFrame(
        {
            "application_type": ["Minor Variance", "Consent", "Minor Variance", "Consent", "Minor Variance"],
            "sub_type": ["A", "B", "A", "C", "B"],
            "ward_number": ["1", "2", "3", "4", "5"],
            "zoning_designation": ["RS", "RM", None, "CR", "RS"],
            "year_submitted": [2019, 2020, 2021, 2022, 2022],
            "coa_approved": [1, 0, 1, 0, 1],
            "coa_days_to_approval": [95, None, 120, None, 60],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    dest = out / "coa.parquet"
    df.write_parquet(dest)
    return dest


# ---------------------------------------------------------------------------
# build_pipeline
# ---------------------------------------------------------------------------

def test_build_pipeline_returns_sklearn_pipeline() -> None:
    from sklearn.ensemble import HistGradientBoostingClassifier
    pipe = build_pipeline(
        cat_cols=["application_type", "ward_number"],
        num_cols=["year_submitted"],
        estimator=HistGradientBoostingClassifier(),
    )
    assert isinstance(pipe, Pipeline)


def test_build_pipeline_steps() -> None:
    from sklearn.ensemble import HistGradientBoostingClassifier
    pipe = build_pipeline(
        cat_cols=["application_type"],
        num_cols=["year_submitted"],
        estimator=HistGradientBoostingClassifier(),
    )
    step_names = [name for name, _ in pipe.steps]
    assert "preprocessor" in step_names
    assert "estimator" in step_names


# ---------------------------------------------------------------------------
# train_source
# ---------------------------------------------------------------------------

def test_train_source_classifier(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_approved",
        cat_cols=["application_type", "ward_number", "zoning_class", "secondary_plan_name"],
        num_cols=["year_submitted", "in_heritage_register", "in_heritage_district",
                  "in_secondary_plan", "has_community_meeting"],
        model_name="dev_applications_approved",
        model_dir=model_dir,
        regressor=False,
    )
    assert (model_dir / "dev_applications_approved.joblib").exists()


def test_train_source_loads_valid_pipeline(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_approved",
        cat_cols=["application_type", "ward_number", "zoning_class", "secondary_plan_name"],
        num_cols=["year_submitted", "in_heritage_register", "in_heritage_district",
                  "in_secondary_plan", "has_community_meeting"],
        model_name="dev_applications_approved",
        model_dir=model_dir,
        regressor=False,
    )
    pipe = joblib.load(model_dir / "dev_applications_approved.joblib")
    assert isinstance(pipe, Pipeline)


def test_train_source_regressor(tmp_path: Path) -> None:
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_source(
        enriched_path=tmp_path / "enriched" / "coa.parquet",
        label_col="coa_days_to_approval",
        cat_cols=["application_type", "sub_type", "ward_number", "zoning_designation"],
        num_cols=["year_submitted"],
        model_name="coa_days_to_approval",
        model_dir=model_dir,
        regressor=True,
    )
    assert (model_dir / "coa_days_to_approval.joblib").exists()


def test_train_source_drops_null_labels(tmp_path: Path) -> None:
    """train_source must not fail when some label rows are null."""
    _make_dev_enriched(tmp_path)
    model_dir = tmp_path / "models"
    # dev_no_appeal has one null row — should still train on non-null rows
    train_source(
        enriched_path=tmp_path / "enriched" / "dev_applications.parquet",
        label_col="dev_no_appeal",
        cat_cols=["application_type", "ward_number", "zoning_class", "secondary_plan_name"],
        num_cols=["year_submitted", "in_heritage_register", "in_heritage_district",
                  "in_secondary_plan", "has_community_meeting"],
        model_name="dev_applications_no_appeal",
        model_dir=model_dir,
        regressor=False,
    )
    assert (model_dir / "dev_applications_no_appeal.joblib").exists()


# ---------------------------------------------------------------------------
# train_all
# ---------------------------------------------------------------------------

def test_train_all_creates_four_models(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    train_all(data_dir=tmp_path, model_dir=model_dir)
    expected = [
        "dev_applications_approved.joblib",
        "dev_applications_no_appeal.joblib",
        "coa_approved.joblib",
        "coa_days_to_approval.joblib",
    ]
    for name in expected:
        assert (model_dir / name).exists(), f"Missing {name}"
```

Run `uv run pytest tests/analytics/test_train.py` — expect
`ImportError: cannot import name 'build_pipeline' from 'zoneto.analytics.train'`.

## Step 2 — Implement `train.py`

Create `src/zoneto/analytics/train.py`:

```python
"""Training pipeline: build sklearn models from enriched parquet."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import polars as pl
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)


def build_pipeline(
    cat_cols: list[str],
    num_cols: list[str],
    estimator: Any,
) -> Pipeline:
    """Return an unfitted sklearn Pipeline with OrdinalEncoder + estimator."""
    preprocessor = ColumnTransformer(
        transformers=[
            (
                "cat",
                Pipeline([
                    ("impute", SimpleImputer(strategy="constant", fill_value="__missing__")),
                    ("encode", OrdinalEncoder(
                        handle_unknown="use_encoded_value",
                        unknown_value=-1,
                    )),
                ]),
                cat_cols,
            ),
            ("num", "passthrough", num_cols),
        ]
    )
    return Pipeline([
        ("preprocessor", preprocessor),
        ("estimator", estimator),
    ])


def train_source(
    enriched_path: Path,
    label_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    model_name: str,
    model_dir: Path,
    *,
    regressor: bool = False,
) -> int:
    """Train one model, serialize to *model_dir*/<model_name>.joblib.

    Returns number of training rows used.
    """
    df = pl.read_parquet(enriched_path)

    # Drop rows with null labels
    df = df.filter(pl.col(label_col).is_not_null())

    all_cols = cat_cols + num_cols
    # Fill missing cat columns with None (polars), num columns left as-is
    for col in cat_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))
    for col in num_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    X = df.select(all_cols).to_pandas()
    y = df[label_col].to_numpy()

    estimator = (
        HistGradientBoostingRegressor(random_state=42)
        if regressor
        else HistGradientBoostingClassifier(random_state=42)
    )
    pipe = build_pipeline(cat_cols=cat_cols, num_cols=num_cols, estimator=estimator)
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")
    return len(df)


def train_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> dict[str, int]:
    """Train all 4 models. Returns {model_name: row_count}."""
    dev_path = data_dir / "enriched" / "dev_applications.parquet"
    coa_path = data_dir / "enriched" / "coa.parquet"

    jobs: list[tuple[Path, str, list[str], list[str], str, bool]] = [
        (dev_path, "dev_approved", DEV_CAT_COLS, DEV_NUM_COLS,
         "dev_applications_approved", False),
        (dev_path, "dev_no_appeal", DEV_CAT_COLS, DEV_NUM_COLS,
         "dev_applications_no_appeal", False),
        (coa_path, "coa_approved", COA_CAT_COLS, COA_NUM_COLS,
         "coa_approved", False),
        (coa_path, "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS,
         "coa_days_to_approval", True),
    ]

    results: dict[str, int] = {}
    for path, label, cat, num, name, is_reg in jobs:
        count = train_source(
            enriched_path=path,
            label_col=label,
            cat_cols=cat,
            num_cols=num,
            model_name=name,
            model_dir=model_dir,
            regressor=is_reg,
        )
        results[name] = count
    return results
```

## Step 3 — Add `train` CLI command

In `src/zoneto/cli.py`, add import:

```python
from zoneto.analytics.train import train_all
```

Add command:

```python
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
```

## Step 4 — Run tests

```bash
uv run pytest tests/analytics/test_train.py -v
```

All tests must pass.

## Step 5 — Lint

```bash
uv run ruff check src/zoneto/analytics/train.py src/zoneto/cli.py
uv run ty check src/zoneto/analytics/train.py src/zoneto/cli.py
```

## Verification

- `uv run pytest tests/analytics/test_train.py` → all pass
- `uv run pytest` → all tests still pass
- `uv run ruff check src/` → no issues
- `uv run ty check src/` → no issues

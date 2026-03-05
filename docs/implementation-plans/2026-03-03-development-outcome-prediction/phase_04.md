# Phase 4: Score CLI

<!-- Plan date: 2026-03-04 -->
<!-- Branch: development-outcome-prediction -->

## Goal

Build `src/zoneto/analytics/score.py` with `score_all` (batch inference on
enriched parquet) and `score_one` (single-application prediction), add
`zoneto score` CLI command, and add justfile recipes for the full pipeline.

## Files Changed

| File | Action |
|---|---|
| `src/zoneto/analytics/score.py` | Create |
| `src/zoneto/cli.py` | Add `score` command |
| `tests/analytics/test_score.py` | Create |
| `justfile` | Add `enrich`, `train`, `score`, `pipeline` recipes |

## Step 1 — Write failing tests

Create `tests/analytics/test_score.py`:

```python
"""Tests for score.py — uses synthetic trained models from tmp_path."""
from __future__ import annotations

from pathlib import Path

import joblib
import polars as pl
import pytest
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

from zoneto.analytics.features import COA_CAT_COLS, COA_NUM_COLS, DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import build_pipeline
from zoneto.analytics.score import score_all, score_one


# ---------------------------------------------------------------------------
# Fixtures: synthetic trained models + enriched parquet
# ---------------------------------------------------------------------------

def _train_dummy_model(
    model_dir: Path,
    model_name: str,
    cat_cols: list[str],
    num_cols: list[str],
    *,
    regressor: bool = False,
) -> None:
    """Train a minimal sklearn Pipeline on 3 synthetic rows and save it."""
    import pandas as pd
    import numpy as np

    n = 6
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in cat_cols})
    X[num_cols] = pd.DataFrame(
        np.random.default_rng(0).integers(0, 5, size=(n, len(num_cols))).astype(float),
        columns=num_cols,
    )
    y = np.array([0, 1, 0, 1, 0, 1], dtype=float if regressor else int)

    est = HistGradientBoostingRegressor(random_state=0) if regressor else HistGradientBoostingClassifier(random_state=0)
    pipe = build_pipeline(cat_cols=cat_cols, num_cols=num_cols, estimator=est)
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")


def _setup_models(tmp_path: Path) -> Path:
    model_dir = tmp_path / "models"
    _train_dummy_model(model_dir, "dev_applications_approved", DEV_CAT_COLS, DEV_NUM_COLS)
    _train_dummy_model(model_dir, "dev_applications_no_appeal", DEV_CAT_COLS, DEV_NUM_COLS)
    _train_dummy_model(model_dir, "coa_approved", COA_CAT_COLS, COA_NUM_COLS)
    _train_dummy_model(model_dir, "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS, regressor=True)
    return model_dir


def _make_dev_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame({
        "application_type": ["Rezoning", "Site Plan"],
        "ward_number": ["Ward 1", "Ward 2"],
        "zoning_class": ["RS", None],
        "secondary_plan_name": [None, "Midtown"],
        "year_submitted": [2021, 2022],
        "in_heritage_register": [0, 1],
        "in_heritage_district": [0, 0],
        "in_secondary_plan": [0, 1],
        "has_community_meeting": [1, 0],
        "dev_approved": [1, 0],
        "dev_no_appeal": [0, 1],
    })
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")


def _make_coa_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame({
        "application_type": ["Minor Variance", "Consent"],
        "sub_type": ["A", "B"],
        "ward_number": ["1", "2"],
        "zoning_designation": ["RS", None],
        "year_submitted": [2021, 2022],
        "coa_approved": [1, 0],
        "coa_days_to_approval": [90, None],
    })
    out = tmp_path / "enriched"
    out.mkdir(exist_ok=True)
    df.write_parquet(out / "coa.parquet")


# ---------------------------------------------------------------------------
# score_all
# ---------------------------------------------------------------------------

def test_score_all_creates_parquet(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    assert (tmp_path / "scores" / "dev_applications.parquet").exists()
    assert (tmp_path / "scores" / "coa.parquet").exists()


def test_score_all_dev_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_approved" in df.columns
    assert "pred_dev_no_appeal" in df.columns
    assert "prob_dev_approved" in df.columns
    assert "prob_dev_no_appeal" in df.columns


def test_score_all_coa_columns(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "coa.parquet")
    assert "pred_coa_approved" in df.columns
    assert "prob_coa_approved" in df.columns
    assert "pred_coa_days_to_approval" in df.columns


def test_score_all_prob_range(tmp_path: Path) -> None:
    """Probability predictions should be in [0, 1]."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    score_all(data_dir=tmp_path, model_dir=model_dir)
    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert df["prob_dev_approved"].min() >= 0.0
    assert df["prob_dev_approved"].max() <= 1.0


# ---------------------------------------------------------------------------
# score_one
# ---------------------------------------------------------------------------

def test_score_one_returns_dict(tmp_path: Path) -> None:
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    result = score_one(
        source="dev_applications",
        features={
            "application_type": "Rezoning",
            "ward_number": "Ward 5",
            "zoning_class": "RS",
            "secondary_plan_name": None,
            "year_submitted": 2022,
            "in_heritage_register": 0,
            "in_heritage_district": 0,
            "in_secondary_plan": 0,
            "has_community_meeting": 0,
        },
        model_dir=model_dir,
    )
    assert "pred_dev_approved" in result
    assert "prob_dev_approved" in result
    assert "pred_dev_no_appeal" in result


def test_score_one_coa(tmp_path: Path) -> None:
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)
    result = score_one(
        source="coa",
        features={
            "application_type": "Minor Variance",
            "sub_type": "A",
            "ward_number": "3",
            "zoning_designation": "RM",
            "year_submitted": 2021,
        },
        model_dir=model_dir,
    )
    assert "pred_coa_approved" in result
    assert "pred_coa_days_to_approval" in result


def test_score_one_unknown_source(tmp_path: Path) -> None:
    model_dir = _setup_models(tmp_path)
    with pytest.raises(ValueError, match="Unknown source"):
        score_one(source="invalid", features={}, model_dir=model_dir)
```

Run `uv run pytest tests/analytics/test_score.py` — expect
`ImportError: cannot import name 'score_all' from 'zoneto.analytics.score'`.

## Step 2 — Implement `score.py`

Create `src/zoneto/analytics/score.py`:

```python
"""Batch and single-application scoring from trained joblib models."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import pandas as pd
import polars as pl

from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)


# Model registry: source → list of (model_name, label_col, is_regressor)
_DEV_MODELS: list[tuple[str, str, bool]] = [
    ("dev_applications_approved", "pred_dev_approved", False),
    ("dev_applications_no_appeal", "pred_dev_no_appeal", False),
]
_COA_MODELS: list[tuple[str, str, bool]] = [
    ("coa_approved", "pred_coa_approved", False),
    ("coa_days_to_approval", "pred_coa_days_to_approval", True),
]


def _load(model_dir: Path, model_name: str) -> Any:
    return joblib.load(model_dir / f"{model_name}.joblib")


def _predict_classifier(pipe: Any, X: pd.DataFrame, pred_col: str, prob_col: str) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    probs = pipe.predict_proba(X)[:, 1].tolist()
    return {pred_col: preds, prob_col: probs}


def _predict_regressor(pipe: Any, X: pd.DataFrame, pred_col: str) -> dict[str, list]:
    preds = pipe.predict(X).tolist()
    return {pred_col: preds}


def score_all(
    data_dir: Path = Path("data"),
    model_dir: Path = Path("models"),
) -> None:
    """Run batch inference on enriched parquet files; write data/scores/*.parquet."""
    scores_dir = data_dir / "scores"
    scores_dir.mkdir(parents=True, exist_ok=True)

    # --- dev_applications ---
    dev_enriched = data_dir / "enriched" / "dev_applications.parquet"
    df_dev = pl.read_parquet(dev_enriched)
    all_dev_cols = DEV_CAT_COLS + DEV_NUM_COLS
    X_dev = df_dev.select(all_dev_cols).to_pandas()

    extra: dict[str, list] = {}
    for model_name, pred_col, is_reg in _DEV_MODELS:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            extra.update(_predict_regressor(pipe, X_dev, pred_col))
        else:
            extra.update(_predict_classifier(pipe, X_dev, pred_col, prob_col))

    df_dev_scored = df_dev.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )
    df_dev_scored.write_parquet(scores_dir / "dev_applications.parquet")

    # --- coa ---
    coa_enriched = data_dir / "enriched" / "coa.parquet"
    df_coa = pl.read_parquet(coa_enriched)
    all_coa_cols = COA_CAT_COLS + COA_NUM_COLS
    X_coa = df_coa.select(all_coa_cols).to_pandas()

    extra_coa: dict[str, list] = {}
    for model_name, pred_col, is_reg in _COA_MODELS:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            extra_coa.update(_predict_regressor(pipe, X_coa, pred_col))
        else:
            extra_coa.update(_predict_classifier(pipe, X_coa, pred_col, prob_col))

    df_coa_scored = df_coa.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra_coa.items()]
    )
    df_coa_scored.write_parquet(scores_dir / "coa.parquet")


def score_one(
    source: str,
    features: dict[str, Any],
    model_dir: Path = Path("models"),
) -> dict[str, Any]:
    """Score a single application dict. Returns prediction dict.

    source must be 'dev_applications' or 'coa'.
    """
    if source == "dev_applications":
        models = _DEV_MODELS
        all_cols = DEV_CAT_COLS + DEV_NUM_COLS
    elif source == "coa":
        models = _COA_MODELS
        all_cols = COA_CAT_COLS + COA_NUM_COLS
    else:
        raise ValueError(f"Unknown source: {source!r}. Must be 'dev_applications' or 'coa'.")

    X = pd.DataFrame([{col: features.get(col) for col in all_cols}])

    result: dict[str, Any] = {}
    for model_name, pred_col, is_reg in models:
        pipe = _load(model_dir, model_name)
        prob_col = pred_col.replace("pred_", "prob_")
        if is_reg:
            result[pred_col] = float(pipe.predict(X)[0])
        else:
            result[pred_col] = int(pipe.predict(X)[0])
            result[prob_col] = float(pipe.predict_proba(X)[0, 1])

    return result
```

## Step 3 — Add `score` CLI command

In `src/zoneto/cli.py`, add import:

```python
from zoneto.analytics.score import score_all
```

Add command:

```python
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
```

## Step 4 — Update justfile

Replace the existing `justfile` with:

```just
sync:
    uv run zoneto sync

status:
    uv run zoneto status

enrich:
    uv run zoneto enrich

train:
    uv run zoneto train

score:
    uv run zoneto score

# Run the full analytics pipeline: enrich → train → score
pipeline:
    just enrich
    just train
    just score

test:
    uv run pytest

lint:
    uv run ruff check src/ && uv run ty check src/

fmt:
    uv run ruff format src/
```

## Step 5 — Run tests

```bash
uv run pytest tests/analytics/test_score.py -v
```

All tests must pass.

## Step 6 — Full test suite

```bash
uv run pytest -v
```

All tests must pass.

## Step 7 — Lint

```bash
uv run ruff check src/
uv run ty check src/
```

## Verification

- `uv run pytest` → all tests pass (including pre-existing tests)
- `uv run ruff check src/` → no issues
- `uv run ty check src/` → no issues
- `just --list` shows: sync, status, enrich, train, score, pipeline, test, lint, fmt
- `zoneto --help` shows: sync, status, enrich, train, score commands

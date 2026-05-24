# Product Strategy Pivot — Phase 3: Model Retirement

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Retire underperforming models from the production pipeline. `coa_approved` and `permit_issuance_days` stop training entirely. `coa_days_to_approval` continues training for metric tracking but is permanently gated `production_ready=False`. Batch scoring produces only dev_applications output.

**Architecture:** Three coordinated changes: (1) `train_all()` jobs list reduced; (2) `score_all()` COA and permits blocks removed; (3) `train` CLI output marks tracking-only models. Retirement test suite verifies the gates hold.

**Tech Stack:** Python, existing sklearn/scikit-survival pipeline, pytest

**Scope:** Phase 3 of 8. Independent of Phases 1–2.

**Codebase verified:** 2026-03-17

---

## Task 1: Remove retired models from train_all()

**Files:**
- Modify: `src/zoneto/analytics/train.py` (lines 392–426 jobs list; lines 477–498 gating block)

**Step 1: Write failing test**

Create `tests/analytics/test_retirement.py`:

```python
"""Tests verifying model retirement gates are enforced."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import train_all


def _make_dev_parquet(tmp_path: Path) -> Path:
    """Minimal 30-row dev_applications parquet with dev_appealed labels."""
    rng = np.random.default_rng(0)
    n = 30
    dev_appealed = (rng.uniform(size=n) < 0.15).astype(float).tolist()
    # make exactly 5 None to test null handling
    for i in range(5):
        dev_appealed[i] = None  # type: ignore[call-overload]
    df = pl.DataFrame(
        {
            "application_type": rng.choice(["OZ", "SA", "OPA"], size=n).tolist(),
            "ward_number": [str(rng.integers(1, 26)) for _ in range(n)],
            "zoning_class": rng.choice(["RS", "RM", None], size=n).tolist(),
            "secondary_plan_name": [None] * n,
            "year_submitted": rng.integers(2018, 2024, size=n).tolist(),
            "in_heritage_register": rng.integers(0, 2, size=n).tolist(),
            "in_heritage_district": rng.integers(0, 2, size=n).tolist(),
            "in_secondary_plan": rng.integers(0, 2, size=n).tolist(),
            "has_community_meeting": rng.integers(0, 2, size=n).tolist(),
            "ward_pct_renters": rng.uniform(0.2, 0.7, size=n).tolist(),
            "ward_median_income": rng.uniform(40_000, 120_000, size=n).tolist(),
            "ward_pop_density": rng.uniform(1000, 8000, size=n).tolist(),
            "ward_pct_detached": rng.uniform(0.1, 0.6, size=n).tolist(),
            "has_parent_application": rng.integers(0, 2, size=n).tolist(),
            "is_combined_application": rng.integers(0, 2, size=n).tolist(),
            "proposed_storeys": pl.Series(
                rng.integers(1, 40, size=n).tolist(), dtype=pl.Int32
            ),
            "proposed_units": pl.Series(
                rng.integers(1, 500, size=n).tolist(), dtype=pl.Int32
            ),
            "ward_appeal_rate_3y": rng.uniform(0.05, 0.25, size=n).tolist(),
            "dev_appealed": pl.Series(dev_appealed, dtype=pl.Float64),
        }
    )
    dest = tmp_path / "enriched" / "dev_applications.parquet"
    dest.parent.mkdir(parents=True)
    df.write_parquet(dest)
    return dest


def test_retired_models_not_in_train_all_output(tmp_path: Path) -> None:
    """coa_approved and permit_issuance_days must not appear in train_all() results."""
    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "coa_approved" not in counts, "coa_approved must be retired from train_all()"
    assert "permit_issuance_days" not in counts, (
        "permit_issuance_days must be retired from train_all()"
    )


def test_coa_days_to_approval_is_tracking_only(tmp_path: Path) -> None:
    """coa_days_to_approval must train but production_ready must be False."""
    # Create minimal COA parquet so the model can train
    rng = np.random.default_rng(1)
    n = 30
    coa_approved = (rng.uniform(size=n) < 0.94).astype(int)
    coa_days: list[float | None] = [
        float(rng.uniform(30, 400)) if coa_approved[i] == 1 else None
        for i in range(n)
    ]
    coa_df = pl.DataFrame(
        {
            "application_type": rng.choice(["Minor Variance", "Consent"], size=n).tolist(),
            "sub_type": rng.choice(["A", "B"], size=n).tolist(),
            "ward_number": [str(rng.integers(1, 26)) for _ in range(n)],
            "zoning_designation": rng.choice(["RS", "RM", None], size=n).tolist(),
            "planning_district": ["Toronto & East York"] * n,
            "work_type": rng.choice(["Variance", "Consent"], size=n).tolist(),
            "year_submitted": rng.integers(2018, 2024, size=n).tolist(),
            "coa_approved": pl.Series(coa_approved.tolist(), dtype=pl.Int8),
            "coa_days_to_approval": pl.Series(coa_days, dtype=pl.Float64),
        }
    )
    coa_path = tmp_path / "enriched" / "coa.parquet"
    coa_path.parent.mkdir(parents=True, exist_ok=True)
    coa_df.write_parquet(coa_path)

    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "coa_days_to_approval" in counts, (
        "coa_days_to_approval must still train for metric tracking"
    )
    assert metrics["coa_days_to_approval"]["production_ready"] is False, (
        "coa_days_to_approval must be production_ready=False regardless of R²"
    )


def test_permits_parquet_does_not_trigger_permit_training(tmp_path: Path) -> None:
    """Even when permits_cleared.parquet exists, permit_issuance_days must not train."""
    rng = np.random.default_rng(2)
    n = 30
    permits_df = pl.DataFrame(
        {
            "permit_type": rng.choice(["New Houses", "Commercial"], size=n).tolist(),
            "structure_type": rng.choice(["Detached House", "Office"], size=n).tolist(),
            "ward_grid": [f"W{rng.integers(1, 30):02d}" for _ in range(n)],
            "est_const_cost": rng.uniform(50_000, 5_000_000, size=n).tolist(),
            "dwelling_units_created": rng.integers(0, 5, size=n).tolist(),
            "dwelling_units_lost": rng.integers(0, 2, size=n).tolist(),
            "residential": rng.integers(0, 2, size=n).tolist(),
            "mercantile": rng.integers(0, 2, size=n).tolist(),
            "industrial": rng.integers(0, 2, size=n).tolist(),
            "institutional": rng.integers(0, 2, size=n).tolist(),
            "permit_issuance_days": rng.integers(10, 300, size=n).tolist(),
        }
    )
    permits_path = tmp_path / "enriched" / "permits_cleared.parquet"
    permits_path.parent.mkdir(parents=True, exist_ok=True)
    permits_df.write_parquet(permits_path)

    _make_dev_parquet(tmp_path)
    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "permit_issuance_days" not in counts, (
        "permit_issuance_days must not train even when enriched parquet exists"
    )
```

**Step 2: Run test to confirm failure**

```bash
uv run pytest tests/analytics/test_retirement.py -v
```

Expected: All three tests fail — retired models are still being trained.

**Step 3: Modify `src/zoneto/analytics/train.py`**

In `train_all()`, replace the `jobs` list (lines 392–413) with:

```python
    jobs: list[tuple[Path, str, list[str], list[str], str, bool]] = [
        # dev_applications_approved retired: dataset frozen (no new records since city
        # retired the dataset), class imbalance is 97.3% approved, and CV variance
        # (±0.267 AUC) is too high for reliable predictions.
        (
            dev_path,
            "dev_appealed",
            DEV_CAT_COLS,
            DEV_NUM_COLS,
            "dev_applications_appealed",
            False,
        ),
        # coa_approved retired: AUC 0.535 with 94% base rate. Worse than predicting
        # the majority class. Cannot be improved with available structured features.
        # COA approval is nearly certain; the meaningful question is "under what conditions"
        # (requires text analysis, not classification).
        #
        # coa_days_to_approval trains for metric tracking only.
        # production_ready is forced False below regardless of R².
        (
            coa_path,
            "coa_days_to_approval",
            COA_CAT_COLS,
            COA_NUM_COLS,
            "coa_days_to_approval",
            True,
        ),
    ]

    # permit_issuance_days retired: R² 0.039 on 133K rows is conclusive.
    # Queue depth (the primary driver) is not in open data.
    # Not trained even when enriched file is present.
```

Remove the permits optional block entirely (the `if permits_path.exists(): jobs.append(...)` block that was lines 415–426).

After the existing production_ready gating loop (after line 493 in the original), add the tracking-only override **before** the `metrics_file` write:

```python
    # Force coa_days_to_approval to production_ready=False regardless of metric score.
    # It trains for tracking only — to monitor whether R² improves as data grows.
    # Serving predictions from a model with R² < 0 is worse than presenting the mean.
    if "coa_days_to_approval" in metrics:
        metrics["coa_days_to_approval"]["production_ready"] = False
```

**Step 4: Run retirement tests**

```bash
uv run pytest tests/analytics/test_retirement.py -v
```

Expected: All three tests pass.

**Step 5: Run full regression tests**

```bash
uv run pytest tests/analytics/ -qq
```

Expected: All tests pass. (Synthetic regression tests for COA and permits still pass — they call `evaluate_source()` directly, which is unchanged.)

**Step 6: Commit**

```bash
git add src/zoneto/analytics/train.py tests/analytics/test_retirement.py
git commit -m "feat: retire coa_approved and permit_issuance_days from train_all; coa_days_to_approval tracking-only"
```

---

## Task 2: Remove COA and permits scoring blocks from score_all()

**Files:**
- Modify: `src/zoneto/analytics/score.py` (lines 29–35 model registries; lines 173–218 scoring blocks)

**Step 1: Write the failing test**

Add to `tests/analytics/test_retirement.py`:

```python
def test_score_all_only_produces_dev_applications_output(tmp_path: Path) -> None:
    """score_all() only writes dev_applications.parquet — no coa or permits output."""
    from pathlib import Path

    import joblib

    from zoneto.analytics.score import score_all
    from zoneto.analytics.train import build_pipeline, train_source
    from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS

    dev_path = _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models"
    model_dir.mkdir(parents=True, exist_ok=True)

    # Train only the appeal model
    train_source(
        enriched_path=dev_path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=model_dir,
        regressor=False,
    )
    # Write minimal metrics.json with production_ready=True for the appeal model
    import json
    (model_dir / "metrics.json").write_text(
        json.dumps({"dev_applications_appealed": {"production_ready": True}})
    )

    # Create stub COA and permits enriched parquet to verify they are NOT scored
    coa_stub = tmp_path / "enriched" / "coa.parquet"
    permits_stub = tmp_path / "enriched" / "permits_cleared.parquet"
    pl.DataFrame({"placeholder": [1]}).write_parquet(coa_stub)
    pl.DataFrame({"placeholder": [1]}).write_parquet(permits_stub)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    scores_dir = tmp_path / "scores"
    assert (scores_dir / "dev_applications.parquet").exists(), (
        "dev_applications.parquet must be written"
    )
    assert not (scores_dir / "coa.parquet").exists(), (
        "coa.parquet must NOT be written — model retired"
    )
    assert not (scores_dir / "permits_cleared.parquet").exists(), (
        "permits_cleared.parquet must NOT be written — model retired"
    )
```

**Step 2: Run test to confirm failure**

```bash
uv run pytest tests/analytics/test_retirement.py::test_score_all_only_produces_dev_applications_output -v
```

Expected: Test fails because coa.parquet is still being written.

**Step 3: Modify `src/zoneto/analytics/score.py`**

**3a.** Replace the model registry lines 29–35:

```python
# Model registry: dev_applications models only.
# coa_approved: AUC 0.535 at 94% base rate — retired.
# coa_days_to_approval: R² < 0 — tracking only, not served.
# permit_issuance_days: R² 0.039 — retired (queue depth signal absent).
_DEV_MODELS: list[tuple[str, str, bool]] = [
    ("dev_applications_appealed", "pred_dev_appealed", False),
]
_COA_MODELS: list[tuple[str, str, bool]] = []
_PERMIT_MODELS: list[tuple[str, str, bool]] = []
```

**3b.** Remove the entire COA scoring block (lines 173–193):

Delete from `# --- coa ---` through `df_coa_scored.write_parquet(scores_dir / "coa.parquet")` inclusive.

**3c.** Remove the entire permits scoring block (lines 195–218):

Delete from `# --- permits_cleared (optional: skip if enriched file absent) ---` through `df_permits_scored.write_parquet(scores_dir / "permits_cleared.parquet")` inclusive.

**Step 4: Run retirement tests**

```bash
uv run pytest tests/analytics/test_retirement.py -v
```

Expected: All four tests pass.

**Step 5: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/zoneto/analytics/score.py tests/analytics/test_retirement.py
git commit -m "feat: remove coa and permits scoring blocks from score_all; batch scoring now dev_applications only"
```

---

## Task 3: Update train CLI output to mark tracking-only models

**Files:**
- Modify: `src/zoneto/cli.py` (lines 160–184, train command table)

**Step 1: Update the train command table in `cli.py`**

Find the train command (lines 147–188). Replace the table-building block to add a Status column:

```python
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
```

**Step 2: Verify CLI still runs**

```bash
uv run zoneto train --help
```

Expected: No import errors.

**Step 3: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 4: Run linter and type checker**

```bash
uv run ruff check src/zoneto/analytics/ src/zoneto/cli.py tests/analytics/test_retirement.py
uv run ty check src/zoneto/analytics/ src/zoneto/cli.py
```

Expected: No errors.

**Step 5: Commit**

```bash
git add src/zoneto/cli.py
git commit -m "feat: add Status column to train output marking tracking-only models"
```

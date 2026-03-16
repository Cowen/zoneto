# Phase 3: Survival Model Training

**Design phase:** Phase 4

**Goal:** Implement `train_survival()` and `evaluate_survival()` in `train.py`. Wire into `train_all()`. The survival model uses `GradientBoostingSurvivalAnalysis` from scikit-survival with a numpy structured label array.

---

### Task 1: Write failing tests for train_survival and evaluate_survival

**Files:**
- Modify: `tests/analytics/test_train.py`

**Context:** `_make_dev_enriched()` (line 16) creates a synthetic enriched parquet without survival columns. `_make_coa_enriched()` exists at line 115. Add a new survival-specific fixture and tests; do not modify the existing fixtures.

**Step 1: Add survival-specific fixture and tests**

Add at the end of `tests/analytics/test_train.py`:

```python
def _make_dev_enriched_survival(tmp_path: Path) -> Path:
    """Write minimal enriched dev_applications.parquet with survival columns.

    Only OZ+SA rows with non-null dev_decision_event are usable for the
    survival model. Active rows (event=0) are right-censored observations.
    """
    n = 20
    df = pl.DataFrame(
        {
            "application_type": (["OZ", "SA"] * 10),
            "ward_number": ([f"Ward {i % 4 + 1}" for i in range(n)]),
            "zoning_class": (["RS", "RM", None, "CR", "RS"] * 4),
            "secondary_plan_name": ([None, "Midtown"] * 10),
            "year_submitted": ([2016 + i % 6 for i in range(n)]),
            "in_heritage_register": ([0, 1] * 10),
            "in_heritage_district": ([0, 0, 1, 0, 0] * 4),
            "in_secondary_plan": ([0, 1] * 10),
            "has_community_meeting": ([1, 0] * 10),
            "ward_pct_renters": ([45.0 + i for i in range(n)]),
            "ward_median_income": ([70000.0 + i * 500 for i in range(n)]),
            "ward_pop_density": ([3000.0 + i * 100 for i in range(n)]),
            "ward_pct_detached": ([20.0 + i * 0.5 for i in range(n)]),
            "has_parent_application": ([0, 1] * 10),
            "is_combined_application": ([1, 0] * 10),
            "dev_approved": ([1, 0] * 10),
            "dev_appealed": ([0, 1] * 10),
            # Survival columns:
            "dev_decision_event": ([1, 1, 0, 1, 0] * 4),   # 1=closed, 0=active
            "dev_days_observed": ([365, 500, 800, 200, 1000] * 4),
            "dev_days_to_decision": ([365, 500, None, 200, None] * 4),
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    dest = out / "dev_applications.parquet"
    df.write_parquet(dest)
    return dest


def test_train_survival_creates_joblib(tmp_path: Path) -> None:
    """train_survival() serializes dev_days_to_decision.joblib."""
    from zoneto.analytics.train import train_survival
    from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS

    path = _make_dev_enriched_survival(tmp_path)
    model_dir = tmp_path / "models"
    count = train_survival(
        enriched_path=path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_days_to_decision",
        model_dir=model_dir,
    )
    assert (model_dir / "dev_days_to_decision.joblib").exists()
    assert count > 0


def test_evaluate_survival_returns_concordance_index(tmp_path: Path) -> None:
    """evaluate_survival() returns dict with concordance_index_mean key."""
    from zoneto.analytics.train import evaluate_survival
    from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS

    path = _make_dev_enriched_survival(tmp_path)
    result = evaluate_survival(
        enriched_path=path,
        time_col="dev_days_observed",
        event_col="dev_decision_event",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
    )
    assert "concordance_index_mean" in result
    assert "concordance_index_std" in result
    assert "n" in result
    assert isinstance(result["concordance_index_mean"], float)


def test_train_all_includes_survival_when_label_present(tmp_path: Path) -> None:
    """train_all() trains dev_days_to_decision model when survival columns exist."""
    import json

    _make_dev_enriched_survival(tmp_path)
    _make_coa_enriched(tmp_path)

    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "dev_days_to_decision" in counts
    assert "dev_days_to_decision" in metrics
    assert "concordance_index_mean" in metrics["dev_days_to_decision"]
    assert "production_ready" in metrics["dev_days_to_decision"]

    metrics_file = tmp_path / "models" / "metrics.json"
    assert metrics_file.exists()
    saved = json.loads(metrics_file.read_text())
    assert "dev_days_to_decision" in saved


def test_train_all_skips_survival_when_label_absent(tmp_path: Path) -> None:
    """train_all() skips survival model gracefully when dev_days_observed absent."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)

    counts, metrics = train_all(data_dir=tmp_path, model_dir=tmp_path / "models")

    assert "dev_days_to_decision" not in counts
    assert "dev_days_to_decision" not in metrics
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/analytics/test_train.py -k "survival" -v
```

Expected: `ImportError: cannot import name 'train_survival'` — confirms implementation missing.

---

### Task 2: Implement train_survival and evaluate_survival

**Files:**
- Modify: `src/zoneto/analytics/train.py`

**Step 1: Add scikit-survival imports**

At the top of `src/zoneto/analytics/train.py`, add after the existing sklearn imports:

```python
import logging

from sksurv.ensemble import GradientBoostingSurvivalAnalysis
from sksurv.metrics import concordance_index_censored
```

Note: `numpy` is already imported as `np` in the file. Add only the three lines above.

Also add at module level (after the imports):

```python
logger = logging.getLogger(__name__)
```

**Step 2: Add `train_survival()` function**

Add after the `train_source()` function (around line 146):

```python
def train_survival(
    enriched_path: Path,
    time_col: str,
    event_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    model_name: str,
    model_dir: Path,
) -> int:
    """Train a GradientBoostingSurvivalAnalysis model. Returns row count used.

    Filters to rows where event_col is not null (OZ+SA only).
    Labels are structured numpy array [(event: bool, time: int)].
    No CalibratedClassifierCV wrapper — survival models are not calibrated.
    Serializes to model_dir/<model_name>.joblib.
    """
    df = pl.read_parquet(enriched_path)
    df = df.filter(pl.col(event_col).is_not_null())

    df = _fill_missing_cols(df, cat_cols, num_cols)
    all_cols = cat_cols + num_cols
    X = df.select(all_cols).to_pandas()

    events = df[event_col].cast(pl.Boolean).to_numpy()
    times = df[time_col].cast(pl.Int32).to_numpy()
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    estimator = GradientBoostingSurvivalAnalysis(random_state=42)
    pipe = build_pipeline(cat_cols=cat_cols, num_cols=num_cols, estimator=estimator)
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")
    return len(df)
```

**Step 3: Add `evaluate_survival()` function**

Add after `train_survival()`:

```python
def evaluate_survival(
    enriched_path: Path,
    time_col: str,
    event_col: str,
    cat_cols: list[str],
    num_cols: list[str],
    *,
    cv: int = 5,
    year_col: str | None = "year_submitted",
) -> dict[str, float | int]:
    """Cross-validate survival model. Returns concordance_index_mean/std and n.

    Uses TimeSeriesSplit when year_col is present (temporal CV to avoid leakage).
    Uses sksurv.metrics.concordance_index_censored for scoring each fold.
    """
    df = pl.read_parquet(enriched_path).filter(pl.col(event_col).is_not_null())
    df = _fill_missing_cols(df, cat_cols, num_cols)

    if year_col is not None and year_col in df.columns:
        df = df.sort(year_col)

    effective_cv = min(cv, len(df) - 1)
    cv_obj: TimeSeriesSplit | KFold = (
        TimeSeriesSplit(n_splits=effective_cv)
        if year_col is not None and year_col in df.columns
        else KFold(effective_cv)
    )

    all_cols = cat_cols + num_cols
    X = df.select(all_cols).to_pandas()
    events = df[event_col].cast(pl.Boolean).to_numpy()
    times = df[time_col].cast(pl.Int32).to_numpy()
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    estimator = GradientBoostingSurvivalAnalysis(random_state=42)
    pipeline = build_pipeline(cat_cols, num_cols, estimator)

    ci_scores: list[float] = []
    for train_idx, test_idx in cv_obj.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y[train_idx], y[test_idx]
        try:
            pipeline.fit(X_train, y_train)
            risk_scores = pipeline.predict(X_test)
            ci, *_ = concordance_index_censored(
                y_test["event"], y_test["time"], risk_scores
            )
            ci_scores.append(ci)
        except Exception as exc:
            logger.warning("Survival CV fold failed: %s", exc)
            ci_scores.append(float("nan"))

    non_nan = [s for s in ci_scores if not np.isnan(s)]
    return {
        "concordance_index_mean": float(np.mean(non_nan)) if non_nan else float("nan"),
        "concordance_index_std": float(np.std(non_nan)) if non_nan else float("nan"),
        "n": len(df),
    }
```

**Step 4: Update `train_all()` to call survival training**

In `train_all()`, find the comment line:

```python
    # Gate each model: mark production_ready based on metric thresholds.
```

Insert the survival model block **before** that line:

```python
    # --- Optional: survival model for dev_days_to_decision ---
    # Only train if enriched dev parquet has dev_days_observed column (AIC scraped)
    if dev_path.exists():
        _dev_df_cols = pl.read_parquet(dev_path).columns
        if "dev_days_observed" in _dev_df_cols:
            surv_count = train_survival(
                enriched_path=dev_path,
                time_col="dev_days_observed",
                event_col="dev_decision_event",
                cat_cols=DEV_CAT_COLS,
                num_cols=DEV_NUM_COLS,
                model_name="dev_days_to_decision",
                model_dir=model_dir,
            )
            counts["dev_days_to_decision"] = surv_count
            surv_eval = evaluate_survival(
                enriched_path=dev_path,
                time_col="dev_days_observed",
                event_col="dev_decision_event",
                cat_cols=DEV_CAT_COLS,
                num_cols=DEV_NUM_COLS,
            )
            metrics["dev_days_to_decision"] = surv_eval
```

Then update the `production_ready` gating block. Find:

```python
    reg_model_names = {job[4] for job in jobs if job[5]}
    for name, m in metrics.items():
        if name in reg_model_names:
            m["production_ready"] = bool(m.get("r2_mean", float("nan")) >= 0.0)
        else:
            m["production_ready"] = bool(m.get("roc_auc_mean", 0.0) >= 0.65)
```

Replace with:

```python
    reg_model_names = {job[4] for job in jobs if job[5]}
    for name, m in metrics.items():
        if name == "dev_days_to_decision":
            # Survival model: gate on Harrell's c-index >= 0.65
            m["production_ready"] = bool(
                m.get("concordance_index_mean", float("nan")) >= 0.65
            )
        elif name in reg_model_names:
            m["production_ready"] = bool(m.get("r2_mean", float("nan")) >= 0.0)
        else:
            m["production_ready"] = bool(m.get("roc_auc_mean", 0.0) >= 0.65)
```

**Step 5: Run the survival tests**

```bash
uv run pytest tests/analytics/test_train.py -k "survival" -v
```

Expected: all 4 survival tests pass.

**Step 6: Run full test suite**

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
```

Expected: all tests pass, lint clean.

**Step 7: Commit**

```bash
git add src/zoneto/analytics/train.py tests/analytics/test_train.py
git commit -m "feat: add train_survival(), evaluate_survival(), wire into train_all()"
```

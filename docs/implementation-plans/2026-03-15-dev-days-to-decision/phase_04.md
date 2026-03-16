# Phase 4: Scoring Update + Importance Registry

**Design phase:** Phase 5

**Goal:** Add `_predict_survival_median()` to `score.py`. Handle the survival model separately in `score_all()` and `score_one()`. Register the new model in `importance.py`.

**Key design note:** The survival model is handled separately from `_DEV_MODELS` (optional, like the permit model). It is NOT added to the `_DEV_MODELS` list. This avoids changing the is_reg boolean handling and keeps the survival logic isolated.

---

### Task 1: Write failing tests for survival scoring

**Files:**
- Modify: `tests/analytics/test_score.py`

**Context:** The current `_make_dev_enriched()` in `test_score.py` (line ~89) has these columns including `postal_fsa`. Update it to add the three new survival columns. Also need a helper to train a dummy survival model.

**Step 1: Add numpy and sksurv imports to test_score.py**

At the top of `tests/analytics/test_score.py`, add after the existing sklearn imports:

```python
import numpy as np
from sksurv.ensemble import GradientBoostingSurvivalAnalysis
```

**Step 2: Update `_make_dev_enriched()` to include survival columns**

Find `_make_dev_enriched()` (line ~89) and replace it:

```python
def _make_dev_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "application_type": ["OZ", "SA"],
            "ward_number": ["Ward 1", "Ward 2"],
            "zoning_class": ["RS", None],
            "secondary_plan_name": [None, "Midtown"],
            "postal_fsa": ["M5V", "M4K"],
            "year_submitted": [2021, 2022],
            "in_heritage_register": [0, 1],
            "in_heritage_district": [0, 0],
            "in_secondary_plan": [0, 1],
            "has_community_meeting": [1, 0],
            "has_parent_application": [0, 1],
            "is_combined_application": [1, 0],
            "ward_pct_renters": [45.5, 50.2],
            "ward_median_income": [75000.0, 80000.0],
            "ward_pop_density": [3500.0, 4200.0],
            "ward_pct_detached": [25.5, 20.0],
            "dev_approved": [1, 0],
            "dev_appealed": [0, 1],
            "dev_decision_event": [1, 0],
            "dev_days_observed": [525, 800],
            "dev_days_to_decision": [525, None],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")
```

**Step 3: Add a helper to train a dummy survival model**

Add after `_train_dummy_model()`:

```python
def _train_dummy_survival_model(
    model_dir: Path,
    model_name: str,
    cat_cols: list[str],
    num_cols: list[str],
) -> None:
    """Train a minimal GradientBoostingSurvivalAnalysis and save as .joblib."""
    import pandas as pd

    n = 20
    X = pd.DataFrame({c: [str(i % 3) for i in range(n)] for c in cat_cols})
    X[num_cols] = pd.DataFrame(
        np.random.default_rng(1).integers(0, 5, size=(n, len(num_cols))).astype(float),
        columns=num_cols,
    )
    events = np.array([True, False] * 10)
    times = np.array([365 + i * 20 for i in range(n)], dtype=np.int32)
    y = np.array(
        list(zip(events, times)),
        dtype=[("event", bool), ("time", np.int32)],
    )

    pipe = build_pipeline(
        cat_cols=cat_cols,
        num_cols=num_cols,
        estimator=GradientBoostingSurvivalAnalysis(random_state=0),
    )
    pipe.fit(X, y)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, model_dir / f"{model_name}.joblib")
```

**Step 4: Update `_setup_models()` to include the survival model**

Find `_setup_models()` and add before `return model_dir`:

```python
    _train_dummy_survival_model(
        model_dir, "dev_days_to_decision", DEV_CAT_COLS, DEV_NUM_COLS
    )
```

**Step 5: Add scoring tests**

Add at the end of `tests/analytics/test_score.py`:

```python
def test_score_all_writes_pred_dev_days_to_decision(
    tmp_path: Path,
) -> None:
    """score_all() appends pred_dev_days_to_decision when model exists."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = _setup_models(tmp_path)

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_days_to_decision" in df.columns
    assert df["pred_dev_days_to_decision"].dtype in (pl.Float32, pl.Float64)


def test_score_all_skips_survival_model_when_absent(
    tmp_path: Path,
) -> None:
    """score_all() runs without pred_dev_days_to_decision when model .joblib absent."""
    _make_dev_enriched(tmp_path)
    _make_coa_enriched(tmp_path)
    model_dir = tmp_path / "models"
    # Only non-survival models
    _train_dummy_model(model_dir, "dev_applications_appealed", DEV_CAT_COLS, DEV_NUM_COLS)
    _train_dummy_model(model_dir, "coa_approved", COA_CAT_COLS, COA_NUM_COLS)
    _train_dummy_model(
        model_dir, "coa_days_to_approval", COA_CAT_COLS, COA_NUM_COLS, regressor=True
    )

    score_all(data_dir=tmp_path, model_dir=model_dir)

    df = pl.read_parquet(tmp_path / "scores" / "dev_applications.parquet")
    assert "pred_dev_days_to_decision" not in df.columns


def test_score_one_returns_pred_dev_days_to_decision(
    tmp_path: Path,
) -> None:
    """score_one() returns pred_dev_days_to_decision for dev_applications source."""
    model_dir = _setup_models(tmp_path)
    features = {col: 0 for col in DEV_CAT_COLS + DEV_NUM_COLS}
    features["application_type"] = "OZ"

    result = score_one("dev_applications", features, model_dir=model_dir)

    assert "pred_dev_days_to_decision" in result
    assert isinstance(result["pred_dev_days_to_decision"], float)
```

**Step 6: Check what `_make_coa_enriched()` looks like in test_score.py**

Verify `_make_coa_enriched()` exists in `tests/analytics/test_score.py`. If it does not exist, add:

```python
def _make_coa_enriched(tmp_path: Path) -> None:
    df = pl.DataFrame(
        {
            "application_type": ["Minor Variance", "Consent"],
            "sub_type": ["A", "B"],
            "ward_number": ["1", "2"],
            "zoning_designation": ["RS", None],
            "planning_district": ["Toronto & East York", "North York"],
            "work_type": ["Construction", "Change of Use"],
            "year_submitted": [2021, 2022],
            "coa_approved": [1, 0],
            "coa_days_to_approval": [90.0, None],
        }
    )
    out = tmp_path / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "coa.parquet")
```

**Step 7: Run tests to verify they fail**

```bash
uv run pytest tests/analytics/test_score.py -k "days_to_decision" -v
```

Expected: tests fail — `score_all()` doesn't handle survival model yet.

---

### Task 2: Implement survival scoring in score.py

**Files:**
- Modify: `src/zoneto/analytics/score.py`

**Step 1: Add numpy import**

At the top of `src/zoneto/analytics/score.py`, add:

```python
import numpy as np
```

**Step 2: Add `_predict_survival_median()`**

After `_predict_regressor()` (line ~51), add:

```python
def _predict_survival_median(pipe: Any, X: pd.DataFrame, pred_col: str) -> dict[str, list]:
    """Extract median survival time (time at which S(t) crosses 0.5).

    Falls back to max observed time if the survival function never crosses 0.5
    within the fitted range (right-censored upper tail).

    IMPORTANT: sklearn Pipeline does not proxy predict_survival_function().
    We preprocess X through all pipeline steps except the last (estimator),
    then call predict_survival_function() on the estimator directly.
    """
    # Preprocess X through all pipeline steps except the final estimator
    X_transformed = pipe[:-1].transform(X)
    # Call predict_survival_function on the estimator (last pipeline step)
    survival_fns = pipe[-1].predict_survival_function(X_transformed)
    medians: list[float] = []
    for fn in survival_fns:
        times = fn.x
        probs = fn.y
        crossing = times[probs <= 0.5]
        if len(crossing) > 0:
            medians.append(float(crossing[0]))
        else:
            medians.append(float(times[-1]))
    return {pred_col: medians}
```

**Step 3: Add survival model handling to `score_all()`**

Find the end of the dev_applications section in `score_all()`:

```python
    df_dev_scored = df_dev.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )
    df_dev_scored.write_parquet(scores_dir / "dev_applications.parquet")
```

Replace with:

```python
    # Survival model (optional — skip if .joblib absent)
    _surv_model_path = model_dir / "dev_days_to_decision.joblib"
    if _surv_model_path.exists():
        _surv_pipe = _load(model_dir, "dev_days_to_decision")
        extra.update(_predict_survival_median(_surv_pipe, X_dev, "pred_dev_days_to_decision"))

    df_dev_scored = df_dev.with_columns(
        [pl.Series(name=k, values=v) for k, v in extra.items()]
    )
    df_dev_scored.write_parquet(scores_dir / "dev_applications.parquet")
```

**Step 4: Update `score_one()` to handle the survival model**

Replace the entire `score_one()` function with:

```python
def score_one(
    source: str,
    features: dict[str, Any],
    model_dir: Path = Path("models"),
) -> dict[str, Any]:
    """Score a single application dict. Returns prediction dict.

    source must be 'dev_applications', 'coa', or 'permits_cleared'.
    """
    if source == "dev_applications":
        models = _DEV_MODELS
        all_cols = DEV_CAT_COLS + DEV_NUM_COLS
    elif source == "coa":
        models = _COA_MODELS
        all_cols = COA_CAT_COLS + COA_NUM_COLS
    elif source == "permits_cleared":
        models = _PERMIT_MODELS
        all_cols = PERMIT_CAT_COLS + PERMIT_NUM_COLS
    else:
        raise ValueError(
            f"Unknown source: {source!r}. Must be 'dev_applications', 'coa',"
            " or 'permits_cleared'."
        )

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

    # Survival model for dev_applications (optional — skip if .joblib absent)
    if source == "dev_applications":
        _surv_path = model_dir / "dev_days_to_decision.joblib"
        if _surv_path.exists():
            _surv_pipe = _load(model_dir, "dev_days_to_decision")
            _surv_preds = _predict_survival_median(_surv_pipe, X, "pred_dev_days_to_decision")
            result["pred_dev_days_to_decision"] = float(
                _surv_preds["pred_dev_days_to_decision"][0]
            )

    return result
```

**Step 5: Run scoring tests**

```bash
uv run pytest tests/analytics/test_score.py -k "days_to_decision" -v
```

Expected: all 3 new tests pass.

**Step 6: Run full test suite**

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
```

Expected: all tests pass, lint clean.

**Step 7: Commit scoring changes**

```bash
git add src/zoneto/analytics/score.py tests/analytics/test_score.py
git commit -m "feat: add survival scoring _predict_survival_median() to score_all/score_one"
```

---

### Task 3: Register survival model in importance.py with compatible scoring

**Files:**
- Modify: `src/zoneto/analytics/importance.py`

**Context:** Two issues with the survival model in importance.py:
1. `_MODEL_META` needs an entry for `dev_days_to_decision`.
2. The existing builtin importance uses `_predictors` (HistGradientBoosting-specific). `GradientBoostingSurvivalAnalysis` uses plain `GradientBoosting` which exposes public `feature_importances_` instead.
3. Permutation importance uses `"r2"` scoring for regression models, but the survival model needs a concordance-based scorer. Instead of implementing a custom scorer, we gate permutation importance for survival models behind a clear error.

**Step 1: Add entry to `_MODEL_META`**

In `src/zoneto/analytics/importance.py`, add to `_MODEL_META`:

```python
    "dev_days_to_decision": (
        "dev_applications",
        "dev_days_observed",
        DEV_CAT_COLS,
        DEV_NUM_COLS,
    ),
```

**Step 2: Update `feature_importance()` to handle the survival model**

Find the `if builtin:` block (line ~111). Before the `if builtin:` check, add a survival model guard:

```python
    # Survival model uses GradientBoostingSurvivalAnalysis which has public
    # feature_importances_ but does not support permutation importance with
    # standard sklearn scorers. Raise a clear error for permutation mode.
    _is_survival = model_name == "dev_days_to_decision"
    if _is_survival and not builtin:
        raise ValueError(
            "Permutation importance is not supported for the survival model "
            "'dev_days_to_decision'. Use --builtin for gain-based importance."
        )
```

Then update the `if builtin:` block to handle the survival model's `feature_importances_` (which is a public attribute, unlike HistGradientBoosting's private `_predictors`):

```python
    if builtin:
        actual_pipe = pipe
        if isinstance(actual_pipe, CalibratedClassifierCV):
            actual_pipe = actual_pipe.calibrated_classifiers_[0].estimator
        estimator = actual_pipe.named_steps["estimator"]
        if _is_survival:
            # GradientBoostingSurvivalAnalysis exposes public feature_importances_
            importances = estimator.feature_importances_
        else:
            importances = _gain_importances(estimator, len(all_cols))
        result = pl.DataFrame(
            {
                "feature": all_cols,
                "importance_mean": importances.tolist(),
                "importance_std": [0.0] * len(all_cols),
            }
        )
```

The `else:` (permutation) branch remains unchanged (the survival guard above already raises before reaching it).

**Step 3: Run tests**

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
```

Expected: all tests pass.

**Step 4: Commit**

```bash
git add src/zoneto/analytics/importance.py
git commit -m "feat: register dev_days_to_decision in importance.py; handle survival builtin importance"
```

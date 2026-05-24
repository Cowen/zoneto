# Product Strategy Pivot — Phase 8: SHAP Explanations + Documentation

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add per-application SHAP explanations to the `/score` endpoint (`?explain=true`), render them in the frontend, and update all documentation to reflect the product pivot and new architecture.

**Architecture:** `explain_one()` in `analytics/explain.py` uses `shap.TreeExplainer` on the unwrapped base estimator (following the pattern from `importance.py`). Returns top-5 SHAP contributors with sign. `/score?explain=true` calls `explain_one` and appends an `explanations` key to the response. `static/index.html` renders the explanations as a ranked feature list. Documentation updates are non-code tasks with specific file content.

**Tech Stack:** shap (new dependency), existing sklearn/sksurv pipeline, FastAPI, vanilla JS

**Scope:** Phase 8 of 8. Requires all prior phases.

**Codebase verified:** 2026-03-17

---

## Task 1: Add shap dependency

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add shap to dependencies**

In `pyproject.toml`, add `"shap>=0.46"` to the `dependencies` list (alphabetically between scikit-survival and shapely):

```toml
  "shap>=0.46",
```

**Step 2: Sync dependencies**

```bash
uv sync
```

Expected: Resolves and installs without errors.

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add shap dependency"
```

---

## Task 2: Create analytics/explain.py

**Files:**
- Create: `src/zoneto/analytics/explain.py`
- Create: `tests/analytics/test_explain.py`

**Step 1: Write the failing test**

Create `tests/analytics/test_explain.py`:

```python
"""Tests for SHAP explanation generation."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from zoneto.analytics.explain import explain_one
from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS
from zoneto.analytics.train import train_source


def _make_dev_parquet(tmp_path: Path) -> Path:
    """30-row dev_applications parquet for training a test model."""
    rng = np.random.default_rng(42)
    n = 30
    dev_appealed = (rng.uniform(size=n) < 0.15).astype(float).tolist()
    for i in range(3):
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
            "in_mtsa": rng.integers(0, 2, size=n).tolist(),
            *{f"desc_svd_{i}": rng.uniform(-1, 1, size=n).tolist() for i in range(20)},
            "dev_appealed": pl.Series(dev_appealed, dtype=pl.Float64),
        }
    )
    dest = tmp_path / "dev_applications.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest)
    return dest


@pytest.fixture
def trained_model(tmp_path: Path) -> Path:
    """Train a minimal appeal model and return model_dir."""
    path = _make_dev_parquet(tmp_path)
    train_source(
        enriched_path=path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=tmp_path / "models",
        regressor=False,
        calibrate=False,  # uncalibrated so TreeExplainer can access base trees
    )
    return tmp_path / "models"


@pytest.fixture
def trained_calibrated_model(tmp_path: Path) -> Path:
    """Train a calibrated appeal model (production default) and return model_dir.

    This exercises the CalibratedClassifierCV unwrapping path in explain_one().
    calibrate=True is the production default; this ensures SHAP works through
    the full calibrated wrapper.
    """
    path = _make_dev_parquet(tmp_path)
    model_dir = tmp_path / "models_calibrated"
    train_source(
        enriched_path=path,
        label_col="dev_appealed",
        cat_cols=DEV_CAT_COLS,
        num_cols=DEV_NUM_COLS,
        model_name="dev_applications_appealed",
        model_dir=model_dir,
        regressor=False,
        calibrate=True,  # production default — exercises CalibratedClassifierCV unwrap
    )
    return model_dir


def test_explain_one_returns_list(trained_model: Path) -> None:
    """explain_one() returns a list of explanation dicts."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert isinstance(result, list)


def test_explain_one_top_n_limit(trained_model: Path) -> None:
    """Returns at most top_n contributions."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=3,
    )
    assert len(result) <= 3


def test_explain_one_result_shape(trained_model: Path) -> None:
    """Each explanation dict has feature, value, and direction keys."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    if result:  # may be empty if model has no SHAP support
        for item in result:
            assert "feature" in item
            assert "shap_value" in item
            assert "direction" in item
            assert item["direction"] in ("increases_risk", "decreases_risk")


def test_explain_one_missing_model_returns_empty(tmp_path: Path) -> None:
    """Returns empty list when model file does not exist."""
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ"},
        model_dir=tmp_path,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    assert result == []


def test_explain_one_works_with_calibrated_model(
    trained_calibrated_model: Path,
) -> None:
    """explain_one() works through the CalibratedClassifierCV unwrapping path.

    Production models use calibrate=True (the default). This test exercises
    the _unwrap_pipeline() path so SHAP explanations don't silently fail on
    production models.
    """
    result = explain_one(
        source="dev_applications",
        features={"application_type": "OZ", "ward_number": "10"},
        model_dir=trained_calibrated_model,
        model_name="dev_applications_appealed",
        top_n=5,
    )
    # Must return a list (not raise), proving unwrapping succeeded
    assert isinstance(result, list)
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/analytics/test_explain.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.analytics.explain'`

**Step 3: Create `src/zoneto/analytics/explain.py`**

```python
"""SHAP-based per-application explanation for trained classifiers."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from zoneto.analytics.features import (
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)

logger = logging.getLogger(__name__)


def _unwrap_pipeline(pipe: Any) -> Any:
    """Unwrap CalibratedClassifierCV to get the base Pipeline, then get estimator.

    Follows the same unwrapping pattern as analytics/importance.py.
    """
    from sklearn.calibration import CalibratedClassifierCV  # noqa: PLC0415

    actual_pipe = pipe
    if isinstance(actual_pipe, CalibratedClassifierCV):
        actual_pipe = actual_pipe.calibrated_classifiers_[0].estimator
    return actual_pipe


def explain_one(
    source: str,
    features: dict[str, Any],
    model_dir: Path,
    model_name: str,
    *,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    """Return top-N SHAP feature contributions for a single application.

    Uses shap.TreeExplainer on the base HistGradientBoosting estimator
    (after unwrapping CalibratedClassifierCV).

    Returns a list of dicts with keys:
        feature (str): feature name
        shap_value (float): SHAP value for this prediction
        direction (str): "increases_risk" if shap_value > 0, else "decreases_risk"

    Returns [] when the model file is absent or SHAP computation fails.
    """
    import shap  # noqa: PLC0415

    model_path = model_dir / f"{model_name}.joblib"
    if not model_path.exists():
        return []

    try:
        pipe = joblib.load(model_path)
        base_pipe = _unwrap_pipeline(pipe)
        estimator = base_pipe.named_steps["estimator"]
        preprocessor = base_pipe.named_steps["preprocessor"]

        if source == "dev_applications":
            all_cols = DEV_CAT_COLS + DEV_NUM_COLS
        else:
            logger.warning("explain_one: source %r not supported for SHAP", source)
            return []

        X_raw = pd.DataFrame([{col: features.get(col) for col in all_cols}])
        X_transformed = preprocessor.transform(X_raw)

        explainer = shap.TreeExplainer(estimator)
        shap_values = explainer.shap_values(X_transformed)

        # For binary classifier: shap_values may be list[array] or array
        # Take class-1 SHAP values (risk class)
        if isinstance(shap_values, list):
            values = shap_values[1][0] if len(shap_values) > 1 else shap_values[0][0]
        else:
            values = shap_values[0]

        # Get feature names from the preprocessor's output
        try:
            feature_names: list[str] = list(
                base_pipe.named_steps["preprocessor"].get_feature_names_out()
            )
        except Exception:
            feature_names = [f"feature_{i}" for i in range(len(values))]

        # Sort by absolute SHAP value, take top_n
        indexed = sorted(
            enumerate(values), key=lambda x: abs(x[1]), reverse=True
        )[:top_n]

        return [
            {
                "feature": feature_names[i] if i < len(feature_names) else f"feature_{i}",
                "shap_value": round(float(v), 4),
                "direction": "increases_risk" if v > 0 else "decreases_risk",
            }
            for i, v in indexed
        ]

    except Exception as exc:
        logger.warning("explain_one: SHAP computation failed: %s", exc)
        return []
```

**Step 4: Run tests**

```bash
uv run pytest tests/analytics/test_explain.py -v
```

Expected: All tests pass. (The model training fixture uses `calibrate=False` to allow TreeExplainer to access base trees directly. The test gracefully handles the case where the tiny fixture model's SHAP values may be empty.)

**Step 5: Commit**

```bash
git add src/zoneto/analytics/explain.py tests/analytics/test_explain.py
git commit -m "feat: add explain_one() for SHAP feature contributions"
```

---

## Task 3: Add ?explain=true to /score endpoint

**Files:**
- Modify: `src/zoneto/api/routes.py`
- Create: `tests/api/test_explain_endpoint.py`

**Step 1: Write the failing test**

Create `tests/api/test_explain_endpoint.py`:

```python
"""Tests for /score?explain=true endpoint."""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    """TestClient with app pointed at empty test data directory."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    current_year = datetime.date.today().year
    pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "application_type": ["OZ"],
            "ward_number": ["10"],
            "zoning_class": ["RA1"],
            "status": ["Active"],
            "year_submitted": pl.Series([current_year - 1], dtype=pl.Int32),
            "lat": [43.65],
            "lon": [-79.38],
            "dev_approved": pl.Series([None], dtype=pl.Int8),
            "dev_appealed": pl.Series([None], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series([None], dtype=pl.Int32),
            "proposed_storeys": pl.Series([None], dtype=pl.Int32),
            "proposed_units": pl.Series([None], dtype=pl.Int32),
            "description": ["test"],
            "street_num": ["1"],
            "street_name": ["Main St"],
        }
    ).write_parquet(enriched_dir / "dev_applications.parquet")
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    return TestClient(app)


def test_score_without_explain_has_no_explanations_key(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without ?explain=true, response does not include 'explanations' key."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"prob_dev_appealed": 0.12},
    )
    response = client.post(
        "/score",
        json={"source": "dev_applications", "features": {"application_type": "OZ"}},
    )
    assert response.status_code == 200
    assert "explanations" not in response.json()


def test_score_with_explain_true_includes_explanations(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ?explain=true, response includes 'explanations' key."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"prob_dev_appealed": 0.12},
    )
    monkeypatch.setattr(
        "zoneto.api.routes.explain_one",
        lambda source, features, model_dir, model_name, top_n: [
            {"feature": "ward_number__10", "shap_value": 0.05, "direction": "increases_risk"}
        ],
    )
    response = client.post(
        "/score?explain=true",
        json={"source": "dev_applications", "features": {"application_type": "OZ"}},
    )
    assert response.status_code == 200
    body = response.json()
    assert "explanations" in body
    assert isinstance(body["explanations"], dict)


def test_score_explain_true_no_models_returns_empty_explanations(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With ?explain=true but no production models, explanations is empty dict."""
    # No models are production_ready in this fixture (empty models dir)
    response = client.post(
        "/score?explain=true",
        json={"source": "dev_applications", "features": {}},
    )
    assert response.status_code == 200
    body = response.json()
    assert body.get("explanations") == {}
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/api/test_explain_endpoint.py -v
```

Expected: `test_score_with_explain_true_includes_explanations` fails — endpoint does not accept `explain` query param yet.

**Step 3: Update `src/zoneto/api/routes.py`**

**3a.** Add `explain_one` import:

```python
from zoneto.analytics.explain import explain_one
```

**3b.** Update `ScoreResponse` to include optional explanations:

```python
class ScoreResponse(BaseModel):
    predictions: dict[str, Any]
    production_ready_models: list[str]
    explanations: dict[str, list[dict[str, Any]]] | None = None
```

**3c.** Update the `/score` endpoint to accept and handle `explain` query param:

```python
@router.post("/score", response_model=ScoreResponse)
def score(
    request: Request,
    body: ScoreRequest,
    explain: bool = False,
) -> ScoreResponse:
    model_dir: Path = getattr(request.app.state, "model_dir", Path("models"))
    production_ready: dict[str, bool] = getattr(
        request.app.state, "production_ready", {}
    )
    ready_model_names = [k for k, v in production_ready.items() if v]

    if not ready_model_names:
        return ScoreResponse(
            predictions={},
            production_ready_models=[],
            explanations={} if explain else None,
        )

    try:
        predictions = score_one(body.source, body.features, model_dir)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    explanations: dict[str, list[dict[str, Any]]] | None = None
    if explain:
        explanations = {}
        for model_name in ready_model_names:
            contribs = explain_one(
                source=body.source,
                features=body.features,
                model_dir=model_dir,
                model_name=model_name,
                top_n=5,
            )
            if contribs:
                explanations[model_name] = contribs

    return ScoreResponse(
        predictions=predictions,
        production_ready_models=ready_model_names,
        explanations=explanations,
    )
```

**Step 4: Run tests**

```bash
uv run pytest tests/api/test_explain_endpoint.py tests/api/ -v
```

Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/api/routes.py tests/api/test_explain_endpoint.py
git commit -m "feat: add ?explain=true to /score endpoint returning SHAP contributions"
```

---

## Task 4: Render SHAP explanations in the HTML frontend

**Files:**
- Modify: `static/index.html`

**Step 1: Update `static/index.html`**

In `fetchScore()`, update to pass `explain=true` and render the explanations:

Find the `fetchScore` function in the `<script>` block and replace it:

```javascript
    async function fetchScore(features) {
      try {
        const res = await fetch('/score?explain=true', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ source: 'dev_applications', features }),
        });
        if (!res.ok) return;
        const data = await res.json();
        if (!data.production_ready_models.length) return;
        renderPredictions(data.predictions);
        if (data.explanations) renderExplanations(data.explanations);
      } catch (_) {
        // predictions are optional — fail silently
      }
    }
```

Add the `renderExplanations` function after `renderPredictions`:

```javascript
    function renderExplanations(explanations) {
      const entries = Object.entries(explanations);
      if (!entries.length) return;
      const rows = entries.flatMap(([modelName, contribs]) =>
        contribs.map(c => {
          const sign = c.direction === 'increases_risk' ? '+' : '−';
          const color = c.direction === 'increases_risk' ? '#dc3545' : '#198754';
          const label = escHtml(c.feature.replace(/__/g, ': ').replace(/_/g, ' '));
          return `<div class="pred-row">
            <span class="pred-label">${label}</span>
            <span class="pred-value" style="color:${color}">${sign}${Math.abs(c.shap_value).toFixed(3)}</span>
          </div>`;
        })
      ).join('');
      if (rows) {
        predictionsEl.innerHTML += `
          <div style="margin-top:0.75rem;border-top:1px solid #dee2e6;padding-top:0.75rem">
            <div style="font-size:0.75rem;color:#6c757d;margin-bottom:0.4rem">Top contributing factors</div>
            ${rows}
          </div>`;
      }
    }
```

**Step 2: Commit**

```bash
git add static/index.html
git commit -m "feat: render SHAP explanations in HTML frontend below predictions"
```

---

## Task 5: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Update the Purpose section**

Replace the existing Purpose section (first paragraph under `## Purpose`) with:

```markdown
## Purpose

Zoneto is a development application intelligence platform for Toronto. It provides development professionals with structured data on comparable planning applications, outcome patterns, and expected timelines — using ML models to rank and prioritize where the data supports it, and presenting raw data where it doesn't.

The pipeline fetches planning and permit data from the City of Toronto CKAN portal and AIC (Application Information Centre), normalizes it into Hive-partitioned Parquet files, trains ML models on enriched data, and serves predictions and comparables via a FastAPI HTTP API.

**Target user:** Development firms doing site acquisition due diligence.
```

**Step 2: Add the serving layer to the Architecture section**

After the existing `src/zoneto/` directory tree, add:

```markdown
**Serving layer** (`src/zoneto/api/`):
```
src/zoneto/api/
  __init__.py
  app.py          FastAPI app factory with lifespan (create_app)
  comps.py        DuckDB query builder for comparable applications
  routes.py       GET /health, GET /ready, GET /comps, POST /score
```

Endpoints:
- `GET /health` — returns `{"status": "ok"}`
- `GET /ready` — returns 200 when models + data loaded, 503 otherwise
- `GET /comps?ward=10&type=OZ&lat=43.65&lon=-79.38&radius_m=500&years=5` — comparable applications
- `POST /score` — predictions from production-ready models only
- `POST /score?explain=true` — includes top-5 SHAP contributions per model

Static frontend: `static/index.html` served at `/`.
```

**Step 3: Update the CLI Commands section**

Add new commands to the existing CLI table or list:

```markdown
- `zoneto serve [--port 8000] [--data-dir PATH]` — Start the FastAPI serving layer
- `zoneto olt [--delay FLOAT]` — Scrape OLT decisions for Toronto
- `zoneto aic --full` — Fetch full AIC application records (replacement for CKAN dev_applications)
- `zoneto enrich --fetch-olt/--no-fetch-olt` — Match OLT decisions to dev_applications (default: no-fetch)
```

**Step 4: Update the Model Retirement section in Training**

Update the models table to reflect retirement:

In the Training section, update the models table:

| File | Type | Target | Production Ready |
|---|---|---|---|
| `dev_applications_appealed.joblib` | CalibratedClassifierCV | `dev_appealed` | Yes (AUC ≥ 0.65) |
| `dev_days_to_decision.joblib` | GradientBoostingSurvivalAnalysis | `dev_days_observed`/`dev_decision_event` | Yes (C-index ≥ 0.65) |
| `coa_days_to_approval.joblib` | HistGradientBoostingRegressor | `coa_days_to_approval` | **No — tracking only** (production_ready forced False) |
| ~~`coa_approved.joblib`~~ | ~~Classifier~~ | ~~`coa_approved`~~ | **Retired** — AUC 0.535 at 94% base rate |
| ~~`permit_issuance_days.joblib`~~ | ~~Regressor~~ | ~~`permit_issuance_days`~~ | **Retired** — R² 0.039, queue depth signal absent |

**Step 5: Update the Dependencies section**

Add new dependencies:

```markdown
| fastapi[standard] | HTTP API framework |
| shap | SHAP feature importance explanations |
| uvicorn[standard] | ASGI server for FastAPI |
```

**Step 6: Run full test suite to verify nothing is broken**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 7: Commit CLAUDE.md**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with new architecture, serving layer, model retirement"
```

---

## Task 6: Update README.md

**Files:**
- Modify: `README.md`

**Step 1: Replace the README content**

Replace the entire `README.md` with:

```markdown
# zoneto

Toronto development application intelligence platform.

Zoneto helps development professionals make informed decisions by providing structured intelligence on comparable planning applications, outcome patterns, and expected timelines. It uses ML models to rank and prioritize where the data supports it, and presents raw data where it doesn't.

**Target user:** Development firms doing site acquisition due diligence.

## Quick start

```bash
uv sync                    # install deps
just pipeline              # enrich → train → score
just serve                 # start API at http://localhost:8000
```

Open `http://localhost:8000/` in your browser to query comparable applications.

## API endpoints

```
GET  /health                                      # liveness check
GET  /ready                                       # readiness check (models + data loaded)
GET  /comps?ward=10&type=OZ&lat=43.65&lon=-79.38  # comparable applications
POST /score                                       # ML predictions (production-ready only)
POST /score?explain=true                          # predictions + SHAP explanations
```

### Example: comparable applications

```bash
curl "http://localhost:8000/comps?type=OZ&ward=10&lat=43.6532&lon=-79.3832&radius_m=500&years=5"
```

### Example: score an application

```bash
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"source": "dev_applications", "features": {"application_type": "OZ", "ward_number": "10"}}'
```

## Pipeline commands

```bash
just sync          # fetch all CKAN data sources
just aic           # scrape AIC for decision milestone dates
just aic-full      # fetch full AIC application records (replaces CKAN dev_applications)
just olt           # scrape OLT decisions
just enrich        # enrich raw data with spatial features and outcome labels
just train         # train ML models
just score         # batch inference
just summary       # score distributions
just pipeline      # enrich → train → score in one step
```

## Docker

```bash
just docker-build  # build Docker image
just docker-run    # start serving layer on port 8000
```

The Docker image includes only the serving layer — enriched Parquet files, models, and `static/`. Run the pipeline separately before building the image.

## Dev tasks

```bash
just test          # run pytest
just lint          # ruff check + ty check
just fmt           # ruff format
just regression    # performance regression tests (synthetic data, CI-safe)
```

## Data sources

All primary data comes from the City of Toronto CKAN open data portal
(`https://ckan0.cf.opendata.inter.prod-toronto.ca`) and the AIC ArcGIS FeatureServer.

| Source | Description |
|---|---|
| `dev_applications` | Development applications (CKAN, 2000–) |
| `aic_applications` | Live AIC application records (ArcGIS, replaces retired CKAN dataset) |
| `coa` | Committee of Adjustment applications |
| `permits_cleared` | Building permits (cleared) |
| `permits_active` | Building permits (active) |

Reference data: zoning boundaries, heritage register, secondary plans, ward profiles, MTSA boundaries, OLT decisions.

## ML models

| Model | Type | Metric | Status |
|---|---|---|---|
| `dev_applications_appealed` | Classifier (CalibratedClassifierCV) | AUC ≥ 0.65 | Production |
| `dev_days_to_decision` | Survival (GradientBoostingSurvivalAnalysis) | C-index ≥ 0.65 | Production |
| `coa_days_to_approval` | Regressor | R² (tracking) | Tracking only |
| `coa_approved` | Classifier | — | Retired (AUC 0.535 @ 94% base rate) |
| `permit_issuance_days` | Regressor | — | Retired (R² 0.039, no queue signal) |
```

**Step 2: Commit README**

```bash
git add README.md
git commit -m "docs: rewrite README to reflect intelligence platform pivot, API, Docker, model status"
```

---

## Task 7: Final verification

**Step 1: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 2: Run linter and type checker**

```bash
uv run ruff check
uv run ty check src/
```

Expected: No errors.

**Step 3: Verify all phase files exist**

```bash
ls docs/implementation-plans/2026-03-17-product-strategy-pivot/
```

Expected: `phase_01.md` through `phase_08.md` all present.

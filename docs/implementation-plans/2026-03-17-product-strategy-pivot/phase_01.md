# Product Strategy Pivot Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Build a FastAPI serving layer that exposes enriched dev_applications data and ML predictions via HTTP endpoints.

**Architecture:** Single FastAPI app factory (`create_app`) with lifespan that loads `metrics.json` production_ready flags. `/comps` queries enriched Parquet via DuckDB at request time. `/score` wraps the existing `score_one()` function. App state carries `data_dir`, `model_dir`, and `production_ready` dict.

**Tech Stack:** FastAPI, uvicorn[standard], DuckDB (already installed), Pydantic (via FastAPI), starlette TestClient (via FastAPI)

**Scope:** Phase 1 of 8

**Codebase verified:** 2026-03-17

---

## Task 1: Add FastAPI and uvicorn dependencies

**Files:**
- Modify: `pyproject.toml` (lines 11–27, dependencies list)

**Step 1: Add dependencies**

In `pyproject.toml`, insert the following two lines into the existing `dependencies` list, in alphabetical order (after `duckdb`, before `httpx` for fastapi; at the end for uvicorn):

```toml
    "fastapi[standard]>=0.115",
    "uvicorn[standard]>=0.30",
```

Do NOT replace the entire list — only add these two lines. The existing entries (including `openpyxl>=3.1.5`) must be preserved.

**Step 2: Sync dependencies**

```bash
uv sync
```

Expected: Resolves and installs without errors.

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add fastapi and uvicorn dependencies"
```

---

## Task 2: Create API package with app factory

**Files:**
- Create: `src/zoneto/api/__init__.py`
- Create: `src/zoneto/api/app.py`
- Create: `tests/api/__init__.py`

**Step 1: Create the package init files**

`src/zoneto/api/__init__.py` — empty file:
```python
```

`tests/api/__init__.py` — empty file:
```python
```

**Step 1b: Create stub `src/zoneto/api/routes.py`**

`app.py` imports `from zoneto.api.routes import router` at the point `create_app()` is called. This stub must exist before any test exercises `create_app()`. It will be replaced with the full implementation in Task 4.

```python
"""API route stubs — replaced by Task 4."""
from fastapi import APIRouter

router = APIRouter()
```

**Step 2: Write the failing test**

Create `tests/api/test_app.py`:

```python
"""Tests for FastAPI app factory."""
from pathlib import Path

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app, _load_production_ready


def test_load_production_ready_missing_file(tmp_path: Path) -> None:
    """Returns empty dict when metrics.json does not exist."""
    result = _load_production_ready(tmp_path)
    assert result == {}


def test_load_production_ready_reads_flags(tmp_path: Path) -> None:
    """Reads production_ready flags from metrics.json."""
    import json
    metrics = {
        "dev_applications_appealed": {"production_ready": True, "n": 100},
        "coa_approved": {"production_ready": False, "n": 50},
    }
    (tmp_path / "metrics.json").write_text(json.dumps(metrics))

    result = _load_production_ready(tmp_path)
    assert result == {"dev_applications_appealed": True, "coa_approved": False}


def test_create_app_returns_fastapi(tmp_path: Path) -> None:
    """create_app returns a FastAPI application."""
    from fastapi import FastAPI
    app = create_app(data_dir=tmp_path, model_dir=tmp_path)
    assert isinstance(app, FastAPI)


def test_app_state_set_on_startup(tmp_path: Path) -> None:
    """Lifespan sets app.state.data_dir and app.state.model_dir."""
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    with TestClient(app) as client:
        assert client.app.state.data_dir == tmp_path
        assert client.app.state.model_dir == tmp_path / "models"
```

**Step 3: Run test to confirm failure**

```bash
uv run pytest tests/api/test_app.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.api'`

**Step 4: Create `src/zoneto/api/app.py`**

```python
"""FastAPI application factory for Zoneto serving layer."""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI


def _load_production_ready(model_dir: Path) -> dict[str, bool]:
    """Load production_ready flags from metrics.json. Returns empty dict if absent."""
    metrics_path = model_dir / "metrics.json"
    if not metrics_path.exists():
        return {}
    with open(metrics_path) as f:
        metrics: dict[str, Any] = json.load(f)
    return {
        name: bool(m.get("production_ready", False))
        for name, m in metrics.items()
    }


def create_app(
    data_dir: Path | None = None,
    model_dir: Path | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    resolved_data_dir = data_dir or Path("data")
    resolved_model_dir = model_dir or Path("models")

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[type-arg]
        app.state.data_dir = resolved_data_dir
        app.state.model_dir = resolved_model_dir
        app.state.production_ready = _load_production_ready(resolved_model_dir)
        app.state.ready = True
        yield

    app = FastAPI(title="Zoneto", version="0.1.0", lifespan=lifespan)

    from zoneto.api.routes import router  # noqa: PLC0415 (deferred to avoid circular import)

    app.include_router(router)
    return app
```

**Step 5: Run tests to confirm they pass**

```bash
uv run pytest tests/api/test_app.py -v
```

Expected: All 4 tests pass.

**Step 6: Commit**

```bash
git add src/zoneto/api/__init__.py src/zoneto/api/app.py src/zoneto/api/routes.py tests/api/__init__.py tests/api/test_app.py
git commit -m "feat: add FastAPI app factory with lifespan state management"
```

---

## Task 3: Create comps.py DuckDB query builder

**Files:**
- Create: `src/zoneto/api/comps.py`
- Create: `tests/api/test_comps.py`

**Step 1: Write the failing test**

Create `tests/api/test_comps.py`:

```python
"""Tests for DuckDB comps query builder."""
from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from zoneto.api.comps import query_comps


@pytest.fixture
def enriched_parquet(tmp_path: Path) -> Path:
    """Minimal enriched dev_applications parquet with known spatial distribution."""
    current_year = datetime.date.today().year
    path = tmp_path / "enriched" / "dev_applications.parquet"
    path.parent.mkdir(parents=True)

    # F001: OZ, ward 10, near (43.65, -79.38), recent
    # F002: OZ, ward 10, near (43.65, -79.38), recent, was appealed
    # F003: SA, ward 11, near, recent
    # F004: OZ, ward 10, far away (Toronto north), recent
    # F005: OZ, ward 10, near, very old (outside 5yr window)
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003", "F004", "F005"],
            "application_type": ["OZ", "OZ", "SA", "OZ", "OZ"],
            "ward_number": ["10", "10", "11", "10", "10"],
            "zoning_class": ["RA1", "RA2", "RM", "RA1", "RA1"],
            "status": ["Approved", "Appealed", "Approved", "Approved", "Approved"],
            "year_submitted": pl.Series(
                [
                    current_year - 1,
                    current_year - 2,
                    current_year - 3,
                    current_year - 1,
                    current_year - 10,
                ],
                dtype=pl.Int32,
            ),
            "lat": [43.650, 43.651, 43.652, 43.700, 43.650],
            "lon": [-79.380, -79.381, -79.382, -79.450, -79.380],
            "dev_approved": pl.Series([1, 1, 1, 1, 1], dtype=pl.Int8),
            "dev_appealed": pl.Series([0, 1, 0, 0, 0], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series([365, 730, 400, 300, 500], dtype=pl.Int32),
            "proposed_storeys": pl.Series([10, 20, None, 5, 8], dtype=pl.Int32),
            "proposed_units": pl.Series([100, 200, None, 50, 80], dtype=pl.Int32),
            "description": ["OZ application"] * 5,
            "street_num": ["100", "200", "300", "400", "500"],
            "street_name": ["King St", "Queen St", "Bloor St", "Yonge St", "Bay St"],
        }
    )
    df.write_parquet(path)
    return path


def test_query_comps_no_filters_respects_years(enriched_parquet: Path) -> None:
    """Default years=5 excludes applications older than 5 years."""
    results = query_comps(enriched_parquet, years=5)
    folderrsns = {r["folderrsn"] for r in results}
    assert "F005" not in folderrsns  # submitted 10 years ago
    assert "F001" in folderrsns


def test_query_comps_by_application_type(enriched_parquet: Path) -> None:
    """Filters by application_type."""
    results = query_comps(enriched_parquet, application_type="SA", years=5)
    assert len(results) == 1
    assert results[0]["application_type"] == "SA"
    assert results[0]["folderrsn"] == "F003"


def test_query_comps_by_ward(enriched_parquet: Path) -> None:
    """Filters by ward_number."""
    results = query_comps(enriched_parquet, ward_number="11", years=5)
    assert len(results) == 1
    assert results[0]["ward_number"] == "11"


def test_query_comps_spatial_excludes_distant(enriched_parquet: Path) -> None:
    """Spatial filter excludes F004 which is ~5 km north."""
    results = query_comps(
        enriched_parquet,
        lat=43.650,
        lon=-79.380,
        radius_m=500,
        years=5,
    )
    folderrsns = {r["folderrsn"] for r in results}
    assert "F004" not in folderrsns
    assert "F001" in folderrsns
    assert "F002" in folderrsns


def test_query_comps_spatial_sorted_by_proximity(enriched_parquet: Path) -> None:
    """When lat/lon provided, results sorted closest-first."""
    results = query_comps(
        enriched_parquet,
        lat=43.650,
        lon=-79.380,
        radius_m=500,
        years=5,
    )
    assert results[0]["folderrsn"] == "F001"  # closest to query point


def test_query_comps_limit(enriched_parquet: Path) -> None:
    """Limit caps result count."""
    results = query_comps(enriched_parquet, years=10, limit=2)
    assert len(results) <= 2


def test_query_comps_empty_when_no_match(enriched_parquet: Path) -> None:
    """Returns empty list when no applications match."""
    results = query_comps(enriched_parquet, application_type="NONEXISTENT", years=5)
    assert results == []


def test_query_comps_result_shape(enriched_parquet: Path) -> None:
    """Each result dict contains expected keys."""
    results = query_comps(enriched_parquet, years=5, limit=1)
    assert len(results) == 1
    rec = results[0]
    for key in (
        "folderrsn",
        "application_type",
        "ward_number",
        "zoning_class",
        "status",
        "year_submitted",
        "lat",
        "lon",
        "dev_approved",
        "dev_appealed",
        "dev_days_to_decision",
        "street_address",
    ):
        assert key in rec, f"Missing key: {key}"
```

**Step 2: Run test to confirm failure**

```bash
uv run pytest tests/api/test_comps.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.api.comps'`

**Step 3: Create `src/zoneto/api/comps.py`**

```python
"""DuckDB query builder for comparable development applications."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import duckdb


def query_comps(
    enriched_path: Path,
    *,
    application_type: str | None = None,
    ward_number: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 500.0,
    years: int = 5,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query comparable development applications from enriched Parquet.

    Returns applications matching filters, sorted by proximity when lat/lon
    provided, otherwise by recency (year_submitted DESC).
    """
    import datetime

    current_year = datetime.date.today().year
    year_cutoff = current_year - years

    # --- build WHERE conditions and positional params ---
    conditions: list[str] = [
        "year_submitted IS NOT NULL",
        f"year_submitted >= {year_cutoff}",
    ]
    params: list[Any] = []

    if application_type is not None:
        conditions.append("application_type = ?")
        params.append(application_type)

    if ward_number is not None:
        conditions.append("CAST(ward_number AS VARCHAR) = ?")
        params.append(str(ward_number))

    # --- spatial bounding box (approximate, safe for < 50 km radius) ---
    distance_expr = "NULL"
    order_by = "year_submitted DESC NULLS LAST"

    if lat is not None and lon is not None:
        lat_delta = radius_m / 111_111.0
        lon_delta = radius_m / (111_111.0 * math.cos(math.radians(lat)))
        lat_min = lat - lat_delta
        lat_max = lat + lat_delta
        lon_min = lon - lon_delta
        lon_max = lon + lon_delta

        conditions.extend(
            [
                "lat IS NOT NULL",
                "lon IS NOT NULL",
                "lat BETWEEN ? AND ?",
                "lon BETWEEN ? AND ?",
            ]
        )
        params.extend([lat_min, lat_max, lon_min, lon_max])
        # squared Euclidean distance in degrees (sufficient for proximity sort)
        distance_expr = f"((lat - {lat}) * (lat - {lat}) + (lon - {lon}) * (lon - {lon}))"
        order_by = "dist_sq ASC"

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            CAST(folderrsn AS VARCHAR)   AS folderrsn,
            application_type,
            CAST(ward_number AS VARCHAR) AS ward_number,
            zoning_class,
            status,
            CAST(year_submitted AS INTEGER) AS year_submitted,
            lat,
            lon,
            CAST(dev_approved AS INTEGER)         AS dev_approved,
            CAST(dev_appealed AS INTEGER)         AS dev_appealed,
            CAST(dev_days_to_decision AS INTEGER) AS dev_days_to_decision,
            CAST(proposed_storeys AS INTEGER)     AS proposed_storeys,
            CAST(proposed_units AS INTEGER)       AS proposed_units,
            description,
            COALESCE(CAST(street_num AS VARCHAR), '') || ' ' ||
                COALESCE(street_name, '')             AS street_address,
            {distance_expr}                           AS dist_sq
        FROM read_parquet('{enriched_path}')
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {limit}
    """

    con = duckdb.connect()
    try:
        result = con.execute(sql, params).df()
        records: list[dict[str, Any]] = result.to_dict(orient="records")
        # drop internal dist_sq column when no spatial filter used
        if lat is None:
            for r in records:
                r.pop("dist_sq", None)
        return records
    finally:
        con.close()
```

**Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/api/test_comps.py -v
```

Expected: All 8 tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/api/comps.py tests/api/test_comps.py
git commit -m "feat: add DuckDB comps query builder with spatial and type filters"
```

---

## Task 4: Create routes.py with all endpoints

**Files:**
- Create: `src/zoneto/api/routes.py`
- Create: `tests/api/test_routes.py`

**Step 1: Write the failing tests**

Create `tests/api/test_routes.py`:

```python
"""Tests for FastAPI endpoint handlers."""
from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from starlette.testclient import TestClient

from zoneto.api.app import create_app


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """Minimal test data directory with enriched dev_applications parquet."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)

    current_year = datetime.date.today().year
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002"],
            "application_type": ["OZ", "SA"],
            "ward_number": ["10", "11"],
            "zoning_class": ["RA1", "RM"],
            "status": ["Approved", "Active"],
            "year_submitted": pl.Series([current_year - 1, current_year - 2], dtype=pl.Int32),
            "lat": [43.650, 43.651],
            "lon": [-79.380, -79.381],
            "dev_approved": pl.Series([1, None], dtype=pl.Int8),
            "dev_appealed": pl.Series([0, None], dtype=pl.Int8),
            "dev_days_to_decision": pl.Series([365, None], dtype=pl.Int32),
            "proposed_storeys": pl.Series([10, None], dtype=pl.Int32),
            "proposed_units": pl.Series([100, None], dtype=pl.Int32),
            "description": ["OZ application", "SA application"],
            "street_num": ["100", "200"],
            "street_name": ["King St", "Queen St"],
        }
    )
    df.write_parquet(enriched_dir / "dev_applications.parquet")
    return tmp_path


@pytest.fixture
def client(data_dir: Path) -> TestClient:
    """TestClient with app pointed at test data directory."""
    app = create_app(data_dir=data_dir, model_dir=data_dir / "models")
    return TestClient(app)


# --- /health ---

def test_health_returns_ok(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- /ready ---

def test_ready_no_enriched_data_returns_503(tmp_path: Path) -> None:
    """503 when enriched/dev_applications.parquet does not exist."""
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    c = TestClient(app, raise_server_exceptions=False)
    response = c.get("/ready")
    assert response.status_code == 503


def test_ready_with_data_returns_200(client: TestClient) -> None:
    response = client.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["data_available"] is True
    assert isinstance(body["models_loaded"], list)


def test_ready_lists_production_ready_models(tmp_path: Path) -> None:
    """Lists only models with production_ready=true."""
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    # create minimal parquet so /ready passes data check
    import polars as pl
    import datetime
    current_year = datetime.date.today().year
    pl.DataFrame({
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
        "description": ["desc"],
        "street_num": ["1"],
        "street_name": ["Main St"],
    }).write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    metrics = {
        "dev_applications_appealed": {"production_ready": True},
        "coa_approved": {"production_ready": False},
    }
    (models_dir / "metrics.json").write_text(json.dumps(metrics))

    app = create_app(data_dir=tmp_path, model_dir=models_dir)
    c = TestClient(app)
    response = c.get("/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["models_loaded"] == ["dev_applications_appealed"]


# --- /comps ---

def test_comps_returns_applications(client: TestClient) -> None:
    response = client.get("/comps")
    assert response.status_code == 200
    body = response.json()
    assert "applications" in body
    assert isinstance(body["applications"], list)
    assert body["total"] == len(body["applications"])


def test_comps_filters_by_type(client: TestClient) -> None:
    response = client.get("/comps?type=OZ")
    assert response.status_code == 200
    apps = response.json()["applications"]
    assert all(a["application_type"] == "OZ" for a in apps)


def test_comps_filters_by_ward(client: TestClient) -> None:
    response = client.get("/comps?ward=10")
    assert response.status_code == 200
    apps = response.json()["applications"]
    assert all(a["ward_number"] == "10" for a in apps)


def test_comps_missing_data_returns_503(tmp_path: Path) -> None:
    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models")
    c = TestClient(app, raise_server_exceptions=False)
    response = c.get("/comps")
    assert response.status_code == 503


# --- /score ---

def test_score_delegates_to_score_one(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POST /score calls score_one and returns its result."""
    monkeypatch.setattr(
        "zoneto.api.routes.score_one",
        lambda source, features, model_dir: {"prob_dev_appealed": 0.15},
    )
    response = client.post(
        "/score",
        json={
            "source": "dev_applications",
            "features": {"application_type": "OZ", "ward_number": "10"},
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["predictions"]["prob_dev_appealed"] == pytest.approx(0.15)


def test_score_invalid_source_returns_422(client: TestClient) -> None:
    response = client.post(
        "/score",
        json={"source": "invalid_source", "features": {}},
    )
    assert response.status_code == 422


def test_score_no_production_ready_models_returns_empty(
    tmp_path: Path,
) -> None:
    """Returns empty predictions when no models are production_ready."""
    import polars as pl
    import datetime
    current_year = datetime.date.today().year
    enriched_dir = tmp_path / "enriched"
    enriched_dir.mkdir(parents=True)
    pl.DataFrame({
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
        "description": ["desc"],
        "street_num": ["1"],
        "street_name": ["Main St"],
    }).write_parquet(enriched_dir / "dev_applications.parquet")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "metrics.json").write_text(
        json.dumps({"dev_applications_appealed": {"production_ready": False}})
    )
    app = create_app(data_dir=tmp_path, model_dir=models_dir)
    c = TestClient(app)
    response = c.post(
        "/score",
        json={"source": "dev_applications", "features": {}},
    )
    assert response.status_code == 200
    assert response.json()["predictions"] == {}
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/api/test_routes.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.api.routes'`

**Step 3: Create `src/zoneto/api/routes.py`**

```python
"""FastAPI route definitions for Zoneto API."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from zoneto.analytics.score import score_one
from zoneto.api.comps import query_comps

router = APIRouter()


# --- response / request models ---


class CompApplication(BaseModel):
    folderrsn: str | None = None
    application_type: str | None = None
    ward_number: str | None = None
    zoning_class: str | None = None
    status: str | None = None
    year_submitted: int | None = None
    lat: float | None = None
    lon: float | None = None
    dev_approved: int | None = None
    dev_appealed: int | None = None
    dev_days_to_decision: int | None = None
    proposed_storeys: int | None = None
    proposed_units: int | None = None
    description: str | None = None
    street_address: str | None = None
    dist_sq: float | None = None


class CompsResponse(BaseModel):
    applications: list[CompApplication]
    total: int


class ScoreRequest(BaseModel):
    source: Literal["dev_applications", "coa", "permits_cleared"]
    features: dict[str, Any]


class ScoreResponse(BaseModel):
    predictions: dict[str, Any]
    production_ready_models: list[str]


# --- endpoints ---


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
def ready(request: Request) -> dict[str, Any]:
    state = request.app.state
    is_ready: bool = getattr(state, "ready", False)
    production_ready: dict[str, bool] = getattr(state, "production_ready", {})
    data_dir: Path = getattr(state, "data_dir", Path("data"))
    data_available = (data_dir / "enriched" / "dev_applications.parquet").exists()

    if not is_ready or not data_available:
        raise HTTPException(status_code=503, detail="Service not ready")

    return {
        "status": "ready",
        "models_loaded": [k for k, v in production_ready.items() if v],
        "data_available": data_available,
    }


@router.get("/comps", response_model=CompsResponse)
def comps(
    request: Request,
    type: str | None = None,
    ward: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 500.0,
    years: int = 5,
    limit: int = 20,
) -> CompsResponse:
    data_dir: Path = getattr(request.app.state, "data_dir", Path("data"))
    enriched_path = data_dir / "enriched" / "dev_applications.parquet"

    if not enriched_path.exists():
        raise HTTPException(status_code=503, detail="Enriched data not available")

    records = query_comps(
        enriched_path,
        application_type=type,
        ward_number=ward,
        lat=lat,
        lon=lon,
        radius_m=radius_m,
        years=years,
        limit=limit,
    )
    applications = [CompApplication(**r) for r in records]
    return CompsResponse(applications=applications, total=len(applications))


@router.post("/score", response_model=ScoreResponse)
def score(request: Request, body: ScoreRequest) -> ScoreResponse:
    model_dir: Path = getattr(request.app.state, "model_dir", Path("models"))
    production_ready: dict[str, bool] = getattr(
        request.app.state, "production_ready", {}
    )
    ready_model_names = [k for k, v in production_ready.items() if v]

    if not ready_model_names:
        return ScoreResponse(predictions={}, production_ready_models=[])

    try:
        predictions = score_one(body.source, body.features, model_dir)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return ScoreResponse(
        predictions=predictions,
        production_ready_models=ready_model_names,
    )
```

**Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/api/ -v
```

Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/api/routes.py tests/api/test_routes.py
git commit -m "feat: add FastAPI routes for /health, /ready, /comps, /score"
```

---

## Task 5: Add `serve` CLI command and `just serve` task

**Files:**
- Modify: `src/zoneto/cli.py` (after line 310, the last command)
- Modify: `justfile` (add `serve` task)

**Step 1: Add `serve` command to `src/zoneto/cli.py`**

Add this function at the end of `cli.py`, after the `score` command:

```python
@app.command()
def serve(
    port: int = typer.Option(8000, help="Port to listen on."),
    host: str = typer.Option("0.0.0.0", help="Host to bind to."),
    data_dir: Path = typer.Option(DATA_DIR, help="Data directory."),
    model_dir: Path = typer.Option(Path("models"), help="Model directory."),
) -> None:
    """Start the FastAPI serving layer."""
    import uvicorn

    from zoneto.api.app import create_app

    application = create_app(data_dir=data_dir, model_dir=model_dir)
    uvicorn.run(application, host=host, port=port)
```

**Step 2: Add `serve` task to `justfile`**

Add after the existing tasks (following the `uv run zoneto <command>` pattern):

```makefile
# Start the FastAPI serving layer
serve:
    uv run zoneto serve
```

**Step 3: Verify CLI command is registered**

```bash
uv run zoneto --help
```

Expected: `serve` appears in the command list.

**Step 4: Run all API tests**

```bash
uv run pytest tests/api/ -v
```

Expected: All tests pass.

**Step 5: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass with no errors.

**Step 6: Run linter and type checker**

```bash
uv run ruff check src/zoneto/api/ tests/api/
uv run ty check src/zoneto/api/
```

Expected: No errors.

**Step 7: Commit**

```bash
git add src/zoneto/cli.py justfile
git commit -m "feat: add 'zoneto serve' CLI command and 'just serve' task"
```

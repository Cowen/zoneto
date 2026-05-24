# Address Search Frontend Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a `GET /geocode` endpoint that proxies address lookups to Nominatim and returns `{ lat, lon, display_name }`.

**Architecture:** Thin synchronous route handler calls `httpx.get()` to Nominatim with the required `User-Agent` header (browsers cannot set this). Returns the first result's coordinates. Raises `HTTPException(404)` if Nominatim returns an empty list. Tests use `pytest-httpx`'s `HTTPXMock` fixture to intercept all httpx calls — no network in CI.

**Tech Stack:** FastAPI, httpx (already a direct dependency), pydantic BaseModel, pytest-httpx

**Scope:** Phase 1 of 2 from original design

**Codebase verified:** 2026-03-24

---

## Task 1: Write failing tests for `GET /geocode`

**Files:**
- Modify: `tests/api/test_routes.py` (append to end of file)

### Step 1: Add `HTTPXMock` import

In `tests/api/test_routes.py`, line 12 currently reads:

```python
from starlette.testclient import TestClient
```

Add `HTTPXMock` import after it:

```python
from starlette.testclient import TestClient

from pytest_httpx import HTTPXMock
```

### Step 2: Append four test functions to `tests/api/test_routes.py`

Add this block at the very end of the file (after line 281):

```python
# --- /geocode ---


def test_geocode_returns_lat_lon(client: TestClient, httpx_mock: HTTPXMock) -> None:
    """GET /geocode returns lat, lon, display_name from Nominatim."""
    httpx_mock.add_response(
        json=[
            {
                "lat": "43.6426",
                "lon": "-79.3871",
                "display_name": "441 King St W, Toronto, ON",
            }
        ]
    )
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 200
    body = response.json()
    assert body["lat"] == pytest.approx(43.6426)
    assert body["lon"] == pytest.approx(-79.3871)
    assert body["display_name"] == "441 King St W, Toronto, ON"


def test_geocode_no_results_returns_404(client: TestClient, httpx_mock: HTTPXMock) -> None:
    """GET /geocode returns 404 when Nominatim returns empty list."""
    httpx_mock.add_response(json=[])
    response = client.get("/geocode?address=nonexistent+place+xyz")
    assert response.status_code == 404


def test_geocode_nominatim_timeout_returns_504(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """GET /geocode returns 504 when Nominatim times out."""
    httpx_mock.add_exception(httpx.TimeoutException("timed out"))
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 504


def test_geocode_nominatim_upstream_error_returns_502(
    client: TestClient, httpx_mock: HTTPXMock
) -> None:
    """GET /geocode returns 502 when Nominatim returns a non-2xx status."""
    httpx_mock.add_response(status_code=503)
    response = client.get("/geocode?address=441+King+St+W")
    assert response.status_code == 502
```

### Step 3: Run tests to verify they fail

```bash
uv run pytest tests/api/test_routes.py::test_geocode_returns_lat_lon tests/api/test_routes.py::test_geocode_no_results_returns_404 tests/api/test_routes.py::test_geocode_nominatim_timeout_returns_504 tests/api/test_routes.py::test_geocode_nominatim_upstream_error_returns_502 -v
```

Expected: All four tests FAIL — the endpoint does not exist yet, so FastAPI returns 404 for the route itself.

### Step 4: Commit failing tests

```bash
git add tests/api/test_routes.py
git commit -m "test: add failing tests for GET /geocode (happy path, 404, timeout, upstream error)"
```

---

## Task 2: Implement `GET /geocode`

**Files:**
- Modify: `src/zoneto/api/routes.py`

### Step 1: Add `httpx` import

`src/zoneto/api/routes.py` currently begins with:

```python
"""FastAPI route definitions for Zoneto API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
```

Add `import httpx` between the stdlib block and the third-party block:

```python
"""FastAPI route definitions for Zoneto API."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
```

### Step 2: Add `GeocodeResult` response model

After the existing response models block (currently ending at line 53 with `ScoreResponse`), before the `# --- endpoints ---` comment, add:

```python
class GeocodeResult(BaseModel):
    lat: float
    lon: float
    display_name: str
```

### Step 3: Add `GET /geocode` handler

After the `health` endpoint (after the `return {"status": "ok"}` line), add:

```python
@router.get("/geocode", response_model=GeocodeResult)
def geocode(address: str) -> GeocodeResult:
    try:
        resp = httpx.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "countrycodes": "ca", "limit": 1},
            headers={"User-Agent": "zoneto/1.0"},
            timeout=10,
        )
        resp.raise_for_status()
    except httpx.TimeoutException as exc:
        raise HTTPException(status_code=504, detail="Geocoding service timed out") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail="Geocoding service unavailable") from exc
    results = resp.json()
    if not results:
        raise HTTPException(status_code=404, detail="Address not found")
    first = results[0]
    return GeocodeResult(
        lat=float(first["lat"]),
        lon=float(first["lon"]),
        display_name=first["display_name"],
    )
```

### Step 4: Run geocode tests to verify they pass

```bash
uv run pytest tests/api/test_routes.py::test_geocode_returns_lat_lon tests/api/test_routes.py::test_geocode_no_results_returns_404 tests/api/test_routes.py::test_geocode_nominatim_timeout_returns_504 tests/api/test_routes.py::test_geocode_nominatim_upstream_error_returns_502 -v
```

Expected: Both tests PASS.

### Step 5: Run full test suite and linters

```bash
uv run pytest tests/ -v
uv run ruff check src/ tests/
uv run ty check src/
```

Expected: All tests pass, zero lint errors, zero type errors.

### Step 6: Commit

```bash
git add src/zoneto/api/routes.py
git commit -m "feat: add GET /geocode endpoint proxying to Nominatim"
```

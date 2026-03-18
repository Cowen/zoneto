# Address Search Frontend Design

## Overview

Replace the raw latitude/longitude number inputs on the Zoneto frontend with a single address text field. The user types a street address; the app geocodes it via a backend proxy to Nominatim (OpenStreetMap) and passes the resulting coordinates to the existing `/comps` and `/score` endpoints unchanged.

**Goals:**
- Users never interact with raw coordinates
- Ward number auto-populates from the first comparable result
- No change to any backend data pipeline or ML endpoints

## Architecture

A thin `GET /geocode` endpoint is added to the FastAPI app. The browser calls `/geocode?address=...`, which proxies to Nominatim with a proper `User-Agent` header (required by Nominatim's usage policy; browsers cannot set this). The returned `lat`/`lon` are stored in JS variables and passed to `/comps` exactly as before.

```
Browser submits address form
  → GET /geocode?address=441+King+St+W
      → FastAPI sets User-Agent: zoneto/1.0
      → Nominatim (countrycodes=ca, city bias: Toronto)
      ← { lat, lon, display_name }
  → GET /comps?lat=...&lon=...&type=...&ward=...
      ← applications[0].ward_number → fills ward input
  → POST /score?explain=true  (if lat+lon+type present)
```

Ward auto-fill reads `ward_number` from the first `/comps` result. No new backend logic needed — the ward field remains a manual override if the user wants to override.

## Existing Patterns

FastAPI routes follow the sync handler pattern in `src/zoneto/api/routes.py`. All existing endpoints use synchronous `httpx` is not used in routes directly — the geocode endpoint will call `httpx.get()` synchronously (httpx is already a transitive dependency via fastapi[standard]).

The frontend is a single vanilla-JS file in `static/index.html`. No build system; all logic is inline `<script>`. The existing `fetchScore()` and `renderResults()` functions are the model for the new geocoding flow.

Tests for routes live in `tests/api/test_routes.py` and use `starlette.testclient.TestClient` with `monkeypatch` to stub external calls.

## Implementation Phases

### Phase 1: Backend `/geocode` endpoint

**Goal:** Add `GET /geocode?address=...` to the FastAPI router with Nominatim proxying.

**Components:**
- Modify: `src/zoneto/api/routes.py`
  - New `GeocodeResult` response model: `{ lat: float, lon: float, display_name: str }`
  - New `GET /geocode` handler: calls `httpx.get("https://nominatim.openstreetmap.org/search", params={...}, headers={"User-Agent": "zoneto/1.0"}, timeout=10)`
  - Raises `HTTPException(404)` if Nominatim returns empty results
  - Bias params: `countrycodes=ca`, `q` prefixed with address (user supplies city if desired)
- Modify: `tests/api/test_routes.py`
  - `test_geocode_returns_lat_lon`: monkeypatches `httpx.get` to return a valid Nominatim JSON response, asserts `{ lat, lon, display_name }`
  - `test_geocode_no_results_returns_404`: monkeypatches to return `[]`, asserts 404

**Dependencies:** None (httpx available, routes.py already exists)

**Done when:** Both new tests pass, `ruff` and `ty` clean

### Phase 2: Frontend address input

**Goal:** Replace lat/lon inputs with an address field; wire geocoding + ward auto-fill.

**Components:**
- Modify: `static/index.html`
  - Remove: `<input type="number">` for Latitude and Longitude
  - Add: `<input type="text" id="address">` (placeholder: "e.g. 441 King St W, Toronto")
  - On form submit: if address non-empty, call `GET /geocode?address=...` first; store `lat`/`lon` in JS variables; show geocoded `display_name` in status line
  - If geocode fails (404 or network error), show error and abort — don't silently submit without coordinates
  - After `/comps` returns results, read `data.applications[0]?.ward_number` and populate the ward input if ward was empty
  - Ward field label updated to "Ward (auto-filled)" to hint at the behaviour

**Dependencies:** Phase 1 (endpoint must exist)

**Done when:** Manual smoke test — type "Dundas St W, Toronto", submit, see comparable results with ward populated

## Additional Considerations

**Rate limiting:** Nominatim's public API allows 1 request/second. The frontend only geocodes on form submit (not on keystroke), so rate limit is not a concern in normal use.

**Timeout:** The `/geocode` endpoint sets `timeout=10` seconds. If Nominatim is slow, the user sees a standard HTTP error; the frontend should surface this as "Geocoding failed — try again."

**No autocomplete:** Nominatim's usage policy prohibits autocomplete (queries on every keystroke). Geocoding only fires on form submit.

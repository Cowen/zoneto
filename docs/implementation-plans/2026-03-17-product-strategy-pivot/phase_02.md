# Product Strategy Pivot — Phase 2: Minimal HTML Frontend

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a single-page HTML frontend served by FastAPI that lets non-technical users query comparable applications and view predictions.

**Architecture:** Vanilla HTML + CSS + `fetch()` — no framework, no build step. FastAPI's `StaticFiles` mounts the `static/` directory at `/`. The form POSTs to `/comps` via fetch and renders results as a table. Predictions from `/score` are shown where available.

**Tech Stack:** HTML5, CSS, vanilla JavaScript (fetch API), FastAPI StaticFiles (already installed)

**Scope:** Phase 2 of 8. Requires Phase 1 (API must exist).

**Codebase verified:** 2026-03-17

---

## Task 1: Create the static HTML frontend

**Files:**
- Create: `static/index.html`

**Step 1: Create `static/index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Zoneto — Toronto Development Application Intelligence</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f8f9fa;
      color: #212529;
      padding: 2rem;
    }

    h1 { font-size: 1.5rem; font-weight: 700; margin-bottom: 0.25rem; }
    .tagline { color: #6c757d; font-size: 0.9rem; margin-bottom: 2rem; }

    .card {
      background: #fff;
      border: 1px solid #dee2e6;
      border-radius: 8px;
      padding: 1.5rem;
      margin-bottom: 1.5rem;
      max-width: 900px;
    }

    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
      margin-bottom: 1rem;
    }

    label { display: block; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.25rem; color: #495057; }
    input, select {
      width: 100%; padding: 0.5rem 0.75rem;
      border: 1px solid #ced4da; border-radius: 4px;
      font-size: 0.9rem;
    }
    input:focus, select:focus { outline: 2px solid #0d6efd; outline-offset: 1px; }

    button[type="submit"] {
      background: #0d6efd; color: #fff;
      border: none; border-radius: 4px;
      padding: 0.6rem 1.5rem; font-size: 0.9rem; font-weight: 600;
      cursor: pointer;
    }
    button[type="submit"]:hover { background: #0b5ed7; }
    button[type="submit"]:disabled { background: #6c757d; cursor: not-allowed; }

    #status { font-size: 0.85rem; color: #6c757d; margin-top: 0.5rem; min-height: 1.2rem; }

    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th {
      text-align: left; padding: 0.6rem 0.75rem;
      border-bottom: 2px solid #dee2e6;
      font-size: 0.75rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.05em;
      color: #6c757d;
    }
    td { padding: 0.5rem 0.75rem; border-bottom: 1px solid #f1f3f5; vertical-align: top; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #f8f9fa; }

    .badge {
      display: inline-block; padding: 0.2rem 0.5rem;
      border-radius: 3px; font-size: 0.75rem; font-weight: 600;
    }
    .badge-appealed { background: #fff3cd; color: #856404; }
    .badge-approved { background: #d1e7dd; color: #0a3622; }
    .badge-active   { background: #cff4fc; color: #055160; }

    .pred-box {
      background: #f8f9fa; border: 1px solid #dee2e6;
      border-radius: 6px; padding: 1rem; margin-top: 0.5rem;
    }
    .pred-box h3 { font-size: 0.85rem; color: #6c757d; margin-bottom: 0.5rem; }
    .pred-row { display: flex; justify-content: space-between; font-size: 0.85rem; margin-bottom: 0.25rem; }
    .pred-label { color: #495057; }
    .pred-value { font-weight: 600; }

    .empty { text-align: center; color: #6c757d; padding: 2rem; }

    #score-section { display: none; }
  </style>
</head>
<body>
  <h1>Zoneto</h1>
  <p class="tagline">Toronto development application intelligence</p>

  <div class="card">
    <form id="query-form">
      <div class="form-grid">
        <div>
          <label for="type">Application Type</label>
          <select id="type" name="type">
            <option value="">Any</option>
            <option value="OZ">OZ — Official Plan / Zoning</option>
            <option value="SA">SA — Site Plan Approval</option>
            <option value="OPA">OPA — Official Plan Amendment</option>
          </select>
        </div>
        <div>
          <label for="ward">Ward Number</label>
          <input type="text" id="ward" name="ward" placeholder="e.g. 10" />
        </div>
        <div>
          <label for="lat">Latitude</label>
          <input type="number" id="lat" name="lat" step="any" placeholder="e.g. 43.6532" />
        </div>
        <div>
          <label for="lon">Longitude</label>
          <input type="number" id="lon" name="lon" step="any" placeholder="e.g. -79.3832" />
        </div>
        <div>
          <label for="radius">Radius (m)</label>
          <input type="number" id="radius" name="radius" value="500" min="50" max="5000" />
        </div>
        <div>
          <label for="years">Years back</label>
          <input type="number" id="years" name="years" value="5" min="1" max="20" />
        </div>
      </div>
      <button type="submit">Find Comparable Applications</button>
      <div id="status"></div>
    </form>
  </div>

  <div class="card" id="score-section">
    <div class="pred-box">
      <h3>ML Predictions (production-ready models only)</h3>
      <div id="predictions"></div>
    </div>
  </div>

  <div class="card" id="results-card" style="display:none">
    <div id="results"></div>
  </div>

  <script>
    const form = document.getElementById('query-form');
    const statusEl = document.getElementById('status');
    const resultsCard = document.getElementById('results-card');
    const resultsEl = document.getElementById('results');
    const scoreSection = document.getElementById('score-section');
    const predictionsEl = document.getElementById('predictions');

    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = form.querySelector('button[type="submit"]');
      btn.disabled = true;
      statusEl.textContent = 'Searching…';
      resultsCard.style.display = 'none';
      scoreSection.style.display = 'none';

      try {
        const params = new URLSearchParams();
        const type = document.getElementById('type').value;
        const ward = document.getElementById('ward').value.trim();
        const lat = document.getElementById('lat').value.trim();
        const lon = document.getElementById('lon').value.trim();
        const radius = document.getElementById('radius').value;
        const years = document.getElementById('years').value;

        if (type)   params.set('type', type);
        if (ward)   params.set('ward', ward);
        if (lat)    params.set('lat', lat);
        if (lon)    params.set('lon', lon);
        if (radius) params.set('radius_m', radius);
        if (years)  params.set('years', years);

        const res = await fetch('/comps?' + params.toString());
        if (!res.ok) {
          const err = await res.json().catch(() => ({ detail: res.statusText }));
          throw new Error(err.detail || res.statusText);
        }

        const data = await res.json();
        renderResults(data);
        statusEl.textContent = `${data.total} comparable application${data.total !== 1 ? 's' : ''} found`;

        // Attempt /score if lat+lon+type provided
        if (lat && lon && type) {
          await fetchScore({ application_type: type, ward_number: ward || null, lat: parseFloat(lat), lon: parseFloat(lon) });
        }
      } catch (err) {
        statusEl.textContent = 'Error: ' + err.message;
      } finally {
        btn.disabled = false;
      }
    });

    function renderResults(data) {
      if (!data.applications.length) {
        resultsEl.innerHTML = '<p class="empty">No comparable applications found. Try broadening your search.</p>';
        resultsCard.style.display = 'block';
        return;
      }

      const rows = data.applications.map(app => {
        const statusBadge = formatStatus(app.status);
        const timeline = app.dev_days_to_decision ? `${Math.round(app.dev_days_to_decision / 30)} mo` : '—';
        const storeys = app.proposed_storeys ?? '—';
        const units = app.proposed_units ?? '—';
        return `<tr>
          <td>${escHtml(app.folderrsn ?? '')}</td>
          <td>${escHtml(app.application_type ?? '')}</td>
          <td>${escHtml(app.ward_number ?? '')}</td>
          <td>${escHtml(app.zoning_class ?? '')}</td>
          <td>${escHtml(app.year_submitted?.toString() ?? '')}</td>
          <td>${statusBadge}</td>
          <td>${timeline}</td>
          <td>${storeys}</td>
          <td>${units}</td>
          <td>${escHtml(app.street_address?.trim() ?? '')}</td>
        </tr>`;
      }).join('');

      resultsEl.innerHTML = `
        <table>
          <thead><tr>
            <th>Folder RSN</th><th>Type</th><th>Ward</th><th>Zone</th>
            <th>Year</th><th>Status</th><th>Timeline</th>
            <th>Storeys</th><th>Units</th><th>Address</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
      resultsCard.style.display = 'block';
    }

    function formatStatus(status) {
      if (!status) return '—';
      const lc = status.toLowerCase();
      if (lc.includes('appeal')) return `<span class="badge badge-appealed">${escHtml(status)}</span>`;
      if (lc.includes('approv')) return `<span class="badge badge-approved">${escHtml(status)}</span>`;
      if (lc.includes('active') || lc.includes('review') || lc.includes('circulat'))
        return `<span class="badge badge-active">${escHtml(status)}</span>`;
      return escHtml(status);
    }

    async function fetchScore(features) {
      try {
        const res = await fetch('/score', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ source: 'dev_applications', features }),
        });
        if (!res.ok) return;
        const data = await res.json();
        if (!data.production_ready_models.length) return;
        renderPredictions(data.predictions);
      } catch (_) {
        // predictions are optional — fail silently
      }
    }

    function renderPredictions(predictions) {
      const rows = Object.entries(predictions).map(([key, val]) => {
        const label = key.replace(/^(prob_|pred_)/, '').replace(/_/g, ' ');
        const formatted = typeof val === 'number'
          ? (val < 1 && val > 0 ? (val * 100).toFixed(1) + '%' : val.toFixed(1))
          : String(val);
        return `<div class="pred-row"><span class="pred-label">${escHtml(label)}</span><span class="pred-value">${escHtml(formatted)}</span></div>`;
      }).join('');

      if (rows) {
        predictionsEl.innerHTML = rows;
        scoreSection.style.display = 'block';
      }
    }

    function escHtml(str) {
      return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
    }
  </script>
</body>
</html>
```

**Step 2: Commit the frontend**

```bash
git add static/index.html
git commit -m "feat: add vanilla HTML frontend for comparable application queries"
```

---

## Task 2: Mount static files in FastAPI app

**Files:**
- Modify: `src/zoneto/api/app.py`

**Step 1: Write the failing test**

Add to `tests/api/test_app.py`:

```python
def test_frontend_served_at_root(tmp_path: Path) -> None:
    """GET / returns the HTML frontend."""
    # create minimal parquet so the app is ready
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

    # create static/index.html in a temp static dir
    static_dir = tmp_path / "static"
    static_dir.mkdir()
    (static_dir / "index.html").write_text("<html><body>test</body></html>")

    app = create_app(data_dir=tmp_path, model_dir=tmp_path / "models", static_dir=static_dir)
    c = TestClient(app)
    response = c.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
```

**Step 2: Run test to confirm failure**

```bash
uv run pytest tests/api/test_app.py::test_frontend_served_at_root -v
```

Expected: `TypeError: create_app() got an unexpected keyword argument 'static_dir'`

**Step 3: Update `src/zoneto/api/app.py` to accept and mount static_dir**

Replace the existing `create_app` function:

```python
def create_app(
    data_dir: Path | None = None,
    model_dir: Path | None = None,
    static_dir: Path | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    resolved_data_dir = data_dir or Path("data")
    resolved_model_dir = model_dir or Path("models")
    resolved_static_dir = static_dir or Path("static")

    @asynccontextmanager
    async def lifespan(app: FastAPI):  # type: ignore[type-arg]
        app.state.data_dir = resolved_data_dir
        app.state.model_dir = resolved_model_dir
        app.state.production_ready = _load_production_ready(resolved_model_dir)
        app.state.ready = True
        yield

    app = FastAPI(title="Zoneto", version="0.1.0", lifespan=lifespan)

    from zoneto.api.routes import router  # noqa: PLC0415

    app.include_router(router)

    if resolved_static_dir.exists():
        from fastapi.staticfiles import StaticFiles  # noqa: PLC0415

        app.mount("/", StaticFiles(directory=resolved_static_dir, html=True), name="static")

    return app
```

**Step 4: Update `src/zoneto/cli.py` `serve` command to pass static_dir**

Find the `serve` command added in Phase 1 and update it:

```python
@app.command()
def serve(
    port: int = typer.Option(8000, help="Port to listen on."),
    host: str = typer.Option("0.0.0.0", help="Host to bind to."),
    data_dir: Path = typer.Option(DATA_DIR, help="Data directory."),
    model_dir: Path = typer.Option(Path("models"), help="Model directory."),
    static_dir: Path = typer.Option(Path("static"), help="Static files directory."),
) -> None:
    """Start the FastAPI serving layer."""
    import uvicorn

    from zoneto.api.app import create_app

    application = create_app(data_dir=data_dir, model_dir=model_dir, static_dir=static_dir)
    uvicorn.run(application, host=host, port=port)
```

**Step 5: Run all API tests**

```bash
uv run pytest tests/api/ -v
```

Expected: All tests pass (including the new `test_frontend_served_at_root`).

**Step 6: Run full test suite**

```bash
uv run pytest -qq
```

Expected: No failures.

**Step 7: Run linter and type checker**

```bash
uv run ruff check src/zoneto/api/ tests/api/
uv run ty check src/zoneto/api/
```

Expected: No errors.

**Step 8: Commit**

```bash
git add src/zoneto/api/app.py src/zoneto/cli.py
git commit -m "feat: mount static files in FastAPI app, pass static_dir to serve command"
```

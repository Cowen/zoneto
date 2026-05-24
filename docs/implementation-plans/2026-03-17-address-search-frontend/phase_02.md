# Address Search Frontend — Phase 2

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Replace the lat/lon number inputs in `static/index.html` with a single address text field. Wire the form to geocode via `GET /geocode` before calling `/comps`, and auto-fill the ward input from the first comparable result.

**Architecture:** All changes are in `static/index.html` (vanilla JS, no build system). On form submit: geocode address → store lat/lon in local variables → call `/comps` with those coordinates → auto-fill ward from `data.applications[0]?.ward_number` if the user left it blank. No backend changes needed. No automated tests — verified by manual smoke test per design.

**Tech Stack:** Vanilla JS, existing `/geocode` endpoint from Phase 1

**Scope:** Phase 2 of 2 from original design

**Codebase verified:** 2026-03-24

**Prerequisite:** Phase 1 must be complete (the `/geocode` endpoint must exist).

---

## Task 1: Update `static/index.html` — form inputs and JS

**Files:**
- Modify: `static/index.html`

### Step 1: Update ward label

Find (line 105):
```html
          <label for="ward">Ward Number</label>
```

Replace with:
```html
          <label for="ward">Ward (auto-filled)</label>
```

### Step 2: Remove lat/lon inputs, add address input

Find and remove these two `<div>` blocks (lines 108–115):
```html
        <div>
          <label for="lat">Latitude</label>
          <input type="number" id="lat" name="lat" step="any" placeholder="e.g. 43.6532" />
        </div>
        <div>
          <label for="lon">Longitude</label>
          <input type="number" id="lon" name="lon" step="any" placeholder="e.g. -79.3832" />
        </div>
```

Replace with a single address div:
```html
        <div>
          <label for="address">Address</label>
          <input type="text" id="address" name="address" placeholder="e.g. 441 King St W, Toronto" />
        </div>
```

### Step 3: Replace the form submit handler in the `<script>` block

Find the entire `form.addEventListener` block (lines 149–192):
```javascript
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
```

Replace with:
```javascript
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = form.querySelector('button[type="submit"]');
      btn.disabled = true;
      resultsCard.style.display = 'none';
      scoreSection.style.display = 'none';

      try {
        const address = document.getElementById('address').value.trim();
        const type = document.getElementById('type').value;
        const wardEl = document.getElementById('ward');
        const radius = document.getElementById('radius').value;
        const years = document.getElementById('years').value;

        if (!address) {
          throw new Error('Please enter an address.');
        }

        // Geocode address via backend proxy
        statusEl.textContent = 'Geocoding address…';
        const geoRes = await fetch('/geocode?' + new URLSearchParams({ address }));
        if (!geoRes.ok) {
          const err = await geoRes.json().catch(() => ({ detail: geoRes.statusText }));
          throw new Error('Geocoding failed — ' + (err.detail || geoRes.statusText) + '. Try again.');
        }
        const geo = await geoRes.json();
        const lat = geo.lat;
        const lon = geo.lon;
        statusEl.textContent = `Geocoded: ${geo.display_name}`;

        // Fetch comparable applications
        const params = new URLSearchParams();
        if (type)                params.set('type', type);
        if (wardEl.value.trim()) params.set('ward', wardEl.value.trim());
        params.set('lat', lat);
        params.set('lon', lon);
        if (radius) params.set('radius_m', radius);
        if (years)  params.set('years', years);

        const res = await fetch('/comps?' + params.toString());
        if (!res.ok) {
          const err = await res.json().catch(() => ({ detail: res.statusText }));
          throw new Error(err.detail || res.statusText);
        }

        const data = await res.json();
        renderResults(data);
        statusEl.textContent = `${geo.display_name} — ${data.total} comparable application${data.total !== 1 ? 's' : ''} found`;

        // Auto-fill ward from first comparable result (only if user left ward blank)
        if (!wardEl.value.trim() && data.applications[0]?.ward_number) {
          wardEl.value = data.applications[0].ward_number;
        }

        // Score if application type provided
        if (type) {
          await fetchScore({ application_type: type, ward_number: wardEl.value || null, lat, lon });
        }
      } catch (err) {
        statusEl.textContent = 'Error: ' + err.message;
      } finally {
        btn.disabled = false;
      }
    });
```

### Step 4: Verify the page loads without JS errors

Start the server (requires enriched data and models to exist; if unavailable, skip to smoke test instructions below):

```bash
just serve
```

Open `http://localhost:8000` and check the browser console for JS errors. Expected: no errors.

### Step 5: Manual smoke test

With the server running against real data:

1. Type `Dundas St W, Toronto` in the Address field
2. Set Application Type to `OZ`
3. Click "Find Comparable Applications"
4. Expected:
   - Status line briefly shows "Geocoding address…" then "Geocoded: ..."
   - Table of comparable OZ applications appears
   - Ward field auto-populates with a ward number (e.g. "10")
5. Try an invalid address (e.g. `zzzznotanaddress`):
   - Expected: status shows "Error: Geocoding failed — Address not found. Try again."
   - Form does not submit to `/comps`

### Step 6: Commit

```bash
git add static/index.html
git commit -m "feat: replace lat/lon inputs with address geocoding in frontend"
```

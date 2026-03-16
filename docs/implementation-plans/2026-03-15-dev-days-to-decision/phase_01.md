# Dev Days-to-Decision Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add AIC scraper, survival labels, survival model training, scoring, and CLI integration to predict days-to-decision for Toronto development applications.

**Architecture:** Scrape AIC portal for OZ+SA decision dates → cache in `data/reference/aic_decisions.parquet` → join in `enrich_dev()` → train `GradientBoostingSurvivalAnalysis` → append `pred_dev_days_to_decision` to scored parquet.

**Tech Stack:** httpx (already installed), beautifulsoup4 (new), scikit-survival (new), polars, sklearn

**Scope:** 5 phases (design phases 2–6; phase 1 skipped — CKANSource rename not feasible without conflicting with existing `CKANSource` concrete class)

**Codebase verified:** 2026-03-15

---

## Phase 1: AIC Scraper Module

**Design phase:** Phase 2

**Goal:** Implement `fetch_aic_decisions()` — scrapes decision milestone dates from the AIC portal and caches them as a parquet file.

---

### Task 1: Add new dependencies to pyproject.toml

**Files:**
- Modify: `pyproject.toml`

**Step 1: Add beautifulsoup4 and scikit-survival**

Run:
```bash
uv add beautifulsoup4 scikit-survival
```

Expected: both packages install without errors. `pyproject.toml` gets two new entries under `[project] dependencies`.

**Step 2: Verify install**

Run:
```bash
uv run python -c "from bs4 import BeautifulSoup; from sksurv.ensemble import GradientBoostingSurvivalAnalysis; print('OK')"
```

Expected: prints `OK` with no errors.

**Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add beautifulsoup4 and scikit-survival dependencies"
```

---

### Task 2: Write failing tests for fetch_aic_decisions

**Files:**
- Create: `tests/test_aic.py`

**Step 1: Write the test file**

```python
"""Tests for the AIC scraper (fetch_aic_decisions)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest
from pytest_httpx import HTTPXMock

from zoneto.sources.aic import fetch_aic_decisions

# ---------------------------------------------------------------------------
# Synthetic AIC HTML fixtures
# ---------------------------------------------------------------------------

_OZ_MILESTONES_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2021-03-15</td></tr>
  <tr><td>Community Consultation Meeting</td><td>2021-05-20</td></tr>
  <tr><td>City Council Decision Made</td><td>2022-11-08</td></tr>
</table>
</body></html>
"""

_SA_MILESTONES_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2020-06-01</td></tr>
  <tr><td>Statement of Approval Issued</td><td>2021-02-14</td></tr>
</table>
</body></html>
"""

_NO_DECISION_HTML = """
<html><body>
<table class="milestones">
  <tr><td>Notice of Complete Application Issued</td><td>2022-01-10</td></tr>
</table>
</body></html>
"""


# ---------------------------------------------------------------------------
# Fixtures: minimal dev_applications parquet
# ---------------------------------------------------------------------------


def _make_dev_parquet(tmp_path: Path) -> None:
    """Write minimal dev_applications parquet with OZ and SA rows."""
    df = pl.DataFrame(
        {
            "folderrsn": ["111", "222", "333"],
            "application_type": ["OZ", "SA", "OZ"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=111",
                "https://app.toronto.ca/AIC/details?folderRsn=222",
                "https://app.toronto.ca/AIC/details?folderRsn=333",
            ],
            "status": ["Closed", "Closed", "Under Review"],
            "date_submitted": ["2021-01-01", "2020-01-01", "2022-01-01"],
            "year": [2021, 2020, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_oz_application_extracts_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """OZ application: extracts 'City Council Decision Made' as decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    out = tmp_path / "reference" / "aic_decisions.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    oz_row = df.filter(pl.col("folderrsn") == "111")
    assert oz_row["decision_date"][0] == date(2022, 11, 8)
    assert oz_row["complete_date"][0] == date(2021, 3, 15)
    assert count == 3


def test_sa_application_extracts_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """SA application: extracts 'Statement of Approval Issued' as decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    sa_row = df.filter(pl.col("folderrsn") == "222")
    assert sa_row["decision_date"][0] == date(2021, 2, 14)


def test_missing_milestone_produces_null_decision_date(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Application with no decision milestone gets null decision_date."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(text=_OZ_MILESTONES_HTML)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    no_decision_row = df.filter(pl.col("folderrsn") == "333")
    assert no_decision_row["decision_date"][0] is None


def test_already_scraped_rows_are_skipped(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Rows already in aic_decisions.parquet are not re-fetched (idempotency)."""
    _make_dev_parquet(tmp_path)

    # Pre-populate cache with folderrsn "111"
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True)
    existing = pl.DataFrame(
        {
            "folderrsn": ["111"],
            "decision_date": [date(2022, 11, 8)],
            "complete_date": [date(2021, 3, 15)],
            "scraped_at": [date.today()],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    existing.write_parquet(ref_dir / "aic_decisions.parquet")

    # Only 2 new rows should be fetched (222 and 333)
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)
    httpx_mock.add_response(text=_NO_DECISION_HTML)

    count = fetch_aic_decisions(tmp_path, delay=0.0)
    assert count == 2

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    assert len(df) == 3  # 1 pre-existing + 2 newly scraped


def test_http_error_skipped_without_crash(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """HTTP 404 for one URL is logged and skipped; other rows still processed."""
    _make_dev_parquet(tmp_path)
    httpx_mock.add_response(status_code=404)  # folderrsn=111 fails
    httpx_mock.add_response(text=_SA_MILESTONES_HTML)  # folderrsn=222 succeeds
    httpx_mock.add_response(text=_NO_DECISION_HTML)  # folderrsn=333 succeeds

    count = fetch_aic_decisions(tmp_path, delay=0.0)

    df = pl.read_parquet(tmp_path / "reference" / "aic_decisions.parquet")
    # Only 2 rows written (111 skipped due to HTTP error)
    assert len(df) == 2
    assert count == 2
```

**Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_aic.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.sources.aic'` — confirms the module doesn't exist yet.

---

### Task 3: Implement fetch_aic_decisions

**Files:**
- Create: `src/zoneto/sources/aic.py`

**Step 1: Write the implementation**

```python
"""AIC (Application Information Centre) scraper for decision milestone dates."""

from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Milestone label → output column
_OZ_DECISION_LABEL = "City Council Decision Made"
_SA_DECISION_LABEL = "Statement of Approval Issued"
_COMPLETE_LABEL = "Notice of Complete Application Issued"

_OUTPUT_SCHEMA = {
    "folderrsn": pl.String,
    "decision_date": pl.Date,
    "complete_date": pl.Date,
    "scraped_at": pl.Date,
}


def _parse_milestones(html: str, application_type: str) -> tuple[date | None, date | None]:
    """Parse AIC milestone HTML. Returns (decision_date, complete_date)."""
    soup = BeautifulSoup(html, "html.parser")
    milestones: dict[str, date] = {}

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True)
            raw_date = cells[1].get_text(strip=True)
            if raw_date:
                try:
                    milestones[label] = date.fromisoformat(raw_date)
                except ValueError:
                    pass

    decision_label = (
        _OZ_DECISION_LABEL if application_type == "OZ" else _SA_DECISION_LABEL
    )
    decision_date = milestones.get(decision_label)
    complete_date = milestones.get(_COMPLETE_LABEL)
    return decision_date, complete_date


def fetch_aic_decisions(
    data_dir: Path,
    *,
    delay: float = 1.0,
) -> int:
    """Scrape AIC milestone dates for OZ+SA applications and cache as parquet.

    Reads dev_applications parquet for OZ/SA rows with non-null application_url.
    Skips rows already present in data/reference/aic_decisions.parquet.
    Fetches each URL via httpx; parses HTML with BeautifulSoup.
    Writes/appends data/reference/aic_decisions.parquet.

    Returns count of newly scraped rows.
    """
    # Load raw dev_applications
    dev_path = data_dir / "dev_applications"
    df = pl.read_parquet(dev_path, hive_partitioning=True)

    # Filter to OZ+SA with a URL
    df = df.filter(
        pl.col("application_type").is_in(["OZ", "SA"])
        & pl.col("application_url").is_not_null()
    )

    # Load existing cache to skip already-scraped rows
    ref_path = data_dir / "reference" / "aic_decisions.parquet"
    if ref_path.exists():
        existing = pl.read_parquet(ref_path)
        scraped_ids = set(existing["folderrsn"].to_list())
    else:
        existing = pl.DataFrame(schema=_OUTPUT_SCHEMA)
        scraped_ids = set()

    # Scrape only un-cached rows
    rows_to_scrape = df.filter(~pl.col("folderrsn").is_in(list(scraped_ids)))

    new_rows: list[dict] = []
    today = date.today()

    with httpx.Client(timeout=30.0) as client:
        for row in rows_to_scrape.iter_rows(named=True):
            folderrsn: str = row["folderrsn"]
            url: str = row["application_url"]
            app_type: str = row["application_type"]

            try:
                response = client.get(url)
                response.raise_for_status()
            except (httpx.HTTPError, httpx.RequestError) as exc:
                logger.warning("AIC fetch failed for %s (%s): %s", folderrsn, url, exc)
                if delay > 0:
                    time.sleep(delay)
                continue

            try:
                decision_date, complete_date = _parse_milestones(
                    response.text, app_type
                )
            except Exception as exc:
                logger.warning("AIC parse failed for %s: %s", folderrsn, exc)
                if delay > 0:
                    time.sleep(delay)
                continue

            new_rows.append(
                {
                    "folderrsn": folderrsn,
                    "decision_date": decision_date,
                    "complete_date": complete_date,
                    "scraped_at": today,
                }
            )

            if delay > 0:
                time.sleep(delay)

    if not new_rows:
        return 0

    new_df = pl.DataFrame(new_rows, schema=_OUTPUT_SCHEMA)
    combined = pl.concat([existing, new_df], how="diagonal")

    ref_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(ref_path)
    return len(new_rows)
```

**Step 2: Run tests to verify they pass**

```bash
uv run pytest tests/test_aic.py -v
```

Expected: all 5 tests pass.

**Step 3: Lint**

```bash
uv run ruff check src/zoneto/sources/aic.py tests/test_aic.py
uv run ty check src/
```

Expected: no errors.

**Step 4: Run full test suite**

```bash
uv run pytest -qq
```

Expected: all tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/sources/aic.py tests/test_aic.py
git commit -m "feat: add AIC scraper fetch_aic_decisions() with tests"
```

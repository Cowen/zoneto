# Product Strategy Pivot — Phase 6: OLT Decision Scraping

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Scrape Ontario Land Tribunal decisions for Toronto applications, then fuzzy-match them to dev_applications via address similarity, enriching the dataset with `olt_case_number`, `olt_outcome`, and `olt_decision_date` columns.

**Architecture:** `fetch_olt_decisions()` in `sources/olt.py` scrapes `olt.gov.on.ca/decisions/` HTML pages filtered by municipality "Toronto". Rate-limited (2.0s default). Writes `data/reference/olt_decisions.parquet`. `match_olt_to_dev()` in `enrich.py` uses `difflib.SequenceMatcher` with a date proximity filter and confidence threshold to join OLT cases to dev_applications by address. CLI gets a new `zoneto olt` command and `zoneto enrich --fetch-olt/--no-fetch-olt` flag.

**Tech Stack:** httpx, BeautifulSoup (already installed), difflib (stdlib), polars, pytest-httpx

**Scope:** Phase 6 of 8. Depends on Phase 5 (AIC data provides application records to match against).

**Codebase verified:** 2026-03-17

---

## Task 1: Create the OLT scraper

**Files:**
- Create: `src/zoneto/sources/olt.py`
- Create: `tests/sources/test_olt.py`

**Step 1: Write the failing test**

Create `tests/sources/test_olt.py`:

```python
"""Tests for OLT decision scraper."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from pytest_httpx import HTTPXMock

from zoneto.sources.olt import fetch_olt_decisions

_OLT_BASE = "https://olt.gov.on.ca"
_OLT_SEARCH_URL = _OLT_BASE + "/decisions/"


def _make_search_html(cases: list[dict]) -> str:
    """Minimal HTML mimicking OLT search results table."""
    rows = ""
    for c in cases:
        rows += f"""
        <tr>
            <td><a href="/decisions/{c['case_number']}">{c['case_number']}</a></td>
            <td>{c.get('municipality', 'Toronto')}</td>
            <td>{c.get('hearing_date', '2023-01-15')}</td>
            <td>{c.get('decision_date', '2023-03-10')}</td>
            <td>{c.get('outcome', 'Dismissed')}</td>
            <td>{c.get('address', '100 King St W, Toronto')}</td>
        </tr>"""
    return f"""<!DOCTYPE html><html><body>
    <table id="decisions-table">
    <thead><tr>
      <th>Case Number</th><th>Municipality</th>
      <th>Hearing Date</th><th>Decision Date</th>
      <th>Outcome</th><th>Address</th>
    </tr></thead>
    <tbody>{rows}</tbody>
    </table>
    </body></html>"""


def _make_empty_page_html() -> str:
    return """<!DOCTYPE html><html><body>
    <table id="decisions-table"><thead></thead><tbody></tbody></table>
    </body></html>"""


def test_fetch_olt_decisions_writes_parquet(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """fetch_olt_decisions() writes olt_decisions.parquet to data/reference/."""
    cases = [
        {
            "case_number": "PL220001",
            "municipality": "Toronto",
            "hearing_date": "2022-09-12",
            "decision_date": "2022-11-30",
            "outcome": "Dismissed",
            "address": "500 Queen St W, Toronto",
        }
    ]
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_search_html(cases))
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_empty_page_html())

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 1

    out_path = tmp_path / "reference" / "olt_decisions.parquet"
    assert out_path.exists()
    df = pl.read_parquet(out_path)
    assert "case_number" in df.columns
    assert "outcome" in df.columns
    assert "decision_date" in df.columns
    assert "address" in df.columns


def test_fetch_olt_decisions_parses_case_fields(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Parsed fields match the source HTML values."""
    cases = [
        {
            "case_number": "OLT-22-000123",
            "municipality": "Toronto",
            "hearing_date": "2022-09-12",
            "decision_date": "2022-11-30",
            "outcome": "Allowed",
            "address": "200 Front St W, Toronto",
        }
    ]
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_search_html(cases))
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_empty_page_html())

    fetch_olt_decisions(tmp_path, delay=0.0)
    df = pl.read_parquet(tmp_path / "reference" / "olt_decisions.parquet")

    assert df["case_number"][0] == "OLT-22-000123"
    assert df["outcome"][0] == "Allowed"
    assert df["address"][0] == "200 Front St W, Toronto"


def test_fetch_olt_decisions_empty_results(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Returns 0 and does not write file when no decisions found."""
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_empty_page_html())

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 0
    assert not (tmp_path / "reference" / "olt_decisions.parquet").exists()


def test_fetch_olt_decisions_paginates(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    """Fetches multiple pages until an empty page is returned."""
    page1 = [{"case_number": f"PL2200{i:02d}", "address": f"{i} King St"} for i in range(3)]
    page2 = [{"case_number": f"PL2200{i:02d}", "address": f"{i} Queen St"} for i in range(3, 5)]

    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_search_html(page1))
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_search_html(page2))
    httpx_mock.add_response(url=_OLT_SEARCH_URL, text=_make_empty_page_html())

    count = fetch_olt_decisions(tmp_path, delay=0.0)
    assert count == 5
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/sources/test_olt.py -v
```

Expected: `ModuleNotFoundError: No module named 'zoneto.sources.olt'`

**Step 3: Create `src/zoneto/sources/olt.py`**

```python
"""OLT (Ontario Land Tribunal) decision scraper."""
from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_OLT_BASE = "https://olt.gov.on.ca"
_OLT_SEARCH_URL = _OLT_BASE + "/decisions/"
_MUNICIPALITY = "Toronto"

_OUTPUT_SCHEMA = {
    "case_number": pl.String,
    "municipality": pl.String,
    "hearing_date": pl.String,
    "decision_date": pl.String,
    "outcome": pl.String,
    "address": pl.String,
    "scraped_at": pl.Date,
}


def _parse_decisions_page(html: str) -> list[dict]:
    """Parse OLT decisions HTML page. Returns list of case dicts."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "decisions-table"})
    if not table:
        # Fall back: look for any table with relevant headers
        for t in soup.find_all("table"):
            headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if "case number" in headers or "outcome" in headers:
                table = t
                break
    if not table:
        return []

    rows = []
    tbody = table.find("tbody") or table
    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 4:
            continue
        # Column order: case_number, municipality, hearing_date, decision_date, outcome, address
        row: dict = {
            "case_number": cells[0].get_text(strip=True),
            "municipality": cells[1].get_text(strip=True) if len(cells) > 1 else "",
            "hearing_date": cells[2].get_text(strip=True) if len(cells) > 2 else "",
            "decision_date": cells[3].get_text(strip=True) if len(cells) > 3 else "",
            "outcome": cells[4].get_text(strip=True) if len(cells) > 4 else "",
            "address": cells[5].get_text(strip=True) if len(cells) > 5 else "",
        }
        if row["case_number"]:
            rows.append(row)
    return rows


def fetch_olt_decisions(
    data_dir: Path,
    *,
    delay: float = 2.0,
    municipality: str = _MUNICIPALITY,
    max_pages: int = 500,
) -> int:
    """Scrape OLT decisions for a given municipality and write to Parquet.

    Paginates OLT search results until an empty page is returned or max_pages
    is reached. Rate-limited to `delay` seconds between requests.

    Writes data_dir/reference/olt_decisions.parquet.
    Returns count of decisions fetched.
    """
    today = date.today()
    all_rows: list[dict] = []

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        for page_num in range(1, max_pages + 1):
            params: dict[str, str | int] = {
                "municipality": municipality,
                "page": page_num,
            }
            resp = client.get(_OLT_SEARCH_URL, params=params)
            resp.raise_for_status()

            rows = _parse_decisions_page(resp.text)
            if not rows:
                logger.info("OLT: empty page at page %d — done", page_num)
                break

            all_rows.extend(rows)
            logger.info("OLT: page %d — %d cases (total: %d)", page_num, len(rows), len(all_rows))

            if delay > 0 and page_num < max_pages:
                time.sleep(delay)

    if not all_rows:
        return 0

    for row in all_rows:
        row["scraped_at"] = today

    df = pl.DataFrame(all_rows).cast({"scraped_at": pl.Date})
    out_path = data_dir / "reference" / "olt_decisions.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    logger.info("OLT: wrote %d decisions to %s", len(df), out_path)
    return len(df)
```

**Step 4: Run tests**

```bash
uv run pytest tests/sources/test_olt.py -v
```

Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/sources/olt.py tests/sources/test_olt.py
git commit -m "feat: add OLT decision scraper (fetch_olt_decisions)"
```

---

## Task 2: Add match_olt_to_dev() fuzzy matching in enrich.py

**Files:**
- Modify: `src/zoneto/analytics/enrich.py`
- Create: `tests/analytics/test_olt_matching.py`

**Step 1: Write the failing test**

Create `tests/analytics/test_olt_matching.py`:

```python
"""Tests for OLT-to-dev_applications fuzzy address matching."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import match_olt_to_dev


@pytest.fixture
def olt_parquet(tmp_path: Path) -> Path:
    """Minimal olt_decisions.parquet with known cases."""
    df = pl.DataFrame(
        {
            "case_number": ["OLT-22-001", "OLT-22-002", "OLT-23-003"],
            "outcome": ["Dismissed", "Allowed", "Dismissed"],
            "decision_date": ["2022-11-30", "2023-02-14", "2023-06-01"],
            "address": [
                "100 King St W, Toronto",
                "200 Queen St W, Toronto",
                "999 Remote Ave, Toronto",
            ],
        }
    )
    path = tmp_path / "reference" / "olt_decisions.parquet"
    path.parent.mkdir(parents=True)
    df.write_parquet(path)
    return path


@pytest.fixture
def dev_parquet(tmp_path: Path) -> Path:
    """Minimal dev_applications parquet with addresses matching OLT cases."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002", "F003"],
            "street_num": ["100", "200", "300"],
            "street_name": ["King St W", "Queen St W", "Bay St"],
            "year_submitted": pl.Series([2021, 2022, 2021], dtype=pl.Int32),
            "application_type": ["OZ", "OZ", "SA"],
        }
    )
    path = tmp_path / "enriched" / "dev_applications_staging.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return path


def test_match_olt_high_confidence_case(olt_parquet: Path, dev_parquet: Path) -> None:
    """F001 address '100 King St W' matches OLT case '100 King St W, Toronto'."""
    dev_df = pl.read_parquet(dev_parquet)
    result = match_olt_to_dev(dev_df, olt_parquet.parent.parent)

    f001 = result.filter(pl.col("folderrsn") == "F001")
    assert f001["olt_case_number"][0] == "OLT-22-001"
    assert f001["olt_outcome"][0] == "Dismissed"


def test_match_olt_no_match_returns_null(olt_parquet: Path, dev_parquet: Path) -> None:
    """F003 '300 Bay St' has no close OLT match — OLT columns are null."""
    dev_df = pl.read_parquet(dev_parquet)
    result = match_olt_to_dev(dev_df, olt_parquet.parent.parent)

    f003 = result.filter(pl.col("folderrsn") == "F003")
    assert f003["olt_case_number"][0] is None


def test_match_olt_columns_present_when_no_olt_data(tmp_path: Path) -> None:
    """When olt_decisions.parquet is absent, columns are added as all-null."""
    dev_df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "street_num": ["100"],
            "street_name": ["King St W"],
            "year_submitted": pl.Series([2021], dtype=pl.Int32),
        }
    )
    result = match_olt_to_dev(dev_df, tmp_path)

    assert "olt_case_number" in result.columns
    assert "olt_outcome" in result.columns
    assert "olt_decision_date" in result.columns
    assert result["olt_case_number"][0] is None


def test_match_olt_confidence_threshold_filters_weak_matches(
    tmp_path: Path,
) -> None:
    """Addresses with similarity below threshold produce null OLT columns."""
    olt_df = pl.DataFrame(
        {
            "case_number": ["OLT-22-999"],
            "outcome": ["Allowed"],
            "decision_date": ["2022-11-30"],
            "address": ["9999 Completely Different Rd, Toronto"],
        }
    )
    olt_path = tmp_path / "reference" / "olt_decisions.parquet"
    olt_path.parent.mkdir(parents=True)
    olt_df.write_parquet(olt_path)

    dev_df = pl.DataFrame(
        {
            "folderrsn": ["F001"],
            "street_num": ["100"],
            "street_name": ["King St W"],
            "year_submitted": pl.Series([2021], dtype=pl.Int32),
        }
    )
    result = match_olt_to_dev(dev_df, tmp_path)
    assert result["olt_case_number"][0] is None
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/analytics/test_olt_matching.py -v
```

Expected: `ImportError: cannot import name 'match_olt_to_dev' from 'zoneto.analytics.enrich'`

**Step 3: Add `match_olt_to_dev()` to `src/zoneto/analytics/enrich.py`**

Add this function near the end of `enrich.py`, before or after `enrich_permits()`:

```python
def match_olt_to_dev(
    dev_df: pl.DataFrame,
    data_dir: Path,
    *,
    confidence_threshold: float = 0.75,
) -> pl.DataFrame:
    """Fuzzy-match OLT decisions to dev_applications via address similarity.

    Reads data_dir/reference/olt_decisions.parquet (if present).
    For each dev_application, finds the OLT case with the highest address
    similarity score above `confidence_threshold`.

    Address similarity is computed using difflib.SequenceMatcher on the
    normalized address strings (street_num + street_name vs OLT address).

    Adds columns: olt_case_number (String|null), olt_outcome (String|null),
    olt_decision_date (String|null).

    Returns the enriched DataFrame. If olt_decisions.parquet is absent,
    adds the three columns as all-null and returns unchanged.
    """
    import difflib  # stdlib

    null_cols = [
        pl.lit(None, dtype=pl.String).alias("olt_case_number"),
        pl.lit(None, dtype=pl.String).alias("olt_outcome"),
        pl.lit(None, dtype=pl.String).alias("olt_decision_date"),
    ]

    olt_path = data_dir / "reference" / "olt_decisions.parquet"
    if not olt_path.exists():
        return dev_df.with_columns(null_cols)

    olt_df = pl.read_parquet(olt_path)
    # Normalize OLT addresses: lowercase, strip city suffix
    olt_addresses = [
        a.lower().split(",")[0].strip()
        for a in olt_df["address"].fill_null("").to_list()
    ]
    olt_cases = olt_df["case_number"].fill_null("").to_list()
    olt_outcomes = olt_df["outcome"].fill_null("").to_list()
    olt_dates = olt_df["decision_date"].fill_null("").to_list()

    matched_cases: list[str | None] = []
    matched_outcomes: list[str | None] = []
    matched_dates: list[str | None] = []

    for row in dev_df.iter_rows(named=True):
        street_num = str(row.get("street_num") or "").strip()
        street_name = str(row.get("street_name") or "").strip()
        dev_address = f"{street_num} {street_name}".lower().strip()

        if not dev_address.strip():
            matched_cases.append(None)
            matched_outcomes.append(None)
            matched_dates.append(None)
            continue

        best_ratio = 0.0
        best_idx = -1
        for i, olt_addr in enumerate(olt_addresses):
            ratio = difflib.SequenceMatcher(None, dev_address, olt_addr).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_idx = i

        if best_ratio >= confidence_threshold and best_idx >= 0:
            matched_cases.append(olt_cases[best_idx])
            matched_outcomes.append(olt_outcomes[best_idx])
            matched_dates.append(olt_dates[best_idx])
        else:
            matched_cases.append(None)
            matched_outcomes.append(None)
            matched_dates.append(None)

    return dev_df.with_columns(
        [
            pl.Series("olt_case_number", matched_cases, dtype=pl.String),
            pl.Series("olt_outcome", matched_outcomes, dtype=pl.String),
            pl.Series("olt_decision_date", matched_dates, dtype=pl.String),
        ]
    )
```

**Step 4: Run tests**

```bash
uv run pytest tests/analytics/test_olt_matching.py -v
```

Expected: All tests pass.

**Step 5: Commit**

```bash
git add src/zoneto/analytics/enrich.py tests/analytics/test_olt_matching.py
git commit -m "feat: add match_olt_to_dev() fuzzy address matching for OLT decisions"
```

---

## Task 3: Add `zoneto olt` CLI command and `--fetch-olt` enrich flag

**Files:**
- Modify: `src/zoneto/cli.py`

**Step 1: Add `olt` command to `cli.py`**

Add after the `aic` command (around line 99):

```python
@app.command()
def olt(
    delay: float = typer.Option(
        2.0, help="Delay between OLT page requests (seconds). Respect the government site."
    ),
) -> None:
    """Scrape Ontario Land Tribunal decisions for Toronto applications.

    Writes data/reference/olt_decisions.parquet. Use 'zoneto enrich --fetch-olt'
    to join OLT decisions to dev_applications after scraping.
    """
    from zoneto.sources.olt import fetch_olt_decisions  # noqa: PLC0415

    console.print("[bold]Scraping OLT decisions...[/bold]")
    try:
        n = fetch_olt_decisions(DATA_DIR, delay=delay)
        console.print(f"[green]✓[/green] OLT decisions: {n} records written")
    except Exception as exc:
        console.print(f"  [red]✗ {exc}[/red]")
        raise typer.Exit(code=1)
```

**Step 2: Add `--fetch-olt/--no-fetch-olt` flag to the `enrich` command**

Find the `enrich` command in `cli.py` (around line 102). It currently has `--fetch-ref` and `--fetch-aic` flags. Add a `--fetch-olt` flag following the same pattern:

```python
@app.command()
def enrich(
    fetch_ref: bool = typer.Option(
        True, "--fetch-ref/--no-fetch-ref", help="Download reference datasets."
    ),
    fetch_aic: bool = typer.Option(
        True, "--fetch-aic/--no-fetch-aic", help="Scrape AIC for decision dates."
    ),
    fetch_olt: bool = typer.Option(
        False,
        "--fetch-olt/--no-fetch-olt",
        help="Match OLT decisions to dev_applications (requires prior 'zoneto olt' run).",
    ),
) -> None:
    """Enrich raw Parquet with spatial features and outcome labels."""
    ...
    # At the end of the enrich command, after existing enrichment calls,
    # add OLT matching:
    if fetch_olt:
        from zoneto.analytics.enrich import match_olt_to_dev  # noqa: PLC0415
        import polars as pl  # noqa: PLC0415

        enriched_dev_path = DATA_DIR / "enriched" / "dev_applications.parquet"
        if enriched_dev_path.exists():
            dev_df = pl.read_parquet(enriched_dev_path)
            dev_df = match_olt_to_dev(dev_df, DATA_DIR)
            dev_df.write_parquet(enriched_dev_path)
            console.print("[green]✓[/green] OLT decisions matched to dev_applications")
        else:
            console.print("[yellow]⚠[/yellow] enriched dev_applications not found — run enrich first")
```

> **Note for implementer:** The `enrich` command currently has specific enrichment calls. Add the `fetch_olt` parameter to the function signature alongside the existing `fetch_ref` and `fetch_aic` parameters, and append the OLT matching block at the end of the try block. Do not restructure the existing enrichment logic.

**Step 3: Add `olt` task to justfile**

```makefile
# Scrape OLT decisions for Toronto
olt:
    uv run zoneto olt
```

**Step 4: Verify CLI**

```bash
uv run zoneto olt --help
uv run zoneto enrich --help
```

Expected: Both commands show expected options.

**Step 5: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 6: Run linter and type checker**

```bash
uv run ruff check src/zoneto/sources/olt.py src/zoneto/analytics/enrich.py src/zoneto/cli.py
uv run ty check src/zoneto/sources/olt.py src/zoneto/analytics/enrich.py src/zoneto/cli.py
```

Expected: No errors.

**Step 7: Commit**

```bash
git add src/zoneto/cli.py justfile
git commit -m "feat: add 'zoneto olt' command and 'enrich --fetch-olt' flag"
```

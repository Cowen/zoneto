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


def _parse_decisions_page(html: str) -> list[dict]:
    """Parse OLT decisions HTML page. Returns list of case dicts."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "decisions-table"})
    if not table:
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
            logger.info(
                "OLT: page %d — %d cases (total: %d)",
                page_num,
                len(rows),
                len(all_rows),
            )

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

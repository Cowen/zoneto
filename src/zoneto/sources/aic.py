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


def _parse_milestones(
    html: str, application_type: str
) -> tuple[date | None, date | None]:
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
    chunk_size: int = 100,
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

    # Scrape only un-cached rows, deduplicated (same folderrsn can appear in
    # multiple year partitions)
    rows_to_scrape = df.filter(~pl.col("folderrsn").is_in(list(scraped_ids))).unique(
        subset=["folderrsn"], keep="first", maintain_order=True
    )
    total_to_scrape = len(rows_to_scrape)

    logger.info(
        "AIC: %d applications to scrape (%d already cached)",
        total_to_scrape,
        len(scraped_ids),
    )

    new_rows: list[dict] = []
    total_new = 0
    today = date.today()

    def _flush() -> None:
        nonlocal existing, new_rows, total_new
        new_df = pl.DataFrame(new_rows, schema=_OUTPUT_SCHEMA)
        existing = pl.concat([existing, new_df], how="diagonal")
        ref_path.parent.mkdir(parents=True, exist_ok=True)
        existing.write_parquet(ref_path)
        total_new += len(new_rows)
        new_rows = []
        logger.info("AIC: scraped %d / %d", total_new, total_to_scrape)

    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
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

            decision_date, complete_date = _parse_milestones(response.text, app_type)

            new_rows.append(
                {
                    "folderrsn": folderrsn,
                    "decision_date": decision_date,
                    "complete_date": complete_date,
                    "scraped_at": today,
                }
            )

            if len(new_rows) >= chunk_size:
                _flush()

            if delay > 0:
                time.sleep(delay)

    if new_rows:
        _flush()

    return total_new

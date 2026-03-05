from __future__ import annotations

import io
import re

import httpx
import polars as pl

from zoneto.models import CKANConfig

CKAN_BASE = "https://ckan0.cf.opendata.inter.prod-toronto.ca"


class CKANSource:
    """Fetches data from a City of Toronto CKAN dataset."""

    def __init__(self, config: CKANConfig) -> None:
        self.config = config

    @property
    def name(self) -> str:
        return self.config.dataset_id

    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        with httpx.Client(base_url=CKAN_BASE, timeout=120.0) as client:
            if self.config.access_mode == "datastore":
                df = self._fetch_datastore(client)
            else:
                df = self._fetch_bulk_csv(client)
        df = self._normalize(df)
        if not df.is_empty():
            # Keep null-date records (year=0) regardless of year_start;
            # exclude records whose year is known but predates the window.
            df = df.filter(
                (pl.col("year") == 0) | (pl.col("year") >= self.config.year_start)
            )
        return df

    def _datastore_resource_id(self, client: httpx.Client) -> str:
        """Return the resource ID of the first datastore-enabled resource."""
        resp = client.get(
            "/api/3/action/package_show",
            params={"id": self.config.dataset_id},
        )
        resp.raise_for_status()
        for resource in resp.json()["result"]["resources"]:
            if resource.get("datastore_active"):
                return str(resource["id"])
        raise ValueError(f"No datastore resource found for {self.config.dataset_id!r}")

    def _fetch_datastore(self, client: httpx.Client) -> pl.DataFrame:
        """Paginate through datastore_search until the response has no records."""
        resource_id = self._datastore_resource_id(client)
        limit = 32000
        offset = 0
        records: list[dict] = []

        while True:
            resp = client.get(
                "/api/3/action/datastore_search",
                params={
                    "id": resource_id,
                    "limit": limit,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            page_records: list[dict] = resp.json()["result"]["records"]
            if not page_records:
                break
            records.extend(page_records)
            offset += limit

        if not records:
            return pl.DataFrame()
        return pl.from_dicts(records, infer_schema_length=None)

    def _fetch_bulk_csv(self, client: httpx.Client) -> pl.DataFrame:
        """Download year-based CSV resources discovered via package_show."""
        resp = client.get(
            "/api/3/action/package_show",
            params={"id": self.config.dataset_id},
        )
        resp.raise_for_status()
        resources: list[dict] = resp.json()["result"]["resources"]

        dfs: list[pl.DataFrame] = []
        for resource in resources:
            if resource.get("format", "").upper() != "CSV":
                continue
            match = re.search(r"\b(20\d{2})\b", resource.get("name", ""))
            if match is None:
                continue
            if int(match.group(1)) < self.config.year_start:
                continue
            csv_resp = client.get(resource["url"])
            csv_resp.raise_for_status()
            df = pl.read_csv(
                io.BytesIO(csv_resp.content),
                infer_schema_length=None,
                truncate_ragged_lines=True,
            )
            dfs.append(df)

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal")

    def _normalize(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize column names, parse dates, derive year, add source_name."""
        if df.is_empty():
            return df

        # 1. Rename all columns to snake_case, appending _2/_3/... on collisions
        seen: dict[str, int] = {}
        rename_map: dict[str, str] = {}
        for c in df.columns:
            base = re.sub(r"[^a-z0-9]+", "_", c.lower()).strip("_")
            count = seen.get(base, 0) + 1
            seen[base] = count
            rename_map[c] = base if count == 1 else f"{base}_{count}"
        df = df.rename(rename_map)

        # 2. Parse all columns whose names contain "date" (best-effort, nulls allowed)
        # If polars cannot auto-detect the date format it raises ComputeError;
        # leave the column as a string rather than crashing.
        date_cols = [c for c in df.columns if "date" in c]
        for col in date_cols:
            try:
                df = df.with_columns(
                    pl.col(col).cast(pl.String).str.to_date(strict=False).alias(col)
                )
            except pl.exceptions.ComputeError:
                pass  # unrecognisable format — leave column as string

        # 3. Derive year from the configured year_column (null dates → year 0)
        # Only possible if the column was successfully parsed as pl.Date.
        year_col = self.config.year_column
        if year_col in df.columns and df.schema[year_col] == pl.Date:
            df = df.with_columns(
                pl.col(year_col).dt.year().fill_null(0).cast(pl.Int32).alias("year")
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int32).alias("year"))

        # 4. Label records with their source dataset ID
        df = df.with_columns(pl.lit(self.config.dataset_id).alias("source_name"))

        return df

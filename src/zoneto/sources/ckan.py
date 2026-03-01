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
        return self._normalize(df)

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
        return pl.DataFrame(records)

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

        # 1. Rename all columns to snake_case
        rename_map = {
            c: re.sub(r"[^a-z0-9]+", "_", c.lower()).strip("_") for c in df.columns
        }
        df = df.rename(rename_map)

        # 2. Parse all columns whose names contain "date" (best-effort, nulls allowed)
        date_cols = [c for c in df.columns if "date" in c]
        for col in date_cols:
            df = df.with_columns(
                pl.col(col).cast(pl.String).str.to_date(strict=False).alias(col)
            )

        # 3. Derive year from application_date (null dates → year 0)
        if "application_date" in df.columns:
            df = df.with_columns(
                pl.col("application_date")
                .dt.year()
                .fill_null(0)
                .cast(pl.Int32)
                .alias("year")
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int32).alias("year"))

        # 4. Label records with their source dataset ID
        df = df.with_columns(pl.lit(self.config.dataset_id).alias("source_name"))

        return df

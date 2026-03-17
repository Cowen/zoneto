"""DuckDB query builder for comparable development applications."""

from __future__ import annotations

import datetime
import math
from pathlib import Path
from typing import Any

import duckdb


def query_comps(
    enriched_path: Path,
    *,
    application_type: str | None = None,
    ward_number: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_m: float = 500.0,
    years: int = 5,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Query comparable development applications from enriched Parquet.

    Returns applications matching filters, sorted by proximity when lat/lon
    provided, otherwise by recency (year_submitted DESC).
    """
    # Defensive path validation: DuckDB does not support parameterized
    # read_parquet paths, so we interpolate the path into SQL. The path is
    # always server-controlled, set via app.state.data_dir by the FastAPI app
    # factory. This check catches missing/invalid files and returns empty result.
    if not enriched_path.exists() or enriched_path.suffix != ".parquet":
        return []

    current_year = datetime.date.today().year
    year_cutoff = current_year - years

    # --- build WHERE conditions and positional params ---
    conditions: list[str] = [
        "year_submitted IS NOT NULL",
        f"year_submitted >= {year_cutoff}",
    ]
    params: list[Any] = []

    if application_type is not None:
        conditions.append("application_type = ?")
        params.append(application_type)

    if ward_number is not None:
        conditions.append("CAST(ward_number AS VARCHAR) = ?")
        params.append(str(ward_number))

    # --- spatial bounding box (approximate, safe for < 50 km radius) ---
    distance_expr = "NULL"
    order_by = "year_submitted DESC NULLS LAST"

    if lat is not None and lon is not None:
        lat_delta = radius_m / 111_111.0
        lon_delta = radius_m / (111_111.0 * math.cos(math.radians(lat)))
        lat_min = lat - lat_delta
        lat_max = lat + lat_delta
        lon_min = lon - lon_delta
        lon_max = lon + lon_delta

        conditions.extend(
            [
                "lat IS NOT NULL",
                "lon IS NOT NULL",
                "lat BETWEEN ? AND ?",
                "lon BETWEEN ? AND ?",
            ]
        )
        params.extend([lat_min, lat_max, lon_min, lon_max])
        # squared Euclidean distance in degrees (sufficient for proximity sort)
        distance_expr = (
            f"((lat - {lat}) * (lat - {lat}) + (lon - {lon}) * (lon - {lon}))"
        )
        order_by = "dist_sq ASC"

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            CAST(folderrsn AS VARCHAR)   AS folderrsn,
            application_type,
            CAST(ward_number AS VARCHAR) AS ward_number,
            zoning_class,
            status,
            CAST(year_submitted AS INTEGER) AS year_submitted,
            lat,
            lon,
            CAST(dev_approved AS INTEGER)         AS dev_approved,
            CAST(dev_appealed AS INTEGER)         AS dev_appealed,
            CAST(dev_days_to_decision AS INTEGER) AS dev_days_to_decision,
            CAST(proposed_storeys AS INTEGER)     AS proposed_storeys,
            CAST(proposed_units AS INTEGER)       AS proposed_units,
            description,
            COALESCE(CAST(street_num AS VARCHAR), '') || ' ' ||
                COALESCE(street_name, '')             AS street_address,
            {distance_expr}                           AS dist_sq
        FROM read_parquet('{enriched_path}')
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {limit}
    """

    con = duckdb.connect()
    try:
        result = con.execute(sql, params).pl()
        records: list[dict[str, Any]] = result.to_dicts()
        # drop internal dist_sq column when no spatial filter used
        if lat is None:
            for r in records:
                r.pop("dist_sq", None)
        return records
    finally:
        con.close()

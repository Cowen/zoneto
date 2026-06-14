"""Spatial lookup of site context for a point (zoning, heritage, MTSA)."""

from __future__ import annotations

import datetime
import math
from pathlib import Path
from typing import Any

import duckdb

# By-law 569-2013 GEN_ZONE numeric codes → human-readable category.
# Source: docs/zoning_readme.txt.
_GEN_ZONE_LABEL: dict[int, str] = {
    0: "Residential",
    1: "Open Space",
    2: "Utility / Transportation",
    4: "Employment Industrial",
    5: "Institutional",
    6: "Commercial Residential Employment (mixed)",
    101: "Residential Apartment",
    201: "Commercial",
    202: "Commercial Residential (mixed)",
}


def _positive_or_none(value: Any) -> float | None:
    """Return value as float when > 0, else None. By-law sentinels (-1, 0) → None."""
    if value is None:
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def lookup_site_context(lat: float, lon: float, ref_dir: Path) -> dict[str, Any]:
    """Look up spatial context for a single lat/lon point against reference geodata.

    Performs point-in-polygon queries against:
    - zoning.geojson → zoning_class
    - heritage_register/*.shp → in_heritage_register
    - heritage_districts/*.shp → in_heritage_district
    - secondary_plans.geojson → secondary_plan_name, in_secondary_plan
    - mtsa.shp → in_mtsa

    Returns a dict with all spatial flags. Missing reference files are handled
    gracefully (defaults to None/0).
    """
    result: dict[str, Any] = {
        "zoning_class": None,
        "zoning_max_units": None,
        "zoning_max_density": None,
        "zoning_max_storeys": None,
        "zoning_max_height_m": None,
        "permitted_use_category": None,
        "zoning_min_frontage_m": None,
        "zoning_min_lot_area_sqm": None,
        "zoning_max_coverage_pct": None,
        "zoning_min_sqm_per_unit": None,
        "zoning_holding": 0,
        "zoning_exception": 0,
        "zoning_exception_no": None,
        "zoning_pct_res": None,
        "zoning_pct_comm": None,
        "zoning_pct_emp": None,
        "in_heritage_register": 0,
        "in_heritage_district": 0,
        "secondary_plan_name": None,
        "in_secondary_plan": 0,
        "in_mtsa": 0,
        "in_trca_regulated_area": 0,
        "in_greenbelt": 0,
        "op_land_use_designation": None,
    }

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    point_sql = f"ST_Point({lon}, {lat})"

    # Zoning (class + permitted density/units + physical constraints from by-law)
    zoning_path = ref_dir / "zoning.geojson"
    if zoning_path.exists():
        escaped = str(zoning_path).replace("'", "''")
        # ST_Read infers columns from the first feature in the GeoJSON; older
        # or trimmed-down fixtures may omit some by-law fields. Introspect the
        # schema and substitute NULL for any column that isn't present so the
        # query still runs.
        desc = con.execute(f"DESCRIBE SELECT * FROM ST_Read('{escaped}')").fetchall()
        available = {str(r[0]).upper() for r in desc}
        wanted = [
            "ZN_ZONE",
            "UNITS",
            "DENSITY",
            "GEN_ZONE",
            "FRONTAGE",
            "ZN_AREA",
            "COVERAGE",
            "AREA_UNITS",
            "ZN_HOLDING",
            "ZN_EXCPTN",
            "EXCPTN_NO",
            "PRCNT_RES",
            "PRCNT_COMM",
            "PRCNT_EMMP",
        ]
        cols_sql = ",\n                ".join(
            f"z.{c}" if c in available else f"NULL AS {c}" for c in wanted
        )
        rows = con.execute(f"""
            SELECT
                {cols_sql}
            FROM ST_Read('{escaped}') z
            WHERE ST_Within({point_sql}, z.geom)
            LIMIT 1
        """).fetchall()
        # Fallback: if point misses all polygons (geocoding imprecision), snap to
        # nearest polygon within ~200m (0.002 degrees at Toronto's latitude).
        if not rows:
            rows = con.execute(f"""
                SELECT
                    {cols_sql}
                FROM ST_Read('{escaped}') z
                WHERE ST_DWithin({point_sql}, z.geom, 0.002)
                ORDER BY ST_Distance({point_sql}, z.geom)
                LIMIT 1
            """).fetchall()

        if rows:
            row = rows[0]
            result["zoning_class"] = row[0]
            units_val = _positive_or_none(row[1])
            if units_val is not None:
                result["zoning_max_units"] = int(units_val)
            density_val = _positive_or_none(row[2])
            if density_val is not None:
                result["zoning_max_density"] = density_val

            gen_zone_raw = row[3]
            if gen_zone_raw is not None:
                try:
                    result["permitted_use_category"] = _GEN_ZONE_LABEL.get(
                        int(gen_zone_raw)
                    )
                except (TypeError, ValueError):
                    pass

            result["zoning_min_frontage_m"] = _positive_or_none(row[4])
            result["zoning_min_lot_area_sqm"] = _positive_or_none(row[5])
            result["zoning_max_coverage_pct"] = _positive_or_none(row[6])
            result["zoning_min_sqm_per_unit"] = _positive_or_none(row[7])

            holding_raw = row[8]
            if isinstance(holding_raw, str) and holding_raw.strip().upper() == "Y":
                result["zoning_holding"] = 1

            exception_raw = row[9]
            if isinstance(exception_raw, str) and exception_raw.strip().upper() == "Y":
                result["zoning_exception"] = 1
                exception_no = row[10]
                if exception_no is not None and str(exception_no).strip():
                    result["zoning_exception_no"] = str(exception_no)

            result["zoning_pct_res"] = _positive_or_none(row[11])
            result["zoning_pct_comm"] = _positive_or_none(row[12])
            result["zoning_pct_emp"] = _positive_or_none(row[13])

    # Heritage register (point data — use buffer intersection)
    hr_dir = ref_dir / "heritage_register"
    if hr_dir.exists():
        shp_files = list(hr_dir.glob("*.shp"))
        if shp_files:
            escaped = str(shp_files[0]).replace("'", "''")
            rows = con.execute(f"""
                SELECT 1
                FROM ST_Read('{escaped}') h
                WHERE ST_Intersects(ST_Buffer({point_sql}, 0.0002), h.geom)
                LIMIT 1
            """).fetchall()
            if rows:
                result["in_heritage_register"] = 1

    # Heritage districts (polygon data)
    hd_dir = ref_dir / "heritage_districts"
    if hd_dir.exists():
        shp_files = list(hd_dir.glob("*.shp"))
        if shp_files:
            escaped = str(shp_files[0]).replace("'", "''")
            rows = con.execute(f"""
                SELECT 1
                FROM ST_Read('{escaped}') h
                WHERE ST_Within({point_sql}, h.geom)
                LIMIT 1
            """).fetchall()
            if rows:
                result["in_heritage_district"] = 1

    # Secondary plans
    sp_path = ref_dir / "secondary_plans.geojson"
    if sp_path.exists():
        escaped = str(sp_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT s.SECONDARY_PLAN_NAME
            FROM ST_Read('{escaped}') s
            WHERE ST_Within({point_sql}, s.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            result["secondary_plan_name"] = rows[0][0]
            result["in_secondary_plan"] = 1

    # MTSA boundaries
    mtsa_path = ref_dir / "mtsa.shp"
    if mtsa_path.exists():
        escaped = str(mtsa_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT 1
            FROM ST_Read('{escaped}') m
            WHERE ST_Within({point_sql}, m.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            result["in_mtsa"] = 1

    # TRCA regulated areas
    trca_path = ref_dir / "trca_regulated_areas.geojson"
    if trca_path.exists():
        escaped = str(trca_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT 1
            FROM ST_Read('{escaped}') t
            WHERE ST_Within({point_sql}, t.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            result["in_trca_regulated_area"] = 1

    # Ontario Greenbelt boundary
    greenbelt_path = ref_dir / "greenbelt.geojson"
    if greenbelt_path.exists():
        escaped = str(greenbelt_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT 1
            FROM ST_Read('{escaped}') g
            WHERE ST_Within({point_sql}, g.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            result["in_greenbelt"] = 1

    # Official Plan land-use designation (interim Borealis source). Like zoning,
    # parcel/designation polygons can miss off-parcel geocodes — snap to the nearest
    # designation within ~200m on a strict-within miss.
    op_path = ref_dir / "op_land_use.geojson"
    if op_path.exists():
        escaped = str(op_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT op.op_designation
            FROM ST_Read('{escaped}') op
            WHERE ST_Within({point_sql}, op.geom)
            LIMIT 1
        """).fetchall()
        if not rows:
            rows = con.execute(f"""
                SELECT op.op_designation
                FROM ST_Read('{escaped}') op
                WHERE ST_DWithin({point_sql}, op.geom, 0.002)
                ORDER BY ST_Distance({point_sql}, op.geom)
                LIMIT 1
            """).fetchall()
        if rows:
            result["op_land_use_designation"] = rows[0][0]

    # Zoning height overlay (HT_STORIES + HT_LABEL for metres)
    # The GeoJSON uses HT_LABEL (double) for height in metres, not HT_HEIGHT.
    # HT_STRING carries the combined label, e.g. "HT 11.0, ST 3".
    height_path = ref_dir / "zoning_height.geojson"
    if height_path.exists():
        escaped = str(height_path).replace("'", "''")
        desc = con.execute(f"DESCRIBE SELECT * FROM ST_Read('{escaped}')").fetchall()
        available_h = {str(r[0]).upper() for r in desc}
        ht_stories_col = "h.HT_STORIES" if "HT_STORIES" in available_h else "NULL"
        ht_height_col = "h.HT_LABEL" if "HT_LABEL" in available_h else "NULL"
        rows = con.execute(f"""
            SELECT {ht_stories_col}, {ht_height_col}
            FROM ST_Read('{escaped}') h
            WHERE ST_Within({point_sql}, h.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            stories_val = _positive_or_none(rows[0][0])
            if stories_val is not None:
                result["zoning_max_storeys"] = int(stories_val)
            height_val = _positive_or_none(rows[0][1])
            if height_val is not None:
                result["zoning_max_height_m"] = height_val

    con.close()
    return result


def nearby_applications(
    lat: float,
    lon: float,
    enriched_path: Path,
    *,
    radius_m: float = 500.0,
    years: int = 5,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Return development applications within radius_m of (lat, lon).

    Reads the enriched dev_applications parquet (which has lat/lon from spatial
    enrichment). Uses a bounding-box pre-filter then computes planar distance
    in metres for sorting and final filtering (same approximation as comps.py).

    Returns at most `limit` records sorted by ascending distance, from the
    last `years` years. Returns [] when enriched_path does not exist.
    """
    if not enriched_path.exists():
        return []

    year_cutoff = datetime.date.today().year - years
    lat_delta = radius_m / 111_111.0
    lon_delta = radius_m / (111_111.0 * math.cos(math.radians(lat)))
    lat_min, lat_max = lat - lat_delta, lat + lat_delta
    lon_min, lon_max = lon - lon_delta, lon + lon_delta

    escaped = str(enriched_path).replace("'", "''")
    distance_m_expr = (
        f"sqrt(power((lat - {lat}) * 111111.0, 2)"
        f" + power((lon - {lon}) * 111111.0 * cos(radians({lat})), 2))"
    )

    con = duckdb.connect()
    try:
        existing_cols: set[str] = set(
            con.execute(f"DESCRIBE SELECT * FROM read_parquet('{escaped}') LIMIT 0")
            .pl()["column_name"]
            .to_list()
        )
        is_active_expr = (
            "CAST(is_active AS BOOLEAN)"
            if "is_active" in existing_cols
            else "CAST(NULL AS BOOLEAN)"
        )
        sql = f"""
            SELECT
                CAST(folderrsn AS VARCHAR) AS folderrsn,
                application_type,
                status,
                COALESCE(CAST(street_num AS VARCHAR), '') || ' ' ||
                    COALESCE(street_name, '') AS street_address,
                date_submitted,
                description,
                {is_active_expr} AS is_active,
                {distance_m_expr} AS distance_m
            FROM read_parquet('{escaped}')
            WHERE lat IS NOT NULL
              AND lon IS NOT NULL
              AND year_submitted IS NOT NULL
              AND year_submitted >= {year_cutoff}
              AND lat BETWEEN {lat_min} AND {lat_max}
              AND lon BETWEEN {lon_min} AND {lon_max}
              AND {distance_m_expr} <= {radius_m}
            ORDER BY distance_m ASC
            LIMIT {limit}
        """
        return con.execute(sql).pl().to_dicts()
    finally:
        con.close()

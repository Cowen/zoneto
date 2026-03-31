"""Spatial lookup of site context for a point (zoning, heritage, MTSA)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


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
        "in_heritage_register": 0,
        "in_heritage_district": 0,
        "secondary_plan_name": None,
        "in_secondary_plan": 0,
        "in_mtsa": 0,
    }

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    point_sql = f"ST_Point({lon}, {lat})"

    # Zoning (class + permitted density/units from by-law)
    zoning_path = ref_dir / "zoning.geojson"
    if zoning_path.exists():
        escaped = str(zoning_path).replace("'", "''")
        rows = con.execute(f"""
            SELECT z.ZN_ZONE, z.UNITS, z.DENSITY
            FROM ST_Read('{escaped}') z
            WHERE ST_Within({point_sql}, z.geom)
            LIMIT 1
        """).fetchall()
        if rows:
            result["zoning_class"] = rows[0][0]
            units_val = rows[0][1]
            density_val = rows[0][2]
            if units_val is not None and units_val > 0:
                result["zoning_max_units"] = int(units_val)
            if density_val is not None and density_val > 0:
                result["zoning_max_density"] = float(density_val)

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

    con.close()
    return result

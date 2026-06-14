"""Spatial enrichment: zoning, heritage, MTSA, ward demographics."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import polars as pl
import pyproj

logger = logging.getLogger(__name__)


def _enrich_ward_features(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """Add ward profile features (ward_pct_renters, ward_median_income, etc.) to df.

    Reads ward_profiles.csv (simple format: one row per ward).
    Left-joins on ward_number (normalizing format — dev uses "Ward N", COA uses "N").
    Returns enriched DataFrame with new columns.
    """
    ref = data_dir / "reference"
    ward_profiles_path = ref / "ward_profiles.csv"

    if not ward_profiles_path.exists():
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("ward_pct_renters"),
            pl.lit(None, dtype=pl.Float64).alias("ward_median_income"),
            pl.lit(None, dtype=pl.Float64).alias("ward_pop_density"),
            pl.lit(None, dtype=pl.Float64).alias("ward_pct_detached"),
        )

    # Simple CSV: one row per ward with precomputed metric columns
    ward_df = pl.read_csv(ward_profiles_path)

    # Build lookup: ward_number (int) → metrics dict
    ward_data: dict[int, dict[str, float | None]] = {}
    for row in ward_df.iter_rows(named=True):
        try:
            w = int(row["ward_number"])
        except (TypeError, ValueError):
            continue
        ward_data[w] = {
            "ward_pct_renters": row.get("ward_pct_renters"),
            "ward_median_income": row.get("ward_median_income"),
            "ward_pop_density": row.get("ward_pop_density"),
            "ward_pct_detached": row.get("ward_pct_detached"),
        }

    def normalize_ward(ward_val: object) -> int | None:
        if ward_val is None:
            return None
        ward_str = str(ward_val).replace("Ward ", "").strip()
        try:
            return int(ward_str)
        except ValueError:
            return None

    ward_nums = [normalize_ward(w) for w in df["ward_number"]]

    def _lookup(metric: str) -> list[float | None]:
        return [
            ward_data.get(w, {}).get(metric) if w is not None else None
            for w in ward_nums
        ]

    F = pl.Float64
    return df.with_columns(
        pl.Series("ward_pct_renters", _lookup("ward_pct_renters"), dtype=F),
        pl.Series("ward_median_income", _lookup("ward_median_income"), dtype=F),
        pl.Series("ward_pop_density", _lookup("ward_pop_density"), dtype=F),
        pl.Series("ward_pct_detached", _lookup("ward_pct_detached"), dtype=F),
    )


def _add_height_feature(df: pl.DataFrame, height_path: Path) -> pl.DataFrame:
    """Add zoning_max_storeys (Int32) via DuckDB spatial join against height overlay.

    Per the by-law data dictionary, HT_STORIES <= 0 means "no limit" — treated
    as null. Rows with null lat/lon or outside height overlay get null.
    """
    if not height_path.exists() or "lat" not in df.columns or "lon" not in df.columns:
        return df.with_columns(pl.lit(None, dtype=pl.Int32).alias("zoning_max_storeys"))

    valid_mask = df["lat"].is_not_null() & df["lon"].is_not_null()
    valid_df = df.filter(valid_mask)

    if len(valid_df) == 0:
        return df.with_columns(pl.lit(None, dtype=pl.Int32).alias("zoning_max_storeys"))

    escaped = str(height_path).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT DISTINCT ON (apps._rid)
            apps._rid,
            CASE WHEN ht.HT_STORIES > 0
                 THEN ht.HT_STORIES ELSE NULL END
                 AS zoning_max_storeys
        FROM apps
        LEFT JOIN ST_Read('{escaped}') ht
            ON ST_Within(ST_Point(apps.lon, apps.lat), ht.geom)
    """).pl()

    con.close()

    rid_to_ht: dict[int, int | None] = dict(
        zip(
            result["_rid"].to_list(),
            result["zoning_max_storeys"].cast(pl.Int32, strict=False).to_list(),
        )
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(len(df)))
    ht_series = pl.Series(
        "zoning_max_storeys",
        [rid_to_ht.get(rid) for rid in all_rids],
        dtype=pl.Int32,
    )
    return df.with_columns(ht_series)


def _add_mtsa_feature(df: pl.DataFrame, mtsa_path: Path) -> pl.DataFrame:
    """Add in_mtsa (Int8) column via DuckDB spatial join against MTSA boundaries.

    Rows with null lat/lon or outside MTSA boundaries get in_mtsa=0.
    If mtsa_path does not exist, all rows get in_mtsa=0.
    mtsa_path can be any format that DuckDB ST_Read understands (.shp, .geojson, etc.).
    """
    if not mtsa_path.exists() or "lat" not in df.columns or "lon" not in df.columns:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_mtsa"))

    valid_mask = df["lat"].is_not_null() & df["lon"].is_not_null()
    valid_df = df.filter(valid_mask)

    if len(valid_df) == 0:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_mtsa"))

    mtsa_path_escaped = str(mtsa_path).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT apps._rid,
               CASE WHEN COUNT(mtsa.geom) > 0 THEN 1 ELSE 0 END AS in_mtsa
        FROM apps
        LEFT JOIN ST_Read('{mtsa_path_escaped}') mtsa
            ON ST_Within(
                ST_Point(apps.lon, apps.lat),
                mtsa.geom
            )
        GROUP BY apps._rid
    """).pl()

    con.close()

    rid_to_mtsa: dict[int, int] = dict(
        zip(result["_rid"].to_list(), result["in_mtsa"].cast(pl.Int8).to_list())
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(len(df)))
    in_mtsa_series = pl.Series(
        "in_mtsa",
        [rid_to_mtsa.get(rid, 0) for rid in all_rids],
        dtype=pl.Int8,
    )
    return df.with_columns(in_mtsa_series)


def _add_trca_feature(df: pl.DataFrame, trca_path: Path) -> pl.DataFrame:
    """Add in_trca_regulated_area (Int8) column via DuckDB spatial join.

    Rows outside the TRCA regulated area, with null coordinates, or when the
    file is absent all get in_trca_regulated_area=0.
    """
    if not trca_path.exists() or "lat" not in df.columns or "lon" not in df.columns:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_trca_regulated_area"))

    valid_mask = df["lat"].is_not_null() & df["lon"].is_not_null()
    valid_df = df.filter(valid_mask)

    if len(valid_df) == 0:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_trca_regulated_area"))

    escaped = str(trca_path).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT apps._rid,
               CASE WHEN COUNT(trca.geom) > 0 THEN 1 ELSE 0 END
                   AS in_trca_regulated_area
        FROM apps
        LEFT JOIN ST_Read('{escaped}') trca
            ON ST_Within(ST_Point(apps.lon, apps.lat), trca.geom)
        GROUP BY apps._rid
    """).pl()

    con.close()

    rid_to_flag: dict[int, int] = dict(
        zip(
            result["_rid"].to_list(),
            result["in_trca_regulated_area"].cast(pl.Int8).to_list(),
        )
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(len(df)))
    return df.with_columns(
        pl.Series(
            "in_trca_regulated_area",
            [rid_to_flag.get(rid, 0) for rid in all_rids],
            dtype=pl.Int8,
        )
    )


def _add_greenbelt_feature(df: pl.DataFrame, greenbelt_path: Path) -> pl.DataFrame:
    """Add in_greenbelt (Int8) via DuckDB spatial join against the Greenbelt boundary.

    Rows outside the Greenbelt, with null coordinates, or when the file is
    absent all get in_greenbelt=0.
    """
    if (
        not greenbelt_path.exists()
        or "lat" not in df.columns
        or "lon" not in df.columns
    ):
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_greenbelt"))

    valid_mask = df["lat"].is_not_null() & df["lon"].is_not_null()
    valid_df = df.filter(valid_mask)

    if len(valid_df) == 0:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_greenbelt"))

    escaped = str(greenbelt_path).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT apps._rid,
               CASE WHEN COUNT(gb.geom) > 0 THEN 1 ELSE 0 END AS in_greenbelt
        FROM apps
        LEFT JOIN ST_Read('{escaped}') gb
            ON ST_Within(ST_Point(apps.lon, apps.lat), gb.geom)
        GROUP BY apps._rid
    """).pl()

    con.close()

    rid_to_flag: dict[int, int] = dict(
        zip(result["_rid"].to_list(), result["in_greenbelt"].cast(pl.Int8).to_list())
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(len(df)))
    return df.with_columns(
        pl.Series(
            "in_greenbelt",
            [rid_to_flag.get(rid, 0) for rid in all_rids],
            dtype=pl.Int8,
        )
    )


def _add_op_land_use_feature(df: pl.DataFrame, op_path: Path) -> pl.DataFrame:
    """Add op_land_use_designation (String) via DuckDB spatial join.

    Official Plan land-use designation polygons (op_land_use.geojson, WGS84) carry
    an op_designation property. Rows with null lat/lon, outside any designation, or
    when the file is absent get null — the layer is optional (interim Borealis
    source; see analytics/reference.py).
    """
    if not op_path.exists() or "lat" not in df.columns or "lon" not in df.columns:
        return df.with_columns(
            pl.lit(None, dtype=pl.String).alias("op_land_use_designation")
        )

    valid_df = df.filter(df["lat"].is_not_null() & df["lon"].is_not_null())
    if len(valid_df) == 0:
        return df.with_columns(
            pl.lit(None, dtype=pl.String).alias("op_land_use_designation")
        )

    escaped = str(op_path).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT DISTINCT ON (apps._rid)
            apps._rid,
            op.op_designation AS op_land_use_designation
        FROM apps
        LEFT JOIN ST_Read('{escaped}') op
            ON ST_Within(ST_Point(apps.lon, apps.lat), op.geom)
    """).pl()

    con.close()

    rid_to_op: dict[int, str | None] = dict(
        zip(
            result["_rid"].to_list(),
            result["op_land_use_designation"].to_list(),
        )
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(len(df)))
    return df.with_columns(
        pl.Series(
            "op_land_use_designation",
            [rid_to_op.get(rid) for rid in all_rids],
            dtype=pl.String,
        )
    )


def _spatial_join_dev(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """Add zoning_class, secondary_plan_name, in_heritage_register,
    in_heritage_district, in_secondary_plan, in_mtsa columns via DuckDB spatial join.

    Rows with null or garbage x/y get null/0 enrichment values.
    """
    ref = data_dir / "reference"

    # Reproject x/y from EPSG:2952 (NAD83 / MTM Zone 10, City of Toronto internal CRS)
    # to EPSG:4326. Median dev application x ≈ 313,000 (MTM false easting 304,800),
    # confirming EPSG:2952 — not EPSG:26917 (UTM 17N, false easting 500,000) which
    # would map Toronto parcels to Michigan.
    transformer = pyproj.Transformer.from_crs("EPSG:2952", "EPSG:4326", always_xy=True)
    xs = df["x"].cast(pl.Float64, strict=False).to_list()
    ys = df["y"].cast(pl.Float64, strict=False).to_list()

    lons: list[float | None] = []
    lats: list[float | None] = []
    for x_val, y_val in zip(xs, ys):
        if x_val is None or y_val is None or y_val < 4_000_000:
            lons.append(None)
            lats.append(None)
        else:
            lon, lat = transformer.transform(x_val, y_val)
            lons.append(lon)
            lats.append(lat)

    df = df.with_columns(
        pl.Series("lon", lons, dtype=pl.Float64),
        pl.Series("lat", lats, dtype=pl.Float64),
        pl.Series("_rid", range(len(df)), dtype=pl.Int64),
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    con.register("apps", df.to_arrow())

    zoning_geojson_path = str(ref / "zoning.geojson").replace("'", "''")
    hr_shp = str(next((ref / "heritage_register").glob("*.shp"))).replace(
        "'",
        "''",
    )
    hd_shp = str(next((ref / "heritage_districts").glob("*.shp"))).replace(
        "'",
        "''",
    )
    sp_geojson = str(ref / "secondary_plans.geojson").replace("'", "''")

    result = con.execute(f"""
        WITH pts AS (
            SELECT
                _rid,
                lon, lat,
                CASE WHEN lon IS NOT NULL AND lat IS NOT NULL
                     THEN ST_Point(lon, lat) END AS geom
            FROM apps
        ),
        zoning_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                z.ZN_ZONE AS zoning_class,
                z.UNITS AS zoning_max_units,
                z.DENSITY AS zoning_max_density
            FROM pts p
            LEFT JOIN ST_Read('{zoning_geojson_path}') z
                ON ST_Within(p.geom, z.geom)
            WHERE p.geom IS NOT NULL
        ),
        hr_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                1::TINYINT AS in_heritage_register
            FROM pts p
            JOIN ST_Read('{hr_shp}') h
                ON ST_Intersects(ST_Buffer(p.geom, 0.0002), h.geom)
            WHERE p.geom IS NOT NULL
        ),
        hd_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                1::TINYINT AS in_heritage_district
            FROM pts p
            JOIN ST_Read('{hd_shp}') h
                ON ST_Within(p.geom, h.geom)
            WHERE p.geom IS NOT NULL
        ),
        sp_join AS (
            SELECT DISTINCT ON (p._rid)
                p._rid,
                s.SECONDARY_PLAN_NAME AS secondary_plan_name
            FROM pts p
            JOIN ST_Read('{sp_geojson}') s
                ON ST_Within(p.geom, s.geom)
            WHERE p.geom IS NOT NULL
        )
        SELECT
            a.*,
            z.zoning_class AS zoning_class,
            z.zoning_max_units AS zoning_max_units,
            z.zoning_max_density AS zoning_max_density,
            COALESCE(h.in_heritage_register, 0)
                AS in_heritage_register,
            COALESCE(d.in_heritage_district, 0)
                AS in_heritage_district,
            sp.secondary_plan_name AS secondary_plan_name,
            CASE WHEN sp.secondary_plan_name IS NOT NULL
                 THEN 1 ELSE 0 END AS in_secondary_plan
        FROM apps a
        LEFT JOIN zoning_join z ON a._rid = z._rid
        LEFT JOIN hr_join h ON a._rid = h._rid
        LEFT JOIN hd_join d ON a._rid = d._rid
        LEFT JOIN sp_join sp ON a._rid = sp._rid
    """).pl()

    con.close()

    # Cast zoning limit columns to expected types (DuckDB may return bigint/double)
    result = result.with_columns(
        pl.col("zoning_max_units").cast(pl.Int32, strict=False),
        pl.col("zoning_max_density").cast(pl.Float64, strict=False),
    )

    # Add zoning max storeys from height overlay
    height_geojson = ref / "zoning_height.geojson"
    result = _add_height_feature(result, height_geojson)

    # Add MTSA feature using the downloaded boundary shapefile
    mtsa_shp = ref / "mtsa.shp"
    result = _add_mtsa_feature(result, mtsa_shp)

    # Add TRCA regulated area flag
    trca_path = ref / "trca_regulated_areas.geojson"
    result = _add_trca_feature(result, trca_path)

    # Add Greenbelt boundary flag
    greenbelt_path = ref / "greenbelt.geojson"
    result = _add_greenbelt_feature(result, greenbelt_path)

    # Add Official Plan land-use designation (interim Borealis source; optional)
    op_path = ref / "op_land_use.geojson"
    result = _add_op_land_use_feature(result, op_path)

    return result

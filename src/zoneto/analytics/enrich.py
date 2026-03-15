"""Enrichment pipeline: fetch reference data, label outcomes, spatial join."""

from __future__ import annotations

import zipfile
from pathlib import Path

import duckdb
import polars as pl
import pyproj

# ---------------------------------------------------------------------------
# Reference dataset URLs
# ---------------------------------------------------------------------------
_ZONING_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/34927e44-fc11-4336-a8aa-a0dfb27658b7"
    "/resource/d75fa1ed-cd04-4a0b-bb6d-2b928ffffa6e"
    "/download/zoning-area-4326.geojson"
)
_HERITAGE_REGISTER_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/e41da515-5ad1-4bc3-85ea-18ec9e55cd33"
    "/resource/108b1080-d048-439f-a9e8-e8d6cd81bddb"
    "/download/heritage_register_address_points_wgs84.zip"
)
_HERITAGE_DISTRICTS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/37a3c911-0813-4e87-90ed-3b9fa6156a63"
    "/resource/8e6b9347-63a8-4dac-91fb-a6491a8c1e5a"
    "/download/heritageconservationdistrict.zip"
)
_SECONDARY_PLANS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/70a544e9-ee83-43a4-b0be-0dc973627ad7"
    "/resource/08099a8c-a598-4ca3-8395-e4159cc1ec1a"
    "/download/secondary-plans-data-2017-4326.geojson"
)

# ---------------------------------------------------------------------------
# Status label sets (lowercase after strip)
# ---------------------------------------------------------------------------
_DEV_APPROVED_SET: frozenset[str] = frozenset(
    {
        "noac issued",
        "council approved",
        "draft plan approved",
        "final approval completed",
        "omb approved",
        "approved",
        "omb partially approved",
    }
)
_DEV_REFUSED_SET: frozenset[str] = frozenset({"refused", "omb refused"})
_DEV_APPEALED_SET: frozenset[str] = frozenset(
    {
        "omb appeal",
        "appeal received",
        "omb approved",
        "omb refused",
        "omb partially approved",
    }
)
_COA_APPROVED_SET: frozenset[str] = frozenset(
    {
        "approved",
        "conditional approval",
        "approved with conditions",
        "approved on condition",
    }
)
_COA_REFUSED_SET: frozenset[str] = frozenset({"refused"})


def _label_from_sets(
    val: str | None,
    positive_set: frozenset[str],
    negative_set: frozenset[str],
) -> int | None:
    """Map a status string to 1/0/null via two frozensets.

    Returns 1 if val (stripped, lowercased) is in positive_set,
    0 if in negative_set, None otherwise.
    """
    if val is None:
        return None
    v = val.strip().lower()
    if v in positive_set:
        return 1
    if v in negative_set:
        return 0
    return None


def _download(url: str, dest: Path) -> None:
    """Download *url* to *dest* (binary)."""
    import httpx

    with httpx.Client(follow_redirects=True, timeout=120) as client:
        r = client.get(url)
        r.raise_for_status()
        dest.write_bytes(r.content)


def fetch_reference(data_dir: Path = Path("data")) -> None:
    """Download all reference datasets to *data_dir*/reference/.

    Idempotent: skips files that already exist.
    """
    ref = data_dir / "reference"
    ref.mkdir(parents=True, exist_ok=True)

    # Zoning GeoJSON (full-city, WGS84)
    zoning_geojson = ref / "zoning.geojson"
    if not zoning_geojson.exists():
        _download(_ZONING_URL, zoning_geojson)

    # Heritage register (ZIP → extract)
    hr_dir = ref / "heritage_register"
    if not hr_dir.exists():
        hr_zip = ref / "heritage_register.zip"
        _download(_HERITAGE_REGISTER_URL, hr_zip)
        hr_dir.mkdir()
        with zipfile.ZipFile(hr_zip) as zf:
            zf.extractall(hr_dir)
        hr_zip.unlink()

    # Heritage conservation districts (ZIP → extract)
    hd_dir = ref / "heritage_districts"
    if not hd_dir.exists():
        hd_zip = ref / "heritage_districts.zip"
        _download(_HERITAGE_DISTRICTS_URL, hd_zip)
        hd_dir.mkdir()
        with zipfile.ZipFile(hd_zip) as zf:
            zf.extractall(hd_dir)
        hd_zip.unlink()

    # Secondary plans GeoJSON
    sp_geojson = ref / "secondary_plans.geojson"
    if not sp_geojson.exists():
        _download(_SECONDARY_PLANS_URL, sp_geojson)


def enrich_coa(data_dir: Path = Path("data")) -> int:
    """Enrich COA parquet with outcome labels; write data/enriched/coa.parquet.

    Returns row count written.
    """
    df = pl.read_parquet(data_dir / "coa", hive_partitioning=True)

    # Deduplicate on application key (consolidated CSV may overlap individual year CSVs)
    if "reference_file" in df.columns:
        df = df.unique(subset=["reference_file"], keep="first", maintain_order=True)

    # Rename ward → ward_number (cast to str for consistency)
    df = df.rename({"ward": "ward_number"}).with_columns(
        pl.col("ward_number").cast(pl.Utf8)
    )

    # year_submitted from in_date
    df = df.with_columns(
        pl.col("in_date").dt.year().cast(pl.Int32).alias("year_submitted")
    )

    # coa_approved label
    df = df.with_columns(
        pl.col("c_of_a_descision")
        .map_elements(
            lambda v: _label_from_sets(v, _COA_APPROVED_SET, _COA_REFUSED_SET),
            return_dtype=pl.Int8,
        )
        .alias("coa_approved")
    )

    # coa_days_to_approval — only for approved rows with both dates present
    days = (pl.col("finaldate") - pl.col("in_date")).dt.total_days().cast(pl.Int32)
    df = df.with_columns(
        pl.when(pl.col("coa_approved") == 1)
        .then(days)
        .otherwise(None)
        .alias("coa_days_to_approval")
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "coa.parquet")
    return len(df)


def _spatial_join_dev(df: pl.DataFrame, data_dir: Path) -> pl.DataFrame:
    """Add zoning_class, secondary_plan_name, in_heritage_register,
    in_heritage_district, in_secondary_plan columns via DuckDB spatial join.

    Rows with null or garbage x/y get null/0 enrichment values.
    """
    ref = data_dir / "reference"

    # Reproject x/y from EPSG:26917 → EPSG:4326
    transformer = pyproj.Transformer.from_crs("EPSG:26917", "EPSG:4326", always_xy=True)
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
                z.ZN_ZONE AS zoning_class
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
    return result


def enrich_dev(data_dir: Path = Path("data")) -> int:
    """Enrich dev_applications parquet with spatial features and outcome labels.

    Writes data/enriched/dev_applications.parquet. Returns row count.
    """
    df = pl.read_parquet(data_dir / "dev_applications", hive_partitioning=True)

    # year_submitted from date_submitted (may be Date/Datetime or String)
    _ds_dtype = df["date_submitted"].dtype
    if _ds_dtype.is_temporal():
        _year_expr = pl.col("date_submitted").dt.year().cast(pl.Int32)
    else:
        # String like "2022-08-09T00:00:00" — extract year from first 4 chars
        _year_expr = (
            pl.col("date_submitted").str.slice(0, 4).cast(pl.Int32, strict=False)
        )
    df = df.with_columns(_year_expr.alias("year_submitted"))

    # has_community_meeting
    df = df.with_columns(
        pl.col("community_meeting_date")
        .is_not_null()
        .cast(pl.Int8)
        .alias("has_community_meeting"),
    )

    # Spatial enrichment (monkeypatchable)
    df = _spatial_join_dev(df, data_dir)

    df = df.with_columns(
        pl.col("status")
        .map_elements(
            lambda v: _label_from_sets(v, _DEV_APPROVED_SET, _DEV_REFUSED_SET),
            return_dtype=pl.Int8,
        )
        .alias("dev_approved"),
        pl.col("status")
        .map_elements(
            # 1 = appeal filed, 0 = approved without appeal
            lambda v: _label_from_sets(v, _DEV_APPEALED_SET, _DEV_APPROVED_SET),
            return_dtype=pl.Int8,
        )
        .alias("dev_appealed"),
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "dev_applications.parquet")
    return len(df)


def enrich_permits(data_dir: Path = Path("data")) -> int:
    """Enrich permits_cleared parquet with issuance timeline label.

    Computes permit_issuance_days = issued_date - application_date (calendar days).
    Rows with null issued_date or application_date get null permit_issuance_days.
    Rows where issuance_days <= 0 are dropped (data quality).

    Writes data/enriched/permits_cleared.parquet. Returns row count written.
    """
    df = pl.read_parquet(data_dir / "permits_cleared", hive_partitioning=True)

    # Coerce string numeric columns to Float64 (remove comma thousands separators)
    _str_num_cols = ["est_const_cost", "dwelling_units_created", "dwelling_units_lost"]
    for _col in _str_num_cols:
        if _col in df.columns and df[_col].dtype == pl.Utf8:
            df = df.with_columns(
                pl.col(_col)
                .str.replace_all(",", "")
                .cast(pl.Float64, strict=False)
            )

    days = (
        (pl.col("issued_date") - pl.col("application_date"))
        .dt.total_days()
        .cast(pl.Int32)
    )
    has_both = (
        pl.col("issued_date").is_not_null() & pl.col("application_date").is_not_null()
    )
    df = df.with_columns(
        pl.when(has_both).then(days).otherwise(None).alias("permit_issuance_days")
    )

    # Drop rows where issuance days is computed but non-positive (bad data)
    df = df.filter(
        pl.col("permit_issuance_days").is_null() | (pl.col("permit_issuance_days") > 0)
    )

    out = data_dir / "enriched"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "permits_cleared.parquet")
    return len(df)

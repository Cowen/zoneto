"""Reference dataset downloads and management."""

from __future__ import annotations

import csv
import logging
import shutil
import tempfile
import zipfile
from pathlib import Path

import httpx
import openpyxl

logger = logging.getLogger(__name__)

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
_ZONING_HEIGHT_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/34927e44-fc11-4336-a8aa-a0dfb27658b7"
    "/resource/eec27e60-7c2d-4c46-8fa1-b64f441bcc39"
    "/download/zoning-height-overlay-4326.geojson"
)
_MTSA_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/f7128f82-810d-4677-8166-8892f51969d3"
    "/resource/2b66ca26-b345-4e62-8568-dcd2cd6c3f91"
    "/download/majortransitstationareadelinations_jan2026.shp.zip"
)
_WARD_CENSUS_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/6678e1a6-d25f-4dff-b2b7-aa8f042bc2eb"
    "/resource/16a31e1d-b4d9-4cf0-b5b3-2e3937cb4121"
    "/download/2023-wardprofiles-2011-2021-censusdata_rev0719.xlsx"
)
_WARD_GEO_URL = (
    "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    "/dataset/6678e1a6-d25f-4dff-b2b7-aa8f042bc2eb"
    "/resource/9398da69-0622-4eb1-a125-4f8c7f1016a4"
    "/download/2023-wardprofiles-geographicareas.xlsx"
)


def _download(url: str, dest: Path) -> None:
    """Download *url* to *dest* (binary)."""
    with httpx.Client(follow_redirects=True, timeout=120) as client:
        r = client.get(url)
        r.raise_for_status()
        dest.write_bytes(r.content)


def _fetch_ward_profiles_csv(ref: Path) -> None:
    """Download ward profile XLSXs, compute metrics, write ward_profiles.csv.

    Output: ward_number,ward_pct_renters,ward_median_income,
    ward_pop_density,ward_pct_detached (one row per ward, wards 1–25).
    """
    census_xlsx = Path(tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False).name)
    geo_xlsx = Path(tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False).name)
    try:
        with httpx.Client(follow_redirects=True, timeout=120) as client:
            census_xlsx.write_bytes(
                client.get(_WARD_CENSUS_URL).raise_for_status().content
            )
            geo_xlsx.write_bytes(client.get(_WARD_GEO_URL).raise_for_status().content)

        # --- parse census XLSX ---
        wb = openpyxl.load_workbook(census_xlsx, read_only=True, data_only=True)
        ws = wb["2021 One Variable"]
        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        # Row index 17: header (None, 'Toronto', 'Ward 1', ..., 'Ward 25')
        header = rows[17]
        # Build index: ward_number (int) → column index in rows
        ward_col_idx: dict[int, int] = {}
        for col_i, val in enumerate(header):
            if val and str(val).startswith("Ward "):
                try:
                    ward_col_idx[int(str(val).replace("Ward ", "").strip())] = col_i
                except ValueError:
                    pass

        def _cell(row_idx: int, ward_num: int) -> float | None:
            col_i = ward_col_idx.get(ward_num)
            if col_i is None:
                return None
            v = rows[row_idx][col_i]
            return float(v) if v is not None else None

        # Row 18: total population ('Total - Age', toronto_total, ward1, ...)
        # Row 43: total occupied dwellings
        # Row 44: single-detached dwellings
        # Row 1384: median household income
        # Row 1390: tenant households
        # Row 1394: owner households

        # --- parse geographic areas XLSX for ward area (sq km) ---
        wb2 = openpyxl.load_workbook(geo_xlsx, read_only=True, data_only=True)
        ws2 = wb2[wb2.sheetnames[0]]
        geo_rows = list(ws2.iter_rows(values_only=True))
        wb2.close()

        # Row 11: header; rows 12+ are (ward_num, area_sq_km, ...)
        ward_area: dict[int, float] = {}
        for geo_row in geo_rows[12:]:
            if geo_row[0] is None:
                break
            try:
                ward_area[int(geo_row[0])] = float(geo_row[1])
            except (TypeError, ValueError):
                pass

        # Write output CSV
        out_path = ref / "ward_profiles.csv"
        with out_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "ward_number",
                    "ward_pct_renters",
                    "ward_median_income",
                    "ward_pop_density",
                    "ward_pct_detached",
                ]
            )
            for ward_num in sorted(ward_col_idx.keys()):
                population = _cell(18, ward_num)
                total_dwellings = _cell(43, ward_num)
                single_detached = _cell(44, ward_num)
                median_income = _cell(1384, ward_num)
                tenant = _cell(1390, ward_num)
                owner = _cell(1394, ward_num)

                pct_renters: float | None = None
                if tenant is not None and owner is not None and (tenant + owner) > 0:
                    pct_renters = tenant / (tenant + owner) * 100.0

                pop_density: float | None = None
                area = ward_area.get(ward_num)
                if population is not None and area is not None and area > 0:
                    pop_density = population / area

                pct_detached: float | None = None
                if (
                    single_detached is not None
                    and total_dwellings is not None
                    and total_dwellings > 0
                ):
                    pct_detached = single_detached / total_dwellings * 100.0

                writer.writerow(
                    [ward_num, pct_renters, median_income, pop_density, pct_detached]
                )
    finally:
        census_xlsx.unlink(missing_ok=True)
        geo_xlsx.unlink(missing_ok=True)


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

    # Zoning height overlay GeoJSON (max storeys/height per area, WGS84)
    zoning_height = ref / "zoning_height.geojson"
    if not zoning_height.exists():
        _download(_ZONING_HEIGHT_URL, zoning_height)

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

    # MTSA boundaries (ZIP → extract SHP, Major Transit Station Areas)
    mtsa_shp = ref / "mtsa.shp"
    if not mtsa_shp.exists():
        mtsa_dir = ref / "mtsa"
        if not mtsa_dir.exists():
            mtsa_zip = ref / "mtsa.zip"
            try:
                _download(_MTSA_URL, mtsa_zip)
                mtsa_dir.mkdir()
                with zipfile.ZipFile(mtsa_zip) as zf:
                    zf.extractall(mtsa_dir)
                mtsa_zip.unlink()
                # Find the .shp file in the extracted directory and move to root ref dir
                shp_files = list(mtsa_dir.glob("*.shp"))
                if shp_files:
                    shp_file = shp_files[0]
                    # Copy all shapefile components to ref dir
                    # (*.shp, *.shx, *.dbf, *.prj, *.cpg, etc.)
                    for ext in ["shp", "shx", "dbf", "prj", "cpg"]:
                        src = mtsa_dir / f"{shp_file.stem}.{ext}"
                        if src.exists():
                            (ref / f"mtsa.{ext}").write_bytes(src.read_bytes())
                # Clean up extracted directory
                shutil.rmtree(mtsa_dir)
            except Exception:
                logger.warning(
                    "MTSA boundaries not available — in_mtsa will be 0 for all rows"
                )

    # Ward profiles CSV (computed from two XLSX downloads)
    ward_profiles_csv = ref / "ward_profiles.csv"
    if not ward_profiles_csv.exists():
        _fetch_ward_profiles_csv(ref)

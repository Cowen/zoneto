# Product Strategy Pivot — Phase 7: Model Improvements (MTSA + NLP)

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add two new feature groups to the appeal and timeline models: (1) `in_mtsa` — a binary spatial flag indicating whether an application falls within a Major Transit Station Area boundary; (2) `desc_svd_0..19` — 20-component TF-IDF+SVD features extracted from the application description field.

**Architecture:** `fetch_reference()` downloads the MTSA GeoJSON to `data/reference/mtsa.geojson`. `_spatial_join_dev()` is extended to add `in_mtsa` via the same DuckDB point-in-polygon join pattern used for heritage and secondary plans. `_extract_text_features()` fits TF-IDF + TruncatedSVD on the description column, serializes the vectorizer to `models/desc_tfidf.joblib`, and produces `desc_svd_0..19` columns. `features.py` adds all 21 new columns to `DEV_NUM_COLS`. `score.py` loads and applies the vectorizer before scoring.

**Tech Stack:** DuckDB spatial (already installed), scikit-learn TfidfVectorizer + TruncatedSVD, polars, joblib

**Scope:** Phase 7 of 8. Depends on Phase 5 (AIC data provides fresh applications to enrich).

**Codebase verified:** 2026-03-17

---

## Task 1: Add MTSA spatial feature

**Files:**
- Modify: `src/zoneto/analytics/enrich.py` (`fetch_reference()` and `_spatial_join_dev()`)
- Create: `tests/analytics/test_mtsa_feature.py`

**Step 1: Write the failing test**

Create `tests/analytics/test_mtsa_feature.py`:

```python
"""Tests for in_mtsa spatial feature extraction."""
from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import _add_mtsa_feature


def _write_mtsa_geojson(path: Path) -> None:
    """Write a minimal GeoJSON with a single MTSA polygon covering downtown Toronto."""
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"STATION_NAME": "Union Station"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-79.40, 43.64],
                            [-79.36, 43.64],
                            [-79.36, 43.66],
                            [-79.40, 43.66],
                            [-79.40, 43.64],
                        ]
                    ],
                },
            }
        ],
    }
    path.write_text(json.dumps(geojson))


def test_in_mtsa_true_for_point_inside(tmp_path: Path) -> None:
    """Point at (43.650, -79.383) falls within the MTSA polygon → in_mtsa=1."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": [43.650],
            "lon": [-79.383],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert "in_mtsa" in result.columns
    assert result["in_mtsa"][0] == 1


def test_in_mtsa_false_for_point_outside(tmp_path: Path) -> None:
    """Point at (43.700, -79.450) is outside the MTSA polygon → in_mtsa=0."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": [43.700],
            "lon": [-79.450],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"][0] == 0


def test_in_mtsa_null_coords_get_zero(tmp_path: Path) -> None:
    """Rows with null lat/lon get in_mtsa=0."""
    mtsa_path = tmp_path / "mtsa.geojson"
    _write_mtsa_geojson(mtsa_path)

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0], dtype=pl.Int64),
            "lat": pl.Series([None], dtype=pl.Float64),
            "lon": pl.Series([None], dtype=pl.Float64),
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"][0] == 0


def test_in_mtsa_missing_file_returns_zeros(tmp_path: Path) -> None:
    """When mtsa.geojson does not exist, in_mtsa is 0 for all rows."""
    mtsa_path = tmp_path / "mtsa.geojson"  # does not exist

    df = pl.DataFrame(
        {
            "_rid": pl.Series([0, 1], dtype=pl.Int64),
            "lat": [43.650, 43.700],
            "lon": [-79.383, -79.450],
        }
    )
    result = _add_mtsa_feature(df, mtsa_path)
    assert result["in_mtsa"].to_list() == [0, 0]
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/analytics/test_mtsa_feature.py -v
```

Expected: `ImportError: cannot import name '_add_mtsa_feature' from 'zoneto.analytics.enrich'`

**Step 3: Add `_add_mtsa_feature()` helper to `src/zoneto/analytics/enrich.py`**

Add this function after the existing `_spatial_join_dev()` function (around line 514):

```python
def _add_mtsa_feature(df: pl.DataFrame, mtsa_geojson: Path) -> pl.DataFrame:
    """Add in_mtsa (Int8) column via DuckDB spatial join against MTSA boundaries.

    Rows with null lat/lon or outside MTSA boundaries get in_mtsa=0.
    If mtsa.geojson does not exist, all rows get in_mtsa=0.
    """
    n = len(df)
    if not mtsa_geojson.exists() or "lat" not in df.columns or "lon" not in df.columns:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_mtsa"))

    in_mtsa_vals = [0] * n
    # Filter to rows with valid coordinates for the spatial query
    valid_mask = df["lat"].is_not_null() & df["lon"].is_not_null()
    valid_df = df.filter(valid_mask)

    if len(valid_df) == 0:
        return df.with_columns(pl.lit(0, dtype=pl.Int8).alias("in_mtsa"))

    mtsa_path_escaped = str(mtsa_geojson).replace("'", "''")
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.register("apps", valid_df.to_arrow())

    result = con.execute(f"""
        SELECT apps._rid,
               CASE WHEN COUNT(mtsa.geom) > 0 THEN 1 ELSE 0 END AS in_mtsa
        FROM apps
        LEFT JOIN ST_Read('{mtsa_path_escaped}') mtsa
            ON ST_Within(
                ST_Transform(ST_Point(apps.lon, apps.lat), 'EPSG:4326', 'EPSG:4326'),
                mtsa.geom
            )
        GROUP BY apps._rid
    """).pl()

    con.close()

    # Map results back to full dataframe by _rid
    rid_to_mtsa: dict[int, int] = dict(
        zip(result["_rid"].to_list(), result["in_mtsa"].cast(pl.Int8).to_list())
    )
    all_rids = df["_rid"].to_list() if "_rid" in df.columns else list(range(n))
    in_mtsa_series = pl.Series(
        "in_mtsa",
        [rid_to_mtsa.get(rid, 0) for rid in all_rids],
        dtype=pl.Int8,
    )
    return df.with_columns(in_mtsa_series)
```

**Step 4: Run tests**

```bash
uv run pytest tests/analytics/test_mtsa_feature.py -v
```

Expected: All tests pass.

**Step 5: Add MTSA download to `fetch_reference()` in `enrich.py`**

Find the MTSA boundary URL. The City of Toronto publishes MTSA boundaries as part of its Official Plan data. Add this constant near the other URL constants at the top of enrich.py:

```python
# MTSA/PMTSA boundary GeoJSON — Major Transit Station Areas from City of Toronto Open Data
_MTSA_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/major-transit-station-areas/resource/mtsa-boundaries.geojson"
```

> **Note for implementer:** Verify the exact CKAN URL for MTSA boundaries before using it. If the URL is not available, search the City of Toronto Open Data portal at https://open.toronto.ca for "Major Transit Station Areas" and use the GeoJSON resource URL. The URL pattern for CKAN resources is: `https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/{dataset-name}/resource/{resource-id}/download/{filename}`.

Then add to `fetch_reference()` (after the secondary plans block):

```python
    # MTSA boundaries GeoJSON (Major Transit Station Areas)
    mtsa_geojson = ref / "mtsa.geojson"
    if not mtsa_geojson.exists():
        _download(_MTSA_URL, mtsa_geojson)
```

**Step 6: Integrate `_add_mtsa_feature()` into `_spatial_join_dev()`**

At the end of `_spatial_join_dev()` (after the return statement's last with_columns call), add the MTSA join. Find where the function currently returns `df` and add MTSA before the return:

```python
    # Add MTSA feature using the downloaded boundary GeoJSON
    mtsa_geojson = ref / "mtsa.geojson"
    df = _add_mtsa_feature(df, mtsa_geojson)
```

(The `df` at this point already has `_rid`, `lat`, `lon` columns from the coordinate reprojection earlier in `_spatial_join_dev()`.)

**Step 7: Update `DEV_NUM_COLS` in `src/zoneto/analytics/features.py`**

Add `"in_mtsa"` to `DEV_NUM_COLS`:

```python
DEV_NUM_COLS: list[str] = [
    "year_submitted",
    "in_heritage_register",
    "in_heritage_district",
    "in_secondary_plan",
    "has_community_meeting",
    "ward_pct_renters",
    "ward_median_income",
    "ward_pop_density",
    "ward_pct_detached",
    "has_parent_application",
    "is_combined_application",
    "proposed_storeys",
    "proposed_units",
    "ward_appeal_rate_3y",
    "in_mtsa",
]
```

**Step 8: Update `tests/analytics/test_features.py`**

Find the assertion for `DEV_NUM_COLS` length (currently 14) and update to 15:

```python
assert len(DEV_NUM_COLS) == 15
assert "in_mtsa" in DEV_NUM_COLS
```

**Step 9: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass. (The existing enrich tests stub `_spatial_join_dev()`, so they are unaffected.)

**Step 10: Commit**

```bash
git add src/zoneto/analytics/enrich.py src/zoneto/analytics/features.py tests/analytics/test_mtsa_feature.py tests/analytics/test_features.py
git commit -m "feat: add in_mtsa spatial feature via MTSA boundary GeoJSON spatial join"
```

---

## Task 2: Add description NLP features (TF-IDF + SVD)

**Files:**
- Modify: `src/zoneto/analytics/enrich.py` (add `_extract_text_features()` function; call it in `enrich_dev()`)
- Modify: `src/zoneto/analytics/features.py` (add `desc_svd_0..19` to `DEV_NUM_COLS`)
- Modify: `src/zoneto/analytics/score.py` (load vectorizer and transform before scoring)
- Create: `tests/analytics/test_nlp_features.py`

**Step 1: Write the failing test**

Create `tests/analytics/test_nlp_features.py`:

```python
"""Tests for TF-IDF + SVD description NLP feature extraction."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from zoneto.analytics.enrich import _extract_text_features


def test_extract_text_features_produces_svd_columns(tmp_path: Path) -> None:
    """_extract_text_features() adds desc_svd_0..19 columns."""
    descriptions = [
        "47-storey mixed-use residential tower with ground floor retail",
        "3-storey office building with underground parking",
        "12-storey condominium with affordable housing units",
        "Heritage property conversion to residential use",
        "Transit-oriented development adjacent to subway station",
    ]
    df = pl.DataFrame(
        {
            "folderrsn": [f"F{i:03d}" for i in range(5)],
            "description": descriptions,
        }
    )

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=5)

    svd_cols = [f"desc_svd_{i}" for i in range(5)]
    for col in svd_cols:
        assert col in result.columns, f"Missing column: {col}"
        assert result[col].dtype in (pl.Float32, pl.Float64)


def test_extract_text_features_serializes_vectorizer(tmp_path: Path) -> None:
    """_extract_text_features() saves desc_tfidf.joblib to model_dir."""
    import joblib

    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002"],
            "description": ["OZ application for tower", "SA for office building"],
        }
    )

    _extract_text_features(df, model_dir=tmp_path, n_components=3)

    joblib_path = tmp_path / "desc_tfidf.joblib"
    assert joblib_path.exists(), "desc_tfidf.joblib must be written to model_dir"

    # Verify the saved pipeline can transform new text
    pipeline = joblib.load(joblib_path)
    import numpy as np
    out = pipeline.transform(["new application description"])
    assert out.shape[1] == 3


def test_extract_text_features_null_descriptions_get_zeros(tmp_path: Path) -> None:
    """Rows with null descriptions get zero-filled SVD columns."""
    df = pl.DataFrame(
        {
            "folderrsn": ["F001", "F002"],
            "description": pl.Series(["OZ tower application", None], dtype=pl.String),
        }
    )

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=3)

    # Row with null description: all SVD components should be 0.0
    f002 = result.filter(pl.col("folderrsn") == "F002")
    for col in [f"desc_svd_{i}" for i in range(3)]:
        assert f002[col][0] == pytest.approx(0.0, abs=1e-6)


def test_extract_text_features_no_description_column(tmp_path: Path) -> None:
    """When description column is absent, adds zero-filled SVD columns."""
    df = pl.DataFrame({"folderrsn": ["F001", "F002"]})

    result, _ = _extract_text_features(df, model_dir=tmp_path, n_components=3)

    for col in [f"desc_svd_{i}" for i in range(3)]:
        assert col in result.columns
        assert result[col][0] == pytest.approx(0.0, abs=1e-6)
```

**Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/analytics/test_nlp_features.py -v
```

Expected: `ImportError: cannot import name '_extract_text_features' from 'zoneto.analytics.enrich'`

**Step 3: Add `_extract_text_features()` to `src/zoneto/analytics/enrich.py`**

Add the necessary imports near the top of enrich.py (with other imports):

```python
import joblib
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline as SklearnPipeline
```

Add the function after `_add_mtsa_feature()`:

```python
def _extract_text_features(
    df: pl.DataFrame,
    model_dir: Path,
    *,
    n_components: int = 20,
) -> tuple[pl.DataFrame, SklearnPipeline]:
    """Extract TF-IDF + TruncatedSVD features from the description column.

    Fits TfidfVectorizer (max_features=5000) + TruncatedSVD on the description
    column. Serializes the pipeline to model_dir/desc_tfidf.joblib.
    Adds desc_svd_0..{n_components-1} columns to the DataFrame.

    Rows with null descriptions are treated as empty strings (→ zero SVD vector).
    Returns (enriched_df, fitted_pipeline).
    """
    svd_col_names = [f"desc_svd_{i}" for i in range(n_components)]
    zero_svd = {col: pl.lit(0.0, dtype=pl.Float64).alias(col) for col in svd_col_names}

    if "description" not in df.columns:
        return df.with_columns(list(zero_svd.values())), _build_tfidf_pipeline(n_components)

    texts = df["description"].fill_null("").cast(pl.String).to_list()

    pipeline = _build_tfidf_pipeline(n_components)
    vectors = pipeline.fit_transform(texts)

    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, model_dir / "desc_tfidf.joblib")

    svd_cols = [
        pl.Series(f"desc_svd_{i}", vectors[:, i].tolist(), dtype=pl.Float64)
        for i in range(n_components)
    ]
    return df.with_columns(svd_cols), pipeline


def _build_tfidf_pipeline(n_components: int) -> SklearnPipeline:
    """Build an unfitted TF-IDF + TruncatedSVD pipeline."""
    return SklearnPipeline(
        [
            (
                "tfidf",
                TfidfVectorizer(
                    max_features=5000,
                    ngram_range=(1, 2),
                    min_df=2,
                    sublinear_tf=True,
                ),
            ),
            ("svd", TruncatedSVD(n_components=n_components, random_state=42)),
        ]
    )
```

**Step 4: Call `_extract_text_features()` inside `enrich_dev()`**

At the end of `enrich_dev()`, before `df.write_parquet(...)`, add:

```python
    # --- NLP features: TF-IDF + SVD on description column ---
    # Derives model_dir from data_dir (sibling directory) so this works correctly
    # regardless of CWD — including in Docker where CWD is /app.
    _model_dir = data_dir.parent / "models"
    df, _ = _extract_text_features(df, _model_dir)
```

> **Note for implementer:** The `enrich_dev()` function signature is `enrich_dev(data_dir: Path = Path("data")) -> int`. To keep the signature stable, `model_dir` defaults to `Path("models")` as a local variable inside the function. If you need to test with a custom model_dir, you can add a `model_dir` parameter with a default, but coordinate this with any callers.

**Step 5: Run NLP tests**

```bash
uv run pytest tests/analytics/test_nlp_features.py -v
```

Expected: All tests pass.

**Step 6: Update `DEV_NUM_COLS` in `src/zoneto/analytics/features.py`**

Add `desc_svd_0` through `desc_svd_19` to `DEV_NUM_COLS`:

```python
DEV_NUM_COLS: list[str] = [
    "year_submitted",
    "in_heritage_register",
    "in_heritage_district",
    "in_secondary_plan",
    "has_community_meeting",
    "ward_pct_renters",
    "ward_median_income",
    "ward_pop_density",
    "ward_pct_detached",
    "has_parent_application",
    "is_combined_application",
    "proposed_storeys",
    "proposed_units",
    "ward_appeal_rate_3y",
    "in_mtsa",
    *[f"desc_svd_{i}" for i in range(20)],
]
```

**Step 7: Update features test**

In `tests/analytics/test_features.py`, update the `DEV_NUM_COLS` count assertion:

```python
assert len(DEV_NUM_COLS) == 35  # 15 original + 20 SVD components
assert "in_mtsa" in DEV_NUM_COLS
assert "desc_svd_0" in DEV_NUM_COLS
assert "desc_svd_19" in DEV_NUM_COLS
```

**Step 8: Update `src/zoneto/analytics/score.py` to apply vectorizer before scoring**

In `score_all()`, at the start of the dev_applications scoring block (before building `X_dev`), add vectorizer loading and application. Find where `X_dev` is built (around line 120 in the original):

```python
    # --- dev_applications ---
    dev_enriched = data_dir / "enriched" / "dev_applications.parquet"
    df_dev = pl.read_parquet(dev_enriched)

    # Apply NLP vectorizer if available (adds desc_svd_0..19 columns)
    _tfidf_path = model_dir / "desc_tfidf.joblib"
    if _tfidf_path.exists() and "description" in df_dev.columns:
        import joblib as _jl  # noqa: PLC0415
        _tfidf_pipe = _jl.load(_tfidf_path)
        _texts = df_dev["description"].fill_null("").cast(pl.String).to_list()
        _vectors = _tfidf_pipe.transform(_texts)
        _svd_cols = [
            pl.Series(f"desc_svd_{i}", _vectors[:, i].tolist(), dtype=pl.Float64)
            for i in range(_vectors.shape[1])
        ]
        df_dev = df_dev.with_columns(_svd_cols)

    all_dev_cols = DEV_CAT_COLS + DEV_NUM_COLS
    # Select only columns that exist in the DataFrame (graceful degradation)
    available_dev_cols = [c for c in all_dev_cols if c in df_dev.columns]
    X_dev = df_dev.select(available_dev_cols).to_pandas()
```

Also update `score_one()` for `dev_applications` source to apply the vectorizer:

```python
    if source == "dev_applications":
        models = _DEV_MODELS
        all_cols = DEV_CAT_COLS + DEV_NUM_COLS
        # Apply NLP vectorizer to add desc_svd features
        _tfidf_path = model_dir / "desc_tfidf.joblib"
        if _tfidf_path.exists() and "description" in features:
            import joblib as _jl  # noqa: PLC0415
            _tfidf_pipe = _jl.load(_tfidf_path)
            _text = str(features.get("description") or "")
            _vec = _tfidf_pipe.transform([_text])
            for i in range(_vec.shape[1]):
                features = {**features, f"desc_svd_{i}": float(_vec[0, i])}
```

> **Note for implementer:** The `score_one` change mutates `features` dict. Since we're creating a new dict with `{**features, ...}` this is safe. Add this block before the `X = pd.DataFrame(...)` line.

**Step 9: Run full test suite**

```bash
uv run pytest -qq
```

Expected: All tests pass.

**Step 10: Run linter and type checker**

```bash
uv run ruff check src/zoneto/analytics/ tests/analytics/test_nlp_features.py tests/analytics/test_mtsa_feature.py
uv run ty check src/zoneto/analytics/
```

Expected: No errors.

**Step 11: Commit**

```bash
git add src/zoneto/analytics/enrich.py src/zoneto/analytics/features.py src/zoneto/analytics/score.py tests/analytics/test_nlp_features.py tests/analytics/test_features.py
git commit -m "feat: add TF-IDF+SVD NLP features (desc_svd_0..19) from description column"
```

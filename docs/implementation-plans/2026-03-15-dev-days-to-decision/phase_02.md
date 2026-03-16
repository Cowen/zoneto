# Phase 2: Label Engineering and Feature Update

**Design phase:** Phase 3

**Goal:** Extend `enrich_dev()` to join AIC decision dates and compute survival analysis labels. Add `is_combined_application` feature.

---

### Task 1: Write failing tests for survival label columns

**Files:**
- Modify: `tests/analytics/test_enrich.py`

**Context:** The current `_make_dev_parquet()` uses `application_type = ["Rezoning", "Site Plan", "Rezoning"]`. Existing tests filter on these values. Do NOT change the existing fixture's `application_type` — add `folderrsn`, `application_url`, and `description` columns to the existing fixture without breaking existing tests. Use a NEW `_make_dev_parquet_oz_sa()` helper for the survival-specific tests.

**Step 1: Verify `from datetime import date` is at the top of test_enrich.py**

Check the imports at the top of `tests/analytics/test_enrich.py`. Add if missing:

```python
from datetime import date
```

**Step 2: Extend `_make_dev_parquet()` to include required columns**

Find `_make_dev_parquet()` (line ~242). Update it to add `folderrsn`, `application_url`, and `description` columns WITHOUT changing `application_type` or `status` (preserving existing tests):

```python
def _make_dev_parquet(tmp_path: Path) -> None:
    """Write minimal dev_applications parquet."""
    df = pl.DataFrame(
        {
            "folderrsn": ["AAA", "BBB", "CCC"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=AAA",
                "https://app.toronto.ca/AIC/details?folderRsn=BBB",
                None,
            ],
            "description": ["Rezoning application", "Site plan approval", "Rezoning"],
            "date_submitted": ["2021-06-01", "2021-09-15", "2022-01-10"],
            "status": ["Closed", "Refused", "Under Review"],
            "application_type": ["Rezoning", "Site Plan", "Rezoning"],
            "ward_number": ["Ward 1", "Ward 5", "Ward 10"],
            "community_meeting_date": ["2021-07-01", None, None],
            "parent_folder_number": ["23 456789 OZ", None, None],
            "postal": ["M5V 2T6", "M4K 1A1", None],
            "x": ["630000.0", "631000.0", None],
            "y": ["4840000.0", "4841000.0", None],
            "source_name": ["dev_applications"] * 3,
            "year": [2021, 2021, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")
```

**Step 3: Add `_make_dev_parquet_oz_sa()` for survival-specific tests**

Add this new helper right after `_make_dev_parquet()`:

```python
def _make_dev_parquet_oz_sa(tmp_path: Path) -> None:
    """Write dev_applications parquet with OZ+SA types for survival label tests."""
    df = pl.DataFrame(
        {
            "folderrsn": ["AAA", "BBB", "CCC"],
            "application_url": [
                "https://app.toronto.ca/AIC/details?folderRsn=AAA",
                "https://app.toronto.ca/AIC/details?folderRsn=BBB",
                None,
            ],
            "description": ["OPA and rezoning", "Site plan approval", "Rezoning"],
            "date_submitted": ["2021-06-01", "2021-09-15", "2022-01-10"],
            "status": ["Closed", "Closed", "Under Review"],
            "application_type": ["OZ", "SA", "OZ"],
            "ward_number": ["Ward 1", "Ward 5", "Ward 10"],
            "community_meeting_date": [None, None, None],
            "parent_folder_number": [None, None, None],
            "postal": ["M5V 2T6", "M4K 1A1", None],
            "x": ["630000.0", "631000.0", None],
            "y": ["4840000.0", "4841000.0", None],
            "source_name": ["dev_applications"] * 3,
            "year": [2021, 2021, 2022],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2021"
    out.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out / "part0.parquet")
```

**Step 4: Add `_make_aic_decisions()` helper**

Add after `_make_dev_parquet_oz_sa()`:

```python
def _make_aic_decisions(tmp_path: Path) -> None:
    """Write minimal aic_decisions.parquet with one OZ and one SA decision."""
    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        {
            "folderrsn": ["AAA", "BBB"],
            "decision_date": [date(2022, 11, 8), date(2021, 2, 14)],
            "complete_date": [date(2021, 3, 15), date(2020, 6, 1)],
            "scraped_at": [date(2026, 3, 15), date(2026, 3, 15)],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    df.write_parquet(ref_dir / "aic_decisions.parquet")
```

**Step 5: Add survival label tests**

Add these tests at the end of `tests/analytics/test_enrich.py`. They use `_make_dev_parquet_oz_sa()` (not the standard fixture):

```python
def test_enrich_dev_survival_labels_for_oz_with_decision(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """OZ with decision date: dev_days_to_decision computed; dev_decision_event=1."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    oz_row = df.filter(pl.col("folderrsn") == "AAA")
    # date_submitted=2021-06-01, decision_date=2022-11-08 → 525 days
    assert oz_row["dev_days_to_decision"][0] == 525
    assert oz_row["dev_decision_event"][0] == 1
    assert oz_row["dev_days_observed"][0] == 525


def test_enrich_dev_decision_event_zero_for_active(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Active OZ (Under Review): dev_decision_event=0; dev_days_observed=today-submitted."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    active_row = df.filter(pl.col("folderrsn") == "CCC")
    assert active_row["dev_decision_event"][0] == 0
    assert active_row["dev_days_to_decision"][0] is None
    assert active_row["dev_days_observed"][0] is not None
    assert active_row["dev_days_observed"][0] > 0


def test_enrich_dev_days_to_decision_cap_at_3650(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """Values > 3650 days become null (outlier cap)."""
    df = pl.DataFrame(
        {
            "folderrsn": ["OLD"],
            "application_url": ["https://app.toronto.ca/AIC/details?folderRsn=OLD"],
            "description": ["Rezoning"],
            "date_submitted": ["2010-01-01"],
            "status": ["Closed"],
            "application_type": ["OZ"],
            "ward_number": ["Ward 1"],
            "community_meeting_date": [None],
            "parent_folder_number": [None],
            "postal": [None],
            "x": [None],
            "y": [None],
            "source_name": ["dev_applications"],
            "year": [2010],
        }
    ).with_columns(pl.col("date_submitted").str.to_date())
    out = tmp_path / "dev_applications" / "year=2010"
    out.mkdir(parents=True)
    df.write_parquet(out / "part0.parquet")

    ref_dir = tmp_path / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    decisions = pl.DataFrame(
        {
            "folderrsn": ["OLD"],
            "decision_date": [date(2020, 6, 1)],  # ~3804 days after 2010-01-01
            "complete_date": [None],
            "scraped_at": [date(2026, 3, 15)],
        }
    ).with_columns(
        pl.col("decision_date").cast(pl.Date),
        pl.col("complete_date").cast(pl.Date),
        pl.col("scraped_at").cast(pl.Date),
    )
    decisions.write_parquet(ref_dir / "aic_decisions.parquet")

    enrich_dev(data_dir=tmp_path)
    df_out = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    old_row = df_out.filter(pl.col("folderrsn") == "OLD")
    assert old_row["dev_days_to_decision"][0] is None


def test_enrich_dev_is_combined_application_oz_opa(
    tmp_path: Path,
    stub_spatial_join: None,
) -> None:
    """OZ application with 'OPA' in description: is_combined_application=1."""
    _make_dev_parquet_oz_sa(tmp_path)
    _make_aic_decisions(tmp_path)
    enrich_dev(data_dir=tmp_path)
    df = pl.read_parquet(tmp_path / "enriched" / "dev_applications.parquet")
    oz_row = df.filter(pl.col("folderrsn") == "AAA")
    # description="OPA and rezoning", application_type="OZ"
    assert oz_row["is_combined_application"][0] == 1
    # SA row should be 0 (not OZ type)
    sa_row = df.filter(pl.col("folderrsn") == "BBB")
    assert sa_row["is_combined_application"][0] == 0
```

**Step 6: Run tests to verify they fail**

```bash
uv run pytest tests/analytics/test_enrich.py -k "survival or combined or decision_event or days_to_decision_cap" -v
```

Expected: failures — `enrich_dev()` doesn't produce the new columns yet.

Also confirm existing tests still pass:

```bash
uv run pytest tests/analytics/test_enrich.py -k "not (survival or combined or decision_event or days_to_decision_cap)" -v
```

Expected: all existing tests pass unchanged.

---

### Task 2: Implement survival label columns in enrich_dev()

**Files:**
- Modify: `src/zoneto/analytics/enrich.py`

**Step 1: Verify `from datetime import date` is in the imports at the top of enrich.py**

Check the existing imports in `src/zoneto/analytics/enrich.py`. Add at the top if missing:

```python
from datetime import date as _date
```

**Step 2: Add constants near other dev constants**

Find the existing `_DEV_APPROVED_SET`, `_DEV_REFUSED_SET` constants in `enrich.py`. Add after them:

```python
_DEV_SURVIVAL_TYPES: frozenset[str] = frozenset({"OZ", "SA"})
_DEV_DAYS_CAP = 3650
```

**Step 3: Add the AIC join and survival label block inside `enrich_dev()`**

Find this line in `enrich_dev()`:

```python
    # Enrich with ward profiles
    df = _enrich_ward_features(df, data_dir)
```

Insert the following block **between** the `_enrich_ward_features` call and the `out = data_dir / "enriched"` block. Note: use `_date.today()` (not inline imports; not the polars `.sub()` method):

```python
    # --- AIC decision date join and survival labels ---
    aic_path = data_dir / "reference" / "aic_decisions.parquet"
    if aic_path.exists() and "folderrsn" in df.columns:
        aic = pl.read_parquet(aic_path).select(["folderrsn", "decision_date"])
        df = df.join(aic, on="folderrsn", how="left")
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Date).alias("decision_date"))

    _today = _date.today()

    # dev_days_to_decision: days from date_submitted to decision_date (OZ/SA only)
    # Intermediate column _raw_days used to simplify capping logic.
    df = df.with_columns(
        pl.when(
            pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES))
            & pl.col("decision_date").is_not_null()
        )
        .then(
            (pl.col("decision_date") - pl.col("date_submitted").cast(pl.Date))
            .dt.total_days()
            .cast(pl.Int32)
        )
        .otherwise(None)
        .alias("_raw_days")
    )
    df = df.with_columns(
        pl.when(
            pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES))
            & pl.col("decision_date").is_not_null()
            & (pl.col("_raw_days") <= _DEV_DAYS_CAP)
        )
        .then(pl.col("_raw_days"))
        .otherwise(None)
        .cast(pl.Int32)
        .alias("dev_days_to_decision")
    ).drop("_raw_days")

    # dev_decision_event: 1 = has decision, 0 = active, null = not OZ/SA
    df = df.with_columns(
        pl.when(~pl.col("application_type").is_in(list(_DEV_SURVIVAL_TYPES)))
        .then(None)
        .when(pl.col("decision_date").is_not_null())
        .then(pl.lit(1, dtype=pl.Int8))
        .when(pl.col("is_active") == 1)
        .then(pl.lit(0, dtype=pl.Int8))
        .otherwise(None)
        .alias("dev_decision_event")
    )

    # dev_days_observed: days_to_decision for events; today-submitted for censored
    df = df.with_columns(
        pl.when(pl.col("dev_decision_event") == 1)
        .then(pl.col("dev_days_to_decision"))
        .when(pl.col("dev_decision_event") == 0)
        .then(
            (pl.lit(_today) - pl.col("date_submitted").cast(pl.Date))
            .dt.total_days()
            .cast(pl.Int32)
        )
        .otherwise(None)
        .alias("dev_days_observed")
    )

    # is_combined_application: OZ with OPA in description field (case-insensitive)
    if "description" in df.columns:
        df = df.with_columns(
            pl.when(
                (pl.col("application_type") == "OZ")
                & pl.col("description").str.to_uppercase().str.contains("OPA")
            )
            .then(pl.lit(1, dtype=pl.Int8))
            .otherwise(pl.lit(0, dtype=pl.Int8))
            .alias("is_combined_application")
        )
    else:
        df = df.with_columns(pl.lit(0, dtype=pl.Int8).alias("is_combined_application"))
```

**Step 4: Run the new tests**

```bash
uv run pytest tests/analytics/test_enrich.py -k "survival or combined or decision_event or days_to_decision_cap" -v
```

Expected: all 4 new tests pass.

**Step 5: Run all enrich tests to catch regressions**

```bash
uv run pytest tests/analytics/test_enrich.py -v
```

Expected: all tests pass.

---

### Task 3: Add `is_combined_application` to DEV_NUM_COLS and test it

**Files:**
- Modify: `src/zoneto/analytics/features.py`
- Modify: `tests/analytics/test_features.py`

**Step 1: Write a failing test**

In `tests/analytics/test_features.py`, add:

```python
def test_dev_num_cols_includes_is_combined_application() -> None:
    from zoneto.analytics.features import DEV_NUM_COLS
    assert "is_combined_application" in DEV_NUM_COLS
```

Run:
```bash
uv run pytest tests/analytics/test_features.py::test_dev_num_cols_includes_is_combined_application -v
```

Expected: FAIL.

**Step 2: Add `is_combined_application` to `DEV_NUM_COLS`**

In `src/zoneto/analytics/features.py`, replace `DEV_NUM_COLS`:

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
]
```

**Step 3: Run all tests**

```bash
uv run pytest -qq
uv run ruff check && uv run ty check src/
```

Expected: all tests pass, lint clean.

**Step 4: Update model regression baselines**

Adding `is_combined_application` to `DEV_NUM_COLS` changes the feature set for the existing `dev_applications_appealed` model. The regression test baselines need updating:

```bash
just update-baselines
```

Expected: `tests/fixtures/model_baselines.json` updated. Verify the command succeeds (requires enriched parquet in `data/enriched/`; if running in CI without real data, skip this step and note the baselines will need updating in the next pipeline run).

**Step 5: Commit**

```bash
git add src/zoneto/analytics/enrich.py src/zoneto/analytics/features.py \
        tests/analytics/test_enrich.py tests/analytics/test_features.py \
        tests/fixtures/model_baselines.json
git commit -m "feat: add survival labels and is_combined_application to enrich_dev()"
```

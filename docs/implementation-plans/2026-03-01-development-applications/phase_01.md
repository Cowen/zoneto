# Development Applications Source — Phase 1: Extend CKANConfig with `year_column`

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add a `year_column: str = "application_date"` field to `CKANConfig` and update `_normalize()` to use it, so any source can declare which date column drives the Hive partition year.

**Architecture:** Backwards-compatible extension. The default value `"application_date"` preserves all existing source behaviour unchanged. Only `_normalize()` changes — the hardcoded string `"application_date"` is replaced with `self.config.year_column`. All three existing sources implicitly keep `year_column="application_date"` via the default.

**Tech Stack:** Python 3.13, Pydantic v2, Polars, pytest, pytest-httpx

**Scope:** Phase 1 of 4

**Codebase verified:** 2026-03-01

---

## Task 1: Write the failing test (TDD — RED)

**Files:**
- Modify: `tests/test_ckan_datastore.py` (append at end of file)

### Step 1: Append the failing test

Add the following function to the **end** of `tests/test_ckan_datastore.py` (after the last existing test on line 261):

```python
def test_custom_year_column_derives_year(httpx_mock: HTTPXMock) -> None:
    """year is derived from the configured year_column, not the hardcoded application_date.

    A source configured with year_column='date_submitted' must extract year
    from that column after snake_case normalization ('Date Submitted' -> 'date_submitted').
    """
    source = CKANSource(
        CKANConfig(
            dataset_id="development-applications",
            access_mode="datastore",
            year_column="date_submitted",
        )
    )
    httpx_mock.add_response(json=_PACKAGE_SHOW)
    httpx_mock.add_response(
        json={
            "result": {
                "records": [
                    {"Date Submitted": "2023-05-15", "APPLICATION#": "OZ-01"},
                    {"Date Submitted": "2021-11-30", "APPLICATION#": "SA-02"},
                ]
            }
        },
    )
    httpx_mock.add_response(json={"result": {"records": []}})

    df = source.fetch()
    years = df["year"].to_list()
    assert 2023 in years
    assert 2021 in years
```

### Step 2: Run the test to confirm it fails

```bash
uv run pytest tests/test_ckan_datastore.py::test_custom_year_column_derives_year -v
```

Expected failure — `year=0` for both records because `_normalize()` currently checks for `"application_date"` which is absent from the mock response:

```
FAILED tests/test_ckan_datastore.py::test_custom_year_column_derives_year
AssertionError: assert 2023 in [0, 0]
```

If the test passes without any code changes, the test is wrong — fix it before proceeding.

---

## Task 2: Implement — add `year_column` to `CKANConfig` (GREEN)

**Files:**
- Modify: `src/zoneto/models.py`

### Step 3: Add `year_column` field

In `src/zoneto/models.py`, append `year_column: str = "application_date"` after the `year_start` field.

Current class body:
```python
    dataset_id: str
    access_mode: Literal["datastore", "bulk_csv"]
    year_start: int = 2015
```

New class body:
```python
    dataset_id: str
    access_mode: Literal["datastore", "bulk_csv"]
    year_start: int = 2015
    year_column: str = "application_date"
```

---

## Task 3: Implement — update `_normalize()` to use `self.config.year_column` (GREEN)

**Files:**
- Modify: `src/zoneto/sources/ckan.py` (lines 137–151)

### Step 4: Replace the hardcoded year derivation block

In `src/zoneto/sources/ckan.py`, locate the year derivation comment and if/else block (currently lines 137–151). Replace the entire block:

**Old:**
```python
        # 3. Derive year from application_date (null dates → year 0)
        # Only possible if the column was successfully parsed as pl.Date.
        if (
            "application_date" in df.columns
            and df.schema["application_date"] == pl.Date
        ):
            df = df.with_columns(
                pl.col("application_date")
                .dt.year()
                .fill_null(0)
                .cast(pl.Int32)
                .alias("year")
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int32).alias("year"))
```

**New:**
```python
        # 3. Derive year from the configured year_column (null dates → year 0)
        # Only possible if the column was successfully parsed as pl.Date.
        year_col = self.config.year_column
        if (
            year_col in df.columns
            and df.schema[year_col] == pl.Date
        ):
            df = df.with_columns(
                pl.col(year_col)
                .dt.year()
                .fill_null(0)
                .cast(pl.Int32)
                .alias("year")
            )
        else:
            df = df.with_columns(pl.lit(0).cast(pl.Int32).alias("year"))
```

---

## Task 4: Verify GREEN and run full suite

### Step 5: Confirm the failing test now passes

```bash
uv run pytest tests/test_ckan_datastore.py::test_custom_year_column_derives_year -v
```

Expected:
```
PASSED tests/test_ckan_datastore.py::test_custom_year_column_derives_year
```

### Step 6: Run the full test suite — no regressions

```bash
just test
```

Expected: All tests pass. The three existing sources work exactly as before because their `CKANConfig` instances use the default `year_column="application_date"`.

---

## Task 5: Commit

### Step 7: Stage and commit

```bash
git add tests/test_ckan_datastore.py src/zoneto/models.py src/zoneto/sources/ckan.py
git commit -m "feat: add year_column to CKANConfig for configurable year derivation

Allows sources to declare which date column drives the Hive partition year.
Default is 'application_date', preserving all existing source behaviour.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

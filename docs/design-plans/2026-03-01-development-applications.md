# Development Applications Source Design

## Overview

Add `dev_applications` as a fourth data source in the zoneto pipeline. The source fetches
all Toronto development applications from the `development-applications` CKAN DataStore
dataset (~26,000 rows, 2008–present) and stores them as Hive-partitioned Parquet files.

Goals:
- Ingest all available development application records (OZ, SA, CD, SB, PL types)
- Extend `CKANConfig` with a configurable `year_column` field so sources can declare
  which date column drives Hive partitioning (backwards-compatible: default = `"application_date"`)
- Document the source fully in README

Success criteria:
- `zoneto sync --source dev_applications` writes `data/dev_applications/year=YYYY/` partitions
- Year is correctly derived from `date_submitted` (not `application_date`, which doesn't exist)
- All existing source tests continue to pass unchanged

## Architecture

`dev_applications` uses the existing `CKANSource` class in DataStore mode — no new
source class is needed.

The only structural change is adding a `year_column: str = "application_date"` field to
`CKANConfig`. The `_normalize()` method in `ckan.py` replaces the hardcoded
`"application_date"` string with `self.config.year_column`. All three existing sources
keep their default (`"application_date"`) and are unaffected.

Registry entry:

```python
"dev_applications": CKANSource(
    CKANConfig(
        dataset_id="development-applications",
        access_mode="datastore",
        year_start=2000,         # fetch all available data (earliest records ~2008)
        year_column="date_submitted",
    )
)
```

Data flow is unchanged: CLI → registry → `source.fetch()` → `storage.write_source()` →
`data/dev_applications/year=YYYY/*.parquet`.

## Existing Patterns

Investigation found these patterns already in use, all followed by this design:

- **CKANConfig model** (`src/zoneto/models.py`): Pydantic `BaseModel` with `dataset_id`,
  `access_mode`, `year_start`. Adding `year_column` as a fourth field with default
  `"application_date"` follows the same pattern.
- **`_normalize()`** (`src/zoneto/sources/ckan.py`): Hardcodes `"application_date"` as
  the year derivation column. This design parameterises that string via `self.config.year_column`.
- **Registry** (`src/zoneto/sources/registry.py`): `SOURCES: dict[str, Source]` mapping
  logical keys to `CKANSource` instances. New entry follows the same pattern.
- **TDD with pytest-httpx**: All HTTP-dependent behaviour is tested by mocking CKAN
  responses; no network calls in tests. This phase adds new tests following that pattern.

## Implementation Phases

### Phase 1: Extend `CKANConfig` and `_normalize()` with configurable year column

**Goal:** Allow any source to specify which date column drives the `year` partition key.

**Components:**
- Modify: `src/zoneto/models.py` — add `year_column: str = "application_date"` field
- Modify: `src/zoneto/sources/ckan.py` — replace hardcoded `"application_date"` with
  `self.config.year_column` in `_normalize()`
- Modify: `tests/test_ckan_datastore.py` — add `test_custom_year_column_derives_year`
  (TDD: write test first, watch it fail, then implement)

**Dependencies:** None (first phase)

**Done when:**
- `test_custom_year_column_derives_year` passes: a source configured with
  `year_column="date_submitted"` correctly derives `year` from that column
- All pre-existing datastore and bulk CSV tests still pass (`just test`)

### Phase 2: Register `dev_applications` source

**Goal:** Make `dev_applications` available to the CLI sync and status commands.

**Components:**
- Modify: `src/zoneto/sources/registry.py` — add `dev_applications` entry with
  `dataset_id="development-applications"`, `access_mode="datastore"`,
  `year_start=2000`, `year_column="date_submitted"`

**Dependencies:** Phase 1 (`year_column` field must exist on `CKANConfig`)

**Done when:**
- `zoneto sync --source dev_applications` runs without error (manual verification)
- `zoneto status` shows `dev_applications` in the output table

### Phase 3: Document in README

**Goal:** README accurately reflects the new source, including limitations.

**Components:**
- Modify: `README.md`
  - Add row for `dev_applications` to the sources summary table
  - Add `### Development Applications (\`dev_applications\`)` section with:
    - CKAN dataset name and what it is
    - Application types table (OZ / SA / CD / SB / PL)
    - Key fields table (24 columns)
    - Known limitations (retired dataset, one row per address, no detailed conditions)

**Dependencies:** Phase 2 (source must exist before documenting it)

**Done when:** README section is complete and accurate per dataset schema

### Phase 4: Update CLAUDE.md

**Goal:** Keep project documentation in sync with code contracts.

**Components:**
- Modify: `CLAUDE.md`
  - Add `year_column` row to the `CKANConfig` fields table
  - Add `dev_applications` row to the Registry table
  - Update the `year` invariant: replace "derived from `application_date`" with
    "derived from `year_column` config field (default `application_date`)"
  - Bump freshness anchor date

**Dependencies:** Phase 1 (contracts changed), Phase 2 (registry changed)

**Done when:** CLAUDE.md table and invariants match the implemented code

## Additional Considerations

**Retired dataset:** The `development-applications` CKAN dataset is marked "Retired" on
the Toronto Open Data Portal. The data continues to be served but is unlikely to receive
new records. Fetching all years (`year_start=2000`) captures the full historical archive
in one sync. If the dataset is eventually removed from CKAN, the sync will fail with an
HTTP error rather than silently returning empty data.

**One row per address:** Each `APPLICATION#` may appear in multiple rows — one per
address/parcel within the development boundary. This structure is preserved as-is.
If per-application deduplication is needed downstream, `application_number` (snake_case
of `APPLICATION#`) is the join key. `PARENT_FOLDER_NUMBER` links related applications.

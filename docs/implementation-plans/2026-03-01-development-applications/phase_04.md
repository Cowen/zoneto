# Development Applications Source — Phase 4: Update CLAUDE.md

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Keep `CLAUDE.md` in sync with the code contracts changed in Phases 1–2: add the `year_column` field to the `CKANConfig` table, add `dev_applications` to the Registry table, update the year-derivation invariant, and bump the freshness date.

**Architecture:** Documentation-only changes. Four targeted edits to `CLAUDE.md`, no code changes.

**Tech Stack:** Markdown

**Scope:** Phase 4 of 4. Requires Phase 1 (`year_column` field added) and Phase 2 (registry updated).

**Codebase verified:** 2026-03-01

---

## Task 1: Update CKANConfig table header and add `year_column` row

**Files:**
- Modify: `CLAUDE.md` (lines 53–59, the CKANConfig section)

### Step 1: Replace the CKANConfig header and table

The current section (lines 51–59):
```markdown
### CKANConfig (`models.py`)

Pydantic model with three fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataset_id` | `str` | required | CKAN package name |
| `access_mode` | `Literal["datastore", "bulk_csv"]` | required | fetch strategy |
| `year_start` | `int` | 2015 | year floor: skip CSV resources and filter rows below this year |
```

Replace with:
```markdown
### CKANConfig (`models.py`)

Pydantic model with four fields:

| Field | Type | Default | Notes |
|---|---|---|---|
| `dataset_id` | `str` | required | CKAN package name |
| `access_mode` | `Literal["datastore", "bulk_csv"]` | required | fetch strategy |
| `year_start` | `int` | 2015 | year floor: skip CSV resources and filter rows below this year |
| `year_column` | `str` | `"application_date"` | date column from which the Hive partition `year` is derived |
```

---

## Task 2: Update Registry table header and add `dev_applications` row

**Files:**
- Modify: `CLAUDE.md` (lines 72–80, the Registry section)

### Step 2: Replace the Registry table

The current table (lines 76–80):
```markdown
| Key | Dataset | Mode | year_start |
|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2020 |
```

Replace with:
```markdown
| Key | Dataset | Mode | year_start | year_column |
|---|---|---|---|---|
| `permits_active` | building-permits-active-permits | datastore | 2020 | `application_date` |
| `permits_cleared` | building-permits-cleared-permits | datastore | 2020 | `application_date` |
| `coa` | committee-of-adjustment-applications | bulk_csv | 2020 | `application_date` |
| `dev_applications` | development-applications | datastore | 2000 | `date_submitted` |
```

---

## Task 3: Update the year-derivation invariant

**Files:**
- Modify: `CLAUDE.md` (lines 110–111, the `year` invariant)

### Step 3: Replace the stale invariant

Current lines 110–111:
```markdown
- `year` is derived from `application_date` only if it was successfully parsed
  as `pl.Date`; otherwise defaults to 0.
```

Replace with:
```markdown
- `year` is derived from `year_column` (default `application_date`) only if that
  column was successfully parsed as `pl.Date`; otherwise defaults to 0.
```

---

## Task 4: Bump freshness anchor

**Files:**
- Modify: `CLAUDE.md` (lines 3–4, the freshness comment)

### Step 4: Update the freshness comment

Current lines 3–4:
```markdown
<!-- Freshness: 2026-03-01 -->
<!-- Last reviewed against: 4b630df -->
```

Replace with (using today's date and the latest commit on this branch after all implementation commits):
```markdown
<!-- Freshness: 2026-03-01 -->
<!-- Last reviewed against: development-applications branch -->
```

After this entire implementation is merged and you know the final commit hash, update `Last reviewed against` to that hash. For now, the branch reference is sufficient.

---

## Task 5: Run tests and commit

### Step 5: Run the full test suite

```bash
just test
```

Expected: All tests pass. CLAUDE.md is documentation; no test changes are needed.

### Step 6: Commit

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with year_column field and dev_applications registry

Adds year_column to CKANConfig table, adds dev_applications row to Registry
table, and updates the year-derivation invariant to reflect the configurable
year_column field introduced in Phase 1.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

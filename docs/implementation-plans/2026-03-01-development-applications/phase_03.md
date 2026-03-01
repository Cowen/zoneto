# Development Applications Source — Phase 3: Document in README

> **For Claude:** REQUIRED SUB-SKILL: Use ed3d-plan-and-execute:subagent-driven-development to implement this plan task-by-task.

**Goal:** Update `README.md` to document `dev_applications` as a first-class pipeline source, including application types, key fields, and known limitations.

**Architecture:** Four changes to `README.md`: (1) update the sources summary table, (2) update the stale "all sources use year_start=2020" paragraph, (3) add a new `### Development Applications` section, (4) remove the now-stale references to `development-applications` in the "Gaps" and "Potentially relevant" sections.

**Tech Stack:** Markdown

**Scope:** Phase 3 of 4. Requires Phase 2 complete (source must exist before documenting it).

**Codebase verified:** 2026-03-01

---

## Task 1: Update the sources summary table

**Files:**
- Modify: `README.md` (lines 43–48, the sources table)

### Step 1: Replace the sources summary table

Locate the table under `### Sources in this pipeline` (around line 43). The current table header says `Rows (2020+)`, which doesn't fit `dev_applications` (all years). Change it to `Rows` and add row-level notes.

**Old:**
```markdown
| Key | Dataset | CKAN mode | Rows (2020+) | Updated |
|-----|---------|-----------|--------------|---------||
| `permits_active` | Building Permits — Active | DataStore | ~98 k | Daily |
| `permits_cleared` | Building Permits — Cleared | DataStore | ~161 k | Daily |
| `coa` | Committee of Adjustment Applications | Bulk CSV | ~5 k | Periodic |
```

**New:**
```markdown
| Key | Dataset | CKAN mode | Rows | Updated |
|-----|---------|-----------|------|---------|
| `permits_active` | Building Permits — Active | DataStore | ~98 k (2020+) | Daily |
| `permits_cleared` | Building Permits — Cleared | DataStore | ~161 k (2020+) | Daily |
| `coa` | Committee of Adjustment Applications | Bulk CSV | ~5 k (2020+) | Periodic |
| `dev_applications` | Development Applications | DataStore | ~26 k (all years) | Retired |
```

---

## Task 2: Update the stale year-filter paragraph

**Files:**
- Modify: `README.md` (lines 49–51, the paragraph after the sources table)

### Step 2: Update the year-filter paragraph

The current paragraph (lines 49–51):
```markdown
All sources are fetched with `year_start=2020` (rows with `year < 2020` are
discarded after fetch, except for records with unparseable dates which land in
`year=0`).
```

Replace with:
```markdown
Building permit sources (`permits_active`, `permits_cleared`) and `coa` are fetched
from 2020 onwards (`year_start=2020`). `dev_applications` uses `year_start=2000` to
capture the full archive (earliest records are from 2008). In all cases, records with
unparseable dates land in `year=0`.
```

---

## Task 3: Add the `### Development Applications` section

**Files:**
- Modify: `README.md` (insert before `## Gaps and what's not here`)

### Step 3: Insert the new section

Find the line `## Gaps and what's not here` (currently around line 172 — it will shift a few lines after Step 2). Insert the following block **immediately before** that heading (including the `---` separator):

```markdown
### Development Applications (`dev_applications`)

**CKAN dataset:** `development-applications`
**What it is:** Every development application filed with the City of Toronto, covering
rezoning (Official Plan/Zoning By-law Amendments), site plan approvals, condominium
registrations, subdivision plans, and part-lot control exemptions. The dataset spans
2008 to present and is the primary planning-application data used by development
trackers such as UrbanToronto.

**Application types:**

| Code | Type |
|------|------|
| OZ | Official Plan Amendment and/or Zoning By-law Amendment |
| SA | Site Plan Control Application |
| CD | Draft Plan of Condominium Application |
| SB | Draft Plan of Subdivision Application |
| PL | Part Lot Control Exemption Application |

**Key fields (24 columns):**

| Column | Description |
|--------|-------------|
| `application_type` | Application type code (OZ / SA / CD / SB / PL) |
| `application` | Application number (e.g. `22 123456 SA`) — from raw column `APPLICATION#` |
| `date_submitted` | Date the application was filed |
| `status` | Current status (e.g. Closed, Under Review, Council Approved) |
| `street_num`, `street_name`, `street_type`, `street_direction`, `postal` | Address |
| `description` | Brief description of the proposed development |
| `reference_file` | Cross-reference file number — from raw column `REFERENCE_FILE#` |
| `folderrsn` | Internal city folder reference number |
| `ward_number`, `ward_name` | City ward |
| `community_meeting_date`, `community_meeting_time`, `community_meeting_location` | Public consultation details |
| `application_url` | Link to the city's planning application viewer |
| `contact_name`, `contact_phone`, `contact_email` | Planner contact |
| `parent_folder_number` | Links related applications (e.g. a rezoning and its site plan) |
| `x`, `y` | Coordinates (city internal CRS) |

**Known limitations:**

- **Retired dataset.** The `development-applications` dataset is marked "Retired" on
  the Toronto Open Data Portal. Data continues to be served but is unlikely to receive
  new records. If the dataset is eventually removed from CKAN, sync will fail with an
  HTTP error rather than silently returning empty data.
- **One row per address/parcel.** Each `application` number may appear in multiple rows —
  one per address or parcel within the development boundary. This structure is preserved
  as-is. If per-application deduplication is needed downstream, `application` is the join
  key; `parent_folder_number` links related applications.
- **`description` is brief.** The field contains a short summary rather than a full
  project description.
- **No detailed conditions or approval text.** The dataset records status and dates but
  not the conditions attached to approvals.

---

```

---

## Task 4: Remove stale references from "Gaps" and "Potentially relevant" sections

`development-applications` was previously listed as a gap and a future candidate. It is now a source, so both references must be removed.

**Files:**
- Modify: `README.md`

### Step 4: Remove the "Development applications" bullet from the Gaps section

In `## Gaps and what's not here`, find and remove the following bullet (including its continuation lines):

```markdown
- **Development applications** — rezoning (OPA/ZBA), site plan approval, plan of
  subdivision, and condominium applications are in a separate CKAN dataset
  (`development-applications`). This is the main dataset used by development
  trackers like UrbanToronto.
```

Delete those three lines entirely. The preceding and following bullets remain unchanged.

### Step 5: Remove the `development-applications` row from the "Potentially relevant" table

In `## Potentially relevant additional datasets`, find and remove this table row:

```markdown
| `development-applications` | Rezoning, site plan, condos, subdivisions — the main planning pipeline |
```

Delete that line entirely. The table will have one fewer row.

---

## Task 5: Verify and commit

### Step 6: Review the README

Read through `README.md` to confirm:
- The sources table has 4 rows and no stale `(2020+)` in the header
- The year-filter paragraph correctly distinguishes 2020+ sources from `dev_applications`
- The new `### Development Applications` section appears before `## Gaps`
- The Gaps section no longer mentions `development-applications`
- The "Potentially relevant" table no longer contains `development-applications`

No automated test is needed for README changes.

### Step 7: Commit

```bash
git add README.md
git commit -m "docs: add Development Applications section to README

Documents the dev_applications source: application type codes (OZ/SA/CD/SB/PL),
all 24 columns, and known limitations (retired dataset, one-row-per-address).
Removes stale references from Gaps and Potentially Relevant sections.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

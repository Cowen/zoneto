# zoneto

Toronto building application data pipeline. Fetches building permit and Committee
of Adjustment data from the City of Toronto Open Data API and stores it locally as
Hive-partitioned Parquet files.

## Quickstart

```bash
uv sync
uv run zoneto sync        # fetch all sources
uv run zoneto status      # print row counts and last sync time
```

## Dev tasks

```bash
just test    # run pytest
just lint    # ruff + ty
just fmt     # ruff format
```

---

## Data sources

### The City of Toronto Open Data portal

All data comes from the City of Toronto's CKAN open data portal
(`https://ckan0.cf.opendata.inter.prod-toronto.ca`). The city publishes datasets
in two modes:

- **CKAN DataStore** — data is loaded into a queryable database, accessible via a
  paginated API (`/api/3/action/datastore_search`). Supports filtering, field
  selection, and is kept up to date automatically. This is the preferred access
  method for active datasets.
- **Bulk file download** — static CSV/XML/JSON exports uploaded directly to CKAN.
  The entire file must be downloaded; no server-side filtering. Typically used for
  historical or archival data that doesn't change.

### Sources in this pipeline

| Key | Dataset | CKAN mode | Rows | Updated |
|-----|---------|-----------|------|---------|
| `permits_active` | Building Permits — Active | DataStore | ~98 k (2020+) | Daily |
| `permits_cleared` | Building Permits — Cleared | DataStore | ~161 k (2020+) | Daily |
| `coa` | Committee of Adjustment Applications | Bulk CSV | ~5 k (2020+) | Periodic |
| `dev_applications` | Development Applications | DataStore | ~26 k (all years) | Retired |

Building permit sources (`permits_active`, `permits_cleared`) and `coa` are fetched
from 2020 onwards (`year_start=2020`). `dev_applications` uses `year_start=2000` to
capture the full archive (earliest records are from 2008). In all cases, records with
unparseable dates land in `year=0`.

---

### Building Permits — Active (`permits_active`)

**CKAN dataset:** `building-permits-active-permits`
**What it is:** Every open building permit application in Toronto that has been
submitted but not yet cleared (closed). Covers work from minor plumbing fixes to
new high-rise towers. A permit is required under the *Building Code Act* for any
construction, demolition, or change of use.

**Key fields (34 columns):**

| Column | Description |
|--------|-------------|
| `permit_num` | Permit number |
| `permit_type` | E.g. "Small Residential Projects", "New Houses", "Plumbing(PS)" |
| `structure_type` | Building type |
| `work` | Description of work |
| `application_date` / `issued_date` / `completed_date` | Lifecycle dates |
| `status` | Current status |
| `street_num`, `street_name`, … `postal` | Address |
| `geo_id`, `ward_grid` | City geographic identifiers |
| `est_const_cost` | Estimated construction cost |
| `dwelling_units_created` / `dwelling_units_lost` | Housing impact |
| `residential`, `assembly`, `institutional`, … | Binary use-type flags |
| `builder_name` | Builder on record |
| `current_use` / `proposed_use` | Use classifications |

**What is not in it:**

- No contractor licence numbers or contact details beyond `builder_name`
- No detailed inspection records or inspection results
- No permit fees
- No floor area or gross floor area (GFA) figures
- No occupant load or building height data
- Permits older than 10 years roll off the active dataset — they either cleared
  or were cancelled

**Known behaviour:** A permit can remain "active" indefinitely if the owner never
calls for a final inspection. The active dataset therefore includes some long-dormant
permits alongside genuinely in-progress work.

---

### Building Permits — Cleared (`permits_cleared`)

**CKAN dataset:** `building-permits-cleared-permits`
**What it is:** Building permits that have completed the full inspection cycle and
been formally closed. "Cleared" means the work passed all required inspections and
the applicant confirmed completion with the city. Coverage begins 2017.

**Schema:** Identical to `permits_active` (same 34 columns). The two datasets
together give a complete picture of construction activity in Toronto: active = in
progress, cleared = finished.

**What is not in it:**

- No reason for the gap between issuance date and clearance date
- Pre-2017 cleared permits are in legacy CSV exports on CKAN but are not fetched
  by this pipeline
- Applications that were cancelled rather than cleared do not appear here

---

### Committee of Adjustment Applications (`coa`)

**CKAN dataset:** `committee-of-adjustment-applications`
**What it is:** Applications heard by Toronto's Committee of Adjustment (CoA), an
independent quasi-judicial body that decides on minor deviations from the Zoning
By-law and lot severance requests. It operates under the *Planning Act* through
four geographic panels: Etobicoke York, North York, Toronto & East York, and
Scarborough.

**Application types heard:**

- **Minor variances** — permission to vary (deviate from) a specific zoning
  requirement, e.g. reduced setback, increased lot coverage, or additional height
- **Consents** — severances and other adjustments to lot boundaries
- **Legal non-conforming uses** — extensions or changes to uses that predate
  current zoning

**Key fields (33 columns):**

| Column | Description |
|--------|-------------|
| `application_type`, `sub_type` | Application category |
| `in_date` | Application intake date |
| `finaldate` | Date of final decision |
| `hearing_date` | Scheduled hearing date |
| `c_of_a_descision` | Committee decision |
| `appeal_expiry_date`, `omb_order_date`, `omb_descision` | Appeal process |
| `condition_expiry_date` | Expiry of any attached conditions |
| `reference_file` | Cross-reference to the associated planning file |
| `zoning_designation`, `zoning_review` | Zoning context |
| `planning_district`, `ward`, `community` | Geographic identifiers |
| `statusdesc` | Current application status |

**Known limitations:**

- **`year=0` for all rows.** The date normalization step derives `year` from a
  column named `application_date`, but this dataset uses `in_date` instead. All
  records therefore land in the `year=0` partition. The actual intake dates are
  stored in `in_date` (correctly parsed as `pl.Date`) and can be used directly.
- **Only closed applications from 2022–2023 are fetched.** The pipeline
  identifies CSV resources by year in their filename. The "Closed Applications
  2022" and "Closed Applications 2023" CSVs are the only ones fetched. Closed
  applications from other years and **all active applications** (whose CKAN
  resource is named "Active Applications", with no year) are not included.
- **`in_date` values can predate 2020.** An application filed in 2016 that was
  only formally closed in 2022 appears in the 2022 CSV with its original 2016
  intake date.
- **No full decision text or conditions.** The dataset records the decision
  outcome but not the detailed conditions attached to an approval.
- **No TLAB/OPA appeal outcomes.** After a CoA decision, applicants can appeal
  to the Toronto Local Appeal Body (TLAB, previously the OMB). The `omb_descision`
  field exists but is often blank.

---

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

## Model Performance

Four outcome-prediction models are trained from the enriched parquet data. All
metrics below are 5-fold cross-validated (not training-set scores).

| Model | Target | N | Key metric | Value |
|---|---|---|---|---|
| `dev_applications_approved` | Approval (0/1) | 20,690 | ROC-AUC | **0.970** ±0.033 |
| `dev_applications_no_appeal` | OMB/TLAB appeal filed (0/1) | 21,800 | ROC-AUC | **0.957** ±0.003 |
| `coa_approved` | CoA approval (0/1) | 4,630 | ROC-AUC | **0.695** ±0.041 |
| `coa_days_to_approval` | Days to CoA decision (regression) | 4,350 | R² / MAE | **0.786** / 54.6 days |

`dev_applications_no_appeal` is the strongest model — AUC 0.957 reliably
identifies applications at high risk of attracting an OMB/TLAB appeal.
`coa_approved` is the weakest (AUC 0.695); the available features do not
capture what CoA panels actually weigh (site-specific facts, neighbour impact,
and planning policy context).

See [`docs/model-metrics.md`](docs/model-metrics.md) for full dated metric
snapshots with per-fold standard deviations and methodology notes.

---

## Gaps and what's not here

**Not captured by any current source:**

- **Property assessments** — MPAC (Municipal Property Assessment Corporation)
  assessed values and property attributes. MPAC data is not open by default;
  partial data surfaces via the City's property tax dataset.
- **Heritage permits** — alterations to designated heritage properties require
  separate permits not reflected here.
- **Inspection results** — individual inspection visits and results are not
  published as open data.
- **Permit fees collected** — financial data is not in the permit datasets.
- **Construction timelines** — the time between application, issuance, and
  clearance can be computed from the date fields but is not a supplied column.

**Partial coverage:**

- **CoA active applications** are not fetched (see above).
- **Pre-2020 cleared permits** exist in the CKAN bulk CSVs (back to 2017) but are
  excluded by the `year_start=2020` filter.

---

## Potentially relevant additional datasets

All available on `https://open.toronto.ca`:

| Dataset | Notes |
|---------|-------|
| `zoning-by-law` | Current Zoning By-law 569-2013 with amendments; spatial + text |
| `building-permits-green-roofs` | Eco-Roof Incentive Program permit data |
| `registered-condominium-plans` | Registered condominiums with unit counts |
| `apartment-building-registration` | Multi-res buildings registered under RentSafeTO |
| `short-term-rentals-registration` | Airbnb-style registrations and complaints |

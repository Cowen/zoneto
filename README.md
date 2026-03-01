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

| Key | Dataset | CKAN mode | Rows (2020+) | Updated |
|-----|---------|-----------|--------------|---------|
| `permits_active` | Building Permits — Active | DataStore | ~98 k | Daily |
| `permits_cleared` | Building Permits — Cleared | DataStore | ~161 k | Daily |
| `coa` | Committee of Adjustment Applications | Bulk CSV | ~5 k | Periodic |

All sources are fetched with `year_start=2020` (rows with `year < 2020` are
discarded after fetch, except for records with unparseable dates which land in
`year=0`).

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

## Gaps and what's not here

**Not captured by any current source:**

- **Development applications** — rezoning (OPA/ZBA), site plan approval, plan of
  subdivision, and condominium applications are in a separate CKAN dataset
  (`development-applications`). This is the main dataset used by development
  trackers like UrbanToronto.
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
| `development-applications` | Rezoning, site plan, condos, subdivisions — the main planning pipeline |
| `zoning-by-law` | Current Zoning By-law 569-2013 with amendments; spatial + text |
| `building-permits-green-roofs` | Eco-Roof Incentive Program permit data |
| `registered-condominium-plans` | Registered condominiums with unit counts |
| `apartment-building-registration` | Multi-res buildings registered under RentSafeTO |
| `short-term-rentals-registration` | Airbnb-style registrations and complaints |

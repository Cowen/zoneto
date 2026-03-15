"""Canonical feature column lists for analytics models."""

DEV_CAT_COLS: list[str] = [
    "application_type",
    "ward_number",
    "zoning_class",
    "secondary_plan_name",
    "postal_fsa",
]

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
]

COA_CAT_COLS: list[str] = [
    "application_type",
    "sub_type",
    "ward_number",
    "zoning_designation",
    "planning_district",
    "work_type",
]

COA_NUM_COLS: list[str] = [
    "year_submitted",
    "ward_pct_renters",
    "ward_median_income",
    "ward_pop_density",
    "ward_pct_detached",
]

PERMIT_CAT_COLS: list[str] = [
    "permit_type",
    "structure_type",
    "ward_grid",
]

PERMIT_NUM_COLS: list[str] = [
    "est_const_cost",
    "dwelling_units_created",
    "dwelling_units_lost",
    "residential",
    "mercantile",
    "industrial",
    "institutional",
    "application_year",
]

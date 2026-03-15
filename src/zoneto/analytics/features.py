"""Canonical feature column lists for analytics models."""

DEV_CAT_COLS: list[str] = [
    "application_type",
    "ward_number",
    "zoning_class",
    "secondary_plan_name",
]

DEV_NUM_COLS: list[str] = [
    "year_submitted",
    "in_heritage_register",
    "in_heritage_district",
    "in_secondary_plan",
    "has_community_meeting",
]

COA_CAT_COLS: list[str] = [
    "application_type",
    "sub_type",
    "ward_number",
    "zoning_designation",
    "planning_district",
    "work_type",
]

COA_NUM_COLS: list[str] = ["year_submitted"]

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
    "commercial",
    "industrial",
    "institutional",
    "application_year",
]

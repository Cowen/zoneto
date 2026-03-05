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
]

COA_NUM_COLS: list[str] = ["year_submitted"]

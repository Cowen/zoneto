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
    "ward_pct_renters",
    "ward_median_income",
    "ward_pop_density",
    "ward_pct_detached",
    "has_parent_application",
    "is_combined_application",
    "proposed_storeys",
    "proposed_units",
    "unit_excess_ratio",
    "storey_excess_ratio",
    "ward_appeal_rate_3y",
    "in_mtsa",
    "in_trca_regulated_area",
    "in_greenbelt",
    *[f"desc_svd_{i}" for i in range(20)],
]

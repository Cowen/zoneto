from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
    PERMIT_CAT_COLS,
    PERMIT_NUM_COLS,
)


def test_dev_cat_cols() -> None:
    assert DEV_CAT_COLS == [
        "application_type",
        "ward_number",
        "zoning_class",
        "secondary_plan_name",
    ]


def test_dev_num_cols() -> None:
    # is_tlab_era removed: redundant with year_submitted (year >= 2017)
    assert DEV_NUM_COLS == [
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


def test_coa_cat_cols() -> None:
    assert COA_CAT_COLS == [
        "application_type",
        "sub_type",
        "ward_number",
        "zoning_designation",
        "planning_district",
        "work_type",
    ]


def test_coa_num_cols() -> None:
    assert COA_NUM_COLS == [
        "year_submitted",
    ]


def test_permit_cat_cols() -> None:
    assert PERMIT_CAT_COLS == [
        "permit_type",
        "structure_type",
        "ward_grid",
    ]


def test_permit_num_cols() -> None:
    assert PERMIT_NUM_COLS == [
        "est_const_cost",
        "dwelling_units_created",
        "dwelling_units_lost",
        "residential",
        "mercantile",
        "industrial",
        "institutional",
    ]

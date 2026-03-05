from zoneto.analytics.features import (
    COA_CAT_COLS,
    COA_NUM_COLS,
    DEV_CAT_COLS,
    DEV_NUM_COLS,
)


def test_dev_cat_cols() -> None:
    assert DEV_CAT_COLS == [
        "application_type",
        "ward_number",
        "zoning_class",
        "secondary_plan_name",
    ]


def test_dev_num_cols() -> None:
    assert DEV_NUM_COLS == [
        "year_submitted",
        "in_heritage_register",
        "in_heritage_district",
        "in_secondary_plan",
        "has_community_meeting",
    ]


def test_coa_cat_cols() -> None:
    assert COA_CAT_COLS == [
        "application_type",
        "sub_type",
        "ward_number",
        "zoning_designation",
    ]


def test_coa_num_cols() -> None:
    assert COA_NUM_COLS == ["year_submitted"]

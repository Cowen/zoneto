from zoneto.analytics.features import DEV_CAT_COLS, DEV_NUM_COLS


def test_dev_cat_cols() -> None:
    assert DEV_CAT_COLS == [
        "application_type",
        "ward_number",
        "zoning_class",
        "secondary_plan_name",
    ]


def test_dev_num_cols() -> None:
    # is_tlab_era removed: redundant with year_submitted (year >= 2017)
    assert len(DEV_NUM_COLS) == 39  # 19 base + 20 SVD
    assert DEV_NUM_COLS[:19] == [
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
    ]
    assert "storey_excess_ratio" in DEV_NUM_COLS
    assert "desc_svd_0" in DEV_NUM_COLS
    assert "desc_svd_19" in DEV_NUM_COLS


def test_dev_num_cols_includes_is_combined_application() -> None:
    assert "is_combined_application" in DEV_NUM_COLS

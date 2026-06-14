"""Tests for the deterministic Planning Act reference layer."""

from __future__ import annotations

import pytest

from zoneto.analytics.compliance import Severity, Violation, check_compliance
from zoneto.analytics.extract import ProjectFeatures, extract_project_features
from zoneto.analytics.planning_act import (
    ADDITIONAL_PROCESS,
    APPLICATION_TYPE_PROCESS,
    MINOR_VARIANCE_TESTS,
    PROCESS_BY_PATH,
    additional_processes,
    format_statutory_context,
    path_for_violations,
    statutory_processes,
    statutory_timeline_days,
)


def _violation(severity: Severity, rule_id: str = "x") -> Violation:
    return Violation(
        rule_id=rule_id,
        section_ref="ref",
        observed="o",
        allowed="a",
        severity=severity,
        suggested_remedy="r",
    )


# ---------------------------------------------------------------------------
# path_for_violations — precedence mirrors _apply_confidence_overrides ordering
# ---------------------------------------------------------------------------


def test_path_empty_is_as_of_right() -> None:
    assert path_for_violations([]) == "as_of_right"


def test_path_only_informational_is_as_of_right() -> None:
    assert path_for_violations([_violation(Severity.INFORMATIONAL)]) == "as_of_right"


def test_path_variance() -> None:
    vios = [_violation(Severity.NEEDS_VARIANCE)]
    assert path_for_violations(vios) == "minor_variance"


def test_path_rezoning_beats_variance() -> None:
    vios = [
        _violation(Severity.NEEDS_VARIANCE),
        _violation(Severity.NEEDS_REZONING),
    ]
    assert path_for_violations(vios) == "rezoning"


def test_path_prohibited_beats_everything() -> None:
    vios = [
        _violation(Severity.NEEDS_REZONING),
        _violation(Severity.NEEDS_REZONING, rule_id="prohibited_use"),
    ]
    assert path_for_violations(vios) == "prohibited"


# ---------------------------------------------------------------------------
# statutory_timeline_days — every type resolves; CD/SB get a defensible baseline
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "app_type,expected",
    [
        ("OZ", 120),  # unknown is_combined → conservative 120
        ("SB", 120),
        ("CD", 120),
        ("SA", None),  # site plan: no non-decision appeal clock (post-Bill 185)
        ("PL", None),  # part lot control: not appealable to the OLT
        ("MV", None),  # minor variance: COA must hear, no non-decision clock
        ("CO", None),  # consent to sever: COA, no non-decision clock
        ("oz", 120),  # case-insensitive
        (" OZ ", 120),  # whitespace-tolerant
        ("TLAB", None),  # appeal body, intentionally unmapped
        ("ZZ", None),  # unknown type
        (None, None),
        ("", None),
    ],
)
def test_statutory_timeline_days(app_type: str | None, expected: int | None) -> None:
    assert statutory_timeline_days(app_type) == expected


@pytest.mark.parametrize(
    "is_combined,expected",
    [
        (True, 120),  # OZ combined with an OPA
        (False, 90),  # standalone ZBA
        (None, 120),  # unknown → conservative
    ],
)
def test_oz_split_90_120(is_combined: bool | None, expected: int) -> None:
    assert statutory_timeline_days("OZ", is_combined=is_combined) == expected


def test_mv_and_co_map_to_committee_of_adjustment() -> None:
    assert APPLICATION_TYPE_PROCESS["MV"].decider == "Committee of Adjustment"
    assert APPLICATION_TYPE_PROCESS["CO"].decider == "Committee of Adjustment"
    assert "s.53" in APPLICATION_TYPE_PROCESS["CO"].act_section


def test_cd_sb_are_non_null_unlike_survival_model() -> None:
    # The OZ/SA-only survival model leaves CD/SB/PL null; the statutory anchor
    # gives the appeal-bearing ones (CD, SB) a deterministic floor instead.
    assert statutory_timeline_days("CD") is not None
    assert statutory_timeline_days("SB") is not None


# ---------------------------------------------------------------------------
# Static tables
# ---------------------------------------------------------------------------


def test_minor_variance_has_four_tests() -> None:
    assert len(MINOR_VARIANCE_TESTS) == 4
    assert all(isinstance(t, str) and t for t in MINOR_VARIANCE_TESTS)


def test_every_application_type_maps_to_a_process() -> None:
    for code in ("OZ", "SA", "SB", "CD", "PL"):
        assert code in APPLICATION_TYPE_PROCESS


def test_rezoning_third_party_appeal_removed_post_bill_23() -> None:
    assert PROCESS_BY_PATH["rezoning"].third_party_appeal is False


# ---------------------------------------------------------------------------
# format_statutory_context
# ---------------------------------------------------------------------------


def test_format_unknown_path_is_empty() -> None:
    assert format_statutory_context("not_a_path") == ""


def test_format_rezoning_mentions_section_and_olt() -> None:
    text = format_statutory_context("rezoning")
    assert "s.34" in text
    assert "Ontario Land Tribunal" in text
    assert "120 days" in text


def test_format_rezoning_standalone_shows_90_days() -> None:
    text = format_statutory_context("rezoning", is_combined=False)
    assert "90 days" in text
    assert "~120 days" not in text


def test_format_rezoning_appeal_rate_note_only_for_rezoning() -> None:
    with_note = format_statutory_context("rezoning", comparable_appeal_rate=0.30)
    assert "30%" in with_note and "Bill 23" in with_note
    # Appeal-rate note is suppressed for non-rezoning paths.
    variance = format_statutory_context("minor_variance", comparable_appeal_rate=0.30)
    assert "30%" not in variance


def test_format_as_of_right_has_no_appeal_route() -> None:
    text = format_statutory_context("as_of_right")
    assert "none under the Planning Act" in text


# ---------------------------------------------------------------------------
# Integration with compliance — the s.45 framing fix
# ---------------------------------------------------------------------------


def test_variance_remedy_cites_s45_not_ten_percent() -> None:
    extracted = ProjectFeatures(
        proposed_storeys=5,  # 2 over a 3-storey limit -> NEEDS_VARIANCE
        proposed_units=None,
        proposed_use=None,
        has_ground_floor_retail=False,
    )
    vios = check_compliance(extracted, {"zoning_max_storeys": 3})
    remedy = vios[0].suggested_remedy
    assert "s.45(1)" in remedy
    assert "typically considered minor" not in remedy
    assert "Planning Act s.45 (minor variance)" in vios[0].section_ref


def test_rezoning_violation_section_ref_cites_s34() -> None:
    extracted = extract_project_features("a 40-storey tower")
    vios = check_compliance(extracted, {"zoning_max_storeys": 3})
    assert any("Planning Act s.34" in v.section_ref for v in vios)


# ---------------------------------------------------------------------------
# Multi-process trigger set (item 4a)
# ---------------------------------------------------------------------------


def test_single_house_triggers_no_additional_processes() -> None:
    ex = extract_project_features("a single detached dwelling")
    assert additional_processes(ex) == []


def test_apartment_triggers_site_plan() -> None:
    ex = extract_project_features("a 40-storey apartment with 400 units")
    assert "site_plan" in additional_processes(ex)


@pytest.mark.parametrize(
    "text,expected_key",
    [
        ("Draft plan of subdivision creating 30 lots", "subdivision"),
        ("a residential condominium", "condominium"),
        ("application for consent to sever the lot", "consent"),
        ("part lot control exemption to convey townhouses", "part_lot_control"),
        ("demolish 12 existing rental units and rebuild", "rental_replacement"),
    ],
)
def test_keyword_triggers(text: str, expected_key: str) -> None:
    assert expected_key in additional_processes(extract_project_features(text))


def test_every_additional_key_resolves_to_a_process() -> None:
    # Every key additional_processes can emit must be in ADDITIONAL_PROCESS.
    for key in (
        "site_plan",
        "subdivision",
        "condominium",
        "consent",
        "part_lot_control",
        "rental_replacement",
    ):
        assert key in ADDITIONAL_PROCESS


def test_statutory_processes_primary_first_then_orthogonal() -> None:
    ex = extract_project_features(
        "Demolish rental apartments; build a 40-storey condominium, 400 units"
    )
    vios = check_compliance(ex, {"zoning_max_storeys": 3})
    pairs = statutory_processes(vios, ex)
    keys = [k for k, _ in pairs]
    assert keys[0] == "rezoning"  # primary zoning path first
    assert "site_plan" in keys and "condominium" in keys
    assert "rental_replacement" in keys
    # Deduplicated by process_label.
    labels = [p.process_label for _, p in pairs]
    assert len(labels) == len(set(labels))

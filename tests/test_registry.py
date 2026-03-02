from __future__ import annotations

from zoneto.sources.base import Source
from zoneto.sources.ckan import CKANSource
from zoneto.sources.registry import SOURCES


def test_all_sources_present() -> None:
    assert set(SOURCES.keys()) == {
        "permits_active",
        "permits_cleared",
        "coa",
        "dev_applications",
    }


def test_source_names_match_dataset_ids() -> None:
    assert SOURCES["permits_active"].name == "building-permits-active-permits"
    assert SOURCES["permits_cleared"].name == "building-permits-cleared-permits"
    assert SOURCES["coa"].name == "committee-of-adjustment-applications"
    assert SOURCES["dev_applications"].name == "development-applications"


def test_all_sources_satisfy_protocol() -> None:
    """Each value is a runtime-checkable Source (has name attr and fetch method)."""
    for key, source in SOURCES.items():
        assert isinstance(source, Source), f"{key}: does not satisfy Source protocol"


def test_all_sources_have_callable_fetch() -> None:
    for key, source in SOURCES.items():
        assert callable(source.fetch), f"{key}: fetch is not callable"


def test_dev_applications_config() -> None:
    """dev_applications uses date_submitted as year column, fetches all years."""
    source = SOURCES["dev_applications"]
    assert isinstance(source, CKANSource)
    cfg = source.config
    assert cfg.dataset_id == "development-applications"
    assert cfg.access_mode == "datastore"
    assert cfg.year_start == 2000
    assert cfg.year_column == "date_submitted"

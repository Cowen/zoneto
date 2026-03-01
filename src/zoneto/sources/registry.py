from __future__ import annotations

from zoneto.models import CKANConfig
from zoneto.sources.base import Source
from zoneto.sources.ckan import CKANSource

SOURCES: dict[str, Source] = {
    "permits_active": CKANSource(CKANConfig(
        dataset_id="building-permits-active-permits",
        access_mode="datastore",
    )),
    "permits_cleared": CKANSource(CKANConfig(
        dataset_id="building-permits-cleared-permits",
        access_mode="bulk_csv",
    )),
    "coa": CKANSource(CKANConfig(
        dataset_id="committee-of-adjustment-applications",
        access_mode="bulk_csv",
    )),
}

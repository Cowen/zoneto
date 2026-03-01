from __future__ import annotations

from typing import Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class Source(Protocol):
    """Protocol for all data sources.

    Any class with a `name` str attribute and a `fetch()` method that
    returns a polars DataFrame satisfies this protocol.
    """

    name: str

    def fetch(self) -> pl.DataFrame:
        """Fetch all records and return as a normalized DataFrame."""
        ...

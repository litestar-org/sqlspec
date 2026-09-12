"""Tests for names promoted to the public package surfaces."""

import sqlspec
from sqlspec.utils import uuids


def test_top_level_uuid_exports() -> None:
    """The top-level package re-exports the identifier generators from ``sqlspec.utils.uuids``."""
    assert sqlspec.uuid4 is uuids.uuid4
    assert sqlspec.uuid6 is uuids.uuid6
    assert sqlspec.uuid7 is uuids.uuid7
    assert sqlspec.nanoid is uuids.nanoid


def test_top_level_exports_resolve() -> None:
    """Every name in ``sqlspec.__all__`` resolves, including lazily loaded names."""
    for name in sqlspec.__all__:
        getattr(sqlspec, name)

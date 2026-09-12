"""Spanner savepoint capability reporting."""

from typing import Any, cast

import pytest

from sqlspec.adapters.spanner.driver import SpannerSyncDriver


@pytest.mark.parametrize("method", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_savepoints_are_reported_unsupported(method: str) -> None:
    driver = SpannerSyncDriver(cast("Any", object()))

    with pytest.raises(NotImplementedError, match="Spanner"):
        getattr(driver, method)("sqlspec_sp_1")

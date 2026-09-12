"""Spanner savepoint capability reporting."""

from typing import Any, cast

import pytest

from sqlspec.adapters.spanner.driver import SpannerSyncDriver
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.service import SQLSpecSyncService


@pytest.mark.parametrize("method", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_savepoints_are_reported_unsupported(method: str) -> None:
    driver = SpannerSyncDriver(cast("Any", object()))

    with pytest.raises(NotImplementedError, match="Spanner"):
        getattr(driver, method)("sqlspec_sp_1")


def test_nested_service_block_reports_missing_savepoints() -> None:
    driver = SpannerSyncDriver(cast("Any", object()))
    service = SQLSpecSyncService(driver)

    with service.begin_transaction() as session:
        with pytest.raises(ImproperConfigurationError, match="savepoints") as raised:
            with service.begin_transaction():
                pytest.fail("nested block entered")
        assert isinstance(raised.value.__cause__, NotImplementedError)
        with pytest.raises(ImproperConfigurationError, match="savepoints"):
            with session.transaction():
                pytest.fail("nested block entered")
    assert driver._transaction_depth == 0

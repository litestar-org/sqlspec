"""BigQuery savepoint capability reporting."""

from typing import Any, cast

import pytest

from sqlspec.adapters.bigquery.driver import BigQueryDriver


@pytest.mark.parametrize("method", ["create_savepoint", "release_savepoint", "rollback_to_savepoint"])
def test_savepoints_are_reported_unsupported(method: str) -> None:
    driver = BigQueryDriver(cast("Any", object()))

    with pytest.raises(NotImplementedError, match="BigQuery"):
        getattr(driver, method)("sqlspec_sp_1")

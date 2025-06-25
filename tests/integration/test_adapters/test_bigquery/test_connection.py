from __future__ import annotations

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.statement.result import SQLResult


@pytest.mark.xdist_group("bigquery")
def test_connection(bigquery_session: BigQueryConfig) -> None:
    """Test database connection."""

    with bigquery_session.provide_session() as driver:
        result = driver.execute("SELECT 1 as one")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data == [{"one": 1}]

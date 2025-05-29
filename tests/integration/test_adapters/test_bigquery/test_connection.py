from __future__ import annotations

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.sql.result import SelectResult


@pytest.mark.xdist_group("bigquery")
def test_connection(bigquery_session: BigQueryConfig) -> None:
    """Test database connection."""

    with bigquery_session.provide_session() as driver:
        result = driver.execute("SELECT 1 as one")
        assert isinstance(result, SelectResult)
        assert result.rows is not None
        assert result.rows == [{"one": 1}]

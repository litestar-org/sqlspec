"""BigQuery connection tests."""

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig
from sqlspec.core.result import SQLResult

pytestmark = pytest.mark.xdist_group("bigquery")


def test_connection(bigquery_config: BigQueryConfig) -> None:
    """Test database connection."""

    with bigquery_config.provide_session() as driver:
        result = driver.execute("SELECT 1 as one")
        assert isinstance(result, SQLResult)
        assert result.data is not None
        assert result.data == [{"one": 1}]

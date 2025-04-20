from __future__ import annotations

import pytest

from sqlspec.adapters.bigquery import BigQueryConfig


@pytest.mark.xdist_group("bigquery")
def test_connection(bigquery_session: BigQueryConfig) -> None:
    """Test database connection."""

    with bigquery_session.provide_session() as driver:
        output = driver.select("SELECT 1 as one")
        assert output == [{"one": 1}]

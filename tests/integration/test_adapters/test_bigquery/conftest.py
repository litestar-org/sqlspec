from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials

from sqlspec.adapters.bigquery.config import BigQueryConfig

if TYPE_CHECKING:
    from pytest_databases.docker.bigquery import BigQueryService


@pytest.fixture
def table_schema_prefix(bigquery_service: BigQueryService) -> str:
    """Create a table schema prefix."""
    return f"`{bigquery_service.project}`.`{bigquery_service.dataset}`"


@pytest.fixture
def bigquery_session(bigquery_service: BigQueryService, table_schema_prefix: str) -> BigQueryConfig:
    """Create a BigQuery sync config session."""
    return BigQueryConfig(
        project=bigquery_service.project,
        dataset_id=table_schema_prefix,
        client_options=ClientOptions(api_endpoint=f"http://{bigquery_service.host}:{bigquery_service.port}"),
        credentials=AnonymousCredentials(),  # type: ignore[no-untyped-call]
    )

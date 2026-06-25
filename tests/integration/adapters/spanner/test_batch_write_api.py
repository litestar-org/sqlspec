"""Integration coverage for the Spanner Batch Write API ingest transport."""

from collections.abc import Generator
from uuid import uuid4

import pyarrow as pa
import pytest
from google.api_core import exceptions as gcore_exceptions
from pytest_databases.docker.spanner import SpannerService

from sqlspec.adapters.spanner import SpannerSyncConfig

pytestmark = pytest.mark.spanner


@pytest.fixture
def spanner_batch_write_config(spanner_service: "SpannerService") -> "Generator[SpannerSyncConfig, None, None]":
    api_endpoint = f"{spanner_service.host}:{spanner_service.port}"
    config = SpannerSyncConfig(
        connection_config={
            "project": spanner_service.project,
            "instance_id": spanner_service.instance_name,
            "database_id": spanner_service.database_name,
            "credentials": spanner_service.credentials,
            "client_options": {"api_endpoint": api_endpoint},
            "size": 5,
        },
        driver_features={"enable_batch_write_api": True},
    )
    try:
        yield config
    finally:
        config.close_pool()


def test_batch_write_ingest(spanner_batch_write_config: SpannerSyncConfig, test_users_table: str) -> None:
    user_ids = [str(uuid4()) for _ in range(8)]
    arrow_table = pa.table({
        "id": user_ids,
        "name": [f"BW {i}" for i in range(8)],
        "email": [f"bw{i}@example.com" for i in range(8)],
        "age": [20 + i for i in range(8)],
    })

    with spanner_batch_write_config.provide_write_session() as session:
        try:
            job = session.load_from_arrow(test_users_table, arrow_table)
        except gcore_exceptions.MethodNotImplemented:
            pytest.skip("Spanner emulator does not implement the Batch Write API")
        assert job.telemetry["rows_processed"] == 8

    with spanner_batch_write_config.provide_session() as session:
        rows = session.select(f"SELECT id FROM {test_users_table} WHERE id IN UNNEST(@ids)", {"ids": user_ids})
        assert len(rows) == 8

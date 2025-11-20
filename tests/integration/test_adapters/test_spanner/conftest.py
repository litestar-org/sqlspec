import os
from collections.abc import Generator
from typing import Any, cast

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import spanner

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerSyncConfig, SpannerSyncDriver

# Emulator host
EMULATOR_HOST = os.environ.get("SPANNER_EMULATOR_HOST", "localhost:9010")
PROJECT_ID = "test-project"
INSTANCE_ID = "test-instance"
DATABASE_ID = "test-database"


@pytest.fixture(scope="session")
def spanner_client() -> Generator[spanner.Client, None, None]:
    """Create a Spanner client for the emulator."""
    if not os.environ.get("SPANNER_EMULATOR_HOST"):
        pytest.skip("SPANNER_EMULATOR_HOST not set")

    # Use anonymous credentials for emulator
    client = spanner.Client(
        project=PROJECT_ID,
        credentials=cast(Any, AnonymousCredentials()),  # type: ignore[no-untyped-call]
        client_options={"api_endpoint": EMULATOR_HOST},
    )

    # Create instance and database if not exist
    instance = client.instance(INSTANCE_ID)
    if not instance.exists():
        config_name = f"{client.project_name}/instanceConfigs/emulator-config"
        instance = client.instance(INSTANCE_ID, configuration_name=config_name)
        instance.create().result(120)

    database = instance.database(DATABASE_ID)
    if not database.exists():
        database.create().result(120)

    yield client

    # Cleanup
    # database.drop()
    # instance.delete()


@pytest.fixture
def spanner_config(spanner_client: spanner.Client) -> SpannerSyncConfig:
    return SpannerSyncConfig(
        pool_config={
            "project": PROJECT_ID,
            "instance_id": INSTANCE_ID,
            "database_id": DATABASE_ID,
            "credentials": cast(Any, AnonymousCredentials()),  # type: ignore[no-untyped-call]
            "client_options": {"api_endpoint": EMULATOR_HOST},
            "min_sessions": 1,
            "max_sessions": 5,
        }
    )


@pytest.fixture
def spanner_session(spanner_config: SpannerSyncConfig) -> Generator[SpannerSyncDriver, None, None]:
    sql = SQLSpec()
    sql.add_config(spanner_config)
    with sql.provide_session(spanner_config) as session:
        yield session

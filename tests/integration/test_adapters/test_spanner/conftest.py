import os
from typing import Generator

import pytest
from google.cloud import spanner
from google.auth.credentials import AnonymousCredentials

from sqlspec import SQLSpec
from sqlspec.adapters.spanner import SpannerConfig, SpannerDriver

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
        credentials=AnonymousCredentials(),
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
def spanner_config(spanner_client: spanner.Client) -> SpannerConfig:
    return SpannerConfig(
        project=PROJECT_ID,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": EMULATOR_HOST},
        pool_config={"min_sessions": 1, "max_sessions": 5},
    )


@pytest.fixture
def spanner_session(spanner_config: SpannerConfig) -> Generator[SpannerDriver, None, None]:
    sql = SQLSpec()
    sql.add_config(spanner_config)
    with sql.provide_session(spanner_config) as session:
        yield session

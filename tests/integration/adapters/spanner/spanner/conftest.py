from collections.abc import Generator
from typing import TYPE_CHECKING, cast

import pytest

from tests.integration.adapters.spanner.spanner._modes import SpannerModeConfig
from tests.integration.fixtures.spanner import drop_table_if_exists, run_ddl

if TYPE_CHECKING:
    from google.cloud.spanner_v1.database import Database


pytestmark = pytest.mark.xdist_group("spanner")


@pytest.fixture(params=("sync", "async"))
def spanner_mode_config(request: pytest.FixtureRequest) -> SpannerModeConfig:
    """Provide the sync or async Spanner configuration for mode-parametrized tests."""
    fixture_name = "spanner_config" if request.param == "sync" else "spanner_async_config"
    return cast("SpannerModeConfig", request.getfixturevalue(fixture_name))


@pytest.fixture
def test_users_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test_users table for CRUD tests."""
    table_name = "test_users"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id STRING(36) NOT NULL,
        name STRING(100),
        email STRING(255),
        age INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)


@pytest.fixture
def test_arrow_table(spanner_database: "Database") -> Generator[str, None, None]:
    """Create test table for Arrow tests."""
    table_name = "test_arrow_data"
    drop_table_if_exists(spanner_database, table_name)

    ddl = f"""
    CREATE TABLE {table_name} (
        id INT64 NOT NULL,
        name STRING(100),
        value INT64
    ) PRIMARY KEY (id)
    """
    run_ddl(spanner_database, [ddl])

    yield table_name

    drop_table_if_exists(spanner_database, table_name)

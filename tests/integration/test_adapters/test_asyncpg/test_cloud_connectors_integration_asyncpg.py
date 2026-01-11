"""Integration tests for Google Cloud SQL and AlloyDB connector support.

These tests require actual Google Cloud credentials and instances.
They are skipped by default unless credentials are available.
"""

import os

import pytest

from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.typing import ALLOYDB_CONNECTOR_INSTALLED, CLOUD_SQL_CONNECTOR_INSTALLED

HAS_CLOUD_SQL_CREDENTIALS = (
    CLOUD_SQL_CONNECTOR_INSTALLED
    and os.environ.get("GOOGLE_CLOUD_SQL_INSTANCE")
    and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)

HAS_ALLOYDB_CREDENTIALS = (
    ALLOYDB_CONNECTOR_INSTALLED
    and os.environ.get("GOOGLE_ALLOYDB_INSTANCE_URI")
    and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)

pytestmark = [
    pytest.mark.skipif(
        not (HAS_CLOUD_SQL_CREDENTIALS or HAS_ALLOYDB_CREDENTIALS), reason="Google Cloud credentials missing"
    ),
    pytest.mark.xdist_group("google_cloud"),
]


@pytest.mark.skipif(not HAS_CLOUD_SQL_CREDENTIALS, reason="Cloud SQL credentials missing")
@pytest.mark.asyncio
async def test_cloud_sql_connection_basic() -> None:
    """Test basic Cloud SQL connection via connector."""
    instance = os.environ["GOOGLE_CLOUD_SQL_INSTANCE"]
    user = os.environ.get("GOOGLE_CLOUD_SQL_USER", "postgres")
    database = os.environ.get("GOOGLE_CLOUD_SQL_DATABASE", "postgres")
    password = os.environ.get("GOOGLE_CLOUD_SQL_PASSWORD")

    config = AsyncpgConfig(
        connection_config={"user": user, "password": password, "database": database, "min_size": 1, "max_size": 2},
        driver_features={"enable_cloud_sql": True, "cloud_sql_instance": instance, "cloud_sql_enable_iam_auth": False},
    )

    await config.create_pool()
    try:
        async with config.provide_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_CLOUD_SQL_CREDENTIALS, reason="Cloud SQL credentials missing")
@pytest.mark.asyncio
async def test_cloud_sql_query_execution() -> None:
    """Test query execution via Cloud SQL connector."""
    instance = os.environ["GOOGLE_CLOUD_SQL_INSTANCE"]
    user = os.environ.get("GOOGLE_CLOUD_SQL_USER", "postgres")
    database = os.environ.get("GOOGLE_CLOUD_SQL_DATABASE", "postgres")
    password = os.environ.get("GOOGLE_CLOUD_SQL_PASSWORD")

    config = AsyncpgConfig(
        connection_config={"user": user, "password": password, "database": database, "min_size": 1, "max_size": 2},
        driver_features={"enable_cloud_sql": True, "cloud_sql_instance": instance, "cloud_sql_enable_iam_auth": False},
    )

    await config.create_pool()
    try:
        async with config.provide_session() as session:
            result = await session.select_one("SELECT 1 as value, 'test' as name")
            assert result["value"] == 1
            assert result["name"] == "test"
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_CLOUD_SQL_CREDENTIALS, reason="Cloud SQL IAM requires credentials")
@pytest.mark.asyncio
async def test_cloud_sql_iam_auth() -> None:
    """Test Cloud SQL with IAM authentication."""
    instance = os.environ["GOOGLE_CLOUD_SQL_INSTANCE"]
    user = os.environ.get("GOOGLE_CLOUD_SQL_IAM_USER", "service-account@project.iam")
    database = os.environ.get("GOOGLE_CLOUD_SQL_DATABASE", "postgres")

    config = AsyncpgConfig(
        connection_config={"user": user, "database": database, "min_size": 1, "max_size": 2},
        driver_features={"enable_cloud_sql": True, "cloud_sql_instance": instance, "cloud_sql_enable_iam_auth": True},
    )

    await config.create_pool()
    try:
        async with config.provide_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_CLOUD_SQL_CREDENTIALS, reason="Cloud SQL credentials missing")
@pytest.mark.asyncio
async def test_cloud_sql_private_ip() -> None:
    """Test Cloud SQL connection using PRIVATE IP type."""
    instance = os.environ["GOOGLE_CLOUD_SQL_INSTANCE"]
    user = os.environ.get("GOOGLE_CLOUD_SQL_USER", "postgres")
    database = os.environ.get("GOOGLE_CLOUD_SQL_DATABASE", "postgres")
    password = os.environ.get("GOOGLE_CLOUD_SQL_PASSWORD")

    config = AsyncpgConfig(
        connection_config={"user": user, "password": password, "database": database, "min_size": 1, "max_size": 2},
        driver_features={
            "enable_cloud_sql": True,
            "cloud_sql_instance": instance,
            "cloud_sql_enable_iam_auth": False,
            "cloud_sql_ip_type": "PRIVATE",
        },
    )

    await config.create_pool()
    try:
        async with config.provide_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_ALLOYDB_CREDENTIALS, reason="AlloyDB credentials missing")
@pytest.mark.asyncio
async def test_alloydb_connection_basic() -> None:
    """Test basic AlloyDB connection via connector."""
    instance_uri = os.environ["GOOGLE_ALLOYDB_INSTANCE_URI"]
    user = os.environ.get("GOOGLE_ALLOYDB_USER", "postgres")
    database = os.environ.get("GOOGLE_ALLOYDB_DATABASE", "postgres")
    password = os.environ.get("GOOGLE_ALLOYDB_PASSWORD")

    config = AsyncpgConfig(
        connection_config={"user": user, "password": password, "database": database, "min_size": 1, "max_size": 2},
        driver_features={
            "enable_alloydb": True,
            "alloydb_instance_uri": instance_uri,
            "alloydb_enable_iam_auth": False,
        },
    )

    await config.create_pool()
    try:
        async with config.provide_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_ALLOYDB_CREDENTIALS, reason="AlloyDB credentials missing")
@pytest.mark.asyncio
async def test_alloydb_query_execution() -> None:
    """Test query execution via AlloyDB connector."""
    instance_uri = os.environ["GOOGLE_ALLOYDB_INSTANCE_URI"]
    user = os.environ.get("GOOGLE_ALLOYDB_USER", "postgres")
    database = os.environ.get("GOOGLE_ALLOYDB_DATABASE", "postgres")
    password = os.environ.get("GOOGLE_ALLOYDB_PASSWORD")

    config = AsyncpgConfig(
        connection_config={"user": user, "password": password, "database": database, "min_size": 1, "max_size": 2},
        driver_features={
            "enable_alloydb": True,
            "alloydb_instance_uri": instance_uri,
            "alloydb_enable_iam_auth": False,
        },
    )

    await config.create_pool()
    try:
        async with config.provide_session() as session:
            result = await session.select_one("SELECT 1 as value, 'test' as name")
            assert result["value"] == 1
            assert result["name"] == "test"
    finally:
        await config.close_pool()


@pytest.mark.skipif(not HAS_ALLOYDB_CREDENTIALS, reason="AlloyDB IAM requires credentials")
@pytest.mark.asyncio
async def test_alloydb_iam_auth() -> None:
    """Test AlloyDB with IAM authentication."""
    instance_uri = os.environ["GOOGLE_ALLOYDB_INSTANCE_URI"]
    user = os.environ.get("GOOGLE_ALLOYDB_IAM_USER", "service-account@project.iam")
    database = os.environ.get("GOOGLE_ALLOYDB_DATABASE", "postgres")

    config = AsyncpgConfig(
        connection_config={"user": user, "database": database, "min_size": 1, "max_size": 2},
        driver_features={"enable_alloydb": True, "alloydb_instance_uri": instance_uri, "alloydb_enable_iam_auth": True},
    )

    await config.create_pool()
    try:
        async with config.provide_connection() as conn:
            result = await conn.fetchval("SELECT 1")
            assert result == 1
    finally:
        await config.close_pool()

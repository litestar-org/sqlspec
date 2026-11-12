"""Native Snowflake fixtures for Embucket and optional live cloud tests."""

import os
from collections.abc import Callable, Generator
from typing import TYPE_CHECKING, Any, cast

import pytest
from pytest_databases.docker.snowflake import SnowflakeService

from sqlspec.adapters.snowflake import SnowflakeConfig
from tests.fixtures.snowflake import _provide_embucket_snowflake_proxy

if TYPE_CHECKING:
    from sqlspec.adapters.snowflake import SnowflakeConnectionPool, SnowflakeDriver

__all__ = (
    "lifecycle_config_snowflake",
    "snowflake_config",
    "snowflake_connection_config",
    "snowflake_driver_config",
    "snowflake_live_config",
    "snowflake_live_connection_config",
    "snowflake_live_session",
    "snowflake_session",
)


@pytest.fixture
def lifecycle_config_snowflake(
    snowflake_connection_config: dict[str, Any],
) -> Generator[Callable[..., SnowflakeConfig], None, None]:
    """Create tracked configs for common connection lifecycle contracts."""
    configs: list[SnowflakeConfig] = []

    def make(
        *,
        pooled: bool = False,
        driver_features: dict[str, Any] | None = None,
        connection_overrides: dict[str, Any] | None = None,
        connection_instance: object | None = None,
    ) -> SnowflakeConfig:
        parameters = {**snowflake_connection_config, **(connection_overrides or {})}
        if pooled:
            parameters.update(pool_min_size=2, pool_max_size=5)
        config = SnowflakeConfig(
            connection_config=parameters,
            driver_features=driver_features,
            connection_instance=cast("SnowflakeConnectionPool | None", connection_instance),
        )
        configs.append(config)
        return config

    try:
        yield make
    finally:
        for config in configs:
            config.close_pool()


@pytest.fixture(scope="session")
def snowflake_connection_config(snowflake_service: SnowflakeService) -> Generator[dict[str, Any], None, None]:
    """Bootstrap Embucket and expose its Snowflake REST endpoint."""
    snowflake_service.query(
        "CREATE EXTERNAL VOLUME IF NOT EXISTS embucket_mem_vol "
        "STORAGE_LOCATIONS = ((NAME = 'mem' STORAGE_PROVIDER = 'MEMORY'))"
    )
    database = '"' + snowflake_service.database.replace('"', '""') + '"'
    schema = '"' + snowflake_service.schema.replace('"', '""') + '"'
    snowflake_service.query(f"CREATE DATABASE IF NOT EXISTS {database} EXTERNAL_VOLUME = 'embucket_mem_vol'")
    snowflake_service.query(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
    with _provide_embucket_snowflake_proxy(snowflake_service.host, snowflake_service.port) as proxy_port:
        yield {
            **snowflake_service.connection_kwargs,
            "host": "127.0.0.1",
            "port": proxy_port,
            "ocsp_fail_open": True,
            "insecure_mode": True,
            "login_timeout": 15,
            "network_timeout": 15,
            "session_parameters": {"PYTHON_CONNECTOR_QUERY_RESULT_FORMAT": "JSON"},
        }


@pytest.fixture(scope="session")
def snowflake_config(snowflake_connection_config: dict[str, Any]) -> Generator[SnowflakeConfig, None, None]:
    """Yield the shared Embucket config and close its pool on teardown."""
    config = SnowflakeConfig(connection_config=dict(snowflake_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def snowflake_driver_config(snowflake_connection_config: dict[str, Any]) -> Generator[SnowflakeConfig, None, None]:
    """Yield an isolated config for extension and pool lifecycle tests."""
    config = SnowflakeConfig(connection_config=dict(snowflake_connection_config))
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def snowflake_session(snowflake_config: SnowflakeConfig) -> Generator["SnowflakeDriver", None, None]:
    """Yield a driver session backed by the shared pool."""
    with snowflake_config.provide_session() as session:
        yield session


@pytest.fixture(scope="session")
def snowflake_live_connection_config() -> dict[str, Any]:
    """Read explicitly enabled live cloud settings or skip before connecting."""
    account = os.getenv("SNOWFLAKE_LIVE_ACCOUNT")
    if not account:
        pytest.skip("SNOWFLAKE_LIVE_ACCOUNT is not set; skipping live Snowflake cloud test")
    config: dict[str, Any] = {"account": account, "schema": "PUBLIC"}
    options = {
        "USER": "user",
        "PASSWORD": "password",
        "WAREHOUSE": "warehouse",
        "DATABASE": "database",
        "SCHEMA": "schema",
        "ROLE": "role",
        "AUTH_TYPE": "auth_type",
        "TOKEN": "token",
        "WIF_PROVIDER": "workload_identity_provider",
        "PRIVATE_KEY": "private_key",
        "PRIVATE_KEY_PASSPHRASE": "private_key_passphrase",
    }
    for environment_suffix, key in options.items():
        if value := os.getenv(f"SNOWFLAKE_LIVE_{environment_suffix}"):
            config[key] = value
    return config


@pytest.fixture(scope="session")
def snowflake_live_config(snowflake_live_connection_config: dict[str, Any]) -> Generator[SnowflakeConfig, None, None]:
    """Yield an explicitly enabled live Snowflake config."""
    config = SnowflakeConfig(
        connection_config=snowflake_live_connection_config,
        driver_features={"enable_stage_parquet_load": True, "stage_parquet_threshold_rows": 1},
    )
    try:
        yield config
    finally:
        config.close_pool()


@pytest.fixture
def snowflake_live_session(snowflake_live_config: SnowflakeConfig) -> Generator["SnowflakeDriver", None, None]:
    """Yield a live session after the account gate passes."""
    with snowflake_live_config.provide_session() as session:
        yield session

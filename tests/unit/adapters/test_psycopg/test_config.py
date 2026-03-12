"""Psycopg configuration tests covering statement config builders."""

from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.psycopg._typing import PsycopgAsyncSessionContext, PsycopgSyncSessionContext
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.core import build_statement_config, default_statement_config
from sqlspec.core import SQL, StatementConfig


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psycopg_sync_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the sync Psycopg statement configuration."""

    def serializer(_: object) -> str:
        return "sync"

    config = PsycopgSyncConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psycopg_async_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the async Psycopg statement configuration."""

    def serializer(_: object) -> str:
        return "async"

    config = PsycopgAsyncConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psycopg_numeric_placeholders_convert_to_pyformat() -> None:
    """Numeric placeholders should be rewritten for psycopg execution."""

    statement = SQL(
        "SELECT * FROM bridge_validation WHERE label IN ($1, $2, $3)",
        "alpha",
        "beta",
        "gamma",
        statement_config=default_statement_config,
    )
    compiled_sql, parameters = statement.compile()

    assert "$1" not in compiled_sql
    assert compiled_sql.count("%s") == 3
    assert parameters == ["alpha", "beta", "gamma"]


def test_psycopg_sync_session_context_uses_lazy_default_statement_config() -> None:
    """Sync session context should resolve default config after acquiring a connection."""
    default_statement_config = StatementConfig(dialect="pgvector")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=None,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psycopg_async_session_context_uses_lazy_default_statement_config() -> None:
    """Async session context should resolve default config after acquiring a connection."""
    default_statement_config = StatementConfig(dialect="pgvector")
    context = PsycopgAsyncSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=None,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


def test_psycopg_sync_session_context_preserves_explicit_statement_config_override() -> None:
    """Explicit sync statement config overrides should bypass lazy defaults."""
    explicit_statement_config = StatementConfig(dialect="postgres")
    default_statement_config = StatementConfig(dialect="pgvector")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=explicit_statement_config,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config is explicit_statement_config


def test_psycopg_sync_create_pool_bootstraps_extensions_before_pool_initialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sync psycopg pool creation should know extensions before opening the pool."""
    config = PsycopgSyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})

    def fake_detect_extensions(_pool: object) -> None:
        config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
        config._paradedb_available = False  # pyright: ignore[reportPrivateUsage]
        config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]

    def fake_connection_pool(*args: object, **kwargs: object) -> object:
        assert config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
        assert kwargs["open"] is True
        return object()

    monkeypatch.setattr(config, "_detect_extensions", fake_detect_extensions)
    monkeypatch.setattr("sqlspec.adapters.psycopg.config.ConnectionPool", fake_connection_pool)

    config._create_pool()  # pyright: ignore[reportPrivateUsage]


@pytest.mark.anyio
async def test_psycopg_async_create_pool_bootstraps_extensions_before_pool_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async psycopg pool creation should know extensions before opening the pool."""
    config = PsycopgAsyncConfig(connection_config={"conninfo": "postgresql://localhost/test"})

    async def fake_detect_extensions(_pool: object) -> None:
        config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
        config._paradedb_available = False  # pyright: ignore[reportPrivateUsage]
        config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]

    class FakeAsyncConnectionPool:
        def __init__(self, *args: object, **kwargs: object) -> None:
            assert config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
            assert kwargs["open"] is False

        async def open(self) -> None:
            return None

    monkeypatch.setattr(config, "_detect_extensions", fake_detect_extensions)
    monkeypatch.setattr("sqlspec.adapters.psycopg.config.AsyncConnectionPool", FakeAsyncConnectionPool)

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

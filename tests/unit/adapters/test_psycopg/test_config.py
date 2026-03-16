"""Psycopg configuration tests covering statement config builders."""

from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.psycopg._typing import PsycopgAsyncSessionContext, PsycopgSyncSessionContext
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.core import (
    build_postgres_extension_probe_names,
    build_statement_config,
    default_statement_config,
    resolve_postgres_extension_state,
)
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


def test_psycopg_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_psycopg_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


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


def test_psycopg_sync_session_context_resolves_callable_statement_config() -> None:
    """Sync session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psycopg_async_session_context_resolves_callable_statement_config() -> None:
    """Async session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsycopgAsyncSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


def test_psycopg_sync_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = PsycopgSyncSessionContext(
        acquire_connection=lambda: object(),
        release_connection=lambda _conn: None,
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    with context as driver:
        assert driver.statement_config is explicit_config


def test_psycopg_sync_provide_session_tracks_promoted_statement_config() -> None:
    """Sync runtime statement config should resolve the current config dialect lazily."""
    config = PsycopgSyncConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"


def test_psycopg_async_provide_session_tracks_promoted_statement_config() -> None:
    """Async runtime statement config should resolve the current config dialect lazily."""
    config = PsycopgAsyncConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"

"""AsyncPG configuration tests covering statement config builders."""

from typing import Any, cast, get_args, get_origin
from unittest.mock import AsyncMock

import pytest
from asyncpg.pool import PoolConnectionProxy, PoolConnectionProxyMeta
from typing_extensions import NotRequired

from sqlspec.adapters.asyncpg._typing import AsyncpgSessionContext
from sqlspec.adapters.asyncpg.config import AsyncpgConfig, AsyncpgConnectionConfig, AsyncpgPoolConfig
from sqlspec.adapters.asyncpg.core import (
    build_postgres_extension_probe_names,
    build_statement_config,
    resolve_postgres_extension_state,
)
from sqlspec.core import StatementConfig


def test_build_default_statement_config_custom_serializers() -> None:
    """Custom serializers should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    def deserializer(_: str) -> object:
        return {"value": "deserialized"}

    statement_config = build_statement_config(json_serializer=serializer, json_deserializer=deserializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncpg_config_applies_driver_feature_serializers() -> None:
    """Driver features should mutate the AsyncPG statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    def deserializer(_: str) -> object:
        return {"feature": True}

    config = AsyncpgConfig(driver_features={"json_serializer": serializer, "json_deserializer": deserializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer
    assert parameter_config.json_deserializer is deserializer


def test_asyncpg_config_connection_type_is_not_metaclass() -> None:
    connection_type = cast("object", AsyncpgConfig.connection_type)
    assert connection_type is PoolConnectionProxy
    assert connection_type is not cast("object", PoolConnectionProxyMeta)


def test_asyncpg_connection_config_types_current_service_and_gss_options() -> None:
    """Typed connection config should include current asyncpg connection options."""
    expected_options = {"service", "servicefile", "timeout", "target_session_attrs", "krbsrvname", "gsslib"}

    assert expected_options <= set(AsyncpgConnectionConfig.__annotations__)

    target_session_attrs_annotation = cast("Any", AsyncpgConnectionConfig.__annotations__["target_session_attrs"])
    assert get_origin(target_session_attrs_annotation) is NotRequired
    assert set(get_args(get_args(target_session_attrs_annotation)[0])) == {
        "any",
        "primary",
        "standby",
        "read-write",
        "read-only",
        "prefer-standby",
    }

    gsslib_annotation = cast("Any", AsyncpgConnectionConfig.__annotations__["gsslib"])
    assert get_origin(gsslib_annotation) is NotRequired
    assert set(get_args(get_args(gsslib_annotation)[0])) == {"gssapi", "sspi"}


def test_asyncpg_pool_config_types_current_pool_callbacks() -> None:
    """Typed pool config should include current asyncpg pool callback options."""
    assert {"connect", "setup", "init", "reset"} <= set(AsyncpgPoolConfig.__annotations__)


async def test_asyncpg_create_pool_routes_current_connect_and_pool_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Current connect kwargs should reach create_pool while reset remains pool-scoped."""
    import sqlspec.adapters.asyncpg.config as config_mod

    captured_connect_kwargs: dict[str, Any] = {}
    captured_pool_kwargs: dict[str, Any] = {}

    async def reset_connection(_: object) -> None:
        return None

    async def connect_factory(**_: Any) -> object:
        return object()

    async def fake_create_pool(*, connect: Any = None, init: Any = None, reset: Any = None, **kwargs: Any) -> object:
        captured_pool_kwargs.update({"connect": connect, "init": init, "reset": reset})
        captured_connect_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(config_mod, "asyncpg_create_pool", fake_create_pool)

    config = AsyncpgConfig(
        connection_config={
            "connect": connect_factory,
            "dsn": "postgresql://localhost/test",
            "gsslib": "gssapi",
            "krbsrvname": "postgres",
            "min_size": 1,
            "reset": reset_connection,
            "service": "analytics",
            "servicefile": "/tmp/pg_service.conf",
            "target_session_attrs": "read-only",
            "timeout": 4.5,
        }
    )

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

    assert captured_pool_kwargs["connect"] is connect_factory
    assert captured_pool_kwargs["init"].__func__ is config._init_connection.__func__  # pyright: ignore[reportPrivateUsage]
    assert captured_pool_kwargs["init"].__self__ is config
    assert captured_pool_kwargs["reset"] is reset_connection
    assert captured_connect_kwargs["dsn"] == "postgresql://localhost/test"
    assert captured_connect_kwargs["gsslib"] == "gssapi"
    assert captured_connect_kwargs["krbsrvname"] == "postgres"
    assert captured_connect_kwargs["service"] == "analytics"
    assert captured_connect_kwargs["servicefile"] == "/tmp/pg_service.conf"
    assert captured_connect_kwargs["target_session_attrs"] == "read-only"
    assert captured_connect_kwargs["timeout"] == 4.5
    assert "reset" not in captured_connect_kwargs


def test_asyncpg_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_asyncpg_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


@pytest.mark.anyio
async def test_asyncpg_session_context_resolves_callable_statement_config() -> None:
    """Session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_asyncpg_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_config


def test_asyncpg_provide_session_tracks_promoted_statement_config() -> None:
    """Runtime statement config should resolve the current config dialect lazily."""
    config = AsyncpgConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"

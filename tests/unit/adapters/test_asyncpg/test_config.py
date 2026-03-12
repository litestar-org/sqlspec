"""AsyncPG configuration tests covering statement config builders."""

from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.asyncpg._typing import AsyncpgSessionContext
from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.core import build_statement_config
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


@pytest.mark.anyio
async def test_asyncpg_session_context_uses_lazy_default_statement_config() -> None:
    """Session context should resolve default config after acquiring a connection."""
    default_statement_config = StatementConfig(dialect="pgvector")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=None,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_asyncpg_session_context_preserves_explicit_statement_config_override() -> None:
    """Explicit statement config overrides should bypass lazy defaults."""
    explicit_statement_config = StatementConfig(dialect="postgres")
    default_statement_config = StatementConfig(dialect="pgvector")
    context = AsyncpgSessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_statement_config,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_statement_config


@pytest.mark.anyio
async def test_asyncpg_create_pool_bootstraps_extensions_before_pool_initialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extension flags should be ready before asyncpg pool initialization runs."""
    config = AsyncpgConfig(connection_config={"dsn": "postgresql://localhost/test"})

    async def fake_detect_extensions(_pool: object) -> None:
        config._pgvector_available = True  # pyright: ignore[reportPrivateUsage]
        config._paradedb_available = False  # pyright: ignore[reportPrivateUsage]
        config._update_dialect_for_extensions()  # pyright: ignore[reportPrivateUsage]

    async def fake_create_pool(**kwargs: object) -> object:
        assert config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
        assert kwargs["init"] == config._init_connection  # pyright: ignore[reportPrivateUsage]
        return object()

    monkeypatch.setattr(config, "_detect_extensions", fake_detect_extensions)
    monkeypatch.setattr("sqlspec.adapters.asyncpg.config.asyncpg_create_pool", fake_create_pool)

    await config._create_pool()  # pyright: ignore[reportPrivateUsage]

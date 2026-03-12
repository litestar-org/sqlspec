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

"""Psqlpy configuration tests covering statement config builders."""

from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpySessionContext
from sqlspec.adapters.psqlpy.config import PsqlpyConfig
from sqlspec.adapters.psqlpy.core import build_statement_config
from sqlspec.core import StatementConfig


def test_build_default_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_statement_config(json_serializer=serializer)

    parameter_config = statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


def test_psqlpy_config_applies_driver_feature_serializer() -> None:
    """Driver features should mutate the Psqlpy statement configuration."""

    def serializer(_: object) -> str:
        return "feature"

    config = PsqlpyConfig(driver_features={"json_serializer": serializer})

    parameter_config = config.statement_config.parameter_config
    assert parameter_config.json_serializer is serializer


@pytest.mark.anyio
async def test_psqlpy_session_context_uses_lazy_default_statement_config() -> None:
    """Session context should resolve default config after acquiring a connection."""
    default_statement_config = StatementConfig(dialect="pgvector")
    context = PsqlpySessionContext(
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
async def test_psqlpy_session_context_preserves_explicit_statement_config_override() -> None:
    """Explicit statement config overrides should bypass lazy defaults."""
    explicit_statement_config = StatementConfig(dialect="postgres")
    default_statement_config = StatementConfig(dialect="pgvector")
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_statement_config,
        default_statement_config_getter=lambda: default_statement_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_statement_config

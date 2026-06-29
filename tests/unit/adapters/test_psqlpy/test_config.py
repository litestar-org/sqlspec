"""Psqlpy configuration tests covering statement config builders."""

from typing import cast
from unittest.mock import AsyncMock

import pytest

from sqlspec.adapters.psqlpy._typing import PsqlpyConnection, PsqlpySessionContext
from sqlspec.adapters.psqlpy.config import PsqlpyConfig, PsqlpyDriverFeatures
from sqlspec.adapters.psqlpy.core import (
    build_postgres_extension_probe_names,
    build_statement_config,
    resolve_postgres_extension_state,
)
from sqlspec.core import StatementConfig


class _ExtensionQueryResult:
    def __init__(self, extension_names: set[str]) -> None:
        self._extension_names = extension_names

    def result(self) -> list[dict[str, str]]:
        return [{"extname": name} for name in self._extension_names]


class _ExtensionConnection:
    def __init__(self, extension_names: set[str]) -> None:
        self._extension_names = extension_names
        self.queries: list[tuple[str, list[list[str]]]] = []

    async def fetch(self, query: str, parameters: list[list[str]]) -> _ExtensionQueryResult:
        self.queries.append((query, parameters))
        return _ExtensionQueryResult(self._extension_names)


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


def test_psqlpy_driver_features_declares_cast_detection() -> None:
    """Driver feature typing should expose every consumed feature flag."""
    assert "enable_cast_detection" in PsqlpyDriverFeatures.__annotations__


def test_psqlpy_config_defaults_cast_detection_feature() -> None:
    """Cast-aware parameter processing should be declared and enabled by default."""
    config = PsqlpyConfig()

    assert config.driver_features["enable_cast_detection"] is True


def test_psqlpy_config_preserves_cast_detection_override() -> None:
    """Callers should be able to disable cast-aware parameter processing."""
    config = PsqlpyConfig(driver_features={"enable_cast_detection": False})

    assert config.driver_features["enable_cast_detection"] is False


def test_psqlpy_runtime_aliases_resolve_to_installed_classes() -> None:
    """Psqlpy public runtime aliases should expose installed psqlpy classes."""
    psqlpy = pytest.importorskip("psqlpy")
    from sqlspec.adapters.psqlpy import PsqlpyConnection as PublicPsqlpyConnection
    from sqlspec.adapters.psqlpy._typing import PsqlpyListener

    namespace = PsqlpyConfig().get_signature_namespace()

    assert PsqlpyConnection is psqlpy.Connection
    assert PublicPsqlpyConnection is psqlpy.Connection
    assert PsqlpyListener is psqlpy.Listener
    assert PsqlpyConfig.connection_type is psqlpy.Connection
    assert namespace["PsqlpyConnection"] is psqlpy.Connection
    assert isinstance(object(), PsqlpyConnection) is False


def test_psqlpy_build_postgres_extension_probe_names_filters_disabled_features() -> None:
    """Only enabled extension probes should be returned."""
    assert build_postgres_extension_probe_names({"enable_pgvector": True, "enable_paradedb": False}) == ["vector"]


def test_psqlpy_resolve_postgres_extension_state_promotes_paradedb() -> None:
    """Detected extensions should promote the runtime dialect."""
    statement_config, pgvector_available, paradedb_available = resolve_postgres_extension_state(
        StatementConfig(dialect="postgres"), {"enable_pgvector": True, "enable_paradedb": True}, {"vector", "pg_search"}
    )

    assert statement_config.dialect == "paradedb"
    assert pgvector_available is True
    assert paradedb_available is True


@pytest.mark.anyio
async def test_psqlpy_enable_pgvector_detects_extension_and_promotes_dialect() -> None:
    """Psqlpy pgvector support should mean extension detection and dialect promotion."""
    config = PsqlpyConfig(driver_features={"enable_pgvector": True, "enable_paradedb": False})
    connection = _ExtensionConnection({"vector"})

    await config._ensure_connection_initialized(cast("PsqlpyConnection", connection))  # pyright: ignore[reportPrivateUsage]

    assert connection.queries[0][1] == [["vector"]]
    assert config._pgvector_available is True  # pyright: ignore[reportPrivateUsage]
    assert config.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psqlpy_session_context_resolves_callable_statement_config() -> None:
    """Session context should call statement_config when it's a callable."""
    expected_config = StatementConfig(dialect="pgvector")
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=lambda: expected_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config.dialect == "pgvector"


@pytest.mark.anyio
async def test_psqlpy_session_context_preserves_explicit_statement_config() -> None:
    """Explicit StatementConfig should be used directly without calling."""
    explicit_config = StatementConfig(dialect="postgres")
    context = PsqlpySessionContext(
        acquire_connection=AsyncMock(return_value=object()),
        release_connection=AsyncMock(),
        statement_config=explicit_config,
        driver_features={},
        prepare_driver=lambda driver: driver,
    )

    async with context as driver:
        assert driver.statement_config is explicit_config


def test_psqlpy_provide_session_tracks_promoted_statement_config() -> None:
    """Runtime statement config should resolve the current config dialect lazily."""
    config = PsqlpyConfig()
    config.statement_config = config.statement_config.replace(dialect="pgvector")

    session_config = config.provide_session()._statement_config  # pyright: ignore[reportPrivateUsage]

    assert callable(session_config)
    assert session_config().dialect == "pgvector"

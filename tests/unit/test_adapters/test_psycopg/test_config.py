"""Psycopg configuration tests covering statement config builders."""

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.driver import build_psycopg_statement_config


def test_build_psycopg_statement_config_custom_serializer() -> None:
    """Custom serializer should propagate into the parameter configuration."""

    def serializer(_: object) -> str:
        return "serialized"

    statement_config = build_psycopg_statement_config(json_serializer=serializer)

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

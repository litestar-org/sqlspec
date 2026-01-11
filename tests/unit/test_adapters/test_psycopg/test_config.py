"""Psycopg configuration tests covering statement config builders."""

from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.adapters.psycopg.core import build_statement_config, default_statement_config
from sqlspec.core import SQL


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

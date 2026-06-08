import sqlite3
from pathlib import Path

from sqlspec.adapters.aiosqlite.config import AiosqliteConfig, AiosqliteConnectionParams
from sqlspec.adapters.aiosqlite.core import build_connection_config


class CustomConnection(sqlite3.Connection):
    """Connection subclass used to validate factory passthrough."""


def test_connection_params_accept_pathlike_and_modern_connection_options(tmp_path: Path) -> None:
    db_path = tmp_path / "typed.db"

    params: AiosqliteConnectionParams = {
        "database": db_path,
        "factory": CustomConnection,
        "iter_chunk_size": 128,
        "autocommit": True,
        "isolation_level": None,
    }

    assert params["database"] == db_path


def test_build_connection_config_filters_pool_keys_and_gates_autocommit(tmp_path: Path) -> None:
    db_path = tmp_path / "config.db"

    connection_config = build_connection_config({
        "database": db_path,
        "timeout": 1.5,
        "isolation_level": None,
        "factory": CustomConnection,
        "iter_chunk_size": 256,
        "autocommit": True,
        "min_size": 2,
        "health_check_interval": 0.25,
        "pool_size": 4,
        "connect_timeout": 0.5,
        "idle_timeout": 1.0,
        "operation_timeout": 1.5,
        "pool_recycle_seconds": 2.0,
        "extra": {"ignored": True},
    })

    assert connection_config["database"] == db_path
    assert connection_config["timeout"] == 1.5
    assert connection_config["isolation_level"] is None
    assert connection_config["factory"] is CustomConnection
    assert connection_config["iter_chunk_size"] == 256

    assert "min_size" not in connection_config
    assert "health_check_interval" not in connection_config
    assert "pool_size" not in connection_config
    assert "connect_timeout" not in connection_config
    assert "idle_timeout" not in connection_config
    assert "operation_timeout" not in connection_config
    assert "pool_recycle_seconds" not in connection_config
    assert "extra" not in connection_config

    if hasattr(sqlite3, "LEGACY_TRANSACTION_CONTROL"):
        assert connection_config["autocommit"] is True
    else:
        assert "autocommit" not in connection_config


async def test_create_pool_routes_pool_settings_without_leaking_to_connect_kwargs(tmp_path: Path) -> None:
    config = AiosqliteConfig(
        connection_config={
            "database": tmp_path / "pool.db",
            "pool_size": 4,
            "min_size": 2,
            "health_check_interval": 0.25,
            "iter_chunk_size": 512,
            "isolation_level": None,
        }
    )

    pool = await config._create_pool()
    try:
        assert pool._pool_size == 4
        assert pool._min_size == 2
        assert pool._health_check_interval == 0.25
        assert pool._connection_parameters["iter_chunk_size"] == 512
        assert pool._connection_parameters["isolation_level"] is None
        assert "min_size" not in pool._connection_parameters
        assert "health_check_interval" not in pool._connection_parameters
    finally:
        await pool.close()


def test_config_accepts_pathlike_database_without_coercing_connection_value(tmp_path: Path) -> None:
    db_path = tmp_path / "pathlike.db"

    config = AiosqliteConfig(connection_config={"database": db_path})

    assert config.connection_config["database"] == db_path
    assert isinstance(config.connection_config["database"], Path)

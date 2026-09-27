"""Native PostgreSQL option forwarding without database services."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from asyncpg import Connection

from sqlspec.adapters.asyncpg.config import AsyncpgConfig
from sqlspec.adapters.asyncpg.driver import AsyncpgDriver
from sqlspec.adapters.cockroach_asyncpg.core import build_connection_config as asyncpg_cockroach_config
from sqlspec.adapters.cockroach_psycopg.core import build_connection_config as psycopg_cockroach_config
from sqlspec.adapters.psycopg import config as psycopg_module
from sqlspec.adapters.psycopg.config import PsycopgAsyncConfig, PsycopgSyncConfig
from sqlspec.core import StatementConfig, StatementStack
from sqlspec.exceptions import ImproperConfigurationError


async def test_asyncpg_timeout_survives_repeated_execution() -> None:
    connection = MagicMock(spec=Connection)
    connection.fetch = AsyncMock(return_value=[])
    driver = AsyncpgDriver(
        connection, statement_config=StatementConfig(dialect="postgres", execution_args={"timeout": 0})
    )
    for _ in range(2):
        await driver.execute("SELECT 1")
    assert [call.kwargs for call in connection.fetch.call_args_list] == [{"timeout": 0}, {"timeout": 0}]


async def test_asyncpg_custom_codec_is_registered_after_internal_setup() -> None:
    encoder = str
    decoder = int
    config = AsyncpgConfig(
        driver_features={"type_codecs": [{"typename": "custom", "encoder": encoder, "decoder": decoder}]}
    )
    config._pgvector_available = False
    connection = MagicMock(spec=Connection)
    connection.set_type_codec = AsyncMock()
    await config._init_connection(connection)
    assert connection.set_type_codec.call_args.args == ("custom",)
    assert connection.set_type_codec.call_args.kwargs == {
        "schema": "public",
        "format": "text",
        "encoder": encoder,
        "decoder": decoder,
    }


def test_asyncpg_pgbouncer_disables_prepared_cache_consistently() -> None:
    config = AsyncpgConfig(connection_config={"pgbouncer": True, "statement_cache_size": 128})
    assert config.connection_config["statement_cache_size"] == 0
    assert config.driver_features["pgbouncer"] is True


@pytest.mark.parametrize(
    "config_type,pool_name",
    [(PsycopgSyncConfig, "NullConnectionPool"), (PsycopgAsyncConfig, "AsyncNullConnectionPool")],
)
async def test_null_pool_preserves_throttling_without_leaking_flag(
    monkeypatch: pytest.MonkeyPatch, config_type: Any, pool_name: str
) -> None:
    constructor = MagicMock()
    monkeypatch.setattr(psycopg_module, pool_name, constructor)
    config = config_type(connection_config={"null_pool": True, "open": False, "max_size": 7, "max_waiting": 2})
    if config_type is PsycopgAsyncConfig:
        await config._create_pool()
    else:
        config._create_pool()
    args = constructor.call_args.kwargs
    assert args["max_size"] == 7
    assert args["max_waiting"] == 2
    assert "null_pool" not in args["kwargs"]
    assert "null_pool" not in config._connection_kwargs()[2]


@pytest.mark.parametrize("builder", [asyncpg_cockroach_config, psycopg_cockroach_config])
def test_cockroach_options_reject_invalid_values(builder: Any) -> None:
    with pytest.raises(ImproperConfigurationError, match="boolean"):
        builder({"default_transaction_use_follower_reads": "invalid"})
    with pytest.raises(ImproperConfigurationError, match="integer"):
        builder({"results_buffer_size": "1024 -c injected=1"})


def test_cockroach_options_preserve_native_controls() -> None:
    async_config = asyncpg_cockroach_config({
        "application_name": "test",
        "default_transaction_use_follower_reads": True,
        "results_buffer_size": 1024,
    })
    assert async_config["server_settings"] == {
        "application_name": "test",
        "default_transaction_use_follower_reads": "on",
        "results_buffer_size": "1024",
    }
    sync_config = psycopg_cockroach_config({
        "options": "-c application_name=test",
        "statement_timeout": 100,
        "results_buffer_size": 1024,
    })
    assert sync_config["options"] == "-c application_name=test -c results_buffer_size=1024 -c statement_timeout=100"


def test_psqlpy_dense_vector_uses_native_wrapper() -> None:
    from psqlpy.extra_types import PgVector

    from sqlspec.adapters.psqlpy.type_converter import coerce_pgvector

    vector = coerce_pgvector([1.0, 2.0])
    assert isinstance(vector, PgVector)
    assert coerce_pgvector(vector) is vector


async def test_pgbouncer_stack_cancellation_keeps_transaction_cleanup() -> None:
    connection = MagicMock(spec=Connection)
    connection.is_in_transaction.return_value = False
    connection.fetch = AsyncMock(side_effect=asyncio.CancelledError)
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock(return_value=False)
    connection.transaction.return_value = transaction
    driver = AsyncpgDriver(connection, driver_features={"pgbouncer": True})
    stack = StatementStack().push_execute("SELECT 1")
    with pytest.raises(asyncio.CancelledError):
        await driver.execute_stack(stack)
    assert transaction.__aexit__.call_args.args[0] is asyncio.CancelledError
    connection.prepare.assert_not_called()

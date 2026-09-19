"""Standalone connections remain owned by the config until initialization succeeds."""

import asyncio
from importlib import import_module
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.parametrize("adapter", ["psycopg", "cockroach_psycopg"])
def test_sync_initialization_failure_closes_connection(adapter: str, monkeypatch: pytest.MonkeyPatch) -> None:
    module = import_module(f"sqlspec.adapters.{adapter}.config")
    config_type = module.PsycopgSyncConfig if adapter == "psycopg" else module.CockroachPsycopgSyncConfig
    connection_type = module.PsycopgConnection if adapter == "psycopg" else module.psycopg_crdb.CrdbConnection
    connection = MagicMock()
    error = ValueError("initialization failed")
    monkeypatch.setattr(connection_type, "connect", MagicMock(return_value=connection))
    config = config_type(connection_config={"configure": MagicMock(side_effect=error)})

    with pytest.raises(ValueError, match="initialization failed") as exc:
        config.create_connection()

    assert exc.value is error
    connection.close.assert_called_once_with()


@pytest.mark.anyio
@pytest.mark.parametrize("adapter", ["asyncpg", "cockroach_asyncpg", "psycopg", "cockroach_psycopg"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_async_initialization_failure_closes_connection(
    adapter: str, cancelled: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = import_module(f"sqlspec.adapters.{adapter}.config")
    connection = AsyncMock()
    error = asyncio.CancelledError() if cancelled else ValueError("initialization failed")
    hook = AsyncMock(side_effect=error)
    if adapter in {"asyncpg", "cockroach_asyncpg"}:
        config_type = module.AsyncpgConfig if adapter == "asyncpg" else module.CockroachAsyncpgConfig
        monkeypatch.setattr(module, "asyncpg_connect", AsyncMock(return_value=connection))
        config = config_type(connection_config={"init": hook})
    else:
        config_type = module.PsycopgAsyncConfig if adapter == "psycopg" else module.CockroachPsycopgAsyncConfig
        connection_type = (
            module.PsycopgAsyncConnection if adapter == "psycopg" else module.psycopg_crdb.AsyncCrdbConnection
        )
        monkeypatch.setattr(connection_type, "connect", AsyncMock(return_value=connection))
        config = config_type(connection_config={"configure": hook})

    with pytest.raises(type(error)) as exc:
        await config.create_connection()

    assert exc.value is error
    connection.close.assert_awaited_once_with()

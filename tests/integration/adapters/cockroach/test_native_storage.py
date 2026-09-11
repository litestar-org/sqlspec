"""SQLSpec native Cockroach storage on local pytest-databases services."""

from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

import asyncpg
import psycopg
import pytest
from psycopg import sql

from sqlspec.adapters.cockroach_asyncpg import CockroachAsyncpgDriver
from sqlspec.adapters.cockroach_psycopg import CockroachPsycopgAsyncDriver, CockroachPsycopgSyncDriver
from tests.integration.adapters.cockroach.test_native_storage_capabilities import _file_uri
from tests.integration.adapters.cockroach.test_native_storage_capabilities import native_connection as native_connection
from tests.integration.adapters.cockroach.test_native_storage_capabilities import (
    native_storage_uri as native_storage_uri,
)

if TYPE_CHECKING:
    from pytest_databases.docker.cockroachdb import CockroachDBService


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
@pytest.mark.parametrize("adapter", ["sync", "async", "asyncpg"])
async def test_native_storage_export_import_roundtrip(
    native_connection: psycopg.Connection,
    native_storage_uri: str,
    cockroachdb_service: "CockroachDBService",
    adapter: str,
    file_format: Literal["csv", "parquet"],
) -> None:
    features = {
        "enable_native_storage": True,
        "native_storage_csv_options": {"skip": 0, "nullas": "NULL", "nullif": "NULL"},
    }
    config = {"host": cockroachdb_service.host, "port": cockroachdb_service.port, "user": "root"}
    connection: Any = native_connection
    if adapter == "async":
        connection = await psycopg.AsyncConnection.connect(
            **config, dbname=cockroachdb_service.database, sslmode="disable", autocommit=True
        )
        driver: Any = CockroachPsycopgAsyncDriver(connection, driver_features=features)
    elif adapter == "asyncpg":
        connection = await asyncpg.connect(**config, database=cockroachdb_service.database, ssl=False)
        driver = CockroachAsyncpgDriver(connection, driver_features=features)
    else:
        driver = CockroachPsycopgSyncDriver(connection, driver_features=features)
    table = "native_driver_" + uuid4().hex
    native_connection.execute(
        sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(sql.Identifier(table))
    )
    try:
        exported = driver.select_to_storage(
            "SELECT :id::INT8 AS id, :label::STRING AS label",
            native_storage_uri,
            {"id": 7, "label": "O'Reilly"},
            format_hint=file_format,
            partitioner={"test": True},
        )
        if adapter != "sync":
            exported = await exported
        assert exported.telemetry["rows_processed"] == 1
        assert exported.telemetry["bytes_processed"] > 0
        assert exported.telemetry["extra"]["partitioner"] == {"test": True}
        files = exported.telemetry["extra"]["files"]
        assert files
        for filename in files:
            imported = driver.load_from_storage(table, _file_uri(native_storage_uri, filename), file_format=file_format)
            if adapter != "sync":
                imported = await imported
            assert imported.telemetry["rows_processed"] == 1
            assert "bytes_processed" not in imported.telemetry
        assert native_connection.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))).fetchall() == [
            (7, "O'Reilly")
        ]
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(table)))
        if adapter != "sync":
            await connection.close()


@pytest.mark.parametrize("adapter", ["sync", "async", "asyncpg"])
async def test_native_storage_export_null_refusal(
    native_connection: psycopg.Connection,
    native_storage_uri: str,
    cockroachdb_service: "CockroachDBService",
    adapter: str,
) -> None:
    from sqlspec.exceptions import SQLSpecError

    config = {"host": cockroachdb_service.host, "port": cockroachdb_service.port, "user": "root"}
    connection: Any = native_connection
    if adapter == "async":
        connection = await psycopg.AsyncConnection.connect(
            **config, dbname=cockroachdb_service.database, sslmode="disable", autocommit=True
        )
        driver: Any = CockroachPsycopgAsyncDriver(connection, driver_features={"enable_native_storage": True})
    elif adapter == "asyncpg":
        connection = await asyncpg.connect(**config, database=cockroachdb_service.database, ssl=False)
        driver = CockroachAsyncpgDriver(connection, driver_features={"enable_native_storage": True})
    else:
        driver = CockroachPsycopgSyncDriver(connection, driver_features={"enable_native_storage": True})
    try:
        with pytest.raises(SQLSpecError, match="NULL value encountered"):
            result = driver.select_to_storage("SELECT NULL::STRING AS label", native_storage_uri, format_hint="csv")
            if adapter != "sync":
                await result
    finally:
        if adapter != "sync":
            await connection.close()

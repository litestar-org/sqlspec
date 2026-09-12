"""Local server contracts required by CockroachDB native object storage."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlencode, urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import psycopg
import pytest
from psycopg import sql

from tests.fixtures.rustfs import rustfs_filesystem

if TYPE_CHECKING:
    from collections.abc import Callable

    from pytest_databases.docker.cockroachdb import CockroachDBService
    from pytest_databases.docker.rustfs import RustfsService


@pytest.fixture
def native_connection(cockroachdb_service: "CockroachDBService") -> Iterator[psycopg.Connection]:
    with psycopg.connect(
        host=cockroachdb_service.host,
        port=cockroachdb_service.port,
        dbname=cockroachdb_service.database,
        user="root",
        sslmode="disable",
        autocommit=True,
    ) as connection:
        yield connection


@pytest.fixture
def native_storage_uri(rustfs_service: "RustfsService", rustfs_bucket_name: str) -> Iterator[str]:
    rustfs_service.container.reload()
    networks = rustfs_service.container.attrs["NetworkSettings"]["Networks"]
    address = next(iter(networks.values()))["IPAddress"]
    prefix = f"native-{uuid4().hex}"
    query = urlencode({
        "AWS_ACCESS_KEY_ID": rustfs_service.access_key,
        "AWS_SECRET_ACCESS_KEY": rustfs_service.secret_key,
        "AWS_ENDPOINT": f"http://{address}:9000",
        "AWS_REGION": "us-east-1",
        "AUTH": "specified",
    })
    try:
        yield f"s3://{rustfs_bucket_name}/{prefix}?{query}"
    finally:
        filesystem = rustfs_filesystem(rustfs_service)
        if filesystem.exists(f"{rustfs_bucket_name}/{prefix}"):
            filesystem.rm(f"{rustfs_bucket_name}/{prefix}", recursive=True)


def _file_uri(destination: str, filename: str) -> str:
    parts = urlsplit(destination)
    return urlunsplit(parts._replace(path=f"{parts.path}/{filename}"))


def test_native_storage_connection_baseline(
    native_connection: psycopg.Connection, record_property: "Callable[[str, object], None]"
) -> None:
    row = native_connection.execute("SELECT version()").fetchone()
    assert row is not None
    assert "CockroachDB" in row[0]
    record_property("cockroach_version", row[0])


@pytest.mark.parametrize("file_format", ["CSV", "PARQUET"])
def test_native_storage_bound_values_roundtrip(
    native_connection: psycopg.Connection, native_storage_uri: str, file_format: Literal["CSV", "PARQUET"]
) -> None:
    cursor = native_connection.execute(
        sql.SQL("EXPORT INTO {} %s FROM (SELECT %s::INT8 AS id, %s::STRING AS label)").format(sql.SQL(file_format)),
        (native_storage_uri, 7, "O'Reilly"),
    )
    assert cursor.description is not None
    assert [column.name for column in cursor.description] == ["filename", "rows", "bytes"]
    files = cursor.fetchall()
    assert len(files) == 1
    filename, row_count, byte_count = files[0]
    assert row_count == 1
    assert byte_count > 0
    source = _file_uri(native_storage_uri, filename)
    target = sql.Identifier(f"native_{uuid4().hex}")
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(target))
    try:
        cursor = native_connection.execute(
            sql.SQL("IMPORT INTO {} {} DATA (%s)").format(target, sql.SQL(file_format)), (source,)
        )
        assert cursor.description is not None
        row = cursor.fetchone()
        assert row is not None
        result = dict(zip((column.name for column in cursor.description), row, strict=True))
        assert result["status"] == "succeeded"
        assert result["rows"] == 1
        assert result["bytes"] > 0
        assert native_connection.execute(sql.SQL("SELECT * FROM {}").format(target)).fetchall() == [(7, "O'Reilly")]
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))


def test_native_csv_requires_explicit_null_encoding(
    native_connection: psycopg.Connection, native_storage_uri: str, record_property: "Callable[[str, object], None]"
) -> None:
    with pytest.raises(psycopg.InternalError, match="NULL value encountered during EXPORT") as caught:
        native_connection.execute("EXPORT INTO CSV %s FROM SELECT NULL::STRING", (native_storage_uri,))
    record_property("csv_null_error", str(caught.value))


@pytest.mark.parametrize("marker", ["", "__SQLSPEC_NULL__"])
def test_native_csv_marker_collides_with_literal_data(
    native_connection: psycopg.Connection, native_storage_uri: str, marker: str
) -> None:
    files = native_connection.execute(
        "EXPORT INTO CSV %s WITH nullas = %s FROM "
        "(SELECT 1::INT8 AS id, NULL::STRING AS label UNION ALL SELECT 2, %s::STRING)",
        (native_storage_uri, marker, marker),
    ).fetchall()
    target = sql.Identifier(f"native_{uuid4().hex}")
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(target))
    try:
        native_connection.execute(
            sql.SQL("IMPORT INTO {} CSV DATA (%s) WITH nullif = %s").format(target),
            (_file_uri(native_storage_uri, files[0][0]), marker),
        )
        # An automatic marker would silently corrupt the second row.
        assert native_connection.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(target)).fetchall() == [
            (1, None),
            (2, None),
        ]
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))


def test_native_parquet_preserves_null_empty_and_marker_values(
    native_connection: psycopg.Connection, native_storage_uri: str
) -> None:
    files = native_connection.execute(
        "EXPORT INTO PARQUET %s FROM (SELECT 1::INT8 AS id, NULL::STRING AS label "
        "UNION ALL SELECT 2, '' UNION ALL SELECT 3, %s::STRING)",
        (native_storage_uri, r"\N"),
    ).fetchall()
    target = sql.Identifier(f"native_{uuid4().hex}")
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(target))
    try:
        native_connection.execute(
            sql.SQL("IMPORT INTO {} PARQUET DATA (%s)").format(target), (_file_uri(native_storage_uri, files[0][0]),)
        )
        assert native_connection.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(target)).fetchall() == [
            (1, None),
            (2, ""),
            (3, r"\N"),
        ]
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))


def test_native_csv_header_requires_explicit_skip(
    native_connection: psycopg.Connection,
    native_storage_uri: str,
    rustfs_service: "RustfsService",
    record_property: "Callable[[str, object], None]",
) -> None:
    parts = urlsplit(native_storage_uri)
    rustfs_filesystem(rustfs_service).pipe(f"{parts.netloc}{parts.path}/header.csv", b"id,label\n7,seven\n8,eight\n")
    source = _file_uri(native_storage_uri, "header.csv")
    target = sql.Identifier(f"native_{uuid4().hex}")
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY, label STRING)").format(target))
    try:
        with pytest.raises(psycopg.Error, match="error parsing row 1") as caught:
            native_connection.execute(sql.SQL("IMPORT INTO {} CSV DATA (%s)").format(target), (source,))
        record_property("csv_header_error", str(caught.value))
        native_connection.execute(sql.SQL("IMPORT INTO {} CSV DATA (%s) WITH skip = '1'").format(target), (source,))
        assert native_connection.execute(sql.SQL("SELECT * FROM {} ORDER BY id").format(target)).fetchall() == [
            (7, "seven"),
            (8, "eight"),
        ]
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))


@pytest.mark.parametrize("operation", ["EXPORT", "IMPORT"])
def test_native_storage_rejects_explicit_transactions(
    native_connection: psycopg.Connection,
    native_storage_uri: str,
    operation: str,
    record_property: "Callable[[str, object], None]",
) -> None:
    target = sql.Identifier(f"native_{uuid4().hex}")
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY)").format(target))
    statement = (
        sql.SQL("EXPORT INTO CSV %s FROM SELECT 1")
        if operation == "EXPORT"
        else sql.SQL("IMPORT INTO {} CSV DATA (%s)").format(target)
    )
    try:
        with (
            pytest.raises(psycopg.Error, match="cannot be used inside a multi-statement transaction") as caught,
            native_connection.transaction(),
        ):
            native_connection.execute(statement, (native_storage_uri,))
        record_property(f"{operation.lower()}_transaction_error", str(caught.value))
        assert native_connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    finally:
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))


def test_native_csv_export_has_no_column_names_option(
    native_connection: psycopg.Connection, native_storage_uri: str, record_property: "Callable[[str, object], None]"
) -> None:
    with pytest.raises(psycopg.Error, match='invalid option "column_names"') as caught:
        native_connection.execute("EXPORT INTO CSV %s WITH column_names FROM SELECT 1 AS id", (native_storage_uri,))
    record_property("csv_column_names_error", str(caught.value))


def test_native_custom_endpoint_requires_external_io_privilege(
    native_connection: psycopg.Connection, native_storage_uri: str, record_property: "Callable[[str, object], None]"
) -> None:
    role = sql.Identifier(f"native_role_{uuid4().hex}")
    target = sql.Identifier(f"native_{uuid4().hex}")
    files = native_connection.execute("EXPORT INTO CSV %s FROM SELECT 1 AS id", (native_storage_uri,)).fetchall()
    source = _file_uri(native_storage_uri, files[0][0])
    native_connection.execute(sql.SQL("CREATE ROLE {}").format(role))
    native_connection.execute(sql.SQL("CREATE TABLE {} (id INT8 PRIMARY KEY)").format(target))
    native_connection.execute(sql.SQL("GRANT ALL ON TABLE {} TO {}").format(target, role))
    try:
        native_connection.execute(sql.SQL("SET ROLE {}").format(role))
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as caught:
            native_connection.execute("EXPORT INTO CSV %s FROM SELECT 1", (native_storage_uri,))
        record_property("custom_endpoint_export_privilege_error", str(caught.value))
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as caught:
            native_connection.execute(sql.SQL("IMPORT INTO {} CSV DATA (%s)").format(target), (source,))
        record_property("custom_endpoint_import_privilege_error", str(caught.value))
        native_connection.execute("RESET ROLE")
        native_connection.execute(sql.SQL("GRANT SYSTEM EXTERNALIOIMPLICITACCESS TO {}").format(role))
        native_connection.execute(sql.SQL("SET ROLE {}").format(role))
        assert (
            native_connection.execute("EXPORT INTO CSV %s FROM SELECT 1", (native_storage_uri,)).fetchall()[0][1] == 1
        )
        native_connection.execute(sql.SQL("IMPORT INTO {} CSV DATA (%s)").format(target), (source,))
        assert native_connection.execute(sql.SQL("SELECT * FROM {}").format(target)).fetchall() == [(1,)]
    finally:
        native_connection.execute("RESET ROLE")
        native_connection.execute(sql.SQL("DROP TABLE {}").format(target))
        native_connection.execute(sql.SQL("REVOKE SYSTEM EXTERNALIOIMPLICITACCESS FROM {}").format(role))
        native_connection.execute(sql.SQL("DROP ROLE {}").format(role))


@pytest.mark.parametrize("client", ["asyncpg", "psycopg"])
async def test_native_export_async_clients_bind_uri_and_query_values(
    cockroachdb_service: "CockroachDBService", native_storage_uri: str, client: str
) -> None:
    if client == "asyncpg":
        pg_connection = await asyncpg.connect(
            host=cockroachdb_service.host,
            port=cockroachdb_service.port,
            database=cockroachdb_service.database,
            user="root",
            ssl=False,
        )
        try:
            pg_rows = await pg_connection.fetch(
                "EXPORT INTO PARQUET $1 FROM (SELECT $2::STRING AS label)", native_storage_uri, "O'Reilly"
            )
            assert len(pg_rows) == 1
            assert pg_rows[0]["rows"] == 1
        finally:
            await pg_connection.close()
    else:
        async with await psycopg.AsyncConnection.connect(
            host=cockroachdb_service.host,
            port=cockroachdb_service.port,
            dbname=cockroachdb_service.database,
            user="root",
            sslmode="disable",
            autocommit=True,
        ) as connection:
            cursor = await connection.execute(
                "EXPORT INTO PARQUET %s FROM (SELECT %s::STRING AS label)", (native_storage_uri, "O'Reilly")
            )
            rows = await cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == 1

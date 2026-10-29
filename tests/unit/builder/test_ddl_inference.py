"""Unit tests for Arrow schema and Python record DDL inference."""

from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

import pyarrow as pa
import pytest

from sqlspec.builder import CreateTable
from sqlspec.exceptions import SQLBuilderError
from sqlspec.utils.arrow_helpers import sql_type_from_arrow_type


@pytest.mark.parametrize(
    ("arrow_type", "dialect", "expected"),
    [
        (pa.bool_(), None, "BOOLEAN"),
        (pa.bool_(), "postgres", "BOOLEAN"),
        (pa.bool_(), "sqlite", "INTEGER"),
        (pa.bool_(), "duckdb", "BOOLEAN"),
        (pa.bool_(), "oracle", "NUMBER(1)"),
        (pa.bool_(), "mssql", "BIT"),
        (pa.int8(), "postgres", "SMALLINT"),
        (pa.int16(), "postgres", "SMALLINT"),
        (pa.uint8(), "postgres", "SMALLINT"),
        (pa.int32(), "postgres", "INTEGER"),
        (pa.uint16(), "postgres", "INTEGER"),
        (pa.int64(), "postgres", "BIGINT"),
        (pa.uint32(), "postgres", "BIGINT"),
        (pa.uint64(), "postgres", "BIGINT"),
        (pa.float32(), "postgres", "REAL"),
        (pa.float64(), None, "DOUBLE PRECISION"),
        (pa.float64(), "postgres", "DOUBLE PRECISION"),
        (pa.float64(), "duckdb", "DOUBLE"),
        (pa.float64(), "mysql", "DOUBLE"),
        (pa.decimal128(12, 2), "postgres", "DECIMAL(12, 2)"),
        (pa.date32(), "postgres", "DATE"),
        (pa.time64("us"), "postgres", "TIME"),
        (pa.timestamp("us"), "postgres", "TIMESTAMP"),
        (pa.timestamp("us", tz="UTC"), "postgres", "TIMESTAMP WITH TIME ZONE"),
        (pa.timestamp("us", tz="UTC"), "mssql", "DATETIME2(6)"),
        (pa.timestamp("us", tz="UTC"), "duckdb", "TIMESTAMP"),
        (pa.duration("us"), "postgres", "INTERVAL"),
        (pa.binary(), "postgres", "BYTEA"),
        (pa.binary(), "sqlite", "BLOB"),
        (pa.binary(), "mssql", "VARBINARY(MAX)"),
        (pa.uuid(), "postgres", "UUID"),
        (pa.uuid(), "sqlite", "TEXT"),
        (pa.uuid(), "oracle", "RAW(16)"),
        (pa.uuid(), "mssql", "UNIQUEIDENTIFIER"),
        (pa.list_(pa.int64()), "postgres", "JSONB"),
        (pa.struct([("a", pa.int64())]), "duckdb", "JSON"),
        (pa.map_(pa.string(), pa.int64()), "mssql", "NVARCHAR(MAX)"),
        (pa.string(), "postgres", "TEXT"),
        (pa.large_string(), "oracle", "CLOB"),
        (pa.string(), "mssql", "NVARCHAR(MAX)"),
    ],
)
def test_sql_type_from_arrow_type_across_dialects(arrow_type: Any, dialect: str | None, expected: str) -> None:
    """Test sql_type_from_arrow_type maps Arrow types to dialect-optimal SQL types."""
    assert sql_type_from_arrow_type(arrow_type, dialect=dialect) == expected


def test_create_table_from_arrow_schema_and_table() -> None:
    """Test CreateTable.from_arrow_schema builds DDL from pyarrow.Schema and pyarrow.Table."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("user_uuid", pa.uuid(), nullable=False),
        pa.field("email", pa.string(), nullable=False),
        pa.field("score", pa.float64(), nullable=True),
        pa.field("amount", pa.decimal128(10, 2), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("tags", pa.list_(pa.string()), nullable=True),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ])

    builder = CreateTable.from_arrow_schema(
        "users", schema, schema="analytics", primary_key="id", if_not_exists=True, dialect="postgres"
    )
    sql = builder.build().sql

    assert 'CREATE TABLE IF NOT EXISTS "analytics"."users"' in sql
    assert '"id" BIGINT NOT NULL PRIMARY KEY' in sql
    assert '"user_uuid" UUID NOT NULL' in sql
    assert '"email" TEXT NOT NULL' in sql
    assert '"score" DOUBLE PRECISION' in sql
    assert '"score" DOUBLE PRECISION NOT NULL' not in sql
    assert '"amount" DECIMAL(10, 2) NOT NULL' in sql
    assert '"is_active" BOOLEAN NOT NULL' in sql
    assert '"tags" JSONB' in sql
    assert '"created_at" TIMESTAMPTZ NOT NULL' in sql

    arrow_table = pa.table({
        "tenant_id": pa.array(["t1", "t2"], type=pa.string()),
        "event_id": pa.array([1, 2], type=pa.int64()),
        "payload": pa.array([b"a", None], type=pa.binary()),
    })
    composite_sql = (
        CreateTable
        .from_arrow_schema("events", arrow_table, primary_key=["tenant_id", "event_id"], dialect="duckdb")
        .build()
        .sql
    )
    assert '"tenant_id" TEXT NOT NULL' in composite_sql
    assert '"event_id" BIGINT NOT NULL' in composite_sql
    assert '"payload" BLOB' in composite_sql
    assert 'PRIMARY KEY ("tenant_id", "event_id")' in composite_sql


def test_create_table_from_records_type_inference_and_nullability() -> None:
    """Test CreateTable.from_records infers types, widening, and nullability across rows."""
    records: list[dict[str, Any]] = [
        {
            "id": UUID("12345678-1234-5678-1234-567812345678"),
            "count": 10,
            "ratio": 1,
            "amount": Decimal("19.99"),
            "active": True,
            "created_at": datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC),
            "birth_date": date(1995, 5, 15),
            "start_time": time(8, 30, 0),
            "metadata": {"source": "api"},
            "tags": ["a", "b"],
            "raw_bytes": b"\x00\x01",
            "optional_note": "present",
            "all_null": None,
        },
        {
            "id": "87654321-4321-8765-4321-876543218765",
            "count": 20,
            "ratio": 2.5,
            "amount": 5,
            "active": False,
            "created_at": "2025-01-03T10:20:30Z",
            "birth_date": "1996-06-16",
            "start_time": time(9, 45, 0),
            "metadata": {"source": "batch"},
            "tags": ["c"],
            "raw_bytes": b"\x02\x03",
            "all_null": None,
        },
    ]

    pg_sql = (
        CreateTable
        .from_records(
            "audit_records", records, schema="public", primary_key="id", if_not_exists=True, dialect="postgres"
        )
        .build()
        .sql
    )
    assert 'CREATE TABLE IF NOT EXISTS "public"."audit_records"' in pg_sql
    assert '"id" UUID NOT NULL PRIMARY KEY' in pg_sql
    assert '"count" BIGINT NOT NULL' in pg_sql
    assert '"ratio" DOUBLE PRECISION NOT NULL' in pg_sql
    assert '"amount" DECIMAL(38, 9) NOT NULL' in pg_sql
    assert '"active" BOOLEAN NOT NULL' in pg_sql
    assert '"created_at" TIMESTAMPTZ NOT NULL' in pg_sql
    assert '"birth_date" DATE NOT NULL' in pg_sql
    assert '"start_time" TIME NOT NULL' in pg_sql
    assert '"metadata" JSONB NOT NULL' in pg_sql
    assert '"tags" JSONB NOT NULL' in pg_sql
    assert '"raw_bytes" BYTEA NOT NULL' in pg_sql
    assert '"optional_note" TEXT' in pg_sql
    assert '"optional_note" TEXT NOT NULL' not in pg_sql
    assert '"all_null" TEXT' in pg_sql
    assert '"all_null" TEXT NOT NULL' not in pg_sql

    mssql_sql = CreateTable.from_records("audit_records", records, primary_key="id", dialect="tsql").build().sql
    assert "[id] UNIQUEIDENTIFIER NOT NULL PRIMARY KEY" in mssql_sql
    assert "[active] BIT NOT NULL" in mssql_sql
    assert "[metadata] NVARCHAR(MAX) NOT NULL" in mssql_sql
    assert "[raw_bytes] VARBINARY(MAX) NOT NULL" in mssql_sql


def test_create_table_from_records_composite_primary_key_and_empty_error() -> None:
    """Test CreateTable.from_records supports composite primary keys and rejects empty records."""
    with pytest.raises(SQLBuilderError, match="Cannot infer table schema from empty records"):
        CreateTable.from_records("empty_table", [])

    sql = (
        CreateTable
        .from_records(
            "memberships",
            [{"org_id": "o1", "user_id": None, "role": "admin"}],
            primary_key=["org_id", "user_id"],
            dialect="postgres",
        )
        .build()
        .sql
    )
    assert '"org_id" TEXT NOT NULL' in sql
    assert '"user_id" TEXT NOT NULL' in sql
    assert 'PRIMARY KEY ("org_id", "user_id")' in sql

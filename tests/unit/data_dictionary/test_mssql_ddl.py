"""Unit tests for SQL Server table DDL reconstruction from sys catalog rows."""

from typing import Any

import duckdb
import pytest

from sqlspec.data_dictionary import ForeignKeyMetadata, MetadataFidelity, MetadataSource, MetadataSupport
from sqlspec.data_dictionary.dialects.mssql.config import MSSQL_DDL_WARNING, build_mssql_table_ddl_result


def _column(name: str, ordinal: int, data_type: str, **overrides: Any) -> dict[str, Any]:
    """Return a column row shaped like ``mssql/sql/columns.sql`` output."""
    row: dict[str, Any] = {
        "schema_name": "Sales",
        "table_name": "Orders",
        "column_name": name,
        "data_type": data_type,
        "max_length": None,
        "numeric_precision": 0,
        "numeric_scale": 0,
        "is_nullable": False,
        "ordinal_position": ordinal,
        "is_identity": False,
        "identity_seed": None,
        "identity_increment": None,
        "is_computed": False,
        "computed_definition": None,
        "is_persisted_computed": None,
        "column_default": None,
        "primary_key_name": None,
        "primary_key_ordinal": None,
    }
    row.update(overrides)
    return row


def _index(name: str, type_desc: str, columns: str, **overrides: Any) -> dict[str, Any]:
    """Return an index row shaped like ``mssql/sql/indexes.sql`` output."""
    row: dict[str, Any] = {
        "schema_name": "Sales",
        "table_name": "Orders",
        "index_name": name,
        "type_desc": type_desc,
        "is_unique": False,
        "is_primary": False,
        "is_unique_constraint": False,
        "has_filter": False,
        "filter_definition": None,
        "data_space_name": "PRIMARY",
        "partition_scheme_name": None,
        "columns": columns,
        "included_columns": None,
    }
    row.update(overrides)
    return row


COLUMNS = [
    _column(
        "OrderId",
        1,
        "int",
        max_length=4,
        numeric_precision=10,
        is_identity=True,
        identity_seed=1,
        identity_increment=1,
        primary_key_name="PK_Orders",
        primary_key_ordinal=1,
    ),
    _column("CustomerName", 2, "nvarchar", max_length=200),
    _column("Total", 3, "decimal", max_length=9, numeric_precision=10, numeric_scale=2, column_default="((0))"),
    _column(
        "CreatedAt",
        4,
        "datetime2",
        max_length=7,
        numeric_precision=23,
        numeric_scale=3,
        column_default="(sysutcdatetime())",
    ),
    _column(
        "Tax",
        5,
        "decimal",
        max_length=9,
        numeric_precision=13,
        numeric_scale=3,
        is_nullable=True,
        is_computed=True,
        computed_definition="([Total]*(0.1))",
        is_persisted_computed=True,
    ),
]
INDEXES = [
    _index("PK_Orders", "CLUSTERED", "OrderId", is_unique=True, is_primary=True),
    _index(
        "IX_Orders_CustomerName",
        "NONCLUSTERED",
        "CustomerName",
        is_unique=True,
        has_filter=True,
        filter_definition="([CustomerName] IS NOT NULL)",
        included_columns="Total",
    ),
    _index("CIX_Orders_Created", "CLUSTERED", "CreatedAt"),
]
CONSTRAINTS = [
    {"constraint_name": "CK_Orders_Total", "constraint_type": "CHECK_CONSTRAINT", "definition": "([Total]>=(0))"}
]
FOREIGN_KEYS = [
    ForeignKeyMetadata(
        table_name="Orders",
        column_name="CustomerName",
        referenced_table="Customers",
        referenced_column="Name",
        constraint_name="FK_Orders_Customers",
        schema="Sales",
        referenced_schema="Sales",
    )
]


def test_mssql_table_ddl_preserves_case_clustered_indexes_and_filter_predicates() -> None:
    """SQL Server DDL keeps identifier case, CLUSTERED/NONCLUSTERED, identity, computed columns, and filters."""
    result = build_mssql_table_ddl_result(
        "Sales", "Orders", COLUMNS, INDEXES, constraint_rows=CONSTRAINTS, foreign_key_rows=FOREIGN_KEYS
    )

    assert result.status == MetadataSupport.SUPPORTED
    assert result.fidelity == MetadataFidelity.GENERATED
    assert result.source == MetadataSource.GENERATED
    assert result.warnings == (MSSQL_DDL_WARNING,)
    assert result.identity.quoted_name == "[Sales].[Orders]"
    assert result.ddl == (
        "CREATE TABLE [Sales].[Orders] (\n"
        "  [OrderId] INTEGER NOT NULL IDENTITY(1, 1),\n"
        "  [CustomerName] NVARCHAR(100) NOT NULL,\n"
        "  [Total] NUMERIC(10, 2) NOT NULL DEFAULT 0,\n"
        "  [CreatedAt] DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),\n"
        "  [Tax] AS (\n"
        "    [Total] * 0.1\n"
        "  ) PERSISTED,\n"
        "  CONSTRAINT [PK_Orders] PRIMARY KEY ([OrderId]),\n"
        "  CONSTRAINT [CK_Orders_Total] CHECK ([Total] >= 0),\n"
        "  CONSTRAINT [FK_Orders_Customers] FOREIGN KEY ([CustomerName]) REFERENCES [Sales].[Customers]([Name])\n"
        ");\n\n"
        "CREATE UNIQUE NONCLUSTERED INDEX [IX_Orders_CustomerName] ON [Sales].[Orders]([CustomerName]) "
        "INCLUDE ([Total])\nWHERE\n  NOT [CustomerName] IS NULL;\n\n"
        "CREATE CLUSTERED INDEX [CIX_Orders_Created] ON [Sales].[Orders]([CreatedAt]);"
    )


def test_mssql_table_ddl_transpiles_types_and_fragments_to_postgres() -> None:
    """A Postgres target parses T-SQL catalog fragments and types instead of raising on ``datetime2(3)``."""
    result = build_mssql_table_ddl_result(
        "Sales",
        "Orders",
        COLUMNS,
        INDEXES,
        constraint_rows=CONSTRAINTS,
        foreign_key_rows=FOREIGN_KEYS,
        target_dialect="postgres",
    )

    assert result.identity.dialect == "postgres"
    assert result.ddl == (
        'CREATE TABLE "Sales"."Orders" (\n'
        '  "OrderId" INT NOT NULL GENERATED ALWAYS AS IDENTITY,\n'
        '  "CustomerName" VARCHAR(100) NOT NULL,\n'
        '  "Total" DECIMAL(10, 2) NOT NULL DEFAULT 0,\n'
        '  "CreatedAt" TIMESTAMP(3) NOT NULL DEFAULT SYSUTCDATETIME(),\n'
        '  "Tax" DECIMAL(13, 3) GENERATED ALWAYS AS ("Total" * 0.1) STORED,\n'
        '  CONSTRAINT "PK_Orders" PRIMARY KEY ("OrderId"),\n'
        '  CONSTRAINT "CK_Orders_Total" CHECK ("Total" >= 0),\n'
        '  CONSTRAINT "FK_Orders_Customers" FOREIGN KEY ("CustomerName") REFERENCES "Sales"."Customers"("Name")\n'
        ");\n\n"
        'CREATE UNIQUE INDEX "IX_Orders_CustomerName" ON "Sales"."Orders"("CustomerName") INCLUDE ("Total")\n'
        'WHERE\n  NOT "CustomerName" IS NULL;\n\n'
        'CREATE INDEX "CIX_Orders_Created" ON "Sales"."Orders"("CreatedAt");'
    )


def test_mssql_table_ddl_for_duckdb_executes_and_reports_lossy_attributes() -> None:
    """DuckDB output executes on a fresh database; identity and stored-computed loss is reported."""
    columns = [{**row, "column_default": None} if row["column_name"] == "CreatedAt" else row for row in COLUMNS]
    indexes = [row for row in INDEXES if row["index_name"] != "IX_Orders_CustomerName"]
    result = build_mssql_table_ddl_result(
        "Sales",
        "Orders",
        columns,
        indexes,
        constraint_rows=CONSTRAINTS,
        foreign_key_rows=FOREIGN_KEYS,
        target_dialect="duckdb",
    )

    assert result.fidelity == MetadataFidelity.LOSSY
    assert result.warnings == (
        MSSQL_DDL_WARNING,
        (
            "Column 'OrderId' is an identity/auto-increment column; duckdb cannot express it on this column, so it "
            "is emitted without automatic value generation"
        ),
        (
            "Column 'Tax' is a stored generated column; duckdb supports only virtual generated columns, so it is "
            "emitted as virtual"
        ),
    )
    assert result.ddl is not None
    connection = duckdb.connect(":memory:")
    connection.execute('CREATE SCHEMA "Sales"')
    connection.execute("SET schema = 'Sales'")
    connection.execute('CREATE TABLE "Customers" ("Name" VARCHAR PRIMARY KEY)')
    connection.execute("INSERT INTO \"Customers\" VALUES ('ann')")
    for statement in result.ddl.split(";\n\n"):
        connection.execute(statement.rstrip(";"))
    connection.execute(
        'INSERT INTO "Sales"."Orders" ("OrderId", "CustomerName", "Total", "CreatedAt") '
        "VALUES (1, 'ann', 10, TIMESTAMP '2024-01-01')"
    )
    assert connection.execute('SELECT "Tax" FROM "Sales"."Orders"').fetchall() == [(pytest.approx(1.0),)]


def test_mssql_table_ddl_omits_unreproducible_columnstore_index_with_warning() -> None:
    """Columnstore indexes are reported instead of being emitted as plain row-store indexes."""
    result = build_mssql_table_ddl_result(
        "Sales", "Orders", COLUMNS, [_index("CCI_Orders", "CLUSTERED COLUMNSTORE", "OrderId")]
    )
    assert result.fidelity == MetadataFidelity.LOSSY
    assert result.warnings == (
        MSSQL_DDL_WARNING,
        "Index CCI_Orders: CLUSTERED COLUMNSTORE indexes are not reproduced; index omitted",
    )
    assert result.ddl is not None
    assert "CCI_Orders" not in result.ddl

"""Tests for additive schema ensure and diff behavior."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlspec import sql
from sqlspec.exceptions import MigrationError
from sqlspec.migrations.schema import SchemaTarget, ensure_schema_async, ensure_schema_sync, metadata_has_index


def test_ensure_diff_engine_reproduces_tracker_self_heal() -> None:
    target = SchemaTarget(
        "ddl_migrations",
        sql.create_table("ddl_migrations").column("version_num", "VARCHAR(32)").column("checksum", "VARCHAR(64)"),
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "DDL_MIGRATIONS"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "VERSION_NUM"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.added_columns == {"ddl_migrations": ["checksum"]}
    driver.execute.assert_called_once()
    assert "ALTER TABLE" in str(driver.execute.call_args.args[0]).upper()
    assert "ADD COLUMN" in str(driver.execute.call_args.args[0]).upper()


def test_ensure_diff_creates_missing_table_idempotently() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    driver.data_dictionary.get_tables.side_effect = [[], [{"table_name": "widgets"}]]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}]

    first = ensure_schema_sync(driver, [target], manage_schema=True)
    second = ensure_schema_sync(driver, [target], manage_schema=True)

    assert first.created_tables == ["widgets"]
    assert second.created_tables == []
    driver.execute.assert_called_once_with(target.create_table)


def test_schema_target_from_ddl_preserves_create_statement_and_parses_columns() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS widgets (
        id INTEGER PRIMARY KEY,
        label VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_widgets_label ON widgets(label);
    """

    target = SchemaTarget.from_ddl("widgets", ddl, dialect="sqlite")

    assert target.create_statement == ddl
    assert [column.name for column in target.create_table.columns] == ["id", "label", "created_at"]
    assert target.create_table.columns[1].not_null is True
    assert target.create_table.columns[2].default == "CURRENT_TIMESTAMP"


def test_ensure_diff_executes_canonical_ddl_as_script() -> None:
    ddl = "CREATE TABLE widgets (id INTEGER); CREATE INDEX idx_widgets_id ON widgets(id);"
    target = SchemaTarget.from_ddl("widgets", ddl)
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = []

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.created_tables == ["widgets"]
    driver.execute_script.assert_called_once_with(ddl)
    driver.execute.assert_not_called()


def test_ensure_diff_additive_only_ignores_rename() -> None:
    target = SchemaTarget(
        "widgets", sql.create_table("widgets").column("id", "INTEGER").column("new_name", "VARCHAR(50)")
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}, {"column_name": "old_name"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.added_columns == {}
    assert result.deferred_tables == ["widgets"]
    driver.execute.assert_not_called()


def test_ensure_diff_checksum_ledger_untouched() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [{"column_name": "id"}]

    result = ensure_schema_sync(driver, [target], manage_schema=True)

    assert result.created_tables == []
    assert result.added_columns == {}
    assert all("DDL_MIGRATIONS" not in str(call.args[0]).upper() for call in driver.execute.call_args_list)


def test_manage_schema_false_is_noop() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()

    result = ensure_schema_sync(driver, [target], manage_schema=False)

    assert result.created_tables == []
    assert result.added_columns == {}
    driver.data_dictionary.get_tables.assert_not_called()
    driver.data_dictionary.get_columns.assert_not_called()
    driver.execute.assert_not_called()


def test_run_migrations_flag_invokes_explicit_migration_path() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    migration_runner = MagicMock()

    result = ensure_schema_sync(
        driver, [target], manage_schema=False, run_migrations=True, migration_runner=migration_runner
    )

    assert result.migrations_run is True
    migration_runner.assert_called_once_with(driver)
    driver.data_dictionary.get_tables.assert_not_called()


@pytest.mark.anyio
async def test_async_ensure_diff_adds_missing_column() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER").column("label", "VARCHAR(50)"))
    driver = MagicMock()
    driver.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "widgets"}])
    driver.data_dictionary.get_columns = AsyncMock(return_value=[{"column_name": "id"}])
    driver.execute = AsyncMock()
    driver.commit = AsyncMock()

    result = await ensure_schema_async(driver, [target], manage_schema=True)

    assert result.added_columns == {"widgets": ["label"]}
    driver.execute.assert_awaited_once()
    driver.commit.assert_awaited_once()


def test_schema_target_from_ddl_ignores_parentheses_in_strings_and_comments() -> None:
    ddl = """
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE widgets (
            id NUMBER PRIMARY KEY,
            /* comment with ) inside */
            delim VARCHAR2(10) DEFAULT '')'',
            label VARCHAR2(50) NOT NULL
        )';
    END;
    """

    target = SchemaTarget.from_ddl("widgets", ddl, dialect="oracle")

    assert [column.name for column in target.create_table.columns] == ["id", "delim", "label"]
    assert target.create_table.columns[2].not_null is True


@pytest.mark.parametrize(
    ("dialect", "ddl", "columns"),
    [
        (
            "tsql",
            "IF OBJECT_ID(N'widgets', N'U') IS NULL BEGIN CREATE TABLE widgets (id INT, [O'Brien] VARCHAR(50)); END;",
            ["id", "O'Brien"],
        ),
        (
            "oracle",
            (
                "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE widgets (id NUMBER, label VARCHAR2(50))'; "
                "EXCEPTION WHEN OTHERS THEN raise_application_error(-20001, q'[can't create table]'); END;"
            ),
            ["id", "label"],
        ),
    ],
)
def test_schema_target_from_wrapped_ddl_preserves_dialect_quotes(dialect: str, ddl: str, columns: list[str]) -> None:
    target = SchemaTarget.from_ddl("widgets", ddl, dialect=dialect)

    assert [column.name for column in target.create_table.columns] == columns
    assert target.create_statement == ddl


@pytest.mark.parametrize(
    "ddl",
    [
        "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE widgets (id NUMBER); END;",
        "BEGIN EXECUTE IMMEDIATE 'CREATE TABLE widgets (id NUMBER, label VARCHAR2(50)'; END;",
    ],
)
def test_schema_target_rejects_incomplete_wrapped_ddl(ddl: str) -> None:
    with pytest.raises(ValueError, match="Unable to parse CREATE TABLE DDL"):
        SchemaTarget.from_ddl("widgets", ddl, dialect="oracle")


def test_schema_target_builder_extracts_constraints_and_unsafe_columns() -> None:
    builder = (
        sql
        .create_table("items")
        .column("id", "INT")
        .column("code", "VARCHAR(32)", unique=True)
        .column("owner_id", "INT")
        .column("qty", "INT")
        .column("inline_chk", "INT", check="inline_chk > 0")
        .column("gen_col", "INT", generated=True, generated_expression="qty * 2")
        .primary_key_constraint("id")
        .unique_constraint(["owner_id", "code"])
        .foreign_key_constraint("owner_id", "accounts", "account_id")
        .check_constraint("qty >= 0")
    )

    target = SchemaTarget("items", builder)

    assert target.constraint_columns == frozenset({"id", "code", "owner_id", "qty", "inline_chk"})
    assert "account_id" not in target.constraint_columns
    assert target.unsafe_add_columns == frozenset({"inline_chk", "gen_col"})


def test_schema_target_from_ddl_extracts_inline_and_table_constraints() -> None:
    ddl = """
    CREATE TABLE items (
        id INT PRIMARY KEY,
        code VARCHAR(32) UNIQUE,
        parent_id INT REFERENCES parents(parent_pk),
        score INT CHECK (score >= 0),
        gen_col INT GENERATED ALWAYS AS (score + 1) STORED,
        org_id INT,
        tenant_id INT,
        slug VARCHAR(32),
        total INT,
        FOREIGN KEY (org_id) REFERENCES orgs(org_pk),
        UNIQUE (tenant_id, slug),
        CONSTRAINT chk_total CHECK (total >= 0)
    )
    """

    target = SchemaTarget.from_ddl("items", ddl)

    assert target.constraint_columns == frozenset({
        "id",
        "code",
        "parent_id",
        "score",
        "org_id",
        "tenant_id",
        "slug",
        "total",
    })
    assert "parent_pk" not in target.constraint_columns
    assert "org_pk" not in target.constraint_columns
    assert target.unsafe_add_columns == frozenset({"score", "gen_col"})


@pytest.mark.parametrize(
    "ddl",
    [
        (
            "CREATE TABLE litestar_session ("
            "shard_id INT64 NOT NULL, session_id STRING(255) NOT NULL, expires_at TIMESTAMP"
            ") PRIMARY KEY (shard_id, session_id DESC), "
            "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 1 DAY))"
        ),
        (
            "CREATE TABLE litestar_session ("
            "shard_id INT64 NOT NULL, session_id STRING(255) NOT NULL, expires_at TIMESTAMP"
            ") PRIMARY KEY (shard_id, session_id DESC)"
        ),
    ],
    ids=["with-row-deletion-policy", "without-row-deletion-policy"],
)
def test_schema_target_from_ddl_googlesql_properties_primary_key_and_row_deletion_policy(ddl: str) -> None:
    target = SchemaTarget.from_ddl("litestar_session", ddl, dialect="bigquery")

    assert target.constraint_columns == frozenset({"shard_id", "session_id"})
    assert target.create_statement == ddl


def test_schema_target_from_ddl_duckdb_struct_and_virtual_generated_columns() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS adk_events (
        id VARCHAR PRIMARY KEY,
        meta STRUCT(author VARCHAR, payload JSON),
        raw JSON,
        version VARCHAR GENERATED ALWAYS AS (json_extract_string(raw, '$.v')) VIRTUAL
    )
    """

    target = SchemaTarget.from_ddl("adk_events", ddl, dialect="duckdb")

    col_names = [col.name for col in target.create_table.columns]
    assert col_names == ["id", "meta", "raw", "version"]
    assert "author" not in col_names
    assert "payload" not in col_names
    assert "version" in target.unsafe_add_columns
    assert "VIRTUAL" in str(target.create_statement)
    assert target.create_statement == ddl


def test_schema_target_from_ddl_tsql_explicit_null_is_nullable() -> None:
    ddl = "CREATE TABLE dbo.sessions (session_id NVARCHAR(255) NOT NULL PRIMARY KEY, expires_at DATETIME2(6) NULL)"

    target = SchemaTarget.from_ddl("sessions", ddl, schema="dbo", dialect="tsql")

    assert target.create_table.columns[0].not_null is True
    assert target.create_table.columns[1].not_null is False
    assert target.constraint_columns == frozenset({"session_id"})


def test_metadata_has_index_case_insensitive_and_schema_scoped() -> None:
    dict_rows = [{"INDEX_NAME": "IDX_SESSIONS_EXPIRES", "TABLE_NAME": "SESSIONS", "TABLE_SCHEMA": "DBO"}]
    attr_rows = [SimpleNamespace(index_name="idx_sessions_expires", table_name="sessions", schema_name="dbo")]

    assert metadata_has_index(dict_rows, "sessions", "idx_sessions_expires") is True
    assert metadata_has_index(dict_rows, "sessions", "idx_sessions_expires", schema_name="dbo") is True
    assert metadata_has_index(dict_rows, "sessions", "idx_sessions_expires", schema_name="audit") is False
    assert metadata_has_index(dict_rows, "other_table", "idx_sessions_expires", schema_name="dbo") is False
    assert metadata_has_index(attr_rows, "SESSIONS", "IDX_SESSIONS_EXPIRES", schema_name="DBO") is True
    assert metadata_has_index(attr_rows, "SESSIONS", "IDX_SESSIONS_EXPIRES", schema_name="audit") is False


@pytest.mark.anyio
async def test_ensure_schema_commit_false_suppresses_driver_commit() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER").column("label", "VARCHAR(50)"))

    sync_no_commit = MagicMock()
    sync_no_commit.data_dictionary.get_tables.return_value = []
    ensure_schema_sync(sync_no_commit, [target], manage_schema=True, commit=False)
    sync_no_commit.commit.assert_not_called()

    sync_with_commit = MagicMock()
    sync_with_commit.data_dictionary.get_tables.return_value = []
    ensure_schema_sync(sync_with_commit, [target], manage_schema=True, commit=True)
    sync_with_commit.commit.assert_called_once()

    async_no_commit = MagicMock()
    async_no_commit.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "widgets"}])
    async_no_commit.data_dictionary.get_columns = AsyncMock(return_value=[{"column_name": "id"}])
    async_no_commit.execute = AsyncMock()
    async_no_commit.commit = AsyncMock()
    await ensure_schema_async(async_no_commit, [target], manage_schema=True, commit=False)
    async_no_commit.commit.assert_not_awaited()

    async_with_commit = MagicMock()
    async_with_commit.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "widgets"}])
    async_with_commit.data_dictionary.get_columns = AsyncMock(return_value=[{"column_name": "id"}])
    async_with_commit.execute = AsyncMock()
    async_with_commit.commit = AsyncMock()
    await ensure_schema_async(async_with_commit, [target], manage_schema=True, commit=True)
    async_with_commit.commit.assert_awaited_once()


def test_ensure_schema_strict_allows_nullable_and_defaulted_not_null_additions() -> None:
    target = SchemaTarget(
        "widgets",
        sql
        .create_table("widgets")
        .column("id", "INT", primary_key=True, not_null=True)
        .column("note", "TEXT")
        .column("attempts", "INT", not_null=True, default="0"),
    )
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "INT", "is_nullable": "NO"},
        {"column_name": "custom_one", "data_type": "TEXT", "is_nullable": "YES"},
        {"column_name": "custom_two", "data_type": "TEXT", "is_nullable": "YES"},
    ]

    result = ensure_schema_sync(driver, [target], manage_schema=True, commit=False, strict=True)

    assert result.added_columns == {"widgets": ["attempts", "note"]}
    assert result.deferred_tables == []
    assert driver.execute.call_count == 2
    driver.commit.assert_not_called()


def test_ensure_schema_strict_refuses_missing_table_when_create_schema_false() -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("id", "INTEGER"))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = []

    permissive = ensure_schema_sync(driver, [target], manage_schema=True, create_schema=False, strict=False)
    assert permissive.deferred_tables == ["widgets"]

    with pytest.raises(MigrationError, match="widgets"):
        ensure_schema_sync(driver, [target], manage_schema=True, create_schema=False, strict=True)


@pytest.mark.anyio
async def test_ensure_schema_strict_refuses_not_null_without_default_and_preflights_multi_targets() -> None:
    missing_table_a = SchemaTarget.from_ddl("table_a", "CREATE TABLE table_a (id INT PRIMARY KEY)")
    existing_table_b = SchemaTarget(
        "table_b",
        sql
        .create_table("table_b")
        .column("id", "INT", primary_key=True, not_null=True)
        .column("required_col", "VARCHAR(32)", not_null=True),
    )

    sync_driver = MagicMock()
    sync_driver.data_dictionary.get_tables.return_value = [{"table_name": "table_b"}]
    sync_driver.data_dictionary.get_columns.return_value = [
        {"column_name": "id", "data_type": "INT", "is_nullable": "NO"}
    ]

    with pytest.raises(MigrationError, match=r"table_b.*required_col|required_col.*table_b") as exc_info:
        ensure_schema_sync(
            sync_driver, [missing_table_a, existing_table_b], manage_schema=True, commit=False, strict=True
        )
    assert 'Add the column with an application migration, or set migration_mode="versioned" for this extension.' in str(
        exc_info.value
    )
    sync_driver.execute_script.assert_not_called()
    sync_driver.execute.assert_not_called()

    async_driver = MagicMock()
    async_driver.data_dictionary.get_tables = AsyncMock(return_value=[{"table_name": "table_b"}])
    async_driver.data_dictionary.get_columns = AsyncMock(
        return_value=[{"column_name": "id", "data_type": "INT", "is_nullable": "NO"}]
    )
    async_driver.execute_script = AsyncMock()
    async_driver.execute = AsyncMock()

    with pytest.raises(MigrationError, match=r"table_b.*required_col|required_col.*table_b"):
        await ensure_schema_async(
            async_driver, [missing_table_a, existing_table_b], manage_schema=True, commit=False, strict=True
        )
    async_driver.execute_script.assert_not_awaited()
    async_driver.execute.assert_not_awaited()


@pytest.mark.parametrize(
    ("target_factory", "expected_col"),
    [
        (
            lambda: SchemaTarget.from_ddl(
                "litestar_session",
                "CREATE TABLE litestar_session (shard_id INT64, session_id STRING(255) NOT NULL) "
                "PRIMARY KEY (shard_id, session_id)",
                dialect="bigquery",
            ),
            "shard_id",
        ),
        (
            lambda: SchemaTarget(
                "items",
                sql
                .create_table("items")
                .column("id", "INT", primary_key=True, not_null=True)
                .column("code", "VARCHAR(32)", unique=True),
            ),
            "code",
        ),
        (
            lambda: SchemaTarget(
                "items",
                sql
                .create_table("items")
                .column("id", "INT", primary_key=True, not_null=True)
                .column("gen_col", "INT", generated=True, generated_expression="id + 1"),
            ),
            "gen_col",
        ),
    ],
    ids=["googlesql-pk", "unique-col", "generated-col"],
)
def test_ensure_schema_strict_refuses_constrained_and_unsafe_missing_columns(
    target_factory: object, expected_col: str
) -> None:
    target = target_factory() if callable(target_factory) else target_factory
    assert isinstance(target, SchemaTarget)
    present_name = "session_id" if expected_col == "shard_id" else "id"
    present_type = "STRING(255)" if expected_col == "shard_id" else "INT"
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": target.table_name}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": present_name, "data_type": present_type, "is_nullable": "NO"}
    ]

    with pytest.raises(MigrationError) as exc_info:
        ensure_schema_sync(driver, [target], manage_schema=True, commit=False, strict=True)

    message = str(exc_info.value)
    assert target.table_name in message
    assert expected_col in message
    assert message.endswith(
        'Add the column with an application migration, or set migration_mode="versioned" for this extension.'
    )


def test_ensure_schema_strict_refuses_nullability_narrowing_and_cross_family_type_drift() -> None:
    narrowed_target = SchemaTarget("widgets", sql.create_table("widgets").column("label", "VARCHAR(50)", not_null=True))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "label", "data_type": "VARCHAR(50)", "is_nullable": "YES"}
    ]
    with pytest.raises(MigrationError) as exc_narrow:
        ensure_schema_sync(driver, [narrowed_target], manage_schema=True, commit=False, strict=True)
    assert str(exc_narrow.value).endswith(
        'Align the column with an application migration, or set migration_mode="versioned" for this extension.'
    )

    type_drift_target = SchemaTarget("widgets", sql.create_table("widgets").column("label", "TEXT"))
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "label", "data_type": "INTEGER", "is_nullable": "YES"}
    ]
    with pytest.raises(MigrationError) as exc_text_int:
        ensure_schema_sync(driver, [type_drift_target], manage_schema=True, commit=False, strict=True)
    assert str(exc_text_int.value).endswith(
        'Align the column with an application migration, or set migration_mode="versioned" for this extension.'
    )

    blob_target = SchemaTarget("widgets", sql.create_table("widgets").column("payload", "BLOB"))
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "payload", "data_type": "TIMESTAMP", "is_nullable": "YES"}
    ]
    with pytest.raises(MigrationError) as exc_blob_ts:
        ensure_schema_sync(driver, [blob_target], manage_schema=True, commit=False, strict=True)
    assert str(exc_blob_ts.value).endswith(
        'Align the column with an application migration, or set migration_mode="versioned" for this extension.'
    )

    widened_target = SchemaTarget("widgets", sql.create_table("widgets").column("label", "VARCHAR(50)", not_null=False))
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "label", "data_type": "VARCHAR(50)", "is_nullable": "NO"}
    ]
    result = ensure_schema_sync(driver, [widened_target], manage_schema=True, commit=False, strict=True)
    assert result.added_columns == {}
    driver.execute.assert_not_called()


@pytest.mark.parametrize(
    ("target_type", "catalog_type"),
    [
        ("INTEGER", "INT"),
        ("BIGINT", "INT8"),
        ("INT64", "INT64"),
        ("NUMBER(10)", "NUMBER"),
        ("DECFLOAT", "DECFLOAT"),
        ("REAL", "REAL"),
        ("BOOLEAN", "TINYINT"),
        ("BOOL", "BOOL"),
        ("BIT", "BIT"),
        ("VARCHAR(64)", "character varying"),
        ("VARCHAR2(64)", "VARCHAR2"),
        ("NVARCHAR(128)", "nvarchar"),
        ("STRING(128)", "STRING(128)"),
        ("TEXT", "LONGTEXT"),
        ("CLOB", "CLOB"),
        ("LONG VARCHAR", "long varchar"),
        ("BLOB", "BLOB"),
        ("BYTEA", "bytea"),
        ("BYTES(MAX)", "BYTES(MAX)"),
        ("IMAGE", "image"),
        ("LONGBLOB", "longblob"),
        ("LONG BINARY", "long binary"),
        ("VARBINARY(MAX)", "varbinary"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("TIMESTAMPTZ", "timestamp with time zone"),
        ("DATETIME2(6)", "datetime2"),
        ("BIGDATETIME", "bigdatetime"),
        ("UNIQUEIDENTIFIER", "uniqueidentifier"),
        ("UUID", "uuid"),
        ("ARRAY<FLOAT32>", "ARRAY<FLOAT32>"),
        ("VECTOR(768)", "vector"),
        ("TSVECTOR", "tsvector"),
        ("TOKENLIST", "TOKENLIST"),
    ],
)
def test_ensure_schema_strict_accepts_every_builtin_ddl_type(target_type: str, catalog_type: str) -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets").column("col", target_type))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "col", "data_type": catalog_type, "is_nullable": "YES"}
    ]

    result = ensure_schema_sync(driver, [target], manage_schema=True, commit=False, strict=True)

    assert result.added_columns == {}
    driver.execute.assert_not_called()


@pytest.mark.parametrize(
    ("dialect", "target_type", "catalog_type"),
    [
        ("postgres", "JSONB", "TEXT"),
        ("postgres", "JSON", "JSON"),
        ("postgres", "TEXT", "JSON"),
        ("mysql", "JSON", "LONGTEXT"),
        ("tsql", "JSON", "NVARCHAR(MAX)"),
        ("oracle", "JSON", "BLOB"),
        ("oracle", "JSON", "CLOB"),
        ("oracle", "BLOB", "JSON"),
        ("sqlite", "JSON", "TEXT"),
    ],
)
def test_ensure_schema_strict_accepts_json_as_text_or_binary(dialect: str, target_type: str, catalog_type: str) -> None:
    target = SchemaTarget("widgets", sql.create_table("widgets", dialect=dialect).column("payload", target_type))
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "payload", "data_type": catalog_type, "is_nullable": "YES"}
    ]

    result = ensure_schema_sync(driver, [target], manage_schema=True, commit=False, strict=True)

    assert result.added_columns == {}
    driver.execute.assert_not_called()


def test_ensure_schema_strict_skips_unknown_tokens(caplog: pytest.LogCaptureFixture) -> None:
    driver = MagicMock()
    driver.data_dictionary.get_tables.return_value = [{"table_name": "widgets"}]

    caplog.set_level(logging.DEBUG, logger="sqlspec.migrations.schema")

    custom_target = SchemaTarget("widgets", sql.create_table("widgets").column("shape", "CUSTOMTYPE"))
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "shape", "data_type": "INTEGER", "is_nullable": "YES"}
    ]
    ensure_schema_sync(driver, [custom_target], manage_schema=True, commit=False, strict=True)

    geom_target = SchemaTarget("widgets", sql.create_table("widgets").column("shape", "INTEGER"))
    driver.data_dictionary.get_columns.return_value = [
        {"column_name": "shape", "data_type": "geometry", "is_nullable": "YES"}
    ]
    ensure_schema_sync(driver, [geom_target], manage_schema=True, commit=False, strict=True)

    unclassified_records = [
        record for record in caplog.records if record.getMessage() == "migration.schema.type_unclassified"
    ]
    assert len(unclassified_records) == 2


@pytest.mark.parametrize(("catalog_type", "target_type"), [("VARIANT", "JSON"), ("JSON", "VARIANT")])
def test_ensure_schema_json_variant_compatibility_is_symmetric(catalog_type: str, target_type: str) -> None:
    from sqlspec.adapters.sqlite import SqliteConfig

    config = SqliteConfig(connection_config={"database": ":memory:"})
    try:
        with config.provide_session() as driver:
            driver.execute(f"CREATE TABLE documents (payload {catalog_type})")
            target = SchemaTarget("documents", sql.create_table("documents").column("payload", target_type))
            result = ensure_schema_sync(driver, [target], manage_schema=True, commit=False, strict=True)
            assert result.created_tables == []
            assert result.added_columns == {}
    finally:
        config.close_pool()

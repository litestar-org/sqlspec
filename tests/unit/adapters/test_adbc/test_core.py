"""Unit tests for ADBC core execute-many helpers."""

import json
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path
from types import NoneType, SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, call
from uuid import UUID

import pyarrow as pa
import pytest
import sqlglot
from adbc_driver_manager import AdbcStatusCode, DatabaseError, NotSupportedError

from sqlspec.adapters.adbc import AdbcConfig, AdbcDriver
from sqlspec.adapters.adbc import core as adbc_core
from sqlspec.adapters.adbc import driver as adbc_driver_mod
from sqlspec.adapters.adbc._typing import AdbcSessionContext
from sqlspec.adapters.adbc.core import (
    _TERADATA_INDEX_EXISTS_SQL,
    _TERADATA_TABLE_EXISTS_SQL,
    build_connection_config,
    collect_rows,
    driver_from_uri,
    driver_kind_from_uri,
    get_statement_config,
    prepare_batch_with_casts,
    prepare_parameters_with_casts,
    prepare_postgres_parameters,
    prepare_postgres_uuid_bindings,
    resolve_column_names,
    resolve_dialect_from_config,
    resolve_many_rowcount,
    resolve_reconcile_adbc_dialect,
    split_teradata_table_name,
)
from sqlspec.adapters.adbc.type_converter import get_adbc_type_converter
from sqlspec.core import SQL, ParameterStyle
from sqlspec.core.compiler import SQLProcessor
from sqlspec.dialects import PGTextSearch
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)


def test_create_mapped_exception_maps_40001_to_serialization_conflict() -> None:
    """SQLSTATE 40001 maps to a serialization conflict instead of a deadlock."""
    error = DatabaseError("serialization failure", status_code=AdbcStatusCode.UNKNOWN, sqlstate="40001")

    mapped = adbc_core.create_mapped_exception(error)

    assert isinstance(mapped, SerializationConflictError)
    assert not isinstance(mapped, DeadlockError)


def test_create_mapped_exception_still_maps_40p01_to_deadlock() -> None:
    """SQLSTATE 40P01 remains classified as a deadlock."""
    error = DatabaseError("deadlock detected", status_code=AdbcStatusCode.UNKNOWN, sqlstate="40P01")

    mapped = adbc_core.create_mapped_exception(error)

    assert isinstance(mapped, DeadlockError)


def test_create_mapped_exception_maps_cancelled_status_to_operation_cancelled() -> None:
    error = DatabaseError("operation cancelled", status_code=AdbcStatusCode.CANCELLED)

    assert isinstance(adbc_core.create_mapped_exception(error), OperationCancelledError)


def test_create_mapped_exception_maps_timeout_status_to_query_timeout() -> None:
    error = DatabaseError("operation timed out", status_code=AdbcStatusCode.TIMEOUT)

    assert isinstance(adbc_core.create_mapped_exception(error), QueryTimeoutError)


def test_create_mapped_exception_maps_ambiguous_57014_to_operational_error() -> None:
    error = DatabaseError("query failed", status_code=AdbcStatusCode.UNKNOWN, sqlstate="57014")

    assert type(adbc_core.create_mapped_exception(error)) is OperationalError


def test_create_mapped_exception_prefers_timeout_marker_over_cancel_marker() -> None:
    error = DatabaseError(
        "canceling statement due to statement timeout", status_code=AdbcStatusCode.UNKNOWN, sqlstate="57014"
    )

    assert isinstance(adbc_core.create_mapped_exception(error), QueryTimeoutError)


def test_prepare_postgres_parameters_fast_path_without_casts() -> None:
    statement_config = get_statement_config("postgres")

    prepared = prepare_postgres_parameters(
        {}, {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared is None


def test_resolve_many_rowcount_prefers_driver_rowcount() -> None:
    cursor = SimpleNamespace(rowcount=7)

    assert resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}]) == 7


def test_resolve_many_rowcount_falls_back_to_parameter_count() -> None:
    cursor = SimpleNamespace(rowcount=-1)

    assert resolve_many_rowcount(cursor, [{"id": 1}, {"id": 2}, {"id": 3}]) == 3


def test_resolve_many_rowcount_returns_unknown_for_unsized_payload() -> None:
    cursor = SimpleNamespace(rowcount=-1)
    payload: Any = iter(({"id": 1}, {"id": 2}))

    assert resolve_many_rowcount(cursor, payload) == -1


def test_resolve_many_rowcount_supports_sized_non_sequence_payload() -> None:
    cursor = SimpleNamespace(rowcount=-1)

    assert resolve_many_rowcount(cursor, range(4)) == 4


def test_resolve_many_rowcount_uses_precomputed_fallback_count() -> None:
    cursor = SimpleNamespace(rowcount=-1)
    payload: Any = iter(({"id": 1}, {"id": 2}))

    assert resolve_many_rowcount(cursor, payload, fallback_count=6) == 6


def test_resolve_column_names_reuses_cached_description() -> None:
    description = [("id", object()), ("name", object())]
    cache: dict[int, tuple[object, list[str]]] = {}

    first = resolve_column_names(description, cache)
    second = resolve_column_names(description, cache)

    assert first == ["id", "name"]
    assert second is first
    assert len(cache) == 1


def test_collect_rows_uses_precomputed_column_names() -> None:
    rows = [(1, "alice")]
    description = [("ignored", object())]

    data, column_names = collect_rows(rows, description, column_names=["id", "name"])

    assert data is rows
    assert column_names == ["id", "name"]


def test_prepare_parameters_with_casts_uses_custom_json_serializer() -> None:
    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            json_serializer=lambda value: json.dumps({"custom": value}, separators=(",", ":"))
        )
    )

    prepared = prepare_parameters_with_casts([{"id": 1}], {}, statement_config, dialect="postgres", json_serializer=str)

    assert prepared == ['{"custom":{"id":1}}']


def test_prepare_batch_with_casts_uses_custom_json_serializer_for_each_row() -> None:
    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            json_serializer=lambda value: json.dumps({"custom": value}, separators=(",", ":"))
        )
    )

    prepared = prepare_batch_with_casts(
        [(1, {"name": "alice"}), (2, {"name": "bob"}), (3, {"name": "carol"})],
        {1: "INT"},
        statement_config,
        dialect="postgres",
        json_serializer=str,
    )

    assert prepared == [
        (1, '{"custom":{"name":"alice"}}'),
        (2, '{"custom":{"name":"bob"}}'),
        (3, '{"custom":{"name":"carol"}}'),
    ]


def test_prepare_parameters_with_casts_supports_subclass_type_dispatch() -> None:
    class MyInt(int):
        pass

    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(type_coercion_map={int: lambda value: value + 1})
    )

    prepared = prepare_parameters_with_casts(
        [MyInt(4)], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [5]


def test_prepare_parameters_with_casts_supports_virtual_abc_dispatch() -> None:
    statement_config = get_statement_config("postgres")
    statement_config = statement_config.replace(
        parameter_config=statement_config.parameter_config.replace(
            type_coercion_map={Sequence: lambda value: tuple(value)}
        )
    )

    prepared = prepare_parameters_with_casts(
        [[1, 2]], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == [(1, 2)]


def test_adbc_driver_resolves_count_stream_rowcount_from_tuple() -> None:
    """FlightSQL count streams should be consumed and used as rowcount."""
    cursor = SimpleNamespace(description=[("rows",)], fetchall=lambda: [(3,)])

    rowcount = AdbcDriver._resolve_count_result_rowcount(cast("Any", cursor), fallback=-1)

    assert rowcount == 3


def test_adbc_driver_resolves_count_stream_rowcount_from_dict() -> None:
    """FlightSQL count streams may return dict-shaped rows."""
    cursor = SimpleNamespace(description=[("rows",)], fetchall=lambda: [{"rows": 4}])

    rowcount = AdbcDriver._resolve_count_result_rowcount(cast("Any", cursor), fallback=-1)

    assert rowcount == 4


def test_adbc_driver_count_stream_rowcount_uses_fallback_without_description() -> None:
    """Non-FlightSQL cursors without count streams should keep fallback rowcount."""
    cursor = SimpleNamespace(description=None, fetchall=lambda: [(3,)])

    rowcount = AdbcDriver._resolve_count_result_rowcount(cast("Any", cursor), fallback=2)

    assert rowcount == 2


def test_adbc_driver_detects_flightsql_connection() -> None:
    """FlightSQL connections should use the row-by-row execute-many path."""
    connection = SimpleNamespace(adbc_get_info=lambda: {"driver_name": "Flight SQL"})

    assert AdbcDriver._detect_flightsql_connection(cast("Any", connection)) is True


def test_detect_dialect_uses_fallback_when_introspection_returns_unknown() -> None:
    """detect_dialect honors fallback_dialect when GetInfo yields no pattern match."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect="sqlite")
    assert result == "sqlite"


def test_detect_dialect_still_defaults_to_postgres_without_fallback() -> None:
    """Without fallback_dialect, the legacy 'default to postgres' path still fires."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect=None)
    assert result == "postgres"


def test_detect_dialect_introspection_match_beats_fallback() -> None:
    """An introspection match always wins over the fallback signal."""

    conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "duckdb", "driver_name": ""})
    result = adbc_core.detect_dialect(conn, fallback_dialect="sqlite")
    assert result == "duckdb"


def test_detect_dialect_uses_gizmosql_duckdb_vendor_version() -> None:
    """GizmoSQL DuckDB metadata should beat the generic Flight SQL driver name."""

    conn = SimpleNamespace(
        adbc_get_info=lambda: {
            "vendor_name": "GizmoSQL",
            "vendor_version": "GizmoSQL DuckDB backend",
            "driver_name": "Flight SQL",
        }
    )
    result = adbc_core.detect_dialect(conn, fallback_dialect="sqlite")
    assert result == "duckdb"


def test_detect_dialect_uses_gizmosql_sqlite_vendor_version() -> None:
    """GizmoSQL SQLite metadata should preserve SQLite backend detection."""

    conn = SimpleNamespace(
        adbc_get_info=lambda: {
            "vendor_name": "GizmoSQL",
            "vendor_version": "GizmoSQL SQLite backend",
            "driver_name": "Flight SQL",
        }
    )
    result = adbc_core.detect_dialect(conn, fallback_dialect="duckdb")
    assert result == "sqlite"


def test_detect_dialect_isolates_class_level_gizmosql_info_cache() -> None:
    """Per-connection metadata must not inherit a class-level _get_info_cache from an earlier backend."""

    class _BaseStubConnection:
        def __init__(self, backend_label: str) -> None:
            self._backend_label = backend_label

        def adbc_get_info(self) -> dict[str, str]:
            return {
                "vendor_name": "GizmoSQL",
                "vendor_version": f"GizmoSQL {self._backend_label} backend",
                "driver_name": "ADBC Flight SQL Driver",
            }

    class _ClassCachingGizmoConnection(_BaseStubConnection):
        _get_info_cache: dict[str, str] | None = None

        def adbc_get_info(self) -> dict[str, str]:
            if self._get_info_cache is None:
                _ClassCachingGizmoConnection._get_info_cache = super().adbc_get_info()
            return self._get_info_cache or {}

    duckdb_conn = _ClassCachingGizmoConnection("DuckDB")
    sqlite_conn = _ClassCachingGizmoConnection("SQLite")

    assert duckdb_conn.adbc_get_info()["vendor_version"] == "GizmoSQL DuckDB backend"
    assert adbc_core.detect_dialect(duckdb_conn) == "duckdb"
    assert adbc_core.detect_dialect(sqlite_conn) == "sqlite"
    assert sqlite_conn.adbc_get_info()["vendor_version"] == "GizmoSQL SQLite backend"


def test_handle_postgres_rollback_issues_rollback_for_pgvector() -> None:
    """pgvector is PostgreSQL-compatible and must receive rollback handling."""
    executed: list[str] = []
    cursor = SimpleNamespace(execute=lambda sql: executed.append(sql))

    adbc_core.handle_postgres_rollback("pgvector", cursor)

    assert executed == ["ROLLBACK"]


def test_handle_postgres_rollback_issues_rollback_for_paradedb() -> None:
    """paradedb is PostgreSQL-compatible and must receive rollback handling."""
    executed: list[str] = []
    cursor = SimpleNamespace(execute=lambda sql: executed.append(sql))

    adbc_core.handle_postgres_rollback("paradedb", cursor)

    assert executed == ["ROLLBACK"]


def test_normalize_postgres_empty_parameters_returns_none_for_pgvector() -> None:
    """pgvector empty dict parameters should use the PostgreSQL empty-params path."""
    assert adbc_core.normalize_postgres_empty_parameters("pgvector", {}) is None


def test_normalize_postgres_empty_parameters_returns_none_for_paradedb() -> None:
    """paradedb empty dict parameters should use the PostgreSQL empty-params path."""
    assert adbc_core.normalize_postgres_empty_parameters("paradedb", {}) is None


def test_base_type_coercion_map_replaces_getter_function() -> None:
    assert isinstance(adbc_core._BASE_TYPE_COERCION_MAP, dict)
    assert not hasattr(adbc_core, "_get_type_coercion_map")
    assert adbc_core.driver_profile is not None


def test_pg_textsearch_retains_postgres_transaction_and_parameter_behavior() -> None:
    executed: list[str] = []
    cursor = SimpleNamespace(execute=lambda statement: executed.append(statement))
    adbc_core.handle_postgres_rollback("pg_textsearch", cursor)
    assert executed == ["ROLLBACK"]
    assert adbc_core.normalize_postgres_empty_parameters("pg_textsearch", {}) is None
    config = get_statement_config("pg_textsearch")
    assert config.parameter_config.default_parameter_style == ParameterStyle.NUMERIC


def test_pg_textsearch_class_and_instance_resolve_as_postgres_family() -> None:
    for dialect in (PGTextSearch, PGTextSearch()):
        name = adbc_core.resolve_dialect_name(dialect)
        assert adbc_core.is_postgres_dialect(name)


def test_adbc_postgres_uuid_binding_runs_across_mypyc_boundary() -> None:
    """The compiled ADBC core should retain UUID rewrite behavior."""
    value = UUID("550e8400-e29b-41d4-a716-446655440000")

    sql, parameters = prepare_postgres_uuid_bindings("SELECT $1", [value], is_many=False, dialect="postgres")

    assert sql == "SELECT CAST($1 AS UUID)"
    assert parameters == [str(value)]


def test_create_mapped_exception_maps_serialization_conflict_from_message() -> None:
    """Message-only serialization conflict patterns map to SerializationConflictError."""
    error = DatabaseError("Transaction aborted due to concurrent update", status_code=AdbcStatusCode.UNKNOWN)

    mapped = adbc_core.create_mapped_exception(error)

    assert isinstance(mapped, SerializationConflictError)


def test_decimal_coercion_preserves_decimal_instance() -> None:
    """Decimal values should remain Decimal instances so Arrow decimal fidelity is preserved."""
    amount = Decimal("123.45")
    assert adbc_core._BASE_TYPE_COERCION_MAP[Decimal](amount) is amount  # pyright: ignore[reportPrivateUsage]
    config = get_statement_config("postgres")
    assert config.parameter_config.type_coercion_map[Decimal](amount) is amount


def test_duckdb_copy_to_inlines_file_path_parameter() -> None:
    """DuckDB statement config should inline COPY file target parameters via AST transformer."""
    config = get_statement_config("duckdb")
    stmt = SQL("COPY (SELECT ? AS id) TO ? (FORMAT PARQUET)", 7, "/tmp/out.parquet", statement_config=config)
    compiled_sql, compiled_params = stmt.compile()

    assert "'/tmp/out.parquet'" in compiled_sql
    assert compiled_params == [7]


def test_case_insensitive_uri_and_grpc_tls_resolution() -> None:
    """URI helpers and build_connection_config must handle uppercase schemes and grpc+tls://."""
    assert driver_from_uri("POSTGRESQL://localhost/db") == "adbc_driver_postgresql.dbapi.connect"
    assert driver_kind_from_uri("GRPC+TLS://localhost:31337") == "flightsql"
    assert resolve_dialect_from_config({"uri": "POSTGRESQL://localhost/db"}) == "postgres"
    assert resolve_dialect_from_config({"uri": "GRPC+TLS://localhost:31337"}) == "duckdb"

    built = build_connection_config({"uri": "grpc+tls://localhost:31337", "tls_skip_verify": True})
    assert built["db_kwargs"]["adbc.flight.sql.client_option.tls_skip_verify"] == "true"


def test_detect_dialect_recognizes_oracle_db2_mssql_and_quack() -> None:
    """detect_dialect maps Oracle, Db2, SQL Server, and Quack ADBC driver info."""
    oracle_conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "Oracle", "driver_name": "ADBC Oracle Driver"})
    db2_conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "IBM Db2", "driver_name": "ADBC Db2 Driver"})
    mssql_conn = SimpleNamespace(
        adbc_get_info=lambda: {"vendor_name": "Microsoft SQL Server", "driver_name": "ADBC MSSQL Driver"}
    )
    quack_conn = SimpleNamespace(adbc_get_info=lambda: {"vendor_name": "DuckDB", "driver_name": "ADBC Quack Driver"})

    assert adbc_core.detect_dialect(oracle_conn) == "oracle"
    assert adbc_core.detect_dialect(db2_conn) == "db2"
    assert adbc_core.detect_dialect(mssql_conn) == "mssql"
    assert adbc_core.detect_dialect(quack_conn) == "duckdb"


@pytest.mark.parametrize(
    ("message", "expected_cls"),
    [
        ("ORA-00001: unique constraint (APP.PK_USERS) violated", UniqueViolationError),
        ("ORA-02291: integrity constraint (APP.FK_DEPT) violated - parent key not found", ForeignKeyViolationError),
        ("ORA-02292: integrity constraint (APP.FK_DEPT) violated - child record found", ForeignKeyViolationError),
        ('ORA-01400: cannot insert NULL into ("APP"."USERS"."EMAIL")', NotNullViolationError),
        ('ORA-01407: cannot update ("APP"."USERS"."EMAIL") to NULL', NotNullViolationError),
        ("ORA-02290: check constraint (APP.CHK_AGE) violated", CheckViolationError),
        ("ORA-00060: deadlock detected while waiting for resource", DeadlockError),
        ("ORA-08177: can't serialize access for this transaction", SerializationConflictError),
        ("ORA-01013: user requested cancel of current operation", QueryTimeoutError),
        ("ORA-12170: TNS:Connect timeout occurred", QueryTimeoutError),
        ("ORA-01017: invalid username/password; logon denied", PermissionDeniedError),
        ("ORA-01031: insufficient privileges", PermissionDeniedError),
        ("ORA-12154: TNS:could not resolve the connect identifier specified", DatabaseConnectionError),
        ("ORA-12541: TNS:no listener", DatabaseConnectionError),
        ("ORA-03113: end-of-file on communication channel", DatabaseConnectionError),
        ("ORA-00942: table or view does not exist", SQLParsingError),
        ('ORA-00904: "BAD_COL": invalid identifier', SQLParsingError),
        ("ORA-01722: invalid number", DataError),
        ("ORA-12899: value too large for column", DataError),
        ("ORA-02299: cannot validate (APP.UQ_TEST) - duplicate keys found", IntegrityError),
    ],
)
def test_create_mapped_exception_oracle_ora_codes(message: str, expected_cls: type[SQLSpecError]) -> None:
    mapped = adbc_core.create_mapped_exception(Exception(message))
    assert isinstance(mapped, expected_cls)


@pytest.mark.parametrize(
    ("message", "sqlstate", "vendor_code", "expected_cls"),
    [
        (
            'SQL0911N The current transaction has been rolled back because of a deadlock or timeout. Reason code "68". SQLSTATE=40001',
            "40001",
            -911,
            QueryTimeoutError,
        ),
        (
            'SQL0911N The current transaction has been rolled back because of a deadlock or timeout. Reason code "2". SQLSTATE=40001',
            "40001",
            -911,
            DeadlockError,
        ),
        (
            'SQL0913N Unsuccessful execution caused by deadlock or timeout. Reason code "2". SQLSTATE=57033',
            "57033",
            -913,
            DeadlockError,
        ),
        (
            "SQL0803N One or more values in the INSERT statement are not valid. SQLSTATE=23505",
            None,
            -803,
            UniqueViolationError,
        ),
        (
            "SQL0530N The insert or update value of the FOREIGN KEY is not equal. SQLSTATE=23503",
            None,
            -530,
            ForeignKeyViolationError,
        ),
        ("SQL0407N Assignment of a NULL value to a NOT NULL column is not allowed.", None, -407, NotNullViolationError),
        (
            "SQL0545N The requested operation is not allowed because a row does not satisfy the check constraint.",
            None,
            -545,
            CheckViolationError,
        ),
        ('SQL0204N "DB2INST1.MISSING" is an undefined name. SQLSTATE=42704', "42704", -204, SQLParsingError),
        ("SQL0104N An unexpected token was found.", None, -104, SQLParsingError),
        ("SQL0551N Does not have the required authorization or privilege.", None, -551, PermissionDeniedError),
        (
            "SQL0302N The value of a host variable in the EXECUTE or OPEN statement is out of range.",
            None,
            -302,
            DataError,
        ),
        ("SQL0952N Processing was cancelled due to an interrupt.", None, -952, QueryTimeoutError),
        ("SQL30081N A communication error has been detected.", None, -30081, DatabaseConnectionError),
    ],
)
def test_create_mapped_exception_db2_diagnostics(
    message: str, sqlstate: str | None, vendor_code: int | None, expected_cls: type[SQLSpecError]
) -> None:
    err = Exception(message)
    if sqlstate is not None:
        setattr(err, "sqlstate", sqlstate)
    if vendor_code is not None:
        setattr(err, "vendor_code", vendor_code)
    mapped = adbc_core.create_mapped_exception(err)
    assert isinstance(mapped, expected_cls)


@pytest.mark.parametrize(
    ("message", "vendor_code", "expected_cls"),
    [
        ("mssql: Msg 2627, Level 14, Violation of PRIMARY KEY constraint", 2627, UniqueViolationError),
        ("mssql: Msg 2601, Cannot insert duplicate key row in object", 2601, UniqueViolationError),
        (
            "mssql: Msg 547, The INSERT statement conflicted with the FOREIGN KEY constraint",
            547,
            ForeignKeyViolationError,
        ),
        ("mssql: Msg 547, The INSERT statement conflicted with the CHECK constraint", 547, CheckViolationError),
        ("mssql: Msg 515, Cannot insert the value NULL into column", 515, NotNullViolationError),
        ("mssql: Msg 1205, Transaction (Process ID 52) was deadlocked on lock resources", 1205, DeadlockError),
        (
            "mssql: Msg 3960, Snapshot isolation transaction aborted due to update conflict",
            3960,
            SerializationConflictError,
        ),
        ("mssql: Msg 1222, Lock request time out period exceeded", 1222, QueryTimeoutError),
        ("mssql: Msg 18456, Login failed for user 'sa'", 18456, PermissionDeniedError),
        ("mssql: Msg 4060, Cannot open database requested by the login", 4060, DatabaseConnectionError),
        ("mssql: Msg 208, Invalid object name 'dbo.missing'", 208, SQLParsingError),
        ("mssql: Msg 8152, String or binary data would be truncated", 8152, DataError),
    ],
)
def test_create_mapped_exception_mssql_diagnostics(
    message: str, vendor_code: int | None, expected_cls: type[SQLSpecError]
) -> None:
    err = Exception(message)
    if vendor_code is not None:
        setattr(err, "vendor_code", vendor_code)
    mapped = adbc_core.create_mapped_exception(err)
    assert isinstance(mapped, expected_cls)


def test_apply_driver_features_defaults_lowercase_columns_for_oracle_and_db2() -> None:
    oracle_cfg, oracle_features = adbc_core.apply_driver_features(get_statement_config("oracle"), None)
    db2_cfg, db2_features = adbc_core.apply_driver_features(get_statement_config("db2"), None)
    _pg_cfg, pg_features = adbc_core.apply_driver_features(get_statement_config("postgres"), None)
    _mssql_cfg, mssql_features = adbc_core.apply_driver_features(get_statement_config("mssql"), None)

    assert oracle_cfg.dialect == "oracle"
    assert oracle_features["enable_lowercase_column_names"] is True
    assert db2_cfg.dialect == "db2"
    assert db2_features["enable_lowercase_column_names"] is True
    assert pg_features["enable_lowercase_column_names"] is False
    assert mssql_features["enable_lowercase_column_names"] is False


def test_normalize_column_names_lowercases_implicit_upper_names() -> None:
    names = ["USER_ID", "EMAIL_ADDR", "MixedCase", "123_INVALID"]
    assert adbc_core.normalize_column_names(names, {"enable_lowercase_column_names": True}) == [
        "user_id",
        "email_addr",
        "MixedCase",
        "123_INVALID",
    ]
    assert adbc_core.normalize_column_names(names, {"enable_lowercase_column_names": False}) == names


def test_adbc_driver_dispatch_execute_lowercases_oracle_columns() -> None:
    table = pa.table({"USER_ID": [1], "MixedCase": ["a"]})
    cursor = SimpleNamespace(
        execute=lambda _sql, parameters=None: None,
        fetch_arrow_table=lambda: table,
        description=[("USER_ID",), ("MixedCase",)],
        rowcount=1,
    )
    conn = SimpleNamespace(
        adbc_get_info=lambda: {"vendor_name": "Oracle", "driver_name": "ADBC Oracle Driver"}, cursor=lambda: cursor
    )
    driver = AdbcDriver(cast("Any", conn), dialect="oracle")
    result = driver.dispatch_execute(cast("Any", cursor), SQL('SELECT USER_ID, "MixedCase" FROM dual'))

    assert result.column_names == ["user_id", "MixedCase"]
    assert result.selected_data == [{"user_id": 1, "MixedCase": "a"}]


@pytest.mark.parametrize("autocommit", [False, True])
def test_adbc_begin_with_native_autocommit(autocommit: bool) -> None:
    """Real explicit transactions commit/roll back and restore the native autocommit mode."""
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "autocommit": autocommit})
    expected_mode = "true" if autocommit else "false"
    with config.provide_session() as session:
        native = session.connection.adbc_connection
        session.execute("CREATE TABLE transaction_values (value INTEGER)")
        session.commit()
        session.begin()
        begun_mode = native.get_option("adbc.connection.autocommit")
        session.execute("INSERT INTO transaction_values (value) VALUES (:value)", value=1)
        session.rollback()
        assert session.select_value("SELECT COUNT(*) FROM transaction_values") == 0
        assert begun_mode == "false"
        assert native.get_option("adbc.connection.autocommit") == expected_mode
        session.begin()
        session.execute("INSERT INTO transaction_values (value) VALUES (:value)", value=2)
        session.commit()
        assert session.select_value("SELECT value FROM transaction_values") == 2
        assert native.get_option("adbc.connection.autocommit") == expected_mode


@pytest.mark.parametrize("capability", ["missing", "unsupported"])
def test_adbc_begin_preserves_wrapper_transactions_without_native_getter(capability: str) -> None:
    """Older/unsupported getters retain ordinary DB-API transaction capability behavior."""
    native = SimpleNamespace()
    if capability == "unsupported":
        native.get_option = Mock(side_effect=NotSupportedError("getter unsupported"))
    committed, rolled_back = Mock(), Mock()
    connection = SimpleNamespace(
        adbc_get_info=lambda: {"driver_name": "sqlite"}, adbc_connection=native, commit=committed, rollback=rolled_back
    )
    driver = AdbcDriver(cast("Any", connection), dialect="sqlite")
    driver.begin()
    driver.commit()
    driver.begin()
    driver.rollback()
    committed.assert_called_once_with()
    rolled_back.assert_called_once_with()


@pytest.mark.parametrize("operation", ["commit", "rollback"])
@pytest.mark.parametrize("interrupted", [False, True])
def test_adbc_native_transaction_preserves_primary_on_restoration_failure(operation: str, interrupted: bool) -> None:
    """Explicitly switched native transactions preserve primary errors while attempting restoration."""
    connection = Mock()
    connection.adbc_get_info.return_value = {"driver_name": "sqlite"}
    native = connection.adbc_connection
    native.get_option.return_value = "true"
    native.set_autocommit.side_effect = [None, RuntimeError("restore failed")]
    primary = (
        KeyboardInterrupt("transaction interrupted")
        if interrupted
        else DatabaseError("transaction failed", status_code=AdbcStatusCode.UNKNOWN)
    )
    getattr(native, operation).side_effect = [primary, None] if operation == "rollback" else primary
    driver = AdbcDriver(connection, dialect="sqlite")
    driver.begin()
    expected = KeyboardInterrupt if interrupted else SQLSpecError
    with pytest.raises(expected) as caught:
        getattr(driver, operation)()
    if interrupted:
        assert caught.value is primary
    else:
        assert caught.value.__cause__ is primary
    assert native.set_autocommit.call_args_list == [call(False), call(True)]
    connection.commit.assert_not_called()
    connection.rollback.assert_not_called()


@pytest.mark.parametrize("interrupted", [False, True])
@pytest.mark.parametrize("operation", ["commit", "rollback"])
def test_adbc_failed_native_rollback_never_commits_pending_rows(
    tmp_path: Path, interrupted: bool, operation: str
) -> None:
    """A failed rollback and cleanup leave real work uncommitted until an explicit retry."""
    path = str(tmp_path / "rollback.sqlite")
    config = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": path, "autocommit": True})
    primary = (
        KeyboardInterrupt(f"{operation} interrupted")
        if interrupted
        else DatabaseError(f"{operation} failed", status_code=AdbcStatusCode.UNKNOWN)
    )

    class NativeBoundary:
        def __init__(self, native: Any) -> None:
            self.native = native
            self.remaining = 2

        def get_option(self, key: str) -> str:
            return cast("str", self.native.get_option(key))

        def set_autocommit(self, value: bool) -> None:
            self.native.set_autocommit(value)

        def commit(self) -> None:
            if operation == "commit" and self.remaining:
                self.remaining -= 1
                raise primary
            self.native.commit()

        def rollback(self) -> None:
            if self.remaining:
                self.remaining -= 1
                if self.remaining == 1:
                    raise primary
                raise RuntimeError("cleanup rollback failed")
            self.native.rollback()

    class ConnectionBoundary:
        def __init__(self, connection: Any) -> None:
            self.connection = connection
            self.adbc_connection = NativeBoundary(connection.adbc_connection)

        def adbc_get_info(self) -> Any:
            return self.connection.adbc_get_info()

        def cursor(self) -> Any:
            return self.connection.cursor()

        def commit(self) -> None:
            self.connection.commit()

        def rollback(self) -> None:
            self.connection.rollback()

    with config.provide_connection() as connection:
        driver = AdbcDriver(cast("Any", ConnectionBoundary(connection)), dialect="sqlite")
        driver.execute("CREATE TABLE pending_values (value INTEGER)")
        driver.begin()
        driver.execute("INSERT INTO pending_values (value) VALUES (:value)", value=1)
        expected = KeyboardInterrupt if interrupted else SQLSpecError
        with pytest.raises(expected) as caught:
            getattr(driver, operation)()
        if interrupted:
            assert caught.value is primary
        else:
            assert caught.value.__cause__ is primary
        observer = AdbcConfig(connection_config={"driver_name": "sqlite", "uri": path, "autocommit": True})
        with observer.provide_session() as session:
            assert session.select_value("SELECT COUNT(*) FROM pending_values") == 0
        assert connection.adbc_connection.get_option("adbc.connection.autocommit") == "false"
        driver.begin()
        driver.rollback()
        assert connection.adbc_connection.get_option("adbc.connection.autocommit") == "true"
        assert driver.select_value("SELECT COUNT(*) FROM pending_values") == 0


def test_adbc_native_begin_failure_restores_mode_and_preserves_native_cause() -> None:
    """A failed native mode switch does not start a logical transaction or hide its error."""
    connection = Mock()
    connection.adbc_get_info.return_value = {"driver_name": "sqlite"}
    native = connection.adbc_connection
    primary = DatabaseError("begin failed", status_code=AdbcStatusCode.UNKNOWN)
    native.get_option.return_value = "true"
    native.set_autocommit.side_effect = [primary, RuntimeError("restore failed")]
    driver = AdbcDriver(connection, dialect="sqlite")
    with pytest.raises(SQLSpecError) as caught:
        driver.begin()
    assert caught.value.__cause__ is primary
    assert native.set_autocommit.call_args_list == [call(False), call(True)]
    driver.rollback()
    connection.rollback.assert_called_once_with()


def test_pending_exception_preserves_cause() -> None:
    """_check_pending_exception preserves original exception cause."""

    class MockConn:
        def adbc_get_info(self) -> dict[str, str]:
            return {"driver_name": "sqlite"}

    driver = AdbcDriver(cast("Any", MockConn()))
    orig_exc = DatabaseError("query failed", status_code=AdbcStatusCode.INTERNAL)
    mapped_exc = adbc_core.create_mapped_exception(orig_exc)
    handler = driver.handle_database_exceptions()
    handler.pending_exception = mapped_exc

    with pytest.raises(OperationalError) as exc_info:
        driver._check_pending_exception(handler)

    assert exc_info.value is mapped_exc
    assert exc_info.value.__cause__ is orig_exc


@pytest.mark.parametrize(
    ("status_code", "expected_cls"),
    [
        (AdbcStatusCode.TIMEOUT, QueryTimeoutError),
        (AdbcStatusCode.CANCELLED, OperationCancelledError),
        (AdbcStatusCode.UNAUTHENTICATED, PermissionDeniedError),
        (AdbcStatusCode.UNAUTHORIZED, PermissionDeniedError),
        (AdbcStatusCode.NOT_FOUND, NotFoundError),
        (AdbcStatusCode.ALREADY_EXISTS, IntegrityError),
        (AdbcStatusCode.INVALID_ARGUMENT, DataError),
        (AdbcStatusCode.IO, DatabaseConnectionError),
        (AdbcStatusCode.INTEGRITY, IntegrityError),
        (AdbcStatusCode.INVALID_STATE, OperationalError),
        (AdbcStatusCode.INTERNAL, OperationalError),
        (AdbcStatusCode.NOT_IMPLEMENTED, SQLSpecError),
    ],
)
def test_adbc_status_code_mappings(status_code: AdbcStatusCode, expected_cls: type[SQLSpecError]) -> None:
    """_map_adbc_status_exception maps ADBC status codes to SQLSpecError classes."""
    error = DatabaseError("adbc error", status_code=status_code)
    mapped = adbc_core.create_mapped_exception(error)
    assert isinstance(mapped, expected_cls)


@pytest.mark.parametrize(
    ("status_code", "sqlstate", "message", "expected_cls"),
    [
        (AdbcStatusCode.INVALID_ARGUMENT, "23505", "duplicate key", UniqueViolationError),
        (AdbcStatusCode.INVALID_ARGUMENT, "23502", "null value", NotNullViolationError),
        (AdbcStatusCode.INVALID_ARGUMENT, "23514", "check constraint", CheckViolationError),
        (AdbcStatusCode.INTEGRITY, "23503", "foreign key constraint", ForeignKeyViolationError),
        (AdbcStatusCode.INVALID_ARGUMENT, "42601", "invalid SQL", SQLParsingError),
        (AdbcStatusCode.INVALID_ARGUMENT, None, "syntax error near SELEC", SQLParsingError),
        (AdbcStatusCode.INTERNAL, None, "ORA-00001: unique constraint violated", UniqueViolationError),
    ],
)
def test_adbc_specific_diagnostics_override_generic_status(
    status_code: AdbcStatusCode, sqlstate: str | None, message: str, expected_cls: type[SQLSpecError]
) -> None:
    error = DatabaseError(message, status_code=status_code, sqlstate=sqlstate)

    mapped = adbc_core.create_mapped_exception(error)

    assert type(mapped) is expected_cls
    assert mapped.__cause__ is error


def test_duckdb_copy_ast_transformer_caching() -> None:
    """DuckDB statement config omits ast_transformer so compiled SQL can be cached."""
    config = get_statement_config("duckdb").replace(enable_caching=True)
    assert config.parameter_config.ast_transformer is None

    stmt = SQL("SELECT 1 WHERE x = :val", {"val": 42}, statement_config=config)
    compiled_sql, _ = stmt.compile()
    assert compiled_sql

    processor = SQLProcessor(config)
    processor.compile("SELECT 1 WHERE x = :val", {"val": 42})
    processor.compile("SELECT 1 WHERE x = :val", {"val": 99})
    assert processor._cache_hits == 1


def test_rewrite_postgres_uuid_placeholders() -> None:
    """_rewrite_postgres_uuid_placeholders rewrites positional parameters to explicit UUID casts."""
    sql = "SELECT $1, $2"
    rewritten, scalars, arrays = adbc_core._rewrite_postgres_uuid_placeholders(sql, (1,), (2,), "postgres")
    assert "UUID[]" in rewritten
    assert scalars == (1,)
    assert arrays == (2,)


def test_repeated_uuid_bindings_preserve_sql_and_current_parameter_values() -> None:
    """Repeated statement rewrites retain each call's current UUID binding."""
    sql = "SELECT $1 WHERE id = $2"
    first = UUID("12345678-1234-5678-1234-567812345678")
    second = UUID("87654321-4321-8765-4321-876543218765")
    first_sql, first_parameters = prepare_postgres_uuid_bindings(sql, [first, 1], is_many=False, dialect="postgres")
    second_sql, second_parameters = prepare_postgres_uuid_bindings(sql, [second, 2], is_many=False, dialect="postgres")
    assert first_sql == second_sql == "SELECT CAST($1 AS UUID) WHERE id = $2"
    assert first_parameters == [str(first), 1]
    assert second_parameters == [str(second), 2]


def test_strict_coercion_types() -> None:
    """_STRICT_COERCION_ALLOWED_TYPES contains NoneType bound once at module top level."""
    assert NoneType in adbc_core._STRICT_COERCION_ALLOWED_TYPES


def test_type_converter_caching() -> None:
    """get_adbc_type_converter caches output converter instances."""
    conv1 = get_adbc_type_converter("postgres")
    conv2 = get_adbc_type_converter("postgres")
    assert conv1 is conv2


def test_bytearray_memoryview_coercion() -> None:
    """_BASE_TYPE_COERCION_MAP coerces bytearray and memoryview to bytes."""
    coercion = adbc_core._BASE_TYPE_COERCION_MAP
    assert coercion[bytearray] is bytes
    assert coercion[memoryview] is bytes


def test_session_context_driver() -> None:
    """AdbcSessionContext uses provided driver_class without lazy import."""
    driver_instance = Mock()
    mock_driver_cls = Mock(return_value=driver_instance)
    mock_conn = Mock()
    ctx = AdbcSessionContext(
        acquire_connection=lambda: mock_conn,
        release_connection=lambda _conn, **_kw: None,
        statement_config=get_statement_config("postgres"),
        driver_features={},
        prepare_driver=lambda d: d,
        driver_class=cast("type[AdbcDriver]", mock_driver_cls),
    )
    with ctx as driver:
        assert driver is driver_instance
    mock_driver_cls.assert_called_once()


def test_select_stream_chunk_size() -> None:
    """AdbcSelectStreamSource buffers and returns chunks of specified chunk_size."""
    batch = pa.record_batch([pa.array([1, 2, 3, 4, 5])], names=["id"])
    reader = iter([batch])

    class _MockDriver:
        driver_features: dict[str, Any] = {}
        _dialect_name = "sqlite"

        def with_cursor(self, _conn: Any) -> Any:
            return Mock()

        def handle_database_exceptions(self) -> Any:
            return Mock()

        def _check_pending_exception(self, _handler: Any) -> None:
            pass

    source = adbc_driver_mod.AdbcSelectStreamSource(cast("Any", _MockDriver()), "SELECT 1", (), chunk_size=2)
    source._reader = reader
    chunk1 = source.fetch_chunk()
    assert len(chunk1) == 2
    chunk2 = source.fetch_chunk()
    assert len(chunk2) == 2
    chunk3 = source.fetch_chunk()
    assert len(chunk3) == 1
    assert source.fetch_chunk() == []


def test_select_to_arrow_lowercase_columns() -> None:
    """select_to_arrow normalizes reader schema to lowercase when enable_lowercase_column_names is set."""
    table = pa.table({"UPPER_COL": [1, 2]})
    mock_cursor = Mock()
    mock_cursor.execute.return_value = None
    mock_cursor.fetch_record_batch.return_value = table.to_reader()
    mock_conn = Mock()
    mock_conn.cursor.return_value = mock_cursor

    driver = AdbcDriver(
        mock_conn,
        statement_config=get_statement_config("oracle"),
        driver_features={"enable_lowercase_column_names": True},
    )
    result = driver.select_to_arrow("SELECT UPPER_COL FROM t", return_format="reader")
    reader = result.data
    assert reader.schema.names == ["upper_col"]
    assert reader.read_all().to_pylist() == [{"upper_col": 1}, {"upper_col": 2}]


def test_adbc_exception_isolation() -> None:
    """AdbcSelectStreamSource routes execution through exception handler with isolation check."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.return_value = None
    batch = pa.record_batch([pa.array([1])], names=["id"])

    driver = AdbcDriver(mock_conn, statement_config=get_statement_config("sqlite"))

    source = adbc_driver_mod.AdbcSelectStreamSource(driver, "SELECT 1", ())
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(adbc_driver_mod, "_fetch_record_batch", lambda _c: iter([batch]))
        source.start()
        assert source._reader is not None


@pytest.mark.parametrize("driver", ["teradata", "vantage", "/opt/libadbc_driver_teradata.so"])
def test_teradata_dialect_routing(driver: str) -> None:
    assert resolve_dialect_from_config({"driver": driver}) == "teradata"


def test_teradata_flightsql_dialect_override() -> None:
    assert resolve_dialect_from_config({"driver": "flightsql", "adbc_dialect": "vantage"}) == "teradata"


@pytest.mark.parametrize(
    "code,expected",
    [
        (2801, UniqueViolationError),
        (3807, NotFoundError),
        (2631, DeadlockError),
        (3706, SQLParsingError),
        (2646, OperationalError),
        (2616, DataError),
        (2620, DataError),
        (2621, DataError),
        (2665, DataError),
        (6706, DataError),
        (6758, DataError),
        (6760, DataError),
        (2700, ForeignKeyViolationError),
        (2701, ForeignKeyViolationError),
        (5312, ForeignKeyViolationError),
        (5313, ForeignKeyViolationError),
        (2802, UniqueViolationError),
        (2803, UniqueViolationError),
        (2644, OperationalError),
        (2843, OperationalError),
        (3932, OperationalError),
        (3110, QueryTimeoutError),
        (3156, QueryTimeoutError),
        (3523, PermissionDeniedError),
        (3524, PermissionDeniedError),
        (8017, PermissionDeniedError),
        (3604, NotNullViolationError),
        (3811, NotNullViolationError),
        (3707, SQLParsingError),
        (3802, NotFoundError),
        (5628, NotFoundError),
        (5317, CheckViolationError),
    ],
)
def test_teradata_error_mapping(code: int, expected: type[SQLSpecError]) -> None:
    error = DatabaseError(
        f"[Teradata Database] [Error {code}] request failed", status_code=AdbcStatusCode.INTERNAL, sqlstate="HY000"
    )
    mapped = adbc_core.create_mapped_exception(error)
    assert isinstance(mapped, expected)
    assert mapped.__cause__ is error


@pytest.mark.parametrize(
    ("sql", "parameters", "expected_sql", "expected_parameters"),
    [
        ("SELECT :name AS name", {"name": "ada"}, "SELECT ? AS name", ["ada"]),
        ("SELECT :1 AS value", [7], "SELECT ? AS value", [7]),
        (
            "SELECT $1 FROM @stage WHERE $1 = :expected",
            {"expected": "ok"},
            "SELECT $1 FROM @stage WHERE $1 = ?",
            ["ok"],
        ),
    ],
)
def test_snowflake_adbc_compiles_to_qmark(
    sql: str, parameters: Any, expected_sql: str, expected_parameters: list[Any]
) -> None:
    """Snowflake source markers convert to qmarks while stage ordinals remain SQL tokens."""
    statement = SQL(sql, parameters, statement_config=get_statement_config("snowflake"))
    compiled_sql, compiled_parameters = statement.compile()
    assert compiled_sql == expected_sql
    assert list(compiled_parameters) == expected_parameters


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("002003", NotFoundError),
        ("000604", QueryTimeoutError),
        ("000606", OperationalError),
        ("250001", DatabaseConnectionError),
    ],
)
@pytest.mark.parametrize("diagnostic", ["message", "vendor_code"])
def test_snowflake_vendor_diagnostics(code: str, expected: type[SQLSpecError], diagnostic: str) -> None:
    """Snowflake vendor diagnostics take precedence over generic SQLSTATE fallbacks."""
    error = Exception(
        f"{code} (42000): Snowflake operation failed" if diagnostic == "message" else "Snowflake operation failed"
    )
    setattr(error, "sqlstate", "42000")
    if diagnostic == "vendor_code":
        setattr(error, "vendor_code", int(code))
    mapped = adbc_core.create_mapped_exception(error)
    assert isinstance(mapped, expected)
    assert mapped.__cause__ is error


@pytest.mark.parametrize("port", [0, 70000, True, "bad"])
def test_teradata_invalid_port(port: Any) -> None:
    with pytest.raises(ImproperConfigurationError, match="port"):
        build_connection_config({"driver": "teradata", "host": "td.corp", "port": port})


@pytest.mark.parametrize(
    "config,expected",
    [
        (
            {
                "driver": "teradata",
                "host": "td.corp",
                "username": "u:1",
                "password": "p@ss/w?rd#1%2",
                "database": "mart",
            },
            "teradata://u%3A1:p%40ss%2Fw%3Frd%231%252@td.corp:1025/mart",
        ),
        ({"uri": "TERADATA://u%3A1:p%40ss@td.corp:1025/prod"}, "TERADATA://u%3A1:p%40ss@td.corp:1025/prod"),
        (
            {
                "uri": "teradata://old:old@td.corp:1025/prod?mode=x#f",
                "username": "u@1",
                "password": "p#1",
                "database": " /new db",
            },
            "teradata://u%401:p%231@td.corp:1025/new%20db?mode=x#f",
        ),
        (
            {"driver": "vantage", "host": "::1", "user": "dbc", "password": "dbc", "port": 1026},
            "teradata://dbc:dbc@[::1]:1026",
        ),
        (
            {
                "uri": "teradata://td.corp:1025",
                "user": "svc",
                "auth_token_provider": lambda: "t@k#1",
                "database": "sales",
            },
            "teradata://svc:t%40k%231@td.corp:1025/sales",
        ),
        (
            {"driver": "teradata", "db_kwargs": {"uri": "teradata://td.corp:1025"}, "user": "svc"},
            "teradata://svc@td.corp:1025",
        ),
    ],
)
def test_teradata_uri_encoding(config: dict[str, Any], expected: str) -> None:
    result = build_connection_config(config)
    assert result == {"driver": "teradata", "db_kwargs": {"uri": expected}}


@pytest.mark.parametrize("option_supported", [True, False])
def test_teradata_autocommit_only_begin(option_supported: bool) -> None:
    error = NotSupportedError("autocommit only")
    native = Mock()
    if option_supported:
        native.get_option.return_value = "true"
    else:
        native.get_option.side_effect = error
    native.set_autocommit.side_effect = error
    connection = Mock(adbc_connection=native, _autocommit=True)
    driver = AdbcDriver(connection, dialect="teradata")
    with pytest.raises(TransactionError, match="autocommit") as exc:
        driver.begin()
    assert exc.value.__cause__ is error
    with pytest.raises(TransactionError, match="autocommit"):
        driver.begin()
    assert native.set_autocommit.call_args_list == [call(False), call(False)]


def test_begin_without_autocommit_option_is_noop_for_non_teradata_backends() -> None:
    native = Mock()
    native.get_option.side_effect = NotSupportedError("option unavailable")
    connection = Mock(adbc_connection=native, _autocommit=True)
    driver = AdbcDriver(connection, dialect="postgres")
    driver.begin()
    native.set_autocommit.assert_not_called()


@pytest.mark.parametrize(
    ("raw_name", "expected"),
    [
        ("litestar_session", (None, "LITESTAR_SESSION")),
        ("my_db.litestar_session", ("MY_DB", "LITESTAR_SESSION")),
        ('"MixedDb"."MixedTable"', ("MixedDb", "MixedTable")),
        ('my_db."Mixed""Table"', ("MY_DB", 'Mixed"Table')),
        ('"Dot.Db".plain_table', ("Dot.Db", "PLAIN_TABLE")),
    ],
)
def test_split_teradata_table_name_normalizes_identifiers(raw_name: str, expected: tuple[str | None, str]) -> None:
    """split_teradata_table_name uppercases unquoted segments and preserves double-quoted identifiers."""
    assert split_teradata_table_name(raw_name) == expected


@pytest.mark.parametrize(
    "invalid_name", ["", "   ", ".tbl", "db.", "cat.db.tbl", "db..tbl", '"unterminated', 'db.""', 'db.bad"quote']
)
def test_split_teradata_table_name_rejects_invalid_identifiers(invalid_name: str) -> None:
    """split_teradata_table_name rejects empty, malformed, and multi-dot table names."""
    with pytest.raises(ImproperConfigurationError, match="Invalid Teradata table name"):
        split_teradata_table_name(invalid_name)


def test_teradata_existence_sql_uses_locking_trim_and_typed_placeholders() -> None:
    """Teradata catalog existence probes use LOCKING ROW FOR ACCESS, TRIM, UPPER, and typed CAST(? AS VARCHAR(128))."""
    for sql in (_TERADATA_TABLE_EXISTS_SQL, _TERADATA_INDEX_EXISTS_SQL):
        assert sql.startswith("LOCKING ROW FOR ACCESS SELECT 1 FROM DBC.")
        assert "UPPER(TRIM(DatabaseName)) = UPPER(TRIM(COALESCE(CAST(? AS VARCHAR(128)), DATABASE)))" in sql
        assert "UPPER(TRIM(TableName)) = UPPER(TRIM(CAST(? AS VARCHAR(128))))" in sql
        parsed = sqlglot.parse_one(sql, read="teradata")
        assert parsed.sql("teradata")
    assert "UPPER(TRIM(IndexName)) = UPPER(TRIM(CAST(? AS VARCHAR(128))))" in _TERADATA_INDEX_EXISTS_SQL


def test_resolve_reconcile_adbc_dialect_normalizes_vantage() -> None:
    """resolve_reconcile_adbc_dialect maps vantage hints and FlightSQL statement dialects to teradata."""
    assert resolve_reconcile_adbc_dialect({"uri": "grpc+tls://host:443", "adbc_dialect": "vantage"}, None) == "teradata"
    assert (
        resolve_reconcile_adbc_dialect({"driver": "flightsql", "uri": "grpc+tls://host:443"}, "vantage") == "teradata"
    )


@pytest.mark.parametrize("detected", ["teradata", "vantage"])
def test_teradata_statement_config_uses_vantage_sqlglot_dialect(detected: str) -> None:
    from sqlglot.dialects.dialect import Dialect

    from sqlspec.adapters.adbc.core import get_statement_config

    assert get_statement_config(detected).dialect == "vantage"
    assert Dialect.get("vantage") is not None


def test_teradata_driver_reports_teradata_for_vantage_statement_config() -> None:
    from sqlspec.adapters.adbc.core import get_statement_config

    def _marker(expression: Any, context: Any) -> Any:
        return expression

    statement_config = get_statement_config("teradata").replace(statement_transformers=(_marker,))
    driver = AdbcDriver(Mock(adbc_connection=None), statement_config=statement_config)
    assert driver.statement_config.statement_transformers == (_marker,)
    assert driver.dialect == "vantage"
    assert driver._dialect_name == "teradata"


def test_teradata_error_codes_map_only_for_teradata_sources() -> None:
    from adbc_driver_manager import OperationalError

    from sqlspec.adapters.adbc.core import create_mapped_exception, create_teradata_mapped_exception
    from sqlspec.adapters.adbc.driver import AdbcExceptionHandler
    from sqlspec.exceptions import PermissionDeniedError

    def make_error(message: str) -> OperationalError:
        return OperationalError(message, status_code=AdbcStatusCode.UNKNOWN, vendor_code=None, sqlstate=None)

    unmarked = make_error("[Error 3523] request rejected")
    assert type(create_mapped_exception(unmarked)) is SQLSpecError
    assert isinstance(create_teradata_mapped_exception(unmarked), PermissionDeniedError)
    assert isinstance(
        create_mapped_exception(make_error("[Teradata Database] [Error 3523] denied")), PermissionDeniedError
    )

    for dialect, expected in (("teradata", PermissionDeniedError), ("postgres", SQLSpecError)):
        handler = AdbcExceptionHandler(dialect)
        with handler:
            raise make_error("[Error 3523] request rejected")
        assert type(handler.pending_exception) is expected


def test_snowflake_script_binds_only_real_placeholders() -> None:
    """Stages, pipe ordinals, and comment markers do not consume subsequent statement binds."""
    executed: list[tuple[str, Any]] = []
    cursor = SimpleNamespace(
        execute=lambda sql, parameters=None: executed.append((sql, parameters)),
        close=lambda: None,
        rowcount=0,
        description=None,
    )
    connection = SimpleNamespace(cursor=lambda: cursor, adbc_get_info=lambda: {"vendor_name": "Snowflake"})
    driver = AdbcDriver(cast("Any", connection), dialect="snowflake")
    result = driver.execute_script("SELECT $1 FROM @stage; SELECT ? AS value; SELECT ? AS other", (7, 8))
    assert executed == [("SELECT $1 FROM @stage", None), ("SELECT 7 AS value", None), ("SELECT 8 AS other", None)]
    assert result.successful_statements == 3


def test_adbc_snowflake_observability_and_statement_query_tags() -> None:
    """Per-session Snowflake telemetry and correlation tags preserve shared runtime identity."""
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime
    from sqlspec.utils.correlation import CorrelationContext

    calls: list[dict[str, Any]] = []
    events: list[Any] = []
    cursor = SimpleNamespace(
        execute=lambda _sql, parameters=None: None,
        close=lambda: None,
        fetch_arrow_table=lambda: pa.table({"OK": [1]}),
        description=[("OK",)],
        rowcount=1,
    )

    def make_cursor(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return cursor

    connection = SimpleNamespace(cursor=make_cursor, adbc_get_info=lambda: {"vendor_name": "Snowflake"})
    driver = AdbcDriver(cast("Any", connection), dialect="snowflake", driver_features={"query_tag": "etl"})
    runtime = ObservabilityRuntime(ObservabilityConfig(statement_observers=[events.append]), config_name="AdbcConfig")
    driver.attach_observability(runtime)
    with CorrelationContext.context("req-7"):
        assert driver.select_one("SELECT 1 AS ok") == {"ok": 1}
    driver.execute("SELECT 1 AS ok")
    assert calls == [
        {"adbc_stmt_kwargs": {"adbc.snowflake.statement.query_tag": "etl:req-7"}},
        {"adbc_stmt_kwargs": {"adbc.snowflake.statement.query_tag": "etl"}},
    ]
    assert [event.db_system for event in events] == ["snowflake", "snowflake"]
    assert events[0].correlation_id == "req-7"
    assert runtime.config_name == "AdbcConfig"


def test_adbc_non_snowflake_cursor_has_no_statement_options() -> None:
    """Other ADBC backends retain the zero-keyword cursor contract."""
    calls: list[dict[str, Any]] = []
    cursor = SimpleNamespace(close=lambda: None)

    def make_cursor(**kwargs: Any) -> Any:
        calls.append(kwargs)
        return cursor

    connection = SimpleNamespace(cursor=make_cursor, adbc_get_info=lambda: {"vendor_name": "SQLite"})
    driver = AdbcDriver(cast("Any", connection), dialect="sqlite", driver_features={"query_tag": "unused"})
    with driver.with_cursor(cast("Any", connection)) as opened:
        assert opened is cursor
    assert calls == [{}]


def test_adbc_snowflake_observability_observes_hooks_registered_after_attach() -> None:
    """Attaching an idle runtime preserves later hook registration and backend identity."""
    from sqlspec.observability import ObservabilityRuntime

    queries: list[dict[str, Any]] = []
    cursor = SimpleNamespace(
        execute=lambda _sql, parameters=None: None,
        close=lambda: None,
        fetch_arrow_table=lambda: pa.table({"OK": [1]}),
        description=[("OK",)],
        rowcount=1,
    )
    connection = SimpleNamespace(cursor=lambda: cursor, adbc_get_info=lambda: {"vendor_name": "Snowflake"})
    driver = AdbcDriver(cast("Any", connection), dialect="snowflake")
    runtime = ObservabilityRuntime(config_name="AdbcConfig")
    driver.attach_observability(runtime)
    runtime.register_lifecycle_hook("on_query_start", queries.append)

    assert driver.select_one("SELECT 1 AS ok") == {"ok": 1}
    assert len(queries) == 1
    assert driver.observability is runtime
    assert queries[0]["sql"] == "SELECT 1 AS ok"
    assert queries[0]["driver"] == "AdbcDriver"
    event = runtime.create_statement_event(
        sql=queries[0]["sql"],
        parameters=None,
        driver=queries[0]["driver"],
        db_system=queries[0]["db_system"],
        operation="SELECT",
    )
    assert event.db_system == "snowflake"
    assert runtime.config_name == "AdbcConfig"


def test_adbc_backend_identity_does_not_leak_across_shared_runtime() -> None:
    """One live runtime keeps distinct Snowflake and SQLite ADBC statement identities."""
    from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime

    events: list[Any] = []
    started: list[dict[str, Any]] = []
    runtime = ObservabilityRuntime(ObservabilityConfig(statement_observers=[events.append]), config_name="AdbcConfig")
    cursor = SimpleNamespace(
        execute=lambda _sql, parameters=None: None,
        close=lambda: None,
        fetch_arrow_table=lambda: pa.table({"VALUE": [1]}),
        description=[("VALUE",)],
        rowcount=1,
    )
    snowflake_connection = SimpleNamespace(cursor=lambda: cursor, adbc_get_info=lambda: {"vendor_name": "Snowflake"})
    sqlite_connection = SimpleNamespace(cursor=lambda: cursor, adbc_get_info=lambda: {"vendor_name": "SQLite"})
    snowflake = AdbcDriver(cast("Any", snowflake_connection), dialect="snowflake")
    sqlite = AdbcDriver(cast("Any", sqlite_connection), dialect="sqlite")
    snowflake.attach_observability(runtime)
    sqlite.attach_observability(runtime)
    runtime.register_lifecycle_hook("on_query_start", started.append)
    for driver in (snowflake, sqlite, snowflake):
        assert driver.select_value("SELECT 1 AS value") == 1
    assert [event.db_system for event in events] == ["snowflake", "adbc", "snowflake"]
    assert [event.driver for event in events] == ["AdbcDriver", "AdbcDriver", "AdbcDriver"]
    assert len(started) == 3
    assert runtime.config_name == "AdbcConfig"

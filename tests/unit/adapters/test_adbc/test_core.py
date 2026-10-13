"""Unit tests for ADBC core execute-many helpers."""

from collections.abc import Sequence
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
import pytest
from adbc_driver_manager import AdbcStatusCode, DatabaseError

from sqlspec.adapters.adbc import AdbcDriver
from sqlspec.adapters.adbc import core as adbc_core
from sqlspec.adapters.adbc.core import (
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
)
from sqlspec.core import SQL, ParameterStyle
from sqlspec.dialects import PGTextSearch
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
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


def test_prepare_parameters_with_casts_uses_type_converter_factory(monkeypatch: Any) -> None:
    statement_config = get_statement_config("postgres")
    factory_calls: list[tuple[str, int]] = []

    class FakeConverter:
        def convert_dict(self, value: dict[str, Any]) -> str:
            return f"factory:{sorted(value.items())!r}"

    def fake_factory(dialect: str, cache_size: int = 5000) -> FakeConverter:
        factory_calls.append((dialect, cache_size))
        return FakeConverter()

    monkeypatch.setattr(adbc_core, "get_adbc_type_converter", fake_factory)

    prepared = prepare_parameters_with_casts(
        [{"id": 1}], {}, statement_config, dialect="postgres", json_serializer=lambda value: str(value)
    )

    assert prepared == ["factory:[('id', 1)]"]
    assert factory_calls == [("postgres", 5000)]


def test_prepare_batch_with_casts_creates_converter_once(monkeypatch: Any) -> None:
    statement_config = get_statement_config("postgres")
    factory_calls: list[tuple[str, int]] = []

    class FakeConverter:
        def convert_dict(self, value: dict[str, Any]) -> str:
            return f"factory:{sorted(value.items())!r}"

    def fake_factory(dialect: str, cache_size: int = 5000) -> FakeConverter:
        factory_calls.append((dialect, cache_size))
        return FakeConverter()

    monkeypatch.setattr(adbc_core, "get_adbc_type_converter", fake_factory)

    prepared = prepare_batch_with_casts(
        [(1, {"name": "alice"}), (2, {"name": "bob"}), (3, {"name": "carol"})],
        {1: "INT"},
        statement_config,
        dialect="postgres",
        json_serializer=lambda value: str(value),
    )

    assert prepared == [
        (1, "factory:[('name', 'alice')]"),
        (2, "factory:[('name', 'bob')]"),
        (3, "factory:[('name', 'carol')]"),
    ]
    assert factory_calls == [("postgres", 5000)]


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
    oracle_cfg, oracle_features = adbc_core.apply_driver_features(
        get_statement_config("oracle"), None, dialect="oracle"
    )
    db2_cfg, db2_features = adbc_core.apply_driver_features(get_statement_config("db2"), None, dialect="db2")
    _pg_cfg, pg_features = adbc_core.apply_driver_features(get_statement_config("postgres"), None, dialect="postgres")
    _mssql_cfg, mssql_features = adbc_core.apply_driver_features(get_statement_config("mssql"), None, dialect="mssql")

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

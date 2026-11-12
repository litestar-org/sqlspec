from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from sqlspec.adapters.teradata.core import build_connection_config, build_set_query_band_sql, create_mapped_exception
from sqlspec.adapters.teradata.type_converter import prepare_teradata_parameters
from sqlspec.exceptions import ImproperConfigurationError, UniqueViolationError


def test_native_connection_options() -> None:
    options = {"host": "td.local", "username": "dbc", "port": 1025, "encryptdata": False}
    assert build_connection_config(options) == {
        "host": "td.local",
        "user": "dbc",
        "dbs_port": "1025",
        "encryptdata": "false",
        "charset": "UTF8",
    }
    assert options["username"] == "dbc"


def test_query_band_escapes_literal() -> None:
    assert build_set_query_band_sql({"app": "O'Reilly"}) == "SET QUERY_BAND = 'app=O''Reilly;' FOR SESSION;"
    with pytest.raises(ImproperConfigurationError):
        build_set_query_band_sql({"bad;key": "value"})


def test_native_error_mapping() -> None:
    assert isinstance(
        create_mapped_exception(Exception("[Teradata Database] [Error 2801] duplicate")), UniqueViolationError
    )


def test_native_parameter_types() -> None:
    assert prepare_teradata_parameters([True, Decimal("1.25"), UUID(int=0), memoryview(b"abc")]) == (
        1,
        Decimal("1.25"),
        "00000000-0000-0000-0000-000000000000",
        b"abc",
    )


@pytest.mark.parametrize("port", [0, 70000, True, "bad"])
def test_connection_rejects_invalid_port(port) -> None:
    with pytest.raises(ImproperConfigurationError):
        build_connection_config({"host": "td.local", "port": port})


@pytest.mark.parametrize("size", [0, -1, True, "10"])
def test_config_rejects_invalid_fetch_chunk(size) -> None:
    from sqlspec.adapters.teradata import TeradataConfig

    with pytest.raises(ImproperConfigurationError):
        TeradataConfig(driver_features={"arraysize": size})


def test_derived_config_preserves_pool_settings() -> None:
    from sqlspec.adapters.teradata import TeradataConfig

    original = TeradataConfig(connection_config={"host": "td.local", "pool_max_size": 2, "pool_timeout": 0.5})
    derived = original.derive_pool(connection_config_overrides={"database": "analytics"})
    pool = derived.provide_pool()
    assert pool.size() == 0
    assert derived.connection_config["database"] == "analytics"
    assert derived.connection_config["host"] == "td.local"
    pool.close()


@pytest.mark.parametrize(
    "codes,expected",
    [
        ((2801, 2802, 2803), "UniqueViolationError"),
        ((2700, 2701, 5312, 5313), "ForeignKeyViolationError"),
        ((3604, 3811), "NotNullViolationError"),
        ((5317,), "CheckViolationError"),
        ((2631,), "DeadlockError"),
        ((3110, 3156), "QueryTimeoutError"),
        ((3523, 3524, 8017), "PermissionDeniedError"),
        ((3706, 3707), "SQLParsingError"),
        ((3802, 3807, 5628), "NotFoundError"),
        ((2616, 2620, 2621, 2665, 6706, 6758, 6760), "DataError"),
        ((2644, 2646, 2843, 3932), "OperationalError"),
    ],
)
def test_error_code_families_preserve_cause(codes: tuple[int, ...], expected: str) -> None:
    import sqlspec.exceptions as exceptions

    for code in codes:
        error = RuntimeError(f"[Teradata Database] [Error {code}] request failed")
        mapped = create_mapped_exception(error)
        assert type(mapped) is getattr(exceptions, expected)
        assert mapped.__cause__ is error
        assert str(code) in str(mapped)


@pytest.mark.parametrize(
    "message",
    [
        "unable to connect",
        "connection refused",
        "connection reset",
        "hostname lookup failed",
        "dial tcp failed",
        "TLS handshake failed",
        "closed connection",
    ],
)
def test_transport_error_messages_map_to_connection_error(message: str) -> None:
    from sqlspec.exceptions import DatabaseConnectionError

    error = RuntimeError(message)
    mapped = create_mapped_exception(error)
    assert isinstance(mapped, DatabaseConnectionError)
    assert mapped.__cause__ is error


def test_unclassified_errors_preserve_generic_or_operational_family() -> None:
    vendor_error_type = type("OperationalError", (Exception,), {})

    from sqlspec.exceptions import OperationalError, SQLSpecError

    assert type(create_mapped_exception(RuntimeError("[Error 9999] unknown"))) is SQLSpecError
    assert type(create_mapped_exception(vendor_error_type("request failed"))) is OperationalError


def test_unmapped_error_code_is_logged_for_diagnostics() -> None:
    logger = MagicMock()
    create_mapped_exception(RuntimeError("[Error 9999] unknown"), logger=logger)
    logger.debug.assert_called_once_with("Unmapped Teradata error number: %s", 9999)


@pytest.mark.parametrize(
    "band",
    [
        {},
        "",
        ";",
        "missing-equals",
        "app=etl;;",
        "app=etl;missing_equals",
        "app=etl; bad=1",
        {"bad key": "value"},
        {"key": "bad;value"},
        {"key": "bad\nvalue"},
        {"key": "bad\rvalue"},
        {"key": "bad\x00value"},
    ],
)
def test_query_band_rejects_unsafe_input(band: Any) -> None:
    with pytest.raises(ImproperConfigurationError):
        build_set_query_band_sql(band)


@pytest.mark.parametrize("band", ["NONE", " none; "])
def test_query_band_none_clears_metadata(band: str) -> None:
    assert build_set_query_band_sql(band) == "SET QUERY_BAND = NONE FOR SESSION;"


def test_query_band_multiple_mapping_values_and_transaction_scope() -> None:
    assert (
        build_set_query_band_sql({"app": "etl", "client": "O'Reilly"}, transaction=True)
        == "SET QUERY_BAND = 'app=etl;client=O''Reilly;' FOR TRANSACTION;"
    )
    assert build_set_query_band_sql("app=etl;") == "SET QUERY_BAND = 'app=etl;' FOR SESSION;"
    assert (
        build_set_query_band_sql("app=etl;client=O'Reilly;")
        == "SET QUERY_BAND = 'app=etl;client=O''Reilly;' FOR SESSION;"
    )


def test_connection_options_merge_extra_without_overwriting_explicit_values() -> None:
    original = {
        "host": "td.local",
        "username": "unused",
        "user": "chosen",
        "extra": {"host": "ignored", "database": "analytics", "lob_support": True},
        "encryptdata": True,
        "column_name": False,
        "teradata_values": True,
        "password": None,
        "pool_min_size": 1,
        "pool_max_size": 4,
        "pool_timeout": 2,
        "pool_recycle_seconds": 100,
        "health_check_interval": 3,
    }
    assert build_connection_config(original) == {
        "host": "td.local",
        "user": "chosen",
        "database": "analytics",
        "lob_support": "true",
        "encryptdata": "true",
        "column_name": "false",
        "teradata_values": "true",
        "dbs_port": "1025",
        "charset": "UTF8",
    }
    assert original["password"] is None
    assert original["username"] == "unused"


def test_connection_raw_json_preserved_without_implicit_options() -> None:
    assert build_connection_config({"sJson": '{"host":"td.local"}'}) == {"sJson": '{"host":"td.local"}'}
    assert build_connection_config({"host": "td.local", "dbs_port": "2048"})["dbs_port"] == "2048"


@pytest.mark.parametrize(
    "sql_text,expected",
    [
        ("/* request */ -- ddl\n CREATE MULTISET TABLE orders (id INTEGER)", True),
        ("COLLECT STATISTICS ON orders", True),
        ("SET QUERY_BAND = 'app=etl;' FOR SESSION;", True),
        ("SET QUERY_BAND = 'app=etl;' FOR TRANSACTION;", False),
        ("SELECT 1", False),
        ("COMMENT ON TABLE orders IS 'orders'", True),
    ],
)
def test_ddl_boundary_classification(sql_text: str, expected: bool) -> None:
    from sqlspec.adapters.teradata.core import is_ddl_or_session_band_sql

    assert is_ddl_or_session_band_sql(sql_text) is expected


def test_qualified_identifier_quoting_and_insert_bindings() -> None:
    from sqlspec.adapters.teradata.core import build_insert_statement, format_identifier, split_schema_and_table

    assert split_schema_and_table('"analytics"."odd.table"') == ("analytics", "odd.table")
    assert split_schema_and_table("orders") == (None, "ORDERS")
    assert format_identifier('"analytics"."odd""table"') == '"analytics"."odd""table"'
    assert (
        build_insert_statement("analytics.orders", ["id", "total"])
        == 'INSERT INTO "analytics"."orders" ("id", "total") VALUES (?, ?)'
    )


@pytest.mark.parametrize("identifier", ["", "catalog.database.table"])
def test_identifier_rejects_invalid_qualification(identifier: str) -> None:
    from sqlspec.adapters.teradata.core import format_identifier
    from sqlspec.exceptions import SQLSpecError

    with pytest.raises(SQLSpecError, match="identifier"):
        format_identifier(identifier)


def test_column_labels_preserve_explicit_case_across_cache_reuse() -> None:
    from sqlspec.adapters.teradata.core import collect_rows, resolve_column_names

    cache: dict[tuple[int, bool], tuple[Any, list[str]]] = {}
    description = [("ID",), ("MixedCase",), ("1UPPER",)]
    assert resolve_column_names(description, cache) == ["id", "MixedCase", "1UPPER"]
    assert resolve_column_names(description, cache, enable_lowercase_column_names=False) == [
        "ID",
        "MixedCase",
        "1UPPER",
    ]
    assert resolve_column_names(description, cache) == ["id", "MixedCase", "1UPPER"]
    for number in range(257):
        assert resolve_column_names([(f"VALUE_{number}",)], cache) == [f"value_{number}"]
    assert resolve_column_names(description, cache) == ["id", "MixedCase", "1UPPER"]
    assert resolve_column_names(None, cache) == []
    assert collect_rows([{"id": 1}], description, cache) == ([{"id": 1}], ["id", "MixedCase", "1UPPER"], "dict")
    assert collect_rows([], description, cache) == ([], ["id", "MixedCase", "1UPPER"], "tuple")


def test_core_exports_profile_coercions_fastexport_and_table_ref() -> None:
    from sqlspec.adapters.teradata import core
    from sqlspec.exceptions import SQLSpecError

    for exported in (
        "DEFAULT_TERADATA_PORT",
        "INDEX_EXISTS_SQL",
        "TABLE_EXISTS_SQL",
        "default_statement_config",
        "driver_profile",
        "format_table_ref",
        "maybe_prefix_fastexport",
    ):
        assert exported in core.__all__

    coercions = core.driver_profile.custom_type_coercions
    assert coercions[bytearray](bytearray(b"abc")) == b"abc"
    assert coercions[memoryview](memoryview(b"xyz")) == b"xyz"

    assert core.maybe_prefix_fastexport("SELECT 1", enable_fastexport=False, in_transaction=False) == "SELECT 1"
    assert core.maybe_prefix_fastexport("SELECT 1", enable_fastexport=True, in_transaction=True) == "SELECT 1"
    assert (
        core.maybe_prefix_fastexport(
            "/* c */\nLOCKING ROW FOR ACCESS SELECT 1", enable_fastexport=True, in_transaction=False
        )
        == "{fn teradata_try_fastexport} /* c */\nLOCKING ROW FOR ACCESS SELECT 1"
    )
    assert (
        core.maybe_prefix_fastexport("UPDATE t SET a = 1", enable_fastexport=True, in_transaction=False)
        == "UPDATE t SET a = 1"
    )

    assert core.format_table_ref("mart.orders") == '"MART"."ORDERS"'
    assert core.format_table_ref('"mart"."odd.table"') == '"mart"."odd.table"'
    assert core.format_table_ref("orders", allow_schema=False) == '"ORDERS"'
    with pytest.raises(SQLSpecError, match="Schema-qualified"):
        core.format_table_ref("mart.orders", allow_schema=False)


@pytest.mark.parametrize(
    "reported,parameters,fallback,expected",
    [(5, [(1,)], None, 5), (-1, [(1,), (2,)], None, 2), (None, iter(()), None, 0), ("unknown", iter(()), 7, 7)],
)
def test_bulk_rowcount_payload_fallback(reported: Any, parameters: Any, fallback: int | None, expected: int) -> None:
    from types import SimpleNamespace

    from sqlspec.adapters.teradata.core import resolve_many_rowcount

    assert resolve_many_rowcount(SimpleNamespace(rowcount=reported), parameters, fallback_count=fallback) == expected


def test_feature_validation_runs_query_band_and_preserves_custom_json_serializer() -> None:
    from sqlspec.adapters.teradata import TeradataConfig

    def serializer(value: Any) -> str:
        return "encoded"

    config = TeradataConfig(
        driver_features={
            "query_band": {"app": "etl"},
            "json_serializer": serializer,
            "enable_lowercase_column_names": False,
        }
    )
    assert config.driver_features["json_serializer"] is serializer
    assert config.driver_features["enable_lowercase_column_names"] is False
    with pytest.raises(ImproperConfigurationError, match="query band"):
        TeradataConfig(driver_features={"query_band": {"bad;key": "x"}})

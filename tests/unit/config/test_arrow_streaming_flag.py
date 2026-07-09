"""Tests for the supports_arrow_streaming capability ClassVar."""

import pytest


def test_base_default_is_false() -> None:
    from sqlspec.config import DatabaseConfigProtocol

    assert DatabaseConfigProtocol.supports_arrow_streaming is False


@pytest.mark.parametrize(
    ("module_path", "class_name"),
    [
        ("sqlspec.adapters.adbc.config", "AdbcConfig"),
        ("sqlspec.adapters.duckdb.config", "DuckDBConfig"),
        ("sqlspec.adapters.bigquery.config", "BigQueryConfig"),
        ("sqlspec.adapters.oracledb.config", "OracleSyncConfig"),
        ("sqlspec.adapters.oracledb.config", "OracleAsyncConfig"),
        ("sqlspec.adapters.mssql_python.config", "MssqlPythonConfig"),
        ("sqlspec.adapters.arrow_odbc.config", "ArrowOdbcConfig"),
    ],
)
def test_streaming_adapters_opt_in(module_path: str, class_name: str) -> None:
    module = pytest.importorskip(module_path)
    assert getattr(module, class_name).supports_arrow_streaming is True

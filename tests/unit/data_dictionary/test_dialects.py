"""Unit tests for built-in data dictionary dialect registration."""

import pytest
from sqlglot import exp

from sqlspec.data_dictionary import get_dialect_config


def test_mssql_dialect_registered() -> None:
    """The MSSQL data-dictionary dialect should be available by canonical name."""
    cfg = get_dialect_config("mssql")

    assert cfg.name == "mssql"


def test_tsql_alias_resolves_to_mssql() -> None:
    """The tsql alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("tsql") is get_dialect_config("mssql")


def test_sqlserver_alias_resolves_to_mssql() -> None:
    """The sqlserver alias should resolve to the MSSQL dialect config."""
    assert get_dialect_config("sqlserver") is get_dialect_config("mssql")


@pytest.mark.parametrize(
    ("dialect", "parser_dialect", "expected_types", "unbounded"),
    [
        ("postgres", "postgres", ("INTEGER", "BIGINT", "DOUBLE PRECISION", "VARCHAR(255)"), "TEXT"),
        ("mssql", "tsql", ("INT", "BIGINT", "FLOAT", "NVARCHAR(255)"), "NVARCHAR(MAX)"),
        ("oracle", "oracle", ("NUMBER(10)", "NUMBER(19)", "BINARY_DOUBLE", "VARCHAR2(255)"), "CLOB"),
        ("mysql", "mysql", ("INT", "BIGINT", "DOUBLE", "VARCHAR(255)"), "TEXT"),
        ("mariadb", "mysql", ("INT", "BIGINT", "DOUBLE", "VARCHAR(255)"), "TEXT"),
        ("sqlite", "sqlite", ("INTEGER", "INTEGER", "REAL", "TEXT"), "TEXT"),
        ("duckdb", "duckdb", ("INTEGER", "BIGINT", "DOUBLE", "TEXT"), "TEXT"),
        ("spanner", "spanner", ("INT64", "INT64", "FLOAT64", "STRING(255)"), "STRING(MAX)"),
        ("bigquery", "bigquery", ("INT64", "INT64", "FLOAT64", "STRING"), "STRING"),
        ("cockroachdb", "postgres", ("INT8", "INT8", "FLOAT8", "VARCHAR(255)"), "STRING"),
    ],
)
def test_logical_types_use_dialect_syntax(
    dialect: str, parser_dialect: str, expected_types: tuple[str, ...], unbounded: str
) -> None:
    config = get_dialect_config(dialect)
    for logical_type, expected in zip(("integer", "bigint", "float", "varchar"), expected_types):
        actual = config.get_optimal_type(logical_type, length=255)
        assert actual == expected
        assert isinstance(exp.DataType.build(actual, dialect=parser_dialect), exp.DataType)
    assert config.get_optimal_type("varchar") == unbounded
    assert config.get_optimal_type("text") == unbounded

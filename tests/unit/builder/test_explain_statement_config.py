"""Unit tests for statement configuration carried through EXPLAIN builds."""

import pytest

from sqlspec.builder import Explain
from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig


@pytest.fixture
def numeric_config() -> StatementConfig:
    """PostgreSQL-shaped config using numeric placeholders."""
    parameter_config = ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
    )
    return StatementConfig(dialect="postgres", parameter_config=parameter_config)


def test_explain_preserves_parameter_style(numeric_config: StatementConfig) -> None:
    """An explained statement keeps the source placeholder style and parameter deduplication."""
    statement = SQL("SELECT * FROM t WHERE a = :x AND b = :x", x=1, statement_config=numeric_config)

    explained = statement.explain(analyze=True)
    sql, parameters = explained.compile()

    assert sql == "EXPLAIN (ANALYZE) SELECT * FROM t WHERE a = $1 AND b = $1"
    assert parameters == (1,)


def test_explain_preserves_config_object(numeric_config: StatementConfig) -> None:
    """The explained statement carries the source configuration rather than a fresh default."""
    explained = SQL("SELECT 1", statement_config=numeric_config).explain()

    assert explained.statement_config.parameter_config.default_parameter_style is ParameterStyle.NUMERIC


def test_explain_builder_dialect_override_wins(numeric_config: StatementConfig) -> None:
    """An explicit builder dialect overrides the source dialect but keeps the rest of the config."""
    statement = SQL("SELECT 1", statement_config=numeric_config)

    explained = Explain(statement, dialect="duckdb").build()

    assert explained.sql == "EXPLAIN SELECT 1"
    assert explained.statement_config.dialect == "duckdb"
    assert explained.statement_config.parameter_config.default_parameter_style is ParameterStyle.NUMERIC


def test_explain_from_raw_string_keeps_current_behavior() -> None:
    """A raw string has no source config, so the builder still derives one from the dialect."""
    explained = Explain("SELECT 1", dialect="postgres").build()

    assert explained.sql == "EXPLAIN SELECT 1"
    assert explained.statement_config.dialect == "postgres"


def test_sql_copy_accepts_statement_config(numeric_config: StatementConfig) -> None:
    """Rebinding a configuration through copy() is supported."""
    statement = SQL("SELECT 1")

    recopied = statement.copy(statement_config=numeric_config)

    assert recopied.statement_config is numeric_config

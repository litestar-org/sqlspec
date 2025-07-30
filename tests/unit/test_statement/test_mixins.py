"""Unit tests for driver mixins.

Tests the mixin classes that provide additional functionality for database drivers,
including SQL translation and result utilities.
"""

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from sqlglot import exp, parse_one

from sqlspec.driver.mixins import SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.sql import SQL, StatementConfig

if TYPE_CHECKING:
    pass


# Test SQLTranslatorMixin
class MockDriverWithTranslator(SQLTranslatorMixin):
    """Mock driver class with SQL translator mixin."""

    def __init__(self, dialect: str = "sqlite") -> None:
        self.dialect = dialect


@pytest.mark.parametrize(
    "input_sql,from_dialect,to_dialect,expected_contains",
    [
        # Basic translation
        ("SELECT * FROM users", "sqlite", "postgres", "SELECT"),
        # Different quote styles
        ('SELECT "name" FROM users', "sqlite", "mysql", "SELECT"),
        # Function differences
        ("SELECT SUBSTR(name, 1, 5) FROM users", "sqlite", "postgres", "SUBSTRING"),
        # Keep same dialect (formatting may differ)
        ("SELECT * FROM users", "sqlite", "sqlite", "FROM users"),
    ],
    ids=["basic", "quotes", "functions", "same_dialect"],
)
def test_sql_translator_convert_to_dialect(
    input_sql: str, from_dialect: str, to_dialect: str, expected_contains: str
) -> None:
    """Test SQL dialect conversion."""
    driver = MockDriverWithTranslator(from_dialect)

    # Create SQL object
    statement = SQL(input_sql, config=StatementConfig(dialect=from_dialect))

    # Convert
    result = driver.convert_to_dialect(statement, to_dialect=to_dialect)

    # Check result is a string
    assert isinstance(result, str)
    assert expected_contains in result


def test_sql_translator_handles_complex_queries() -> None:
    """Test SQL translation with complex queries."""
    driver = MockDriverWithTranslator("sqlite")

    complex_sql = """
    WITH ranked_users AS (
        SELECT name, age,
               ROW_NUMBER() OVER (PARTITION BY age ORDER BY name) as rn
        FROM users
    )
    SELECT * FROM ranked_users WHERE rn = 1
    """

    statement = SQL(complex_sql, config=StatementConfig(dialect="sqlite"))
    result = driver.convert_to_dialect(statement, to_dialect="postgres")

    # Should contain CTE and window function
    assert "WITH ranked_users AS" in result
    assert "ROW_NUMBER() OVER" in result


def test_sql_translator_convert_expression() -> None:
    """Test converting sqlglot expressions between dialects."""
    driver = MockDriverWithTranslator("sqlite")

    # Create a sqlglot expression
    expression = parse_one("SELECT SUBSTR(name, 1, 5) FROM users", dialect="sqlite")

    # Convert expression
    result_str = driver.convert_to_dialect(expression, to_dialect="postgres")

    # Should get converted string
    assert isinstance(result_str, str)
    assert "SUBSTRING" in result_str


def test_sql_translator_invalid_expression() -> None:
    """Test error handling for invalid expressions."""
    driver = MockDriverWithTranslator("sqlite")

    # Create an invalid expression (mock)
    expression = MagicMock(spec=exp.Expression)
    expression.sql.side_effect = Exception("Invalid SQL")

    with pytest.raises(SQLConversionError, match="Failed to convert SQL expression"):
        driver.convert_to_dialect(expression, to_dialect="postgres")


def test_sql_translator_parse_error() -> None:
    """Test error handling when SQL cannot be parsed."""
    driver = MockDriverWithTranslator("sqlite")

    # Pass invalid string that can't be parsed
    with pytest.raises(SQLConversionError, match="Failed to parse SQL statement"):
        driver.convert_to_dialect("NOT VALID SQL ;;; !!!", to_dialect="postgres")


def test_sql_translator_string_input() -> None:
    """Test converting raw string SQL between dialects."""
    driver = MockDriverWithTranslator("sqlite")

    # Pass raw string
    result = driver.convert_to_dialect("SELECT * FROM users", to_dialect="postgres")

    assert isinstance(result, str)
    assert "SELECT" in result
    assert "FROM users" in result


# Test ToSchemaMixin
class MockDriverWithToSchema(ToSchemaMixin):
    """Mock driver class with to-schema mixin."""

    def __init__(self) -> None:
        self.config = MagicMock()


def test_to_schema_mixin_import() -> None:
    """Test that ToSchemaMixin can be imported and instantiated."""
    driver = MockDriverWithToSchema()
    assert hasattr(driver, "config")


# Test multiple mixin inheritance
class MockDriverWithMultipleMixins(SQLTranslatorMixin, ToSchemaMixin):
    """Mock driver with multiple mixins."""

    def __init__(self) -> None:
        self.dialect = "sqlite"
        self.config = MagicMock()


def test_multiple_mixin_inheritance() -> None:
    """Test that a driver can inherit from multiple mixins without conflicts."""
    driver = MockDriverWithMultipleMixins()

    # From SQLTranslatorMixin
    assert hasattr(driver, "convert_to_dialect")
    assert callable(driver.convert_to_dialect)

    # From ToSchemaMixin
    assert hasattr(driver, "config")

    # Test that SQLTranslator method works
    statement = SQL("SELECT * FROM users", config=StatementConfig(dialect="sqlite"))
    result = driver.convert_to_dialect(statement, to_dialect="postgres")
    assert isinstance(result, str)
    assert "SELECT" in result

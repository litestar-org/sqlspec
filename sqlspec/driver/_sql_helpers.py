"""Pure helper functions for SQL dialect translation.

These functions are extracted from SQLTranslatorMixin to eliminate
cross-trait attribute access that causes mypyc segmentation faults.
"""

from typing import TYPE_CHECKING, Final, NoReturn

from sqlglot import exp, parse_one

from sqlspec.core import SQL, Statement
from sqlspec.exceptions import SQLConversionError

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType


__all__ = (
    "DEFAULT_PRETTY",
    "convert_to_dialect",
    "generate_sql_safely",
    "parse_statement_safely",
    "raise_conversion_error",
    "raise_parse_error",
    "raise_statement_parse_error",
)


DEFAULT_PRETTY: Final[bool] = True


def parse_statement_safely(statement: "Statement", dialect: "DialectType | None") -> "exp.Expression":
    """Parse statement with error handling.

    Args:
        statement: SQL statement to parse.
        dialect: Source dialect for parsing.

    Returns:
        Parsed expression.

    Raises:
        SQLConversionError: If parsing fails.

    """
    try:
        sql_string = str(statement)
        return parse_one(sql_string, dialect=dialect)
    except Exception as e:
        raise_parse_error(e)


def generate_sql_safely(expression: "exp.Expression", dialect: "DialectType | None", pretty: bool) -> str:
    """Generate SQL with error handling.

    Args:
        expression: Parsed expression to convert.
        dialect: Target SQL dialect.
        pretty: Whether to format the output SQL.

    Returns:
        Generated SQL string.

    Raises:
        SQLConversionError: If generation fails.

    """
    try:
        return expression.sql(dialect=dialect, pretty=pretty)
    except Exception as e:
        raise_conversion_error(dialect, e)


def convert_to_dialect(
    statement: "Statement",
    source_dialect: "DialectType | None",
    to_dialect: "DialectType | None" = None,
    pretty: bool = DEFAULT_PRETTY,
) -> str:
    """Convert a statement to a target SQL dialect.

    Args:
        statement: SQL statement to convert.
        source_dialect: Source dialect for parsing.
        to_dialect: Target dialect (defaults to source_dialect).
        pretty: Whether to format the output SQL.

    Returns:
        SQL string in target dialect.

    Raises:
        SQLConversionError: If conversion fails.

    """
    parsed_expression: exp.Expression | None = None

    if statement is not None and isinstance(statement, SQL):
        if statement.expression is None:
            raise_statement_parse_error()
        parsed_expression = statement.expression
    elif isinstance(statement, exp.Expression):
        parsed_expression = statement
    else:
        parsed_expression = parse_statement_safely(statement, source_dialect)

    target_dialect = to_dialect or source_dialect

    return generate_sql_safely(parsed_expression, target_dialect, pretty)


def raise_statement_parse_error() -> "NoReturn":
    """Raise error for unparsable statements.

    Raises:
        SQLConversionError: Always raised.

    """
    msg = "Statement could not be parsed"
    raise SQLConversionError(msg)


def raise_parse_error(e: Exception) -> "NoReturn":
    """Raise error for parsing failures.

    Args:
        e: Original exception that caused the failure.

    Raises:
        SQLConversionError: Always raised.

    """
    error_msg = f"Failed to parse SQL statement: {e!s}"
    raise SQLConversionError(error_msg) from e


def raise_conversion_error(dialect: "DialectType | None", e: Exception) -> "NoReturn":
    """Raise error for conversion failures.

    Args:
        dialect: Target dialect that caused the failure.
        e: Original exception that caused the failure.

    Raises:
        SQLConversionError: Always raised.

    """
    error_msg = f"Failed to convert SQL expression to {dialect}: {e!s}"
    raise SQLConversionError(error_msg) from e

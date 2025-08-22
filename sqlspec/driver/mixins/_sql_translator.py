"""SQL translation mixin for cross-database compatibility."""

from typing import Final, NoReturn, Optional

from mypy_extensions import trait
from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.core.statement import SQL, Statement
from sqlspec.exceptions import SQLConversionError

__all__ = ("SQLTranslatorMixin",)


_DEFAULT_PRETTY: Final[bool] = True


@trait
class SQLTranslatorMixin:
    """Mixin for drivers supporting SQL translation."""

    __slots__ = ()
    dialect: "Optional[DialectType]"

    def convert_to_dialect(
        self, statement: "Statement", to_dialect: "Optional[DialectType]" = None, pretty: bool = _DEFAULT_PRETTY
    ) -> str:
        """Convert a statement to a target SQL dialect.

        Args:
            statement: SQL statement to convert
            to_dialect: Target dialect (defaults to current dialect)
            pretty: Whether to format the output SQL

        Returns:
            SQL string in target dialect


        """

        parsed_expression: Optional[exp.Expression] = None

        if statement is not None and isinstance(statement, SQL):
            if statement.expression is None:
                self._raise_statement_parse_error()
            parsed_expression = statement.expression
        elif isinstance(statement, exp.Expression):
            parsed_expression = statement
        else:
            parsed_expression = self._parse_statement_safely(statement)

        target_dialect = to_dialect or self.dialect

        return self._generate_sql_safely(parsed_expression, target_dialect, pretty)

    def _parse_statement_safely(self, statement: "Statement") -> "exp.Expression":
        """Parse statement with error handling.

        Args:
            statement: SQL statement to parse

        Returns:
            Parsed expression

        """
        try:
            sql_string = str(statement)

            return parse_one(sql_string, dialect=self.dialect, copy=False)
        except Exception as e:
            self._raise_parse_error(e)

    def _generate_sql_safely(self, expression: "exp.Expression", dialect: DialectType, pretty: bool) -> str:
        """Generate SQL with error handling.

        Args:
            expression: Parsed expression to convert
            dialect: Target SQL dialect
            pretty: Whether to format the output SQL

        Returns:
            Generated SQL string

        """
        try:
            return expression.sql(dialect=dialect, pretty=pretty)
        except Exception as e:
            self._raise_conversion_error(dialect, e)

    def _raise_statement_parse_error(self) -> NoReturn:
        """Raise error for unparsable statements.

        Raises:
            SQLConversionError: Always raised
        """
        msg = "Statement could not be parsed"
        raise SQLConversionError(msg)

    def _raise_parse_error(self, e: Exception) -> NoReturn:
        """Raise error for parsing failures.

        Args:
            e: Original exception that caused the failure

        Raises:
            SQLConversionError: Always raised
        """
        error_msg = f"Failed to parse SQL statement: {e!s}"
        raise SQLConversionError(error_msg) from e

    def _raise_conversion_error(self, dialect: DialectType, e: Exception) -> NoReturn:
        """Raise error for conversion failures.

        Args:
            dialect: Target dialect that caused the failure
            e: Original exception that caused the failure

        Raises:
            SQLConversionError: Always raised
        """
        error_msg = f"Failed to convert SQL expression to {dialect}: {e!s}"
        raise SQLConversionError(error_msg) from e

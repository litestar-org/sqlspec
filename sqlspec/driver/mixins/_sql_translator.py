from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.sql import SQL, Statement

__all__ = ("SQLTranslatorMixin",)


class SQLTranslatorMixin:
    """Mixin for drivers supporting SQL translation."""

    __slots__ = ()

    def convert_to_dialect(self, statement: "Statement", to_dialect: DialectType = None, pretty: bool = True) -> str:
        parsed_expression: exp.Expression
        if statement is not None and isinstance(statement, SQL):
            if statement.expression is None:
                msg = "Statement could not be parsed"
                raise SQLConversionError(msg)
            parsed_expression = statement.expression
        elif isinstance(statement, exp.Expression):
            parsed_expression = statement
        else:
            try:
                parsed_expression = parse_one(statement, dialect=self.dialect)  # type: ignore[attr-defined]
            except Exception as e:
                error_msg = f"Failed to parse SQL statement: {e!s}"
                raise SQLConversionError(error_msg) from e
        target_dialect = to_dialect if to_dialect is not None else self.dialect  # type: ignore[attr-defined]
        try:
            return parsed_expression.sql(dialect=target_dialect, pretty=pretty)
        except Exception as e:
            error_msg = f"Failed to convert SQL expression to {target_dialect}: {e!s}"
            raise SQLConversionError(error_msg) from e

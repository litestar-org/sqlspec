from mypy_extensions import trait
from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.core.statement import SQL, Statement
from sqlspec.exceptions import SQLConversionError

__all__ = ("SQLTranslatorMixin",)


@trait
class SQLTranslatorMixin:
    """Mixin for drivers supporting SQL translation."""

    __slots__ = ()

    def convert_to_dialect(self, statement: "Statement", to_dialect: DialectType = None, pretty: bool = True) -> str:
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
        target_dialect = to_dialect or self.dialect  # type: ignore[attr-defined]
        try:
            return parsed_expression.sql(dialect=target_dialect, pretty=pretty)
        except Exception as e:
            error_msg = f"Failed to convert SQL expression to {target_dialect}: {e!s}"
            raise SQLConversionError(error_msg) from e

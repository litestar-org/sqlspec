from typing import TYPE_CHECKING

from sqlglot import exp, parse_one
from sqlglot.dialects.dialect import DialectType

from sqlspec.exceptions import SQLConversionError
from sqlspec.statement.sql import SQL, Statement

if TYPE_CHECKING:
    from sqlspec.statement.parameters import ParameterStyle

__all__ = ("SQLTranslatorMixin",)


class SQLTranslatorMixin:
    """Mixin for drivers supporting SQL translation."""

    def convert_to_dialect(
        self,
        statement: "Statement",
        to_dialect: DialectType = None,
        pretty: bool = True,
    ) -> str:
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

    def convert_placeholders_in_raw_sql(self, sql: str, target_style: "ParameterStyle") -> str:
        """Convert placeholders in raw SQL string to target style using the parameter system.

        This is used when parsing is disabled but we still need placeholder conversion
        to match the database driver's expected parameter style.

        Args:
            sql: The SQL string with placeholders to convert
            target_style: The target parameter style to convert to

        Returns:
            The SQL string with converted placeholders
        """
        from sqlspec.statement.parameters import ParameterStyle, ParameterValidator

        validator = ParameterValidator()
        parameters_info = validator.extract_parameters(sql)

        if not parameters_info:
            return sql

        # Convert placeholders from back to front to preserve positions
        result_sql = sql
        for param_info in reversed(parameters_info):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)

            # Generate new placeholder based on target style
            if target_style == ParameterStyle.NUMERIC:
                new_placeholder = f"${param_info.ordinal + 1}"
            elif target_style == ParameterStyle.PYFORMAT_POSITIONAL:
                new_placeholder = "%s"
            elif target_style == ParameterStyle.PYFORMAT_NAMED:
                new_placeholder = f"%({param_info.name})s"
            elif target_style == ParameterStyle.QMARK:
                new_placeholder = "?"
            elif target_style == ParameterStyle.NAMED_COLON:
                new_placeholder = f":{param_info.name}"
            elif target_style == ParameterStyle.NAMED_AT:
                new_placeholder = f"@{param_info.name}"
            elif target_style == ParameterStyle.NAMED_DOLLAR:
                new_placeholder = f"${param_info.name}"
            else:
                # Keep the original placeholder if we don't know how to convert
                new_placeholder = param_info.placeholder_text

            result_sql = result_sql[:start_pos] + new_placeholder + result_sql[end_pos:]

        return result_sql

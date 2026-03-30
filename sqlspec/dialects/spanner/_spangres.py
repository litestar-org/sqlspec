r"""Google Cloud Spanner PostgreSQL-interface dialect ("Spangres")."""

from typing import Any, Final, cast

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator
from sqlglot.parsers.postgres import PostgresParser
from sqlglot.tokenizer_core import TokenType

__all__ = ("Spangres",)

_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_TTL_MIN_COMPONENTS = 2
_ORIGINAL_PARSE_PROPERTY_ATTR: Final[str] = "_sqlspec_original_parse_property"
_HOOKS_REGISTERED_ATTR: Final[str] = "_sqlspec_spangres_hooks_registered"


def _is_spangres_dialect(parser: Any) -> bool:
    dialect = getattr(parser, "dialect", None)
    return dialect is not None and dialect.__class__.__name__ == "Spangres"


def _original_postgres_parse_property() -> Any:
    original = getattr(PostgresParser, _ORIGINAL_PARSE_PROPERTY_ATTR, None)
    if callable(original):
        return original
    original = PostgresParser._parse_property
    setattr(PostgresParser, _ORIGINAL_PARSE_PROPERTY_ATTR, original)
    return original


def _normalize_interval_expression(expression: exp.Expr) -> exp.Expr:
    if isinstance(expression, exp.Alias):
        alias = expression.args.get("alias")
        if isinstance(alias, exp.Identifier) and isinstance(expression.this, exp.Expr):
            return exp.Interval(this=expression.this.copy(), unit=alias.copy())
    return expression


def _render_interval_sql(generator: Any, expression: exp.Expr) -> str:
    if isinstance(expression, exp.Interval):
        unit = expression.args.get("unit")
        if isinstance(expression.this, exp.Literal) and not expression.this.is_string and isinstance(unit, exp.Expr):
            return f"INTERVAL {generator.sql(expression.this)} {generator.sql(unit)}"

    interval_sql = cast("str", generator.sql(expression))
    if not interval_sql.upper().startswith("INTERVAL"):
        return f"INTERVAL {interval_sql}"
    return interval_sql


def _spangres_parse_property(self: Any) -> exp.Expr:
    if _is_spangres_dialect(self) and self._match_text_seq("ROW", "DELETION", "POLICY"):
        self._match(TokenType.L_PAREN)
        self._match_text_seq("OLDER_THAN")
        self._match(TokenType.L_PAREN)
        column = cast("exp.Expr", self._parse_id_var())
        self._match(TokenType.COMMA)
        self._match_text_seq("INTERVAL")
        interval = _normalize_interval_expression(cast("exp.Expr", self._parse_expression()))
        self._match(TokenType.R_PAREN)
        self._match(TokenType.R_PAREN)

        return exp.Property(
            this=exp.Literal.string(_ROW_DELETION_NAME), value=exp.Tuple(expressions=[column, interval])
        )

    return cast("exp.Expr", _original_postgres_parse_property()(self))


def _register_postgres_spangres_parser_hooks() -> None:
    if getattr(PostgresParser, _HOOKS_REGISTERED_ATTR, False):
        return

    _original_postgres_parse_property()
    setattr(PostgresParser, "_parse_property", _spangres_parse_property)
    setattr(PostgresParser, _HOOKS_REGISTERED_ATTR, True)


class SpangresGenerator(PostgresGenerator):
    """Generate Spanner row deletion policies."""

    def property_sql(self, expression: exp.Property) -> str:
        if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _ROW_DELETION_NAME:
            values = expression.args.get("value")
            if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
                column = self.sql(values.expressions[0])
                interval_sql = _render_interval_sql(self, values.expressions[1])
                return f"ROW DELETION POLICY (OLDER_THAN({column}, {interval_sql}))"

        return str(super().property_sql(expression))


_register_postgres_spangres_parser_hooks()


class Spangres(Postgres):
    """Spanner PostgreSQL-compatible dialect."""

    Parser = Postgres.Parser
    Generator = SpangresGenerator

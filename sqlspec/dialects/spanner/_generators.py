"""Spanner dialect generators shared by the GoogleSQL and PostgreSQL variants.

Spanner DDL extensions are modeled as canonical ``exp.Property`` nodes so the
same AST renders to either dialect grammar:

- GoogleSQL: ``) PRIMARY KEY (...), INTERLEAVE IN [PARENT] t [ON DELETE ...],
  ROW DELETION POLICY (OLDER_THAN(col, INTERVAL n DAY))`` (comma-separated).
- PostgreSQL: ``) INTERLEAVE IN [PARENT] t [ON DELETE ...] TTL INTERVAL
  'n days' ON col`` (no commas, TTL replaces ROW DELETION POLICY).

Extension happens through ``TRANSFORMS`` entries on the base generators:
sqlglot invokes those with explicit ``(generator, expression)`` arguments, so
the callables stay compilable. Subclassing or monkeypatching generator methods
is not an option because sqlglot[c] generator classes reject interpreted
subclasses and mypyc-compiled functions do not bind as methods.
"""

import re
from typing import Any, Final, cast

from sqlglot import exp
from sqlglot.generator import _DISPATCH_CACHE  # pyright: ignore[reportPrivateUsage]
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.postgres import PostgresGenerator

__all__ = ("SpangresGenerator", "SpannerGenerator")

_TTL_MIN_COMPONENTS = 2
_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_INTERLEAVE_NAME = "INTERLEAVE_IN_PARENT"
_INTERLEAVE_IN_NAME = "INTERLEAVE_IN"

_SPANNER_PROPERTY_NAMES: Final[frozenset[str]] = frozenset({_INTERLEAVE_NAME, _INTERLEAVE_IN_NAME, _ROW_DELETION_NAME})
_DAYS_PATTERN: Final["re.Pattern[str]"] = re.compile(r"^\s*(\d+)\s*days?\s*$", re.IGNORECASE)

# Capture originals before any patching
_original_bq_property_sql = BigQueryGenerator.property_sql
_original_bq_properties_sql = BigQueryGenerator.properties_sql
_original_pg_property_sql = PostgresGenerator.property_sql
_original_pg_properties_sql = PostgresGenerator.properties_sql


def _normalize_interval_expression(expression: exp.Expr) -> exp.Expr:
    if isinstance(expression, exp.Alias):
        alias = expression.args.get("alias")
        if isinstance(alias, exp.Identifier) and isinstance(expression.this, exp.Expr):
            return exp.Interval(this=expression.this.copy(), unit=alias.copy())
    return expression


def _is_post_schema_spanner_property(expression: exp.Expr) -> bool:
    if not isinstance(expression, exp.Property) or not isinstance(expression.this, exp.Literal):
        return False
    return expression.this.name.upper() in _SPANNER_PROPERTY_NAMES


def _get_dialect_name(generator: Any) -> "str | None":
    dialect_class = getattr(generator.dialect, "__class__", None)
    return dialect_class.__name__ if dialect_class else None


def _interval_days(expression: exp.Expr) -> "int | None":
    """Extract a whole-day count from an interval expression when possible."""
    if isinstance(expression, exp.Interval):
        unit = expression.args.get("unit")
        unit_name = unit.name.upper() if isinstance(unit, (exp.Identifier, exp.Var)) else ""
        literal = expression.this
        if isinstance(literal, exp.Literal):
            if not literal.is_string and unit_name in {"DAY", "DAYS"}:
                try:
                    return int(literal.name)
                except ValueError:
                    return None
            if literal.is_string:
                match = _DAYS_PATTERN.match(literal.name)
                if match:
                    return int(match.group(1))
    if isinstance(expression, exp.Literal) and expression.is_string:
        match = _DAYS_PATTERN.match(expression.name)
        if match:
            return int(match.group(1))
    return None


def _render_interval_sql(generator: Any, expression: exp.Expr) -> str:
    if isinstance(expression, exp.Interval):
        unit = expression.args.get("unit")
        if isinstance(expression.this, exp.Literal) and not expression.this.is_string and isinstance(unit, exp.Expr):
            return f"INTERVAL {generator.sql(expression.this)} {generator.sql(unit)}"

    interval_sql = cast("str", generator.sql(expression))
    if not interval_sql.upper().startswith("INTERVAL"):
        return f"INTERVAL {interval_sql}"
    return interval_sql


def _render_googlesql_interval(generator: Any, expression: exp.Expr) -> str:
    """Render a row-deletion interval in GoogleSQL form (``INTERVAL n DAY``)."""
    days = _interval_days(expression)
    if days is not None:
        return f"INTERVAL {days} DAY"
    return _render_interval_sql(generator, expression)


def _render_pg_interval_spec(generator: Any, expression: exp.Expr) -> str:
    """Render a TTL interval spec in PostgreSQL form (``'n days'``)."""
    days = _interval_days(expression)
    if days is not None:
        return f"'{days} days'"
    if isinstance(expression, exp.Interval) and isinstance(expression.this, exp.Literal):
        return cast("str", generator.sql(expression.this))
    return cast("str", generator.sql(expression))


def _render_interleave_sql(generator: Any, expression: exp.Property) -> "str | None":
    """Render INTERLEAVE IN [PARENT] for either dialect, or None if not interleave."""
    if not isinstance(expression.this, exp.Literal):
        return None
    name = expression.this.name.upper()
    if name not in {_INTERLEAVE_NAME, _INTERLEAVE_IN_NAME}:
        return None
    values = expression.args.get("value")
    if not isinstance(values, exp.Tuple) or not values.expressions:
        return None

    parent = generator.sql(values.expressions[0])
    if name == _INTERLEAVE_IN_NAME:
        return f"INTERLEAVE IN {parent}"

    sql = f"INTERLEAVE IN PARENT {parent}"
    if len(values.expressions) >= _TTL_MIN_COMPONENTS:
        on_delete_expr = values.expressions[1]
        if isinstance(on_delete_expr, exp.Literal):
            sql = f"{sql} ON DELETE {on_delete_expr.this}"
    return sql


def _row_deletion_components(expression: exp.Property) -> "tuple[exp.Expr, exp.Expr] | None":
    if not isinstance(expression.this, exp.Literal) or expression.this.name.upper() != _ROW_DELETION_NAME:
        return None
    values = expression.args.get("value")
    if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
        return values.expressions[0], values.expressions[1]
    return None


# ---------------------------------------------------------------------------
# Spanner property rendering (GoogleSQL, BigQuery-based)
# ---------------------------------------------------------------------------


def _spanner_property_sql(self: Any, expression: exp.Property) -> str:
    """Render Spanner GoogleSQL properties (INTERLEAVE, ROW DELETION POLICY)."""
    interleave_sql = _render_interleave_sql(self, expression)
    if interleave_sql is not None:
        return interleave_sql

    row_deletion = _row_deletion_components(expression)
    if row_deletion is not None:
        column, interval = row_deletion
        interval_sql = _render_googlesql_interval(self, interval)
        return f"ROW DELETION POLICY (OLDER_THAN({self.sql(column)}, {interval_sql}))"

    return str(_original_bq_property_sql(self, expression))


def _spanner_properties_sql(self: Any, expression: exp.Properties) -> str:
    """Render Spanner post-schema properties comma-separated per GoogleSQL DDL."""
    root_properties: list[exp.Expr] = []
    with_properties: list[exp.Expr] = []
    spanner_properties: list[exp.Expr] = []

    for property_expression in expression.expressions:
        if _is_post_schema_spanner_property(property_expression):
            spanner_properties.append(property_expression)
            continue

        property_location = self.PROPERTIES_LOCATION[property_expression.__class__]
        if property_location == exp.Properties.Location.POST_WITH:
            with_properties.append(property_expression)
        elif property_location == exp.Properties.Location.POST_SCHEMA:
            root_properties.append(property_expression)

    root_props_ast = exp.Properties(expressions=root_properties)
    root_props_ast.parent = expression.parent
    with_props_ast = exp.Properties(expressions=with_properties)
    with_props_ast.parent = expression.parent

    root_props = str(self.root_properties(root_props_ast))
    with_props = str(self.with_properties(with_props_ast))

    if root_props and with_props and not self.pretty:
        with_props = f" {with_props}"

    rendered = root_props + with_props
    spanner_block = ", ".join(
        _spanner_property_sql(self, cast("exp.Property", spanner_property)) for spanner_property in spanner_properties
    )
    if not spanner_block:
        return rendered
    return f"{rendered}, {spanner_block}" if rendered else f", {spanner_block}"


# ---------------------------------------------------------------------------
# Spangres property rendering (PostgreSQL-based)
# ---------------------------------------------------------------------------


def _spangres_property_sql(self: Any, expression: exp.Property) -> str:
    """Render Spangres properties (INTERLEAVE, TTL) in PostgreSQL-dialect form."""
    interleave_sql = _render_interleave_sql(self, expression)
    if interleave_sql is not None:
        return interleave_sql

    row_deletion = _row_deletion_components(expression)
    if row_deletion is not None:
        column, interval = row_deletion
        interval_spec = _render_pg_interval_spec(self, interval)
        return f"TTL INTERVAL {interval_spec} ON {self.sql(column)}"

    return str(_original_pg_property_sql(self, expression))


def _spangres_properties_sql(self: Any, expression: exp.Properties) -> str:
    """Render Spangres post-schema properties space-separated per PostgreSQL DDL."""
    spanner_properties = [p for p in expression.expressions if _is_post_schema_spanner_property(p)]
    if not spanner_properties:
        return str(_original_pg_properties_sql(self, expression))

    other_properties = [p for p in expression.expressions if not _is_post_schema_spanner_property(p)]
    other_ast = exp.Properties(expressions=other_properties)
    other_ast.parent = expression.parent
    rendered = str(_original_pg_properties_sql(self, other_ast)) if other_properties else ""
    parts = [rendered] if rendered else []
    parts.extend(
        _spangres_property_sql(self, cast("exp.Property", spanner_property)) for spanner_property in spanner_properties
    )
    return " ".join(parts)


# ---------------------------------------------------------------------------
# TRANSFORMS wiring (single code path for sqlglot and sqlglot[c])
# ---------------------------------------------------------------------------

# BigQuery / Spanner
_original_bq_property_transform = BigQueryGenerator.TRANSFORMS.get(exp.Property)
_original_bq_properties_transform = BigQueryGenerator.TRANSFORMS.get(exp.Properties)
_original_bq_create_transform = BigQueryGenerator.TRANSFORMS.get(exp.Create)


def _bq_property_transform(self: Any, expression: exp.Property) -> str:
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spanner":
        return _spanner_property_sql(self, expression)
    if _original_bq_property_transform is not None:
        return str(_original_bq_property_transform(self, expression))
    return str(_original_bq_property_sql(self, expression))


def _bq_properties_transform(self: Any, expression: exp.Properties) -> str:
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spanner":
        return _spanner_properties_sql(self, expression)
    if _original_bq_properties_transform is not None:
        return str(_original_bq_properties_transform(self, expression))
    return str(_original_bq_properties_sql(self, expression))


def _bq_create_transform(self: Any, expression: exp.Create) -> str:
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spanner" and expression.this and expression.kind == "TABLE":
        properties = expression.args.get("properties")
        if properties:
            spanner_props = [p for p in properties.expressions if _is_post_schema_spanner_property(p)]
            other_props = [p for p in properties.expressions if not _is_post_schema_spanner_property(p)]
            properties.set("expressions", other_props + spanner_props)

    if _original_bq_create_transform is not None:
        return str(_original_bq_create_transform(self, expression))
    return str(self.create_sql(expression))


BigQueryGenerator.TRANSFORMS[exp.Property] = _bq_property_transform
BigQueryGenerator.TRANSFORMS[exp.Properties] = _bq_properties_transform
BigQueryGenerator.TRANSFORMS[exp.Create] = _bq_create_transform

_DISPATCH_CACHE.pop(BigQueryGenerator, None)

SpannerGenerator = BigQueryGenerator  # pyright: ignore[reportAssignmentType]


# Postgres / Spangres
_original_pg_property_transform = PostgresGenerator.TRANSFORMS.get(exp.Property)
_original_pg_properties_transform = PostgresGenerator.TRANSFORMS.get(exp.Properties)


def _pg_property_transform(self: Any, expression: exp.Property) -> str:
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spangres":
        return _spangres_property_sql(self, expression)
    if _original_pg_property_transform is not None:
        return str(_original_pg_property_transform(self, expression))
    return str(_original_pg_property_sql(self, expression))


def _pg_properties_transform(self: Any, expression: exp.Properties) -> str:
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spangres":
        return _spangres_properties_sql(self, expression)
    if _original_pg_properties_transform is not None:
        return str(_original_pg_properties_transform(self, expression))
    return str(_original_pg_properties_sql(self, expression))


PostgresGenerator.TRANSFORMS[exp.Property] = _pg_property_transform
PostgresGenerator.TRANSFORMS[exp.Properties] = _pg_properties_transform

_DISPATCH_CACHE.pop(PostgresGenerator, None)

SpangresGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]

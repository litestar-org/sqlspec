"""Spanner dialect generators with sqlglot[c] compatibility.

When sqlglot[c] is installed, generators are compiled and reject interpreted
subclasses. We detect this and use TRANSFORMS-based extension on the base
generators. When running pure-Python sqlglot, we create real subclasses.
"""

from typing import Any, cast

from sqlglot import exp
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.postgres import PostgresGenerator

__all__ = ("SpangresGenerator", "SpannerGenerator")

_TTL_MIN_COMPONENTS = 2
_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_INTERLEAVE_NAME = "INTERLEAVE_IN_PARENT"

# Capture originals before any patching
_original_bq_property_sql = BigQueryGenerator.property_sql
_original_bq_properties_sql = BigQueryGenerator.properties_sql
_original_bq_locate_properties = BigQueryGenerator.locate_properties
_original_pg_property_sql = PostgresGenerator.property_sql


def _is_post_schema_spanner_property(expression: exp.Expr) -> bool:
    if not isinstance(expression, exp.Property) or not isinstance(expression.this, exp.Literal):
        return False
    return expression.this.name.upper() in {_INTERLEAVE_NAME, _ROW_DELETION_NAME, "TTL"}


def _render_interval_sql(generator: Any, expression: exp.Expr) -> str:
    if isinstance(expression, exp.Interval):
        unit = expression.args.get("unit")
        if isinstance(expression.this, exp.Literal) and not expression.this.is_string and isinstance(unit, exp.Expr):
            return f"INTERVAL {generator.sql(expression.this)} {generator.sql(unit)}"

    interval_sql = cast("str", generator.sql(expression))
    if not interval_sql.upper().startswith("INTERVAL"):
        return f"INTERVAL {interval_sql}"
    return interval_sql


# ---------------------------------------------------------------------------
# Spanner property rendering (BigQuery-based)
# ---------------------------------------------------------------------------


def _spanner_property_sql(self: Any, expression: exp.Property) -> str:
    """Render Spanner-specific properties (INTERLEAVE, ROW_DELETION_POLICY, TTL)."""
    if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _INTERLEAVE_NAME:
        values = expression.args.get("value")
        if isinstance(values, exp.Tuple) and values.expressions:
            parent = self.sql(values.expressions[0])
            sql = f"INTERLEAVE IN PARENT {parent}"
            if len(values.expressions) >= _TTL_MIN_COMPONENTS:
                on_delete_expr = values.expressions[1]
                if isinstance(on_delete_expr, exp.Literal):
                    sql = f"{sql} ON DELETE {on_delete_expr.this}"
            return sql

    if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _ROW_DELETION_NAME:
        values = expression.args.get("value")
        if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
            column = self.sql(values.expressions[0])
            interval_sql = _render_interval_sql(self, values.expressions[1])
            return f"ROW DELETION POLICY (OLDER_THAN({column}, {interval_sql}))"

    if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == "TTL":
        values = expression.args.get("value")
        if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
            interval = _render_interval_sql(self, values.expressions[0]).removeprefix("INTERVAL ")
            column = self.sql(values.expressions[1])
            return f"TTL INTERVAL {interval} ON {column}"

    return str(_original_bq_property_sql(self, expression))


def _spanner_properties_sql(self: Any, expression: exp.Properties) -> str:
    """Render custom Spanner properties without BigQuery's OPTIONS wrapper."""
    root_properties: list[exp.Expr] = []
    with_properties: list[exp.Expr] = []

    for property_expression in expression.expressions:
        if _is_post_schema_spanner_property(property_expression):
            root_properties.append(property_expression)
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

    root_props = self.root_properties(root_props_ast)
    with_props = self.with_properties(with_props_ast)

    if root_props and with_props and not self.pretty:
        with_props = f" {with_props}"

    return str(root_props) + str(with_props)


# ---------------------------------------------------------------------------
# Spangres property rendering (Postgres-based)
# ---------------------------------------------------------------------------


def _spangres_property_sql(self: Any, expression: exp.Property) -> str:
    """Render Spangres row deletion policies."""
    if isinstance(expression.this, exp.Literal) and expression.this.name.upper() == _ROW_DELETION_NAME:
        values = expression.args.get("value")
        if isinstance(values, exp.Tuple) and len(values.expressions) >= _TTL_MIN_COMPONENTS:
            column = self.sql(values.expressions[0])
            interval_sql = _render_interval_sql(self, values.expressions[1])
            return f"ROW DELETION POLICY (OLDER_THAN({column}, {interval_sql}))"

    return str(_original_pg_property_sql(self, expression))


# ---------------------------------------------------------------------------
# Unified extension logic (sqlglot[c] & Pure-Python)
# ---------------------------------------------------------------------------

# BigQuery / Spanner
_original_bq_property_transform = BigQueryGenerator.TRANSFORMS.get(exp.Property)
_original_bq_properties_transform = BigQueryGenerator.TRANSFORMS.get(exp.Properties)
_original_bq_create_transform = BigQueryGenerator.TRANSFORMS.get(exp.Create)


def _bq_property_transform(self: Any, expression: exp.Property) -> str:
    dialect_class = getattr(self.dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name == "Spanner":
        return _spanner_property_sql(self, expression)
    if _original_bq_property_transform is not None:
        return str(_original_bq_property_transform(self, expression))
    return str(_original_bq_property_sql(self, expression))


def _bq_properties_transform(self: Any, expression: exp.Properties) -> str:
    dialect_class = getattr(self.dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name == "Spanner":
        return _spanner_properties_sql(self, expression)
    if _original_bq_properties_transform is not None:
        return str(_original_bq_properties_transform(self, expression))
    return str(_original_bq_properties_sql(self, expression))


def _bq_create_transform(self: Any, expression: exp.Create) -> str:
    dialect_class = getattr(self.dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name == "Spanner" and expression.this and expression.kind == "TABLE":
        properties = expression.args.get("properties")
        if properties:
            # Re-order properties so Spanner ones stay at the schema boundary
            new_expressions = []
            for p in properties.expressions:
                if _is_post_schema_spanner_property(p):
                    # Force to POST_SCHEMA if it's a Spanner property
                    new_expressions.append(p)
                else:
                    new_expressions.append(p)
            properties.set("expressions", new_expressions)

    if _original_bq_create_transform is not None:
        return str(_original_bq_create_transform(self, expression))
    return str(self.create_sql(expression))


BigQueryGenerator.TRANSFORMS[exp.Property] = _bq_property_transform
BigQueryGenerator.TRANSFORMS[exp.Properties] = _bq_properties_transform
BigQueryGenerator.TRANSFORMS[exp.Create] = _bq_create_transform

SpannerGenerator = BigQueryGenerator  # pyright: ignore[reportAssignmentType]


# Postgres / Spangres
_original_pg_property_transform = PostgresGenerator.TRANSFORMS.get(exp.Property)


def _pg_property_transform(self: Any, expression: exp.Property) -> str:
    dialect_class = getattr(self.dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name == "Spangres":
        return _spangres_property_sql(self, expression)
    if _original_pg_property_transform is not None:
        return str(_original_pg_property_transform(self, expression))
    return str(_original_pg_property_sql(self, expression))


PostgresGenerator.TRANSFORMS[exp.Property] = _pg_property_transform

SpangresGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]

"""Spanner dialect generators with sqlglot[c] compatibility.

When sqlglot[c] is installed, generators are compiled and reject interpreted
subclasses. We detect this and use TRANSFORMS-based extension on the base
generators. When running pure-Python sqlglot, we create real subclasses.
"""

from typing import Any, cast

from sqlglot import exp
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.postgres import PostgresGenerator

from sqlspec.dialects._compat import is_generator_compiled

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


def _spanner_locate_properties(self: Any, properties: exp.Properties) -> Any:
    """Keep custom Spanner CREATE TABLE properties at the schema boundary."""
    properties_locs = _original_bq_locate_properties(self, properties)
    with_properties = list(properties_locs[exp.Properties.Location.POST_WITH])
    if not with_properties:
        return properties_locs

    retained_with_properties: list[exp.Expr] = []
    for property_expression in with_properties:
        if _is_post_schema_spanner_property(property_expression):
            properties_locs[exp.Properties.Location.POST_SCHEMA].append(property_expression)
        else:
            retained_with_properties.append(property_expression)

    properties_locs[exp.Properties.Location.POST_WITH] = retained_with_properties
    return properties_locs


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
# Conditional path: compiled vs pure-Python
# ---------------------------------------------------------------------------

_bq_compiled = is_generator_compiled(BigQueryGenerator)
_pg_compiled = is_generator_compiled(PostgresGenerator)

if _bq_compiled:
    # sqlglot[c]: use TRANSFORMS entries with dialect name checks

    _original_bq_property_transform = BigQueryGenerator.TRANSFORMS.get(exp.Property)
    _original_bq_properties_transform = BigQueryGenerator.TRANSFORMS.get(exp.Properties)

    def _bq_property_transform(self: Any, expression: exp.Property) -> str:
        dialect = getattr(self, "dialect", None)
        if dialect and type(dialect).__name__ == "Spanner":
            return _spanner_property_sql(self, expression)
        if _original_bq_property_transform is not None:
            return _original_bq_property_transform(self, expression)
        return self.property_sql(expression)

    def _bq_properties_transform(self: Any, expression: exp.Properties) -> str:
        dialect = getattr(self, "dialect", None)
        if dialect and type(dialect).__name__ == "Spanner":
            return _spanner_properties_sql(self, expression)
        if _original_bq_properties_transform is not None:
            return _original_bq_properties_transform(self, expression)
        return self.properties_sql(expression)

    BigQueryGenerator.TRANSFORMS[exp.Property] = _bq_property_transform
    BigQueryGenerator.TRANSFORMS[exp.Properties] = _bq_properties_transform

    # Also need to patch locate_properties via setattr since it's not a TRANSFORMS entry
    def _patched_bq_locate_properties(self: Any, properties: exp.Properties) -> Any:
        dialect = getattr(self, "dialect", None)
        if dialect and type(dialect).__name__ == "Spanner":
            return _spanner_locate_properties(self, properties)
        return _original_bq_locate_properties(self, properties)

    setattr(BigQueryGenerator, "locate_properties", _patched_bq_locate_properties)

    SpannerGenerator = BigQueryGenerator
else:
    # Pure-Python sqlglot: real subclass

    class SpannerGenerator(BigQueryGenerator):  # type: ignore[no-redef]
        """Generator for Google Cloud Spanner (GoogleSQL variant)."""

        def property_sql(self, expression: exp.Property) -> str:
            return _spanner_property_sql(self, expression)

        def locate_properties(self, properties: exp.Properties) -> Any:
            return _spanner_locate_properties(self, properties)

        def properties_sql(self, expression: exp.Properties) -> str:
            return _spanner_properties_sql(self, expression)


if _pg_compiled:
    # sqlglot[c]: use TRANSFORMS entry with dialect name check

    _original_pg_property_transform = PostgresGenerator.TRANSFORMS.get(exp.Property)

    def _pg_property_transform(self: Any, expression: exp.Property) -> str:
        dialect = getattr(self, "dialect", None)
        if dialect and type(dialect).__name__ == "Spangres":
            return _spangres_property_sql(self, expression)
        if _original_pg_property_transform is not None:
            return _original_pg_property_transform(self, expression)
        return self.property_sql(expression)

    PostgresGenerator.TRANSFORMS[exp.Property] = _pg_property_transform

    SpangresGenerator = PostgresGenerator
else:
    # Pure-Python sqlglot: real subclass

    class SpangresGenerator(PostgresGenerator):  # type: ignore[no-redef]
        """Generator for Spanner PostgreSQL-interface (Spangres)."""

        def property_sql(self, expression: exp.Property) -> str:
            return _spangres_property_sql(self, expression)

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
from sqlglot.generators.bigquery import BigQueryGenerator
from sqlglot.generators.postgres import PostgresGenerator

from sqlspec.builder._generation import invalidate_generator_dispatch
from sqlspec.dialects.spanner._expressions import (
    ApproxCosineDistance,
    CosineDistance,
    DotProduct,
    EuclideanDistance,
    GetNextSequenceValue,
    Score,
    Search,
    SearchSubstring,
    TokenizeFulltext,
    TokenizeNgrams,
    TokenizeSubstring,
)

__all__ = ("SpangresGenerator", "SpannerGenerator")

_TTL_MIN_COMPONENTS = 2
_ROW_DELETION_NAME = "ROW_DELETION_POLICY"
_INTERLEAVE_NAME = "INTERLEAVE_IN_PARENT"
_INTERLEAVE_IN_NAME = "INTERLEAVE_IN"

_SPANNER_PROPERTY_NAMES: Final[frozenset[str]] = frozenset({_INTERLEAVE_NAME, _INTERLEAVE_IN_NAME, _ROW_DELETION_NAME})
_DAYS_PATTERN: Final["re.Pattern[str]"] = re.compile(r"^\s*(\d+)\s*days?\s*$", re.IGNORECASE)

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


_original_bq_property_transform = BigQueryGenerator.TRANSFORMS.get(exp.Property)
_original_bq_properties_transform = BigQueryGenerator.TRANSFORMS.get(exp.Properties)
_original_bq_create_transform = BigQueryGenerator.TRANSFORMS.get(exp.Create)


def _bq_property_transform(self: Any, expression: exp.Property) -> str:
    """Transform properties for Spanner or delegate to original BigQuery transform."""
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spanner":
        return _spanner_property_sql(self, expression)
    if _original_bq_property_transform is not None:
        return str(_original_bq_property_transform(self, expression))
    return str(_original_bq_property_sql(self, expression))


def _bq_properties_transform(self: Any, expression: exp.Properties) -> str:
    """Transform property collections for Spanner or delegate to original BigQuery transform."""
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spanner":
        return _spanner_properties_sql(self, expression)
    if _original_bq_properties_transform is not None:
        return str(_original_bq_properties_transform(self, expression))
    return str(_original_bq_properties_sql(self, expression))


def _bq_create_transform(self: Any, expression: exp.Create) -> str:
    """Transform CREATE TABLE for Spanner to order post-schema properties correctly."""
    dialect_name = _get_dialect_name(self)
    if (
        (dialect_name == "Spanner" or isinstance(self, SpannerGenerator))
        and expression.this
        and expression.kind == "TABLE"
    ):
        properties = expression.args.get("properties")
        if properties:
            spanner_props = [p for p in properties.expressions if _is_post_schema_spanner_property(p)]
            other_props = [p for p in properties.expressions if not _is_post_schema_spanner_property(p)]
            properties.set("expressions", other_props + spanner_props)

    if _original_bq_create_transform is not None:
        return str(_original_bq_create_transform(self, expression))
    return str(self.create_sql(expression))


def _render_approx_cosine_distance(generator: Any, expression: ApproxCosineDistance) -> str:
    """Render APPROX_COSINE_DISTANCE function with optional neighbor count options."""
    this = generator.sql(expression, "this")
    expr = generator.sql(expression, "expression")
    options = expression.args.get("options")
    if options is not None:
        opts_sql = generator.sql(options)
        return f"APPROX_COSINE_DISTANCE({this}, {expr}, {opts_sql})"
    return f"APPROX_COSINE_DISTANCE({this}, {expr})"


def _render_tokenize_fulltext(generator: Any, expression: TokenizeFulltext) -> str:
    """Render TOKENIZE_FULLTEXT function with optional extra parameters."""
    this = generator.sql(expression, "this")
    exprs = expression.args.get("expressions")
    if exprs:
        extra_args = ", ".join(generator.sql(x) for x in exprs)
        return f"TOKENIZE_FULLTEXT({this}, {extra_args})"
    return f"TOKENIZE_FULLTEXT({this})"


def _spanner_index_sql(generator: Any, expression: exp.Index) -> str:
    """Render Spanner INDEX, VECTOR INDEX, or SEARCH INDEX DDL."""
    kind = expression.args.get("kind")
    if kind == "VECTOR":
        name = generator.sql(expression, "this")
        table = generator.sql(expression, "table")
        params = generator.sql(expression, "params")
        cols_sql = f"({params})" if not params.startswith("(") else params
        parts = [f"CREATE VECTOR INDEX {name} ON {table} {cols_sql}"]
        where = expression.args.get("where")
        if where:
            parts.append(generator.sql(where))
        options = expression.args.get("options")
        if options:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in options.expressions]
            opts_str = ", ".join(opts_list)
            parts.append(f"OPTIONS ({opts_str})")
        return " ".join(parts)
    if kind == "SEARCH":
        name = generator.sql(expression, "this")
        table = generator.sql(expression, "table")
        params = generator.sql(expression, "params")
        cols_sql = f"({params})" if not params.startswith("(") else params
        parts = [f"CREATE SEARCH INDEX {name} ON {table} {cols_sql}"]
        storing = expression.args.get("storing")
        if storing:
            storing_cols = ", ".join(generator.sql(c) for c in storing)
            parts.append(f"STORING ({storing_cols})")
        partition_by = expression.args.get("partition_by")
        if partition_by:
            part_cols = ", ".join(generator.sql(c) for c in partition_by)
            parts.append(f"PARTITION BY {part_cols}")
        order = expression.args.get("order")
        if order:
            parts.append(generator.sql(order).strip())
        options = expression.args.get("options")
        if options:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in options.expressions]
            opts_str = ", ".join(opts_list)
            parts.append(f"OPTIONS ({opts_str})")
        return " ".join(parts)
    return str(generator.index_sql(expression))


def _spanner_create_transform(generator: Any, expression: exp.Create) -> str:
    """Transform CREATE statements for Spanner including TABLE, SEQUENCE, and CHANGE STREAM."""
    if expression.kind == "SEQUENCE":
        name = generator.sql(expression, "this")
        props = expression.args.get("properties")
        if props:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in props.expressions]
            opts_str = ", ".join(opts_list)
            return f"CREATE SEQUENCE {name} OPTIONS ({opts_str})"
        return f"CREATE SEQUENCE {name}"
    if expression.kind == "CHANGE STREAM":
        name = generator.sql(expression, "this")
        parts = [f"CREATE CHANGE STREAM {name}"]
        exprs = expression.expressions
        if exprs:
            if len(exprs) == 1 and isinstance(exprs[0], exp.Var) and exprs[0].name.upper() == "ALL":
                parts.append("FOR ALL")
            else:
                target_str = ", ".join(generator.sql(x) for x in exprs)
                parts.append(f"FOR {target_str}")
        props = expression.args.get("properties")
        if props:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in props.expressions]
            opts_str = ", ".join(opts_list)
            parts.append(f"OPTIONS ({opts_str})")
        return " ".join(parts)
    return _bq_create_transform(generator, expression)


def _spanner_alter_sql(generator: Any, expression: exp.Alter) -> str:
    """Render ALTER statements for Spanner including SEQUENCE and CHANGE STREAM."""
    kind = expression.args.get("kind")
    if kind == "SEQUENCE":
        name = generator.sql(expression, "this")
        options = expression.args.get("options")
        if options:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in options.expressions]
            opts_str = ", ".join(opts_list)
            return f"ALTER SEQUENCE {name} SET OPTIONS ({opts_str})"
        return f"ALTER SEQUENCE {name}"
    if kind == "CHANGE STREAM":
        name = generator.sql(expression, "this")
        options = expression.args.get("options")
        if options:
            opts_list = [f"{generator.sql(p.this)} = {generator.sql(p.args.get('value'))}" for p in options.expressions]
            opts_str = ", ".join(opts_list)
            return f"ALTER CHANGE STREAM {name} SET OPTIONS ({opts_str})"
        return f"ALTER CHANGE STREAM {name}"
    return str(generator.alter_sql(expression))


def _spanner_drop_sql(generator: Any, expression: exp.Drop) -> str:
    """Render DROP statements for Spanner including CHANGE STREAM."""
    kind = expression.args.get("kind")
    if kind == "CHANGE STREAM":
        name = generator.sql(expression, "this")
        return f"DROP CHANGE STREAM {name}"
    return str(generator.drop_sql(expression))


def _render_spanner_hint(generator: Any, expression: exp.Hint) -> str:
    """Render @{key=val, ...} hint syntax for Spanner GoogleSQL."""
    parts: list[str] = []
    for e in expression.expressions:
        if isinstance(e, exp.EQ):
            parts.append(f"{generator.sql(e.this)}={generator.sql(e.expression)}")
        else:
            parts.append(generator.sql(e))
    inner = ", ".join(parts)
    return f"@{{{inner}}}"


def _render_spangres_hint(generator: Any, expression: exp.Hint) -> str:
    """Render /*@ key=val, ... */ hint syntax for Spangres PostgreSQL."""
    parts: list[str] = []
    for e in expression.expressions:
        if isinstance(e, exp.EQ):
            parts.append(f"{generator.sql(e.this)}={generator.sql(e.expression)}")
        else:
            parts.append(generator.sql(e))
    inner = ", ".join(parts)
    return f"/*@ {inner} */"


def _spanner_select_sql(generator: Any, expression: exp.Select) -> str:
    """Render SELECT with preceding @{...} statement hint if present."""
    hint = expression.args.get("hint")
    if hint:
        expr_copy = expression.copy()
        expr_copy.set("hint", None)
        hint_sql = generator.sql(hint)
        body_sql = BigQueryGenerator.select_sql(generator, expr_copy)
        return f"{hint_sql} {body_sql}"
    return BigQueryGenerator.select_sql(generator, expression)


def _spangres_select_sql(generator: Any, expression: exp.Select) -> str:
    """Render SELECT with preceding /*@ ... */ statement hint if present."""
    hint = expression.args.get("hint")
    if hint:
        expr_copy = expression.copy()
        expr_copy.set("hint", None)
        hint_sql = generator.sql(hint)
        body_sql = PostgresGenerator.select_sql(generator, expr_copy)
        return f"{hint_sql} {body_sql}"
    return PostgresGenerator.select_sql(generator, expression)


def _spanner_table_sql(generator: Any, expression: exp.Table) -> str:
    """Render table reference followed by @{...} table hint if present."""
    table_sql = BigQueryGenerator.table_sql(generator, expression)
    hints = expression.args.get("hints")
    if hints:
        hint_strs = [generator.sql(h) for h in hints]
        joined_hints = " ".join(hint_strs)
        return f"{table_sql} {joined_hints}"
    return table_sql


def _spangres_table_sql(generator: Any, expression: exp.Table) -> str:
    """Render table reference followed by /*@ ... */ table hint if present."""
    table_sql = PostgresGenerator.table_sql(generator, expression)
    hints = expression.args.get("hints")
    if hints:
        hint_strs = [generator.sql(h) for h in hints]
        joined_hints = " ".join(hint_strs)
        return f"{table_sql} {joined_hints}"
    return table_sql


class SpannerGenerator(BigQueryGenerator):
    """Generator for Cloud Spanner GoogleSQL dialect."""

    TYPE_MAPPING = {
        **BigQueryGenerator.TYPE_MAPPING,
        exp.DataType.Type.FLOAT: "FLOAT32",
        exp.DataType.Type.USERDEFINED: "TOKENLIST",
        exp.DataType.Type.DOUBLE: "FLOAT64",
    }

    TRANSFORMS = {
        **BigQueryGenerator.TRANSFORMS,
        exp.Property: _spanner_property_sql,
        exp.Properties: _spanner_properties_sql,
        exp.Create: _spanner_create_transform,
        exp.Index: _spanner_index_sql,
        exp.Alter: _spanner_alter_sql,
        exp.Drop: _spanner_drop_sql,
        exp.ComputedColumnConstraint: lambda s, e: f"AS {s.sql(e, 'this')} STORED",
        CosineDistance: lambda s, e: f"COSINE_DISTANCE({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        EuclideanDistance: lambda s, e: f"EUCLIDEAN_DISTANCE({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        DotProduct: lambda s, e: f"DOT_PRODUCT({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        ApproxCosineDistance: _render_approx_cosine_distance,
        Search: lambda s, e: f"SEARCH({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        SearchSubstring: lambda s, e: f"SEARCH_SUBSTRING({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        Score: lambda s, e: f"SCORE({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        TokenizeFulltext: _render_tokenize_fulltext,
        TokenizeSubstring: lambda s, e: f"TOKENIZE_SUBSTRING({s.sql(e, 'this')})",
        TokenizeNgrams: lambda s, e: f"TOKENIZE_NGRAMS({s.sql(e, 'this')})",
        GetNextSequenceValue: lambda s, e: f"GET_NEXT_SEQUENCE_VALUE(SEQUENCE {s.sql(e, 'this')})",
        exp.Hint: _render_spanner_hint,
        exp.Select: _spanner_select_sql,
        exp.Table: _spanner_table_sql,
    }


_original_pg_property_transform = PostgresGenerator.TRANSFORMS.get(exp.Property)
_original_pg_properties_transform = PostgresGenerator.TRANSFORMS.get(exp.Properties)


def _pg_property_transform(self: Any, expression: exp.Property) -> str:
    """Transform properties for Spangres or delegate to original Postgres transform."""
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spangres":
        return _spangres_property_sql(self, expression)
    if _original_pg_property_transform is not None:
        return str(_original_pg_property_transform(self, expression))
    return str(_original_pg_property_sql(self, expression))


def _pg_properties_transform(self: Any, expression: exp.Properties) -> str:
    """Transform property collections for Spangres or delegate to original Postgres transform."""
    dialect_name = _get_dialect_name(self)
    if dialect_name == "Spangres":
        return _spangres_properties_sql(self, expression)
    if _original_pg_properties_transform is not None:
        return str(_original_pg_properties_transform(self, expression))
    return str(_original_pg_properties_sql(self, expression))


class SpangresGenerator(PostgresGenerator):
    """Generator for Cloud Spanner PostgreSQL-compatible dialect."""

    TRANSFORMS = {
        **PostgresGenerator.TRANSFORMS,
        exp.Property: _spangres_property_sql,
        exp.Properties: _spangres_properties_sql,
        CosineDistance: lambda s, e: f"COSINE_DISTANCE({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        EuclideanDistance: lambda s, e: f"EUCLIDEAN_DISTANCE({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        DotProduct: lambda s, e: f"DOT_PRODUCT({s.sql(e, 'this')}, {s.sql(e, 'expression')})",
        ApproxCosineDistance: _render_approx_cosine_distance,
        GetNextSequenceValue: lambda s, e: f"GET_NEXT_SEQUENCE_VALUE(SEQUENCE {s.sql(e, 'this')})",
        exp.Hint: _render_spangres_hint,
        exp.Select: _spangres_select_sql,
        exp.Table: _spangres_table_sql,
    }


invalidate_generator_dispatch(SpannerGenerator, SpangresGenerator)

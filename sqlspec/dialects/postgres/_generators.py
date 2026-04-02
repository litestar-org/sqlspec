"""Postgres dialect generators with sqlglot[c] compatibility.

When sqlglot[c] is installed, generators are compiled and reject interpreted
subclasses at instantiation. We detect this and use TRANSFORMS patching on
the base generator. When running pure-Python sqlglot, we create real subclasses
for cleaner isolation.
"""

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator

from sqlspec.builder._vector_distance import (
    is_vector_distance_expression,
    render_vector_distance_postgres,
    vector_distance_metric,
)
from sqlspec.dialects.postgres._operators import is_postgres_extension_operator, postgres_extension_operator

__all__ = ("PGVectorGenerator", "ParadeDBGenerator")

_BASE_OPERATOR_TRANSFORM = Postgres.Generator.TRANSFORMS[exp.Operator]


def _postgres_extension_operator_sql(generator: PostgresGenerator, expression: exp.Operator) -> str:
    dialect_class = getattr(generator.dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name in {"PGVector", "ParadeDB"}:
        if is_vector_distance_expression(expression):
            return render_vector_distance_postgres(
                generator.sql(expression, "this"),
                generator.sql(expression, "expression"),
                vector_distance_metric(expression),
            )

        if is_postgres_extension_operator(expression):
            return (
                f"{generator.sql(expression, 'this')} "
                f"{postgres_extension_operator(expression)} "
                f"{generator.sql(expression, 'expression')}"
            )

    return _BASE_OPERATOR_TRANSFORM(generator, expression)


# sqlglot[c] and pure-Python: patch TRANSFORMS on the base class and alias.
# Compiled classes reject interpreted subclasses, so we use a dialect-aware
# patch on the base class to support both environments consistently.
PostgresGenerator.TRANSFORMS[exp.Operator] = _postgres_extension_operator_sql
PGVectorGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]
ParadeDBGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]

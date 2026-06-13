"""Postgres dialect generators for the PGVector and ParadeDB extensions.

Extension happens through ``TRANSFORMS`` entries on the base Postgres
generator: sqlglot invokes those with explicit ``(generator, expression)``
arguments, so the callables stay compilable and one code path serves both
pure-Python sqlglot and sqlglot[c], whose compiled generator classes reject
interpreted subclasses.
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
from sqlspec.utils.sqlglot_compat import invalidate_generator_dispatch

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

invalidate_generator_dispatch(PostgresGenerator)

PGVectorGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]
ParadeDBGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]

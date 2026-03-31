"""Postgres dialect generators with sqlglot[c] compatibility.

When sqlglot[c] is installed, generators are compiled without
``allow_interpreted_subclasses`` and cross-compilation-unit subclassing
causes segfaults.  Instead of subclassing, we patch TRANSFORMS on the
base generator class and alias it.
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


PostgresGenerator.TRANSFORMS[exp.Operator] = _postgres_extension_operator_sql
PGVectorGenerator = PostgresGenerator
ParadeDBGenerator = PostgresGenerator

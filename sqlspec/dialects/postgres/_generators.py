"""Postgres dialect generators for the PGVector and ParadeDB extensions.

Extension happens through ``TRANSFORMS`` entries on the base Postgres
generator: sqlglot invokes those with explicit ``(generator, expression)``
arguments, so the callables stay compilable and one code path serves both
pure-Python sqlglot and sqlglot[c], whose compiled generator classes reject
interpreted subclasses.
"""

from typing import Any, Final

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.generators.postgres import PostgresGenerator

from sqlspec.builder._generation import invalidate_generator_dispatch
from sqlspec.builder._vector_distance import (
    is_vector_distance_expression,
    render_vector_distance_bigquery,
    render_vector_distance_postgres,
    vector_distance_metric,
)
from sqlspec.dialects.postgres._operators import is_postgres_extension_operator, postgres_extension_operator

__all__ = ("PGTextSearchGenerator", "PGVectorGenerator", "ParadeDBGenerator")

_GENERATOR_TRANSFORMS_REGISTERED_ATTR: Final[str] = "_sqlspec_postgres_generator_transforms"
_ORIGINAL_OPERATOR_TRANSFORM_ATTR: Final[str] = "_sqlspec_postgres_original_operator_transform"
_POSTGRES_EXTENSION_DIALECT_NAMES: Final[frozenset[str]] = frozenset({
    "Postgres",
    "PGVector",
    "ParadeDB",
    "PGTextSearch",
})

if not hasattr(PostgresGenerator, _ORIGINAL_OPERATOR_TRANSFORM_ATTR):
    setattr(PostgresGenerator, _ORIGINAL_OPERATOR_TRANSFORM_ATTR, Postgres.Generator.TRANSFORMS[exp.Operator])
_BASE_OPERATOR_TRANSFORM: Any = getattr(PostgresGenerator, _ORIGINAL_OPERATOR_TRANSFORM_ATTR)


def _postgres_extension_operator_sql(generator: PostgresGenerator, expression: exp.Operator) -> str:
    dialect = generator.dialect
    dialect_class = getattr(dialect, "__class__", None)
    dialect_name = dialect_class.__name__ if dialect_class else None
    if dialect_name == "Spangres" and is_vector_distance_expression(expression):
        return render_vector_distance_bigquery(
            generator.sql(expression, "this"),
            generator.sql(expression, "expression"),
            vector_distance_metric(expression),
        )
    if dialect_name and dialect_name in _POSTGRES_EXTENSION_DIALECT_NAMES:
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

    return str(_BASE_OPERATOR_TRANSFORM(generator, expression))


def _register_postgres_generator_transforms() -> None:
    if getattr(PostgresGenerator, _GENERATOR_TRANSFORMS_REGISTERED_ATTR, False):
        return
    PostgresGenerator.TRANSFORMS[exp.Operator] = _postgres_extension_operator_sql
    setattr(PostgresGenerator, _GENERATOR_TRANSFORMS_REGISTERED_ATTR, True)
    invalidate_generator_dispatch(PostgresGenerator)


_register_postgres_generator_transforms()

PGTextSearchGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]
PGVectorGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]
ParadeDBGenerator = PostgresGenerator  # pyright: ignore[reportAssignmentType]

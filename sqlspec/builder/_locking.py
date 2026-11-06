"""SQLGlot lock-clause generator registration."""

from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, ClassVar, Protocol, cast

from sqlglot import Dialect, exp
from sqlglot.generator import Generator

from sqlspec.builder._generation import generator_class_for_dialect, invalidate_generator_dispatch

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlglot.dialects.dialect import DialectType

__all__ = ("register_lock_generator",)


class _GeneratorClass(Protocol):
    TRANSFORMS: ClassVar[MutableMapping[type[exp.Lock], Callable[[Generator, exp.Lock], str]]]


_REGISTERED_LOCK_GENERATORS: set[type[_GeneratorClass]] = set()


def register_lock_generator(dialect: "DialectType | str | None") -> None:
    """Register lock-clause rendering for the dialect being rendered."""
    generator_class = cast("type[_GeneratorClass]", generator_class_for_dialect(dialect))
    if generator_class in _REGISTERED_LOCK_GENERATORS:
        return

    generator_class.TRANSFORMS[exp.Lock] = _lock_sql
    invalidate_generator_dispatch(generator_class)

    _REGISTERED_LOCK_GENERATORS.add(generator_class)


def _render_lock_target(generator: "Generator", expression: exp.Expr) -> str:
    if isinstance(expression, exp.Identifier) and not expression.args.get("quoted"):
        return expression.name
    return str(generator.sql(expression))


def _render_lock_targets(generator: "Generator", expressions: "Iterable[exp.Expr]") -> str:
    return ", ".join(_render_lock_target(generator, expression) for expression in expressions)


def _lock_sql(generator: "Generator", expression: exp.Lock) -> str:
    """Render a lock clause.

    The dialect-neutral base generator keeps lock clauses so builds without a
    dialect preserve ``FOR UPDATE`` / ``FOR SHARE``; dialects that cannot express
    locking reads still drop them.
    """
    dialect_class = type(generator.dialect)
    if not generator.LOCKING_READS_SUPPORTED and dialect_class is not Dialect and dialect_class.__name__ != "Spanner":
        generator.unsupported("Locking reads using 'FOR UPDATE/SHARE' are not supported")
        return ""

    update = expression.args["update"]
    key = expression.args.get("key")
    lock_type = ("FOR NO KEY UPDATE" if key else "FOR UPDATE") if update else "FOR KEY SHARE" if key else "FOR SHARE"

    if expression.args.get("sqlspec_share_mode"):
        lock_type = "LOCK IN SHARE MODE"

    targets = _render_lock_targets(generator, expression.expressions)
    target_sql = f" OF {targets}" if targets else ""
    wait = expression.args.get("wait")

    if wait is not None:
        if isinstance(wait, exp.Literal):
            wait = f" WAIT {generator.sql(wait)}"
        else:
            wait = " NOWAIT" if wait else " SKIP LOCKED"

    return f"{lock_type}{target_sql}{wait or ''}"

"""SQLGlot lock-clause generator registration."""

from collections.abc import Callable, MutableMapping
from typing import TYPE_CHECKING, ClassVar, Protocol, cast

from sqlglot import exp
from sqlglot.generator import Generator

from sqlspec.builder._generation import invalidate_generator_dispatch

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlglot.dialects.dialect import DialectType

__all__ = ("register_lock_generator",)


class _GeneratorClass(Protocol):
    TRANSFORMS: ClassVar[MutableMapping[type[exp.Lock], Callable[[Generator, exp.Lock], str]]]


class _DialectClass(Protocol):
    Generator: ClassVar[type[_GeneratorClass]]


_REGISTERED_LOCK_GENERATORS: set[type[_GeneratorClass]] = set()


def _render_lock_target(generator: "Generator", expression: exp.Expr) -> str:
    if isinstance(expression, exp.Identifier) and not expression.args.get("quoted"):
        return expression.name
    return str(generator.sql(expression))


def _render_lock_targets(generator: "Generator", expressions: "Iterable[exp.Expr]") -> str:
    return ", ".join(_render_lock_target(generator, expression) for expression in expressions)


def _lock_sql(generator: "Generator", expression: exp.Lock) -> str:
    if not generator.LOCKING_READS_SUPPORTED:
        generator.unsupported("Locking reads using 'FOR UPDATE/SHARE' are not supported")
        return ""

    update = expression.args["update"]
    key = expression.args.get("key")
    lock_type = ("FOR NO KEY UPDATE" if key else "FOR UPDATE") if update else "FOR KEY SHARE" if key else "FOR SHARE"

    targets = _render_lock_targets(generator, expression.expressions)
    target_sql = f" OF {targets}" if targets else ""
    wait = expression.args.get("wait")

    if wait is not None:
        if isinstance(wait, exp.Literal):
            wait = f" WAIT {generator.sql(wait)}"
        else:
            wait = " NOWAIT" if wait else " SKIP LOCKED"

    return f"{lock_type}{target_sql}{wait or ''}"


def _generator_class_for_dialect(dialect: "DialectType | str | None") -> "type[_GeneratorClass]":
    if dialect is None:
        return cast("type[_GeneratorClass]", Generator)

    from sqlglot import Dialect

    dialect_class: type[Dialect]
    if isinstance(dialect, str):
        dialect_class = type(Dialect.get_or_raise(dialect))
    elif isinstance(dialect, type) and issubclass(dialect, Dialect):
        dialect_class = dialect
    elif isinstance(dialect, Dialect):
        dialect_class = type(dialect)
    else:
        dialect_class = type(Dialect.get_or_raise(str(dialect)))

    return cast("_DialectClass", dialect_class).Generator


def register_lock_generator(dialect: "DialectType | str | None") -> None:
    """Register lock-clause rendering for the dialect being rendered."""
    generator_class = _generator_class_for_dialect(dialect)
    if generator_class in _REGISTERED_LOCK_GENERATORS:
        return

    generator_class.TRANSFORMS[exp.Lock] = _lock_sql
    invalidate_generator_dispatch(generator_class)

    _REGISTERED_LOCK_GENERATORS.add(generator_class)

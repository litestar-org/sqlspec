"""CockroachDB session tenant identity over asyncpg connections."""

from mypy_extensions import mypyc_attr

from sqlspec.adapters.asyncpg.tenant import AsyncpgRLSStrategy

__all__ = ("CockroachAsyncpgRLSStrategy",)


@mypyc_attr(allow_interpreted_subclasses=True)
class CockroachAsyncpgRLSStrategy(AsyncpgRLSStrategy):
    """Apply and restore tenant session settings on CockroachDB."""

    __slots__ = ()

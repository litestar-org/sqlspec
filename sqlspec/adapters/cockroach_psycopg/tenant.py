"""CockroachDB session tenant identity over psycopg connections."""

from mypy_extensions import mypyc_attr

from sqlspec.adapters.psycopg.tenant import PsycopgAsyncRLSStrategy, PsycopgSyncRLSStrategy

__all__ = ("CockroachPsycopgAsyncRLSStrategy", "CockroachPsycopgSyncRLSStrategy")


@mypyc_attr(allow_interpreted_subclasses=True)
class CockroachPsycopgSyncRLSStrategy(PsycopgSyncRLSStrategy):
    """Apply and restore synchronous CockroachDB session settings."""

    __slots__ = ()


@mypyc_attr(allow_interpreted_subclasses=True)
class CockroachPsycopgAsyncRLSStrategy(PsycopgAsyncRLSStrategy):
    """Apply and restore asynchronous CockroachDB session settings."""

    __slots__ = ()

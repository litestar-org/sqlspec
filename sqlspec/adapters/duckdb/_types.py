from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from typing import TypeAlias

    DuckDBConnection: TypeAlias = DuckDBPyConnection
else:
    DuckDBConnection = DuckDBPyConnection

__all__ = ("DuckDBConnection",)

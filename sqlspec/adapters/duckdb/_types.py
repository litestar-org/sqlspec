from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    DuckDBConnection: TypeAlias = DuckDBPyConnection
else:
    DuckDBConnection = DuckDBPyConnection

__all__ = ("DuckDBConnection",)

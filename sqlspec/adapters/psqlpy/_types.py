from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TypeAlias

    from psqlpy import Connection

    PsqlpyConnection: TypeAlias = Connection
else:
    from psqlpy import Connection as PsqlpyConnection

__all__ = ("PsqlpyConnection",)

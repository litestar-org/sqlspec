from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from psqlpy import Connection
    from typing_extensions import TypeAlias

    PsqlpyConnection: TypeAlias = Connection
else:
    from psqlpy import Connection as PsqlpyConnection

__all__ = ("PsqlpyConnection",)

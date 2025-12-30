from typing import TYPE_CHECKING

from asyncmy import Connection  # pyright: ignore

if TYPE_CHECKING:
    from typing import TypeAlias

    AsyncmyConnection: TypeAlias = Connection
else:
    AsyncmyConnection = Connection

__all__ = ("AsyncmyConnection",)

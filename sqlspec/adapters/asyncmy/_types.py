from typing import TYPE_CHECKING

from asyncmy import Connection

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    AsyncmyConnection: TypeAlias = Connection
else:
    AsyncmyConnection = Connection

__all__ = ("AsyncmyConnection",)

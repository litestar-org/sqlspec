# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING

from adbc_driver_manager.dbapi import Connection

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    AdbcConnection: TypeAlias = Connection
else:
    AdbcConnection = Connection
__all__ = ("AdbcConnection",)

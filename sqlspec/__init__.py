"""SQLSpec: Safe and elegant SQL query building for Python."""

from sqlspec import adapters, base, exceptions, extensions, statement, typing, utils
from sqlspec.__metadata__ import __version__
from sqlspec._sql import SQLFactory
from sqlspec.base import SQLSpec

sql = SQLFactory()

__all__ = (
    "SQLSpec",
    "__version__",
    "adapters",
    "base",
    "exceptions",
    "extensions",
    "sql",
    "statement",
    "typing",
    "utils",
)

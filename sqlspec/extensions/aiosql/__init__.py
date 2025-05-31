"""AioSQL extension for SQLSpec.

This extension provides integration between aiosql query loaders and SQLSpec drivers,
enabling users to load SQL queries from files while leveraging SQLSpec's advanced
features like filters, instrumentation, and validation.
"""

from sqlspec.extensions.aiosql.adapter import (
    AiosqlAsyncAdapter,
    AiosqlService,
    AiosqlSyncAdapter,
)

__all__ = (
    "AiosqlAsyncAdapter",
    "AiosqlService",
    "AiosqlSyncAdapter",
)

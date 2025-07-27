"""Driver mixins for instrumentation, storage, and utilities."""

from sqlspec.driver.mixins._cache import AsyncAdapterCacheMixin, SyncAdapterCacheMixin
from sqlspec.driver.mixins._result_tools import ToSchemaMixin
# ToArrowSchemaMixin doesn't exist yet
ToArrowSchemaMixin = ToSchemaMixin  # Temporary alias
from sqlspec.driver.mixins._sql_translator import SQLTranslatorMixin

# Legacy imports for backward compatibility
class AsyncQueryMixin:
    """Legacy mixin - functionality moved to AsyncDriverAdapterBase."""
    pass

class SyncQueryMixin:
    """Legacy mixin - functionality moved to SyncDriverAdapterBase."""
    pass

class AsyncStorageMixin:
    """Legacy mixin - functionality moved to AsyncDriverAdapterBase."""
    pass

class SyncStorageMixin:
    """Legacy mixin - functionality moved to SyncDriverAdapterBase."""
    pass

__all__ = (
    "AsyncAdapterCacheMixin",
    "SQLTranslatorMixin", 
    "SyncAdapterCacheMixin",
    "ToArrowSchemaMixin",
    "ToSchemaMixin",
    # Legacy
    "AsyncQueryMixin",
    "SyncQueryMixin",
    "AsyncStorageMixin",
    "SyncStorageMixin",
)

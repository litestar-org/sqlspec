"""Slotted application service bases backed by the compiled service runtime."""

from sqlspec.service._base import SQLSpecAsyncService, SQLSpecSyncService

__all__ = ("SQLSpecAsyncService", "SQLSpecSyncService")

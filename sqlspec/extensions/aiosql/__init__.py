"""SQLSpec aiosql integration for loading SQL files.

This module provides a simple way to load aiosql-style SQL files and use them
with SQLSpec drivers. It focuses on just the file parsing functionality,
returning SQL objects that work with existing SQLSpec execution.
"""

from sqlspec.extensions.aiosql.adapter import AiosqlAsyncAdapter, AiosqlSyncAdapter

__all__ = ("AiosqlAsyncAdapter", "AiosqlSyncAdapter")

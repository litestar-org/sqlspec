"""Utility functions and classes for SQLSpec.

This package provides various utility modules for deprecation handling,
fixture loading, logging, module loading, portal pattern for async bridging,
singleton patterns, sync/async conversion, text processing, and type guards.
"""

from sqlspec.utils import (
    deprecation,
    fixtures,
    logging,
    module_loader,
    portal,
    singleton,
    sync_tools,
    text,
    type_guards,
)

__all__ = (
    "deprecation",
    "fixtures",
    "logging",
    "module_loader",
    "portal",
    "singleton",
    "sync_tools",
    "text",
    "type_guards",
)

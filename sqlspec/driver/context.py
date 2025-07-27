"""Context-based driver access for SQL objects.

This module provides a way for SQL objects to access the current driver
adapter without requiring explicit passing through the API.
"""

from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sqlspec.driver._common import CommonDriverAttributesMixin

__all__ = ("current_driver", "get_current_driver", "set_current_driver")

# Context variable to hold the current driver adapter
current_driver: ContextVar[Optional["CommonDriverAttributesMixin"]] = ContextVar(
    "current_driver", default=None
)


def get_current_driver() -> Optional["CommonDriverAttributesMixin"]:
    """Get the current driver adapter from context.
    
    Returns:
        The current driver adapter or None if not set.
    """
    return current_driver.get()


def set_current_driver(driver: Optional["CommonDriverAttributesMixin"]) -> None:
    """Set the current driver adapter in context.
    
    Args:
        driver: The driver adapter to set as current, or None to clear.
    """
    current_driver.set(driver)
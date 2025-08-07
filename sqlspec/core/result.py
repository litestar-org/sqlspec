"""Preserved result classes with exact same interfaces.

This module preserves the existing result system completely unchanged,
as the current result classes work correctly and drivers depend on them.

CRITICAL: NO CHANGES to result classes - they work perfectly and drivers
have complex dependencies on the exact interfaces and behavior.

Classes Preserved:
- StatementResult: ABC base class with exact same __slots__ and methods
- SQLResult: Main implementation with complete interface preservation
- ArrowResult: Arrow-based results with same capabilities

The result system is one component that does NOT need optimization - 
it already works efficiently and changing it would only introduce risk
without meaningful benefit.

Preservation Strategy:
- Exact copy from current statement/result.py
- Same __slots__ for memory efficiency
- Same method signatures and behavior
- Same type annotations and compatibility
- Same error handling and edge cases
"""

# EXACT COPY from current statement/result.py
# No changes - these classes work correctly and drivers depend on them

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

if TYPE_CHECKING:
    from sqlspec.core.statement import SQL

__all__ = ("ArrowResult", "SQLResult", "StatementResult")

# Preserve exact same operation type definition
OperationType = Literal["SELECT", "INSERT", "UPDATE", "DELETE", "COPY", "EXECUTE", "SCRIPT"]


class StatementResult(ABC):
    """Base class for SQL statement execution results - PRESERVED EXACTLY.
    
    This abstract base class defines the interface for all SQL execution results.
    The interface and behavior must remain identical to ensure driver compatibility.
    
    CRITICAL: This class is preserved exactly as-is from current implementation.
    Any changes could break driver compatibility and integration tests.
    """
    
    __slots__ = ("data", "execution_time", "last_inserted_id", "metadata", "rows_affected", "statement")
    
    def __init__(
        self,
        data: Any = None,
        execution_time: Optional[float] = None,
        last_inserted_id: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        rows_affected: Optional[int] = None,
        statement: Optional["SQL"] = None,
    ) -> None:
        """Initialize statement result - PRESERVED EXACTLY.
        
        Args:
            data: Result data from query execution
            execution_time: Execution time in seconds
            last_inserted_id: Last inserted ID (for INSERT operations)
            metadata: Additional metadata about the result
            rows_affected: Number of rows affected by the operation
            statement: The SQL statement that produced this result
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Must preserve identical initialization behavior
        raise NotImplementedError("BUILD phase - will copy exact StatementResult implementation")
    
    @abstractmethod
    def is_success(self) -> bool:
        """Check if the statement execution was successful - PRESERVED EXACTLY.
        
        Returns:
            True if execution was successful, False otherwise
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact is_success implementation")
    
    def get_data(self) -> Any:
        """Get result data - PRESERVED EXACTLY.
        
        Returns:
            The result data from query execution
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact get_data implementation")
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value by key - PRESERVED EXACTLY.
        
        Args:
            key: Metadata key
            default: Default value if key not found
            
        Returns:
            Metadata value or default
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact get_metadata implementation")
    
    def set_metadata(self, key: str, value: Any) -> None:
        """Set metadata value - PRESERVED EXACTLY.
        
        Args:
            key: Metadata key
            value: Metadata value
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact set_metadata implementation")
    
    @property
    def operation_type(self) -> OperationType:
        """Get operation type - PRESERVED EXACTLY.
        
        Returns:
            The type of SQL operation that produced this result
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact operation_type implementation")


class SQLResult(StatementResult):
    """Unified result class for SQL operations - PRESERVED EXACTLY.
    
    This is the main result implementation used by most drivers.
    The implementation must remain identical to ensure compatibility.
    
    CRITICAL: This class is preserved exactly as-is from current implementation.
    All methods, properties, and behavior must remain unchanged.
    """
    
    def __init__(
        self,
        data: Any = None,
        execution_time: Optional[float] = None,
        last_inserted_id: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        rows_affected: Optional[int] = None,
        statement: Optional["SQL"] = None,
    ) -> None:
        """Initialize SQL result - PRESERVED EXACTLY.
        
        Args:
            data: Result data from query execution
            execution_time: Execution time in seconds
            last_inserted_id: Last inserted ID (for INSERT operations)
            metadata: Additional metadata about the result
            rows_affected: Number of rows affected by the operation
            statement: The SQL statement that produced this result
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Must preserve identical behavior to current SQLResult
        raise NotImplementedError("BUILD phase - will copy exact SQLResult implementation")
    
    def is_success(self) -> bool:
        """Check if execution was successful - PRESERVED EXACTLY.
        
        Returns:
            True if execution was successful, False otherwise
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact is_success implementation")
    
    # All other methods will be copied exactly from current SQLResult implementation
    # during the BUILD phase to ensure 100% compatibility


class ArrowResult(StatementResult):
    """Arrow-based result class for high-performance data processing - PRESERVED EXACTLY.
    
    This class handles Arrow-based results for drivers that support Apache Arrow.
    The implementation must remain identical for Arrow integration compatibility.
    
    CRITICAL: This class is preserved exactly as-is from current implementation.
    Arrow integration is complex and any changes could break compatibility.
    """
    
    def __init__(
        self,
        data: Any = None,
        execution_time: Optional[float] = None,
        last_inserted_id: Optional[int] = None,
        metadata: Optional[dict[str, Any]] = None,
        rows_affected: Optional[int] = None,
        statement: Optional["SQL"] = None,
    ) -> None:
        """Initialize Arrow result - PRESERVED EXACTLY.
        
        Args:
            data: Arrow-based result data
            execution_time: Execution time in seconds
            last_inserted_id: Last inserted ID (for INSERT operations)
            metadata: Additional metadata about the result
            rows_affected: Number of rows affected by the operation
            statement: The SQL statement that produced this result
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Must preserve identical Arrow handling behavior
        raise NotImplementedError("BUILD phase - will copy exact ArrowResult implementation")
    
    def is_success(self) -> bool:
        """Check if execution was successful - PRESERVED EXACTLY.
        
        Returns:
            True if execution was successful, False otherwise
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact Arrow is_success implementation")
    
    # All Arrow-specific methods will be copied exactly from current ArrowResult
    # implementation during the BUILD phase


# Utility functions for result creation - PRESERVED EXACTLY
def create_sql_result(
    data: Any = None,
    execution_time: Optional[float] = None,
    last_inserted_id: Optional[int] = None,
    metadata: Optional[dict[str, Any]] = None,
    rows_affected: Optional[int] = None,
    statement: Optional["SQL"] = None,
) -> SQLResult:
    """Create SQLResult instance - PRESERVED EXACTLY.
    
    Factory function for creating SQLResult instances with consistent interface.
    
    Args:
        data: Result data from query execution
        execution_time: Execution time in seconds
        last_inserted_id: Last inserted ID (for INSERT operations)
        metadata: Additional metadata about the result
        rows_affected: Number of rows affected by the operation
        statement: The SQL statement that produced this result
        
    Returns:
        SQLResult instance
    """
    # PLACEHOLDER - Will copy exact implementation during BUILD phase
    raise NotImplementedError("BUILD phase - will copy exact create_sql_result implementation")


def create_arrow_result(
    data: Any = None,
    execution_time: Optional[float] = None,
    last_inserted_id: Optional[int] = None,
    metadata: Optional[dict[str, Any]] = None,
    rows_affected: Optional[int] = None,
    statement: Optional["SQL"] = None,
) -> ArrowResult:
    """Create ArrowResult instance - PRESERVED EXACTLY.
    
    Factory function for creating ArrowResult instances with Arrow data.
    
    Args:
        data: Arrow-based result data
        execution_time: Execution time in seconds
        last_inserted_id: Last inserted ID (for INSERT operations)
        metadata: Additional metadata about the result
        rows_affected: Number of rows affected by the operation
        statement: The SQL statement that produced this result
        
    Returns:
        ArrowResult instance
    """
    # PLACEHOLDER - Will copy exact implementation during BUILD phase
    raise NotImplementedError("BUILD phase - will copy exact create_arrow_result implementation")


# Implementation status tracking
__module_status__ = "PRESERVATION"  # This module preserves existing implementation
__compatibility_requirement__ = "100%"  # Must maintain exact compatibility
__change_policy__ = "NO CHANGES"  # Result classes must not be modified
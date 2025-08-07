"""SQL statement splitter - moved from statement/ and preserved.

This module handles SQL script splitting into individual statements for execution.
It was moved from statement/splitter.py to the core module for better organization
but is preserved exactly as-is since it works correctly.

CRITICAL: NO CHANGES to splitter logic - it handles complex SQL parsing edge cases
correctly and drivers depend on the exact splitting behavior.

Key Components:
- StatementSplitter: Main splitting engine with delimiter detection
- SQLScript: Container for multiple statements with execution metadata
- Statement position tracking for error reporting
- Comment and string literal handling for accurate parsing

The splitter handles complex cases:
- Multiple statement types (DDL, DML, stored procedures)
- String literals with embedded semicolons
- SQL comments (line and block comments)
- Dollar-quoted strings in PostgreSQL
- GO batch separators in SQL Server
- Complex stored procedure and function definitions

Preservation Strategy:
- Exact copy from statement/splitter.py
- Same class interfaces and method signatures
- Same parsing logic and edge case handling
- Same error reporting and position tracking
"""

import re
from typing import Iterator, NamedTuple, Optional
from enum import Enum

__all__ = ("StatementSplitter", "SQLScript", "StatementInfo", "SplitterMode")


class SplitterMode(str, Enum):
    """SQL splitting mode - preserved from statement/splitter.py.
    
    Controls how the splitter handles different SQL dialects and contexts:
    - STANDARD: Standard SQL with semicolon delimiters
    - POSTGRESQL: PostgreSQL with dollar-quoted strings and extensions
    - SQLSERVER: SQL Server with GO batch separators
    - ORACLE: Oracle PL/SQL with complex block structure
    - MYSQL: MySQL with delimiter commands and stored procedures
    """
    STANDARD = "standard"
    POSTGRESQL = "postgresql" 
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    MYSQL = "mysql"


class StatementInfo(NamedTuple):
    """Information about a split statement - preserved interface.
    
    Contains metadata about each statement found during splitting:
    - statement: The SQL statement text
    - start_pos: Starting character position in original script
    - end_pos: Ending character position in original script
    - line_number: Line number where statement starts (1-based)
    - statement_type: Detected type (DDL, DML, stored procedure, etc.)
    """
    statement: str
    start_pos: int
    end_pos: int
    line_number: int
    statement_type: Optional[str] = None


class SQLScript:
    """Container for multiple SQL statements - preserved from statement/splitter.py.
    
    Represents a SQL script that has been split into individual statements
    with position tracking and metadata for execution and error reporting.
    
    CRITICAL: This class is preserved exactly as-is from current implementation.
    Drivers use this for batch execution and depend on the exact interface.
    """
    
    __slots__ = ('statements', 'original_script', 'mode', '_statement_cache')
    
    def __init__(
        self, 
        statements: "list[StatementInfo]",
        original_script: str,
        mode: SplitterMode = SplitterMode.STANDARD
    ) -> None:
        """Initialize SQL script container - preserved exactly.
        
        Args:
            statements: List of split statement information
            original_script: Original SQL script text
            mode: Splitting mode used
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Must preserve identical behavior for batch execution
        raise NotImplementedError("BUILD phase - will copy exact SQLScript implementation")
    
    def __len__(self) -> int:
        """Number of statements in script - preserved interface."""
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact __len__ implementation")
    
    def __iter__(self) -> Iterator[StatementInfo]:
        """Iterate over statements - preserved interface."""
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact __iter__ implementation")
    
    def __getitem__(self, index: int) -> StatementInfo:
        """Get statement by index - preserved interface."""
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact __getitem__ implementation")
    
    @property
    def statement_count(self) -> int:
        """Number of statements - preserved interface."""
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact statement_count implementation")
    
    def get_statement_at_position(self, char_pos: int) -> Optional[StatementInfo]:
        """Get statement containing character position - preserved interface.
        
        Used for error reporting to map character positions back to statements.
        
        Args:
            char_pos: Character position in original script
            
        Returns:
            StatementInfo containing the position, or None if not found
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact position mapping")


class StatementSplitter:
    """SQL statement splitter with dialect-specific handling - preserved exactly.
    
    Handles splitting SQL scripts into individual statements while correctly
    handling complex cases like string literals, comments, and dialect-specific
    constructs.
    
    CRITICAL: This class is preserved exactly as-is from statement/splitter.py.
    The splitting logic handles many edge cases and any changes could break
    script execution in subtle ways.
    
    Complex Cases Handled:
    - String literals with embedded semicolons
    - SQL comments (both line and block comments)
    - Dollar-quoted strings in PostgreSQL ($tag$...$tag$)
    - GO batch separators in SQL Server
    - Stored procedure and function definitions
    - MySQL DELIMITER commands
    - Oracle PL/SQL block structure
    """
    
    __slots__ = ('mode', '_delimiter_patterns', '_string_patterns', '_comment_patterns')
    
    def __init__(self, mode: SplitterMode = SplitterMode.STANDARD) -> None:
        """Initialize splitter with dialect mode - preserved exactly.
        
        Args:
            mode: Splitting mode for dialect-specific handling
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Must initialize with same regex patterns and dialect handling
        raise NotImplementedError("BUILD phase - will copy exact StatementSplitter initialization")
    
    def split(self, script: str) -> SQLScript:
        """Split SQL script into individual statements - preserved interface.
        
        Main splitting method that handles all the complex cases and returns
        a SQLScript with statement metadata.
        
        Args:
            script: SQL script to split
            
        Returns:
            SQLScript with individual statements and metadata
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Critical method that must preserve exact splitting behavior
        raise NotImplementedError("BUILD phase - will copy exact split implementation")
    
    def _find_statement_boundaries(self, script: str) -> "list[tuple[int, int]]":
        """Find statement boundaries in script - preserved logic.
        
        Core parsing method that identifies where statements begin and end
        while handling string literals, comments, and dialect-specific constructs.
        
        Args:
            script: SQL script to parse
            
        Returns:
            List of (start_pos, end_pos) tuples for each statement
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        # Complex parsing logic that must remain identical
        raise NotImplementedError("BUILD phase - will copy exact boundary detection")
    
    def _handle_string_literals(self, script: str, pos: int) -> int:
        """Skip over string literals - preserved logic.
        
        Handles different string literal formats:
        - Single quotes with escape sequences
        - Double quotes (if supported by dialect)
        - Dollar-quoted strings in PostgreSQL
        
        Args:
            script: SQL script
            pos: Current position in script
            
        Returns:
            Position after the string literal
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact string literal handling")
    
    def _handle_comments(self, script: str, pos: int) -> int:
        """Skip over SQL comments - preserved logic.
        
        Handles both line comments (--) and block comments (/* */) with
        proper nesting support where applicable.
        
        Args:
            script: SQL script
            pos: Current position in script
            
        Returns:
            Position after the comment
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact comment handling")
    
    def _detect_statement_type(self, statement: str) -> Optional[str]:
        """Detect statement type - preserved logic.
        
        Identifies the type of SQL statement for metadata:
        - DDL: CREATE, ALTER, DROP, etc.
        - DML: SELECT, INSERT, UPDATE, DELETE
        - DCL: GRANT, REVOKE
        - TCL: COMMIT, ROLLBACK
        - Stored procedure/function calls
        
        Args:
            statement: SQL statement text
            
        Returns:
            Statement type string or None if unknown
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact type detection")
    
    def _calculate_line_numbers(self, script: str, boundaries: "list[tuple[int, int]]") -> "list[int]":
        """Calculate line numbers for statements - preserved logic.
        
        Maps character positions to line numbers for error reporting.
        
        Args:
            script: Original SQL script
            boundaries: List of statement boundaries
            
        Returns:
            List of line numbers (1-based) for each statement
        """
        # PLACEHOLDER - Will copy exact implementation during BUILD phase
        raise NotImplementedError("BUILD phase - will copy exact line number calculation")


# Utility functions for common splitting scenarios - preserved interfaces
def split_sql_script(script: str, mode: SplitterMode = SplitterMode.STANDARD) -> SQLScript:
    """Split SQL script - convenience function with preserved interface.
    
    High-level function for splitting SQL scripts with default configuration.
    
    Args:
        script: SQL script to split
        mode: Splitting mode for dialect handling
        
    Returns:
        SQLScript with split statements
    """
    # PLACEHOLDER - Will copy exact implementation during BUILD phase
    raise NotImplementedError("BUILD phase - will copy exact split_sql_script implementation")


def is_single_statement(script: str, mode: SplitterMode = SplitterMode.STANDARD) -> bool:
    """Check if script contains single statement - preserved interface.
    
    Quick check to determine if a script needs splitting or can be executed directly.
    
    Args:
        script: SQL script to check
        mode: Splitting mode for dialect handling
        
    Returns:
        True if script contains exactly one statement
    """
    # PLACEHOLDER - Will copy exact implementation during BUILD phase
    raise NotImplementedError("BUILD phase - will copy exact single statement detection")


def extract_first_statement(script: str, mode: SplitterMode = SplitterMode.STANDARD) -> str:
    """Extract first statement from script - preserved interface.
    
    Utility for getting just the first statement without full splitting overhead.
    
    Args:
        script: SQL script
        mode: Splitting mode for dialect handling
        
    Returns:
        First statement text
    """
    # PLACEHOLDER - Will copy exact implementation during BUILD phase
    raise NotImplementedError("BUILD phase - will copy exact first statement extraction")


# Implementation status tracking
__module_status__ = "PRESERVATION"  # This module preserves existing implementation
__compatibility_requirement__ = "100%"  # Must maintain exact compatibility
__change_policy__ = "NO CHANGES"  # Splitter logic must not be modified
__migration_source__ = "statement/splitter.py"  # Original location
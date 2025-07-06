"""New, refactored SQL statement handling."""
from dataclasses import dataclass, field
from typing import Any, Optional

from sqlglot import exp

from sqlspec.statement_new.config import SQLConfig
from sqlspec.statement_new.protocols import SQLProcessingContext
from sqlspec.utils import hash_expression

__all__ = ("SQLState",)

@dataclass
class SQLState:
    """Encapsulates the immutable state of a SQL query."""
    original_sql: str
    expression: Optional[exp.Expression] = None
    parameters: Any = None
    dialect: Optional[str] = None
    config: SQLConfig = field(default_factory=SQLConfig)
    processed: bool = False
    processing_context: Optional[SQLProcessingContext] = None
    validation_errors: list[str] = field(default_factory=list)
    _compiled_cache: dict[str, tuple[str, Any]] = field(default_factory=dict)

    def cache_key(self) -> str:
        """Generate a cache key for the SQL state."""
        param_hash = hash(str(self.parameters)) if self.parameters else 0
        effective_dialect = self.dialect or (self.config.dialect if self.config else "default")

        if self.expression:
            # New AST-based approach - no SQL rendering needed
            ast_hash = hash_expression(self.expression)
            return f"ast:{ast_hash}:{param_hash}:{effective_dialect}"
        # Fallback for non-parsed SQL (also include param_hash for consistency)
        return f"sql:{hash(self.original_sql)}:{param_hash}:{effective_dialect}"

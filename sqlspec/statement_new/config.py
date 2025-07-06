"""Configuration for SQL statement handling."""

from dataclasses import dataclass
from typing import Optional

__all__ = ("SQLConfig",)

# Constants
DEFAULT_CACHE_MAX_SIZE = 1000


@dataclass
class SQLConfig:
    """Configuration for SQL statement behavior."""

    dialect: Optional[str] = None
    allowed_parameter_styles: Optional[tuple[str, ...]] = None
    allow_mixed_parameter_styles: bool = False
    enable_parameter_literal_extraction: bool = True
    enable_validation: bool = True
    enable_transformations: bool = True
    enable_caching: bool = True
    cache_max_size: int = DEFAULT_CACHE_MAX_SIZE

    def validate_parameter_style(self, style: str) -> bool:
        """Check if a parameter style is allowed."""
        if self.allowed_parameter_styles is None:
            return True
        return style in self.allowed_parameter_styles

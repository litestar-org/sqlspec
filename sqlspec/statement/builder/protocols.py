from typing import Any, Optional, Protocol

from sqlglot import exp

__all__ = ("BuilderProtocol", )


class BuilderProtocol(Protocol):
    _expression: Optional[exp.Expression]
    _parameters: dict[str, Any]
    _parameter_counter: int
    dialect: Any
    dialect_name: Optional[str]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]: ...

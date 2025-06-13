from typing import Any, Optional, Protocol, Union

from sqlglot import exp
from typing_extensions import Self

__all__ = ("BuilderProtocol", "SelectBuilderProtocol")


class BuilderProtocol(Protocol):
    _expression: Optional[exp.Expression]
    _parameters: dict[str, Any]
    _parameter_counter: int
    dialect: Any
    dialect_name: Optional[str]

    def add_parameter(self, value: Any, name: Optional[str] = None) -> tuple[Any, str]: ...


class SelectBuilderProtocol(BuilderProtocol, Protocol):
    def select(self, *columns: Union[str, exp.Expression]) -> Self: ...

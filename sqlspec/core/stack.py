"""Immutable builder utilities for multi-statement execution stacks."""

from collections.abc import Iterator, Mapping, Sequence
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from sqlspec.builder import QueryBuilder
    from sqlspec.core.filters import StatementFilter
    from sqlspec.core.statement import Statement, StatementConfig
    from sqlspec.typing import StatementParameters
__all__ = ("StackOperation", "StatementStack")


ALLOWED_METHODS: "tuple[str, ...]" = ("execute", "execute_many", "execute_script", "execute_arrow")


class StackOperation:
    """Single SQL operation captured inside a statement stack."""

    __slots__ = ("arguments", "keyword_arguments", "method", "statement")

    def __init__(
        self,
        method: str,
        statement: Any,
        arguments: "tuple[Any, ...] | None" = None,
        keyword_arguments: "Mapping[str, Any] | None" = None,
    ) -> None:
        if method not in ALLOWED_METHODS:
            msg = f"Unsupported stack method {method!r}"
            raise ValueError(msg)
        self.method = method
        self.statement = statement
        self.arguments = arguments if arguments is not None else ()
        self.keyword_arguments = keyword_arguments


class StatementStack:
    """Immutable builder that preserves ordered SQL operations."""

    __slots__ = ("_operations",)

    def __init__(self, operations: "tuple[StackOperation, ...] | None" = None) -> None:
        self._operations = operations if operations is not None else ()

    def __iter__(self) -> "Iterator[StackOperation]":
        return iter(self._operations)

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._operations)

    def __bool__(self) -> bool:  # pragma: no cover - trivial
        return bool(self._operations)

    def __repr__(self) -> str:
        return f"StatementStack(size={len(self._operations)})"

    @property
    def operations(self) -> "tuple[StackOperation, ...]":
        return self._operations

    def push_execute(
        self,
        statement: "str | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "StatementStack":
        normalized_statement = _validate_statement(statement)
        frozen_kwargs = _freeze_kwargs(kwargs, statement_config)
        operation = StackOperation("execute", normalized_statement, tuple(parameters), frozen_kwargs)
        return self._append(operation)

    def push_execute_many(
        self,
        statement: "str | Statement | QueryBuilder",
        parameter_sets: "Sequence[StatementParameters]",
        /,
        *filters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "StatementStack":
        normalized_statement = _validate_statement(statement)
        _validate_execute_many_payload(parameter_sets)
        normalized_sets = tuple(parameter_sets)
        arguments = (normalized_sets, *filters)
        frozen_kwargs = _freeze_kwargs(kwargs, statement_config)
        operation = StackOperation("execute_many", normalized_statement, tuple(arguments), frozen_kwargs)
        return self._append(operation)

    def push_execute_script(
        self,
        statement: "str | Statement",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "StatementStack":
        normalized_statement = _validate_statement(statement)
        frozen_kwargs = _freeze_kwargs(kwargs, statement_config)
        operation = StackOperation("execute_script", normalized_statement, tuple(parameters), frozen_kwargs)
        return self._append(operation)

    def push_execute_arrow(
        self,
        statement: "str | Statement | QueryBuilder",
        /,
        *parameters: "StatementParameters | StatementFilter",
        statement_config: "StatementConfig | None" = None,
        **kwargs: Any,
    ) -> "StatementStack":
        normalized_statement = _validate_statement(statement)
        frozen_kwargs = _freeze_kwargs(kwargs, statement_config)
        operation = StackOperation("execute_arrow", normalized_statement, tuple(parameters), frozen_kwargs)
        return self._append(operation)

    def extend(self, *stacks: "StatementStack") -> "StatementStack":
        operations = list(self._operations)
        for stack in stacks:
            operations.extend(stack._operations)
        return StatementStack(tuple(operations))

    @classmethod
    def from_operations(cls, operations: "Sequence[StackOperation] | None" = None) -> "StatementStack":
        if not operations:
            return cls()
        return cls(tuple(operations))

    def _append(self, operation: StackOperation) -> "StatementStack":
        return StatementStack((*self._operations, operation))


def _validate_statement(statement: Any) -> Any:
    if isinstance(statement, StatementStack):
        msg = "Nested StatementStack instances are not supported"
        raise TypeError(msg)
    if isinstance(statement, str):
        stripped = statement.strip()
        if not stripped:
            msg = "Stack statements require non-empty SQL strings"
            raise ValueError(msg)
        return statement
    return statement


def _validate_execute_many_payload(parameter_sets: Any) -> None:
    if not isinstance(parameter_sets, Sequence) or isinstance(parameter_sets, (str, bytes, bytearray)):
        msg = "execute_many payload must be a sequence of parameter sets"
        raise TypeError(msg)
    if not parameter_sets:
        msg = "execute_many payload cannot be empty"
        raise ValueError(msg)


def _freeze_kwargs(kwargs: "dict[str, Any]", statement_config: "StatementConfig | None") -> "Mapping[str, Any] | None":
    if not kwargs and statement_config is None:
        return None
    payload = dict(kwargs)
    if statement_config is not None:
        payload["statement_config"] = statement_config
    return MappingProxyType(payload)

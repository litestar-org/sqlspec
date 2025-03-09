"""This is a simple wrapper around a few important classes in each library.

This is used to ensure compatibility when one or more of the libraries are installed.
"""

from enum import Enum
from typing import (
    Any,
    ClassVar,
    Final,
    Protocol,
    Union,
    cast,
    runtime_checkable,
)

from typing_extensions import Literal, TypeVar, dataclass_transform


@runtime_checkable
class DataclassProtocol(Protocol):
    """Protocol for instance checking dataclasses."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)

try:
    from pydantic import (
        BaseModel,
        FailFast,  # pyright: ignore[reportGeneralTypeIssues,reportAssignmentType]
        TypeAdapter,
    )

    PYDANTIC_INSTALLED = True
except ImportError:
    from dataclasses import dataclass

    @runtime_checkable
    class BaseModel(Protocol):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        model_fields: ClassVar[dict[str, Any]]

        def model_dump(
            self,
            /,
            *,
            include: "Optional[Any]" = None,
            exclude: "Optional[Any]" = None,
            context: "Optional[Any]" = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: "Union[bool, Literal['none', 'warn', 'error']]" = True,
            serialize_as_any: bool = False,
        ) -> "dict[str, Any]":
            """Placeholder"""
            return {}

        def model_dump_json(
            self,
            /,
            *,
            include: "Optional[Any]" = None,
            exclude: "Optional[Any]" = None,
            context: "Optional[Any]" = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: "Union[bool, Literal['none', 'warn', 'error']]" = True,
            serialize_as_any: bool = False,
        ) -> str:
            """Placeholder"""
            return ""

    @runtime_checkable
    class TypeAdapter(Protocol[T_co]):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        def __init__(
            self,
            type: Any,  # noqa: A002
            *,
            config: "Optional[Any]" = None,
            _parent_depth: int = 2,
            module: "Optional[str]" = None,
        ) -> None:
            """Init"""

        def validate_python(
            self,
            object: Any,  # noqa: A002
            /,
            *,
            strict: "Optional[bool]" = None,
            from_attributes: "Optional[bool]" = None,
            context: "Optional[dict[str, Any]]" = None,
            experimental_allow_partial: "Union[bool, Literal['off', 'on', 'trailing-strings']]" = False,
        ) -> "T_co":
            """Stub"""
            return cast("T_co", object)

    @dataclass
    class FailFast:  # type: ignore[no-redef]
        """Placeholder Implementation for FailFast"""

        fail_fast: bool = True

    PYDANTIC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

try:
    from msgspec import (
        UNSET,
        Struct,
        UnsetType,  # pyright: ignore[reportAssignmentType,reportGeneralTypeIssues]
        convert,
    )

    MSGSPEC_INSTALLED: bool = True
except ImportError:
    import enum
    from collections.abc import Iterable
    from typing import TYPE_CHECKING, Callable, Optional, Union

    if TYPE_CHECKING:
        from collections.abc import Iterable

    @dataclass_transform()
    @runtime_checkable
    class Struct(Protocol):  # type: ignore[no-redef]
        """Placeholder Implementation"""

        __struct_fields__: "ClassVar[tuple[str, ...]]"

    def convert(  # type: ignore[no-redef]
        obj: Any,
        type: "Union[Any, type[T]]",  # noqa: A002
        *,
        strict: bool = True,
        from_attributes: bool = False,
        dec_hook: "Optional[Callable[[type, Any], Any]]" = None,
        builtin_types: "Union[Iterable[type], None]" = None,
        str_keys: bool = False,
    ) -> "Union[T, Any]":
        """Placeholder implementation"""
        return {}

    class UnsetType(enum.Enum):  # type: ignore[no-redef]
        UNSET = "UNSET"

    UNSET = UnsetType.UNSET  # pyright: ignore[reportConstantRedefinition]
    MSGSPEC_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]

try:
    from litestar.dto.data_structures import DTOData  # pyright: ignore[reportUnknownVariableType]

    LITESTAR_INSTALLED = True
except ImportError:

    @runtime_checkable
    class DTOData(Protocol[T]):  # type: ignore[no-redef]
        """Placeholder implementation"""

        __slots__ = ("_backend", "_data_as_builtins")

        def __init__(self, backend: Any, data_as_builtins: Any) -> None:
            """Placeholder init"""

        def create_instance(self, **kwargs: Any) -> "T":
            """Placeholder implementation"""
            return cast("T", kwargs)

        def update_instance(self, instance: "T", **kwargs: Any) -> "T":
            """Placeholder implementation"""
            return cast("T", kwargs)

        def as_builtins(self) -> Any:
            """Placeholder implementation"""
            return {}

    LITESTAR_INSTALLED = False  # pyright: ignore[reportConstantRedefinition]


class EmptyEnum(Enum):
    """A sentinel enum used as placeholder."""

    EMPTY = 0


EmptyType = Union[Literal[EmptyEnum.EMPTY], UnsetType]
Empty: Final = EmptyEnum.EMPTY


__all__ = (
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "PYDANTIC_INSTALLED",
    "UNSET",
    "BaseModel",
    "DTOData",
    "DataclassProtocol",
    "Empty",
    "EmptyEnum",
    "EmptyType",
    "FailFast",
    "Struct",
    "TypeAdapter",
    "UnsetType",
    "convert",
)

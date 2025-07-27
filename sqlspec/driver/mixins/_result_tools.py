# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import datetime
import logging
from collections.abc import Sequence
from enum import Enum
from functools import partial
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast, overload
from uuid import UUID

from mypy_extensions import trait
from sqlglot import exp

from sqlspec.exceptions import SQLSpecError, wrap_exceptions
from sqlspec.statement.result import OperationType, SQLResult
from sqlspec.statement.sql import SQL
from sqlspec.typing import ModelDTOT, ModelT, convert, get_type_adapter
from sqlspec.utils.type_guards import is_dataclass, is_msgspec_struct, is_pydantic_model, is_select_builder

if TYPE_CHECKING:
    from sqlspec.statement import Statement
    from sqlspec.statement.builder import Select
    from sqlspec.statement.sql import SQLConfig


__all__ = ("_DEFAULT_TYPE_DECODERS", "_default_msgspec_deserializer")


WINDOWS_PATH_MIN_LENGTH = 3
logger = logging.getLogger(__name__)
_DEFAULT_TYPE_DECODERS: list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]] = [
    (lambda x: x is UUID, lambda t, v: t(v.hex)),
    (lambda x: x is datetime.datetime, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.date, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.time, lambda t, v: t(v.isoformat())),
    (lambda x: x is Enum, lambda t, v: t(v.value)),
]


def _default_msgspec_deserializer(
    target_type: Any, value: Any, type_decoders: "Optional[Sequence[tuple[Any, Any]]]" = None
) -> Any:
    """Default msgspec deserializer with type conversion support.

    Converts values to appropriate types for msgspec deserialization, including
    UUID, datetime, date, time, Enum, Path, and PurePath types.
    """
    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)
    if target_type is UUID and isinstance(value, UUID):
        return value.hex
    if target_type in {datetime.datetime, datetime.date, datetime.time}:
        with wrap_exceptions(suppress=AttributeError):
            return value.isoformat()
    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value
    if isinstance(value, target_type):
        return value
    if issubclass(target_type, (Path, PurePath, UUID)):
        return target_type(value)
    return value


@trait
class ToSchemaMixin:
    def _determine_operation_type(self, statement: "Any") -> OperationType:
        """Determine operation type from SQL statement expression.

        Examines the statement's expression type to determine if it's
        INSERT, UPDATE, DELETE, SELECT, SCRIPT, or generic EXECUTE.

        Args:
            statement: SQL statement object with expression attribute

        Returns:
            OperationType literal value
        """
        # Check if it's a script first
        if hasattr(statement, 'is_script') and statement.is_script:
            return "SCRIPT"
        
        try:
            expression = statement.expression
        except AttributeError:
            return "EXECUTE"

        if not expression:
            return "EXECUTE"

        expr_type = type(expression).__name__.upper()
        if "INSERT" in expr_type:
            return "INSERT"
        if "UPDATE" in expr_type:
            return "UPDATE"
        if "DELETE" in expr_type:
            return "DELETE"
        if "SELECT" in expr_type:
            return "SELECT"
        return "EXECUTE"

    def _build_modify_result(self, cursor: "Any", statement: "Any") -> "SQLResult":
        """Build result for non-SELECT operations.

        Standard implementation for INSERT, UPDATE, DELETE, and other
        non-SELECT operations that return a row count.

        Args:
            cursor: Database cursor object with rowcount attribute
            statement: SQL statement object

        Returns:
            SQLResult object with operation metadata
        """

        return SQLResult(
            statement=statement,
            data=cast("list[dict[str, Any]]", []),
            rows_affected=cursor.rowcount,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    def _build_select_result(self, cursor: "Any", statement: "Any") -> "SQLResult":
        """Build result for SELECT operations.

        Standard implementation for SELECT operations that return rows.
        Fetches all data and extracts column names from cursor description.

        Args:
            cursor: Database cursor object with fetchall() and description
            statement: SQL statement object

        Returns:
            SQLResult object with fetched data and metadata
        """
        fetched_data = cursor.fetchall()
        # Convert to list if needed (some drivers return iterators)
        if hasattr(fetched_data, "__iter__") and not isinstance(fetched_data, list):
            fetched_data = list(fetched_data)

        return SQLResult(
            statement=statement,
            data=cast("list[dict[str, Any]]", fetched_data),
            column_names=[col[0] for col in cursor.description or []],
            rows_affected=len(fetched_data),
            operation_type="SELECT",
        )

    @overload
    @staticmethod
    def to_schema(data: "list[ModelT]", *, schema_type: None = None) -> "list[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[ModelDTOT]") -> "list[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "ModelT", *, schema_type: None = None) -> "ModelT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[ModelDTOT]") -> "ModelDTOT": ...

    @staticmethod
    def to_schema(
        data: "Union[ModelT, dict[str, Any], list[ModelT], list[dict[str, Any]]]",
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelT, ModelDTOT, Sequence[ModelT], Sequence[ModelDTOT]]":
        """Convert data to a specified schema type.

        Supports conversion to dataclasses, msgspec structs, and Pydantic models.
        Handles both single objects and sequences.
        """
        if schema_type is None:
            if not isinstance(data, Sequence):
                return cast("ModelT", data)
            return cast("Sequence[ModelT]", data)
        if is_dataclass(schema_type):
            # Check if this is a list of items first (before checking Sequence)
            if isinstance(data, list):
                return cast("Sequence[ModelDTOT]", [schema_type(**dict(item) if hasattr(item, 'keys') else item) for item in data])  # type: ignore[operator]
            # Handle single items (including sqlite3.Row objects)
            if hasattr(data, 'keys'):
                # sqlite3.Row and similar objects have keys() method
                return cast("ModelDTOT", schema_type(**dict(data)))  # type: ignore[operator]
            if isinstance(data, dict):
                return cast("ModelDTOT", schema_type(**data))  # type: ignore[operator]
            # Fallback for other types
            return cast("ModelDTOT", data)
        if is_msgspec_struct(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    convert(
                        obj=data,
                        type=schema_type,
                        from_attributes=True,
                        dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
                    ),
                )
            return cast(
                "Sequence[ModelDTOT]",
                convert(
                    obj=data,
                    type=list[schema_type],  # type: ignore[valid-type]  # pyright: ignore
                    from_attributes=True,
                    dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
                ),
            )
        if schema_type is not None and is_pydantic_model(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    get_type_adapter(schema_type).validate_python(data, from_attributes=True),  # pyright: ignore
                )
            return cast(
                "Sequence[ModelDTOT]",
                get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True),  # type: ignore[valid-type]  # pyright: ignore
            )
        msg = "`schema_type` should be a valid Dataclass, Pydantic model or Msgspec struct"
        raise SQLSpecError(msg)

    def _transform_to_sql(
        self,
        statement: "Union[Statement, Select]",
        params: "Optional[dict[str, Any]]" = None,
        config: "Optional[SQLConfig]" = None,
    ) -> "SQL":
        """Normalize a statement of any supported type into a SQL object.

        Args:
            statement: The statement to normalize (str, Expression, SQL, or Select)
            params: Optional parameters (ignored for Select and SQL objects)
            config: Optional SQL configuration

        Returns:
            A converted SQL object
        """

        if is_select_builder(statement):
            # Select has its own parameters via build(), ignore external params
            safe_query = statement.build()
            return SQL(safe_query.sql, parameters=safe_query.parameters, config=config)

        if isinstance(statement, SQL):
            # SQL object is already complete, ignore external params
            return statement

        if isinstance(statement, (str, exp.Expression)):
            # Parameters will be processed by SQL compile with driver context
            return SQL(statement, parameters=params, config=config)

        # Fallback for type safety
        msg = f"Unsupported statement type: {type(statement).__name__}"
        raise TypeError(msg)

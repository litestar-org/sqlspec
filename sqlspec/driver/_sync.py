"""Synchronous driver protocol implementation."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Union

from mypy_extensions import mypyc_attr

from sqlspec.driver._common import CommonDriverAttributesMixin
from sqlspec.driver.parameters import process_execute_many_parameters
from sqlspec.statement.builder import QueryBuilder
from sqlspec.statement.sql import SQL, SQLConfig, Statement
from sqlspec.typing import ModelDTOT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.statement.filters import StatementFilter
    from sqlspec.statement.result import SQLResult
    from sqlspec.typing import ConnectionT, ModelDTOT, StatementParameters

logger = get_logger("sqlspec")

__all__ = ("SyncDriverAdapterBase",)


EMPTY_FILTERS: "list[StatementFilter]" = []


@mypyc_attr(allow_interpreted_subclasses=True, native_class=False)
class SyncDriverAdapterBase(CommonDriverAttributesMixin, ABC):
    __slots__ = ()

    def __init__(
        self,
        connection: "ConnectionT",  # pyright: ignore
        config: "Optional[SQLConfig]" = None,
    ) -> None:
        """Initialize sync driver adapter.

        Args:
            connection: Database connection instance
            config: SQL configuration
        """
        self.connection = connection
        self.config = config or SQLConfig()
        super().__init__()

    def _build_statement(
        self,
        statement: "Union[Statement, QueryBuilder]",
        *parameters: "Union[StatementParameters, StatementFilter]",
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQL":
        """Build SQL statement from various input types.

        Ensures dialect is set and preserves existing state when rebuilding SQL objects.
        """
        _config = _config or self.config
        
        # Filter out driver-specific kwargs that shouldn't be passed to SQL
        # These are handled by the driver, not part of SQL parameters
        driver_kwargs = {"schema_type", "_suppress_warnings"}
        sql_kwargs = {k: v for k, v in kwargs.items() if k not in driver_kwargs}

        if isinstance(statement, QueryBuilder):
            return statement.to_statement(config=_config)
        if isinstance(statement, SQL):
            if parameters or kwargs:
                new_config = _config
                if self.dialect and new_config and not new_config.dialect:
                    new_config = new_config.replace(dialect=self.dialect)
                sql_source = statement._raw_sql or statement._statement
                existing_state = {
                    "is_many": statement._is_many,
                    "is_script": statement._is_script,
                    "original_parameters": statement._original_parameters,
                    "filters": statement._filters,
                    "positional_params": statement._positional_params,
                    "named_params": statement._named_params,
                }
                return SQL(sql_source, *parameters, config=new_config, _existing_state=existing_state, **sql_kwargs)
            if self.dialect and (not statement._config.dialect or statement._config.dialect != self.dialect):
                new_config = statement._config.replace(dialect=self.dialect)
                sql_source = statement._raw_sql or statement._statement
                existing_state = {
                    "is_many": statement._is_many,
                    "is_script": statement._is_script,
                    "original_parameters": statement._original_parameters,
                    "filters": statement._filters,
                    "positional_params": statement._positional_params,
                    "named_params": statement._named_params,
                }
                return SQL(sql_source, config=new_config, _existing_state=existing_state)
            return statement
        new_config = _config
        if self.dialect and new_config and not new_config.dialect:
            new_config = new_config.replace(dialect=self.dialect)
        return SQL(statement, *parameters, config=new_config, **sql_kwargs)

    @abstractmethod
    def _execute_statement(self, statement: "SQL", connection: Optional[Any] = None, **kwargs: Any) -> "SQLResult":
        """Actual execution implementation by concrete drivers, using the raw connection.

        Returns SQLResult directly based on the statement type.
        """
        raise NotImplementedError

    def execute(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        schema_type: "Optional[type[ModelDTOT]]" = None,
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        sql_statement = self._build_statement(statement, *parameters, _config=_config or self.config, **kwargs)
        result = self._execute_statement(statement=sql_statement, connection=self._connection(_connection), **kwargs)

        # Apply schema conversion if provided
        if schema_type is not None and hasattr(self, "to_schema"):
            result.data = self.to_schema(result.data, schema_type=schema_type)  # type: ignore[attr-defined]

        return result

    def execute_many(
        self,
        statement: "Union[SQL, Statement, QueryBuilder]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: Optional[Any] = None,
        _config: "Optional[SQLConfig]" = None,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute statement multiple times with different parameters.

        Passes first parameter set through pipeline to enable literal extraction
        and consistent parameter processing.
        """
        # Handle parameters passed as keyword argument for backward compatibility
        if not parameters and "parameters" in kwargs:
            parameters = (kwargs.pop("parameters"),)

        filters, param_sequence = process_execute_many_parameters(parameters)

        first_params = param_sequence[0] if param_sequence else None

        sql_statement = self._build_statement(
            statement, first_params, *filters, _config=_config or self.config, **kwargs
        )

        sql_statement = sql_statement.as_many(param_sequence)

        return self._execute_statement(statement=sql_statement, connection=self._connection(_connection), **kwargs)

    def execute_script(
        self,
        statement: "Union[str, SQL]",
        /,
        *parameters: "Union[StatementParameters, StatementFilter]",
        _connection: "Optional[ConnectionT]" = None,
        _config: "Optional[SQLConfig]" = None,
        _suppress_warnings: bool = False,
        **kwargs: Any,
    ) -> "SQLResult":
        """Execute a multi-statement script.

        By default, validates each statement and logs warnings for dangerous
        operations. Use _suppress_warnings=True for migrations and admin scripts.
        """
        script_config = _config or self.config

        sql_statement = self._build_statement(statement, *parameters, _config=script_config, **kwargs)
        sql_statement = sql_statement.as_script()

        if _suppress_warnings:
            kwargs["_suppress_warnings"] = True

        return self._execute_statement(statement=sql_statement, connection=self._connection(_connection), **kwargs)

import logging
import re
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from asyncpg import Connection
from sqlglot import exp
from typing_extensions import TypeAlias

from sqlspec.base import AsyncDriverAdapterProtocol
from sqlspec.sql.filters import StatementFilter
from sqlspec.sql.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.sql.statement import SQLStatement, Statement

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from asyncpg import Record
    from asyncpg.connection import Connection
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.exceptions import RiskLevel
    from sqlspec.sql.result import StatementResult
    from sqlspec.typing import ModelDTOT, StatementParameterType, T

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[Connection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = "Union[Connection, PoolConnectionProxy]"

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE) \d+ (\d+)$")


class AsyncpgDriver(
    SQLTranslatorMixin["AsyncpgConnection"],
    AsyncDriverAdapterProtocol["AsyncpgConnection"],
    ResultConverter,
):
    """AsyncPG Postgres Driver Adapter."""

    connection: "AsyncpgConnection"
    dialect: str = "postgres"

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    def _process_sql_params(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        **kwargs: Any,
    ) -> "tuple[str, Union[list[Any], dict[str, Any]], SQLStatement]":
        """Process SQL and parameters for AsyncPG.

        Leverages SQLStatement to parse the SQL, validate parameters, and obtain
        a sqlglot AST. This method then transforms the AST to use PostgreSQL-style
        $N placeholders (e.g., $1, $2) as required by asyncpg.

        Args:
            sql: SQL statement (string or sqlglot expression).
            parameters: Query parameters (data or StatementFilter).
            *filters: Statement filters to apply.
            **kwargs: Additional keyword arguments for SQLStatement.

        Returns:
            A tuple containing the processed SQL string (with $N placeholders)
            and an ordered tuple of parameter values for asyncpg.
        """
        data_params_for_statement: Optional[Union[Mapping[str, Any], Sequence[Any]]] = None
        combined_filters_list: list[StatementFilter] = list(filters)

        if parameters is not None:
            if isinstance(parameters, StatementFilter):
                combined_filters_list.insert(0, parameters)
            else:
                data_params_for_statement = parameters

        statement = SQLStatement(sql, data_params_for_statement, kwargs=kwargs, dialect=self.dialect)

        for filter_obj in combined_filters_list:
            statement = statement.apply_filter(filter_obj)

        # SQLStatement.process() now returns:
        # 1. final_sql_str: The processed SQL string.
        # 2. final_ordered_params: list or dict of parameter values, correctly ordered/named.
        # 3. validation_result: The result of the validation (unused here).
        final_sql_str, final_params_for_adapter, _ = statement.process()

        if not statement.parameter_info_list:  # Use statement.parameter_info_list
            # No placeholders, so no transformation needed; return SQL as is from the AST.
            return final_sql_str, final_params_for_adapter or [], statement  # Ensure params is not None

        # Map original placeholder AST nodes to new $N style parameter expressions.
        placeholder_map: dict[int, exp.Expression] = {
            id(p_info.expression_node): exp.Parameter(
                this=exp.Identifier(this=str(i + 1))
            )  # Changed p_info.node to p_info.expression_node
            for i, p_info in enumerate(statement.parameter_info_list)  # Use statement.parameter_info_list
        }

        def replace_with_pg_style(node: exp.Expression) -> exp.Expression:
            """AST transformer: Replaces known placeholder nodes with $N style parameters.
            This is used to transform the AST to use PostgreSQL style $N placeholders
            as required by asyncpg.

            Args:
                node: The AST node to transform.

            Returns:
                The transformed AST node.
            """
            return placeholder_map.get(id(node), node)

        # Transform the AST. `copy=True` prevents modification of the original SQLStatement's AST.
        # Ensure we are transforming the expression from the statement object that `process` was called on.
        transformed_expr = statement.expression.transform(replace_with_pg_style, copy=True)  # Use statement.expression
        transformed_final_sql = transformed_expr.sql(dialect=self.dialect)

        return transformed_final_sql, final_params_for_adapter or [], statement  # Ensure params is not None

    @overload
    async def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Sequence[dict[str, Any]]": ...
    @overload
    async def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Sequence[ModelDTOT]": ...
    async def select(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Sequence[Union[dict[str, Any], ModelDTOT]]":
        """Fetch data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            List of row data as either model instances or dictionaries.
        """
        connection = self._connection(connection)
        # _process_sql_params now returns sql_str, params_for_adapter, statement_obj
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)
        # asyncpg expects a tuple for positional parameters
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        results = await connection.fetch(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        if not results:
            return []
        return self.to_schema([dict(row.items()) for row in results], schema_type=schema_type)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]

    @overload
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def select_one(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Union[dict[str, Any], ModelDTOT]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        row = await connection.fetchrow(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        result = self.check_not_found(row)
        return self.to_schema(dict(result.items()), schema_type=schema_type)

    @overload
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[dict[str, Any]]": ...
    @overload
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "Optional[ModelDTOT]": ...
    async def select_one_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Fetch one row from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first row of the query results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(
            sql, parameters, *filters, **kwargs
        )  # Unpack 3 values
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        row = await connection.fetchrow(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        if row is None:
            return None
        return self.to_schema(dict(row.items()), schema_type=schema_type)

    @overload
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Any": ...
    @overload
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "T": ...
    async def select_value(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Union[T, Any]":
        """Fetch a single value from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(
            sql, parameters, *filters, **kwargs
        )  # Unpack 3 values
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        result = await connection.fetchval(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        result = self.check_not_found(result)
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    @overload
    async def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "Optional[Any]": ...
    @overload
    async def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[T]",
        **kwargs: Any,
    ) -> "Optional[T]": ...
    async def select_value_or_none(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[T]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[T, Any]]":
        """Fetch a single value from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The first value from the first row of results, or None if no results.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(
            sql, parameters, *filters, **kwargs
        )  # Unpack 3 values
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        result = await connection.fetchval(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        if result is None:
            return None
        if schema_type is None:
            return result
        return schema_type(result)  # type: ignore[call-arg]

    async def insert_update_delete(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        risk_level: Optional["RiskLevel"] = None,  # Ensure risk_level is here as per base
        **kwargs: Any,
    ) -> int:
        """Insert, update, or delete data from the database.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            **kwargs: Additional keyword arguments.

        Returns:
            Row count affected by the operation.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        status_str = await connection.execute(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        # asyncpg returns e.g. 'INSERT 0 1', 'UPDATE 0 2', etc.
        match = ROWCOUNT_REGEX.match(status_str)
        if match:
            return int(match.group(1))
        return 0

    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: None = None,
        **kwargs: Any,
    ) -> "dict[str, Any]": ...
    @overload
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "type[ModelDTOT]",
        **kwargs: Any,
    ) -> "ModelDTOT": ...
    async def insert_update_delete_returning(
        self,
        sql: "Statement",
        parameters: "Optional[StatementParameterType]" = None,
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        schema_type: "Optional[type[ModelDTOT]]" = None,
        **kwargs: Any,
    ) -> "Optional[Union[dict[str, Any], ModelDTOT]]":
        """Insert, update, or delete data from the database and return the affected row.

        Args:
            sql: SQL statement.
            parameters: Query parameters. Can be data or a StatementFilter.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            schema_type: Optional schema class for the result.
            **kwargs: Additional keyword arguments.

        Returns:
            The affected row data as either a model instance or dictionary.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(sql, parameters, *filters, **kwargs)
        db_params = (
            tuple(processed_params_for_adapter)
            if isinstance(processed_params_for_adapter, list)
            else processed_params_for_adapter
        )

        row = await connection.fetchrow(processed_sql, *(db_params or ()))  # Ensure db_params is not None
        if row is None:
            return None

        return self.to_schema(dict(row.items()), schema_type=schema_type)

    async def execute_many(
        self,
        sql: "Statement",
        parameters: "Sequence[StatementParameterType]",
        *filters: "StatementFilter",
        connection: "Optional[AsyncpgConnection]" = None,
        risk_level: Optional["RiskLevel"] = None,
        **kwargs: Any,
    ) -> "StatementResult[Any]":  # Changed ExecuteResult to StatementResult
        """Execute a SQL command against all parameter sequences or mappings.

        Args:
            sql: SQL statement.
            parameters: Sequence of parameter mappings or sequences.
            *filters: Statement filters to apply.
            connection: Optional connection to use.
            risk_level: Optional risk level for the operation.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        connection = self._connection(connection)
        # Process the base SQL and filters once to get the template and the master SQLStatement object
        # The parameters argument to _process_sql_params is None here because we only want the template.
        processed_sql_template, _, master_statement_obj = self._process_sql_params(sql, None, *filters, **kwargs)

        if not parameters:
            parameters = []  # Ensure parameters is a list

        # Process each parameter set using the master_statement_obj to ensure correct ordering for $n placeholders
        all_db_params = []
        for param_set in parameters:
            # Create a temporary statement with the individual param_set to get ordered params
            # We don't need to re-validate or re-transform the SQL template itself.
            _, temp_ordered_params, _ = SQLStatement(
                master_statement_obj.expression, param_set, dialect=self.dialect
            ).process()
            all_db_params.append(tuple(temp_ordered_params) if temp_ordered_params is not None else ())

        await connection.executemany(processed_sql_template, all_db_params)
        # asyncpg's executemany does not directly return total row count for all statements.
        # It's often used for INSERTs where individual row counts aren't the primary concern for the return.
        # For simplicity and consistency with other drivers that might not return accurate batch counts,
        # we'll return a generic ExecuteResult. If specific counts are needed, it might require multiple
        # execute calls or specific DB features.
        return ExecuteResult(raw_result=None, rows_affected=len(all_db_params), metadata={"dialect": self.dialect})

    async def execute_script(
        self,
        sql: "Statement",
        connection: "Optional[AsyncpgConnection]" = None,
        risk_level: Optional["RiskLevel"] = None,
        **kwargs: Any,
    ) -> "list[StatementResult[Any]]":
        """Execute a script.

        Args:
            sql: SQL statement.
            connection: Optional connection to use.
            risk_level: Optional risk level for the operation.
            **kwargs: Additional keyword arguments.

        Returns:
            Status message for the operation.
        """
        connection = self._connection(connection)
        processed_sql, processed_params_for_adapter, _ = self._process_sql_params(sql, **kwargs)
        processed_params_for_adapter = processed_params_for_adapter if processed_params_for_adapter is not None else ()
        return await connection.execute(processed_sql, *processed_params_for_adapter)  # pyright: ignore

    def _connection(self, connection: "Optional[AsyncpgConnection]" = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection

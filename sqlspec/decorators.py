from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, ClassVar, TypeVar

from sqlspec.utils.dataclass import DataclassProtocol

DataclassT = TypeVar("DataclassT", bound="DataclassProtocol")  # Generic type variable for a "mapped" statement
MappedStatementT = TypeVar(
    "MappedStatementT", bound="_MappedStatement"
)  # Generic type variable for a "mapped" statement

T = TypeVar("T", bound="MappedStatement[DataclassProtocol]")  # Generic type variable for a "mapped" statement


class SQLStatement:
    """Represents a SQL statement with an optional description.

    Attributes:
        value (str): The SQL statement string.
        description (str, optional): A description of the statement. Defaults to None.
    """

    def __init__(self, value: str, description: str | None = None):
        self.value = value
        self.description = description


class SelectStatement(SQLStatement):
    """Select Statement"""


class SelectOneStatement(SQLStatement):
    """Select One Statement"""


class DDLStatement(SQLStatement):
    """DDL Statement"""


class ExecuteStatement(SQLStatement):
    """Execute Statement"""


class ExecuteManyStatement(SQLStatement):
    """Execute Many Statement"""


class sql:  # Main decorator class  # noqa: N801
    """Decorator class to annotate a dataclass with SQL statements."""

    cache: dict[str, dict[str, MappedStatementT]] = {}

    def __new__(cls, query: str | MappedStatementT, statement_type: str = "sql") -> MappedStatementT:
        """Creates and caches an SQLStatement object.

        Args:
            query (str): The SQL statement string.
            statement_type (str, optional): The type of SQL statement
                                            ("sql", "select", "ddl").
                                            Defaults to "sql".

        Returns:
            SQLStatement: The created SQLStatement object.
        """
        if statement_type not in cls.cache:
            cls.cache[statement_type] = {}

        if query not in cls.cache[statement_type]:
            cls.cache[statement_type][query] = SQLStatement(query)
        return cls.cache[statement_type][query]

    @classmethod
    def select(cls, query: str | SelectStatement) -> Callable[[T], T]:
        """Decorator to add a SELECT statement to a dataclass.

        Args:
            query (str | SelectStatement): The SELECT statement string or SelectStatement object.

        Returns:
            Callable[[T], T]: A decorator function.
        """

        def decorator(cls: T) -> T:
            cls.__select = SelectStatement(query) if isinstance(query, str) else query  # type: ignore[attr-defined]
            return cls

        return decorator

    @classmethod
    def ddl(cls, query: str | DDLStatement) -> Callable[[T], T]:
        """Decorator to add a DDL statement to a dataclass.

        Args:
            query (str | DDLStatement): The DDL statement string or DDLStatement object.

        Returns:
            Callable[[T], T]: A decorator function.
        """

        def decorator(cls: T) -> T:
            cls.__ddl = DDLStatement(query) if isinstance(query, str) else query  # type: ignore[attr-defined]
            return cls

        return decorator


class _MappedStatement(DataclassProtocol):
    __select: ClassVar[SelectStatement | None]
    __select_one: ClassVar[SelectOneStatement | None]
    __execute: ClassVar[ExecuteStatement | None]
    __execute_many: ClassVar[ExecuteManyStatement | None]
    __ddl: ClassVar[DDLStatement | None]


class MappedStatement(_MappedStatement):
    """Base class for dataclasses that will be annotated with SQL statements."""

    __select: SelectStatement | None = None
    __select_one: SelectOneStatement | None = None
    __execute: ExecuteStatement | None = None
    __execute_many: ExecuteManyStatement | None = None
    __ddl: DDLStatement | None = None


# --- Example Usage ---


@sql.select(
    """
    SELECT
        pkey, dma_source_id, dma_manual_id, variable_category,
        variable_name, variable_value
    FROM
        some_mysql_table;
    """,
    "select tag_name from tags t join table_tags tt on t.id = tt.tag_id",
)
@dataclass
class CollectionMysqlConfig(MappedStatement):
    """Dataclass representing a MySQL configuration."""

    pkey: str
    dma_source_id: str
    dma_manual_id: str
    variable_category: str
    variable_name: str
    variable_value: str
    tags: list[str]

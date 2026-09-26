"""Recording stand-ins for arrow-odbc connections used by the Db2 unit tests.

The fakes mirror the public ``arrow_odbc.Connection`` surface: ``execute`` and
``read_arrow_batches`` take text parameters, ``read_arrow_batches`` returns a
batch reader exposing ``schema`` and ``into_pyarrow_record_batch_reader()``,
and the connection has no ``dbms_name`` or autocommit accessor.
"""

import re
from collections.abc import Callable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa

if TYPE_CHECKING:
    from sqlspec.adapters.arrow_odbc._typing import ArrowOdbcConnection

__all__ = (
    "DB2_CONNECTION_STRING",
    "DB2_TIMESTAMP",
    "EMPTY_RESULT",
    "ISO_TIMESTAMP",
    "MSSQL_CONNECTION_STRING",
    "FakeArrowOdbcConnection",
    "FakeOdbcError",
    "FakeReader",
    "Responder",
    "ScriptedResponder",
    "as_connection",
    "db2_error_message",
    "normalized_calls",
)

DB2_CONNECTION_STRING = "Driver={IBM DB2 ODBC DRIVER};Database=SAMPLE;"
MSSQL_CONNECTION_STRING = "Driver={ODBC Driver 18 for SQL Server};Server=h;Database=d;"
EMPTY_RESULT = pa.table({})

Responder = Callable[[str, "list[str | None] | None"], "pa.Table | None"]


class FakeOdbcError(Exception):
    """Constructible stand-in for ``arrow_odbc.Error``."""


def db2_error_message(sqlstate: str, native_error: int, detail: str = "Error condition") -> str:
    """Return an arrow-odbc diagnostic string in the IBM CLI driver's format."""
    return (
        f"ODBC emitted an error calling 'SQLExecDirect':\nState: {sqlstate}, Native error: {native_error}, "
        f"Message: [IBM][CLI Driver][DB2/LINUXX8664] {detail}"
    )


class FakeReader:
    """Batch reader exposing the arrow-odbc ``BatchReader`` surface."""

    def __init__(self, table: pa.Table, max_chunksize: int = 2) -> None:
        self._batches = table.to_batches(max_chunksize=max_chunksize)
        self.schema = table.schema

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return iter(self._batches)

    def into_pyarrow_record_batch_reader(self) -> pa.RecordBatchReader:
        return pa.RecordBatchReader.from_batches(self.schema, self._batches)


class FakeArrowOdbcConnection:
    """Connection that records every call and answers queries from a responder.

    Args:
        result: Table returned for any query the responder does not answer.
        responder: Optional callable receiving the SQL text and text parameters;
            a returned table answers ``read_arrow_batches``.
        error: Optional exception raised by ``execute`` and ``read_arrow_batches``.
    """

    def __init__(
        self, result: "pa.Table | None" = None, responder: "Responder | None" = None, error: "Exception | None" = None
    ) -> None:
        self.result = result if result is not None else pa.table({"id": [1, 2], "name": ["Ada", "Grace"]})
        self.responder = responder
        self.error = error
        self.calls: list[tuple[str, list[str | None] | None]] = []
        self.executed: list[tuple[str, list[str | None] | None]] = []
        self.read_calls: list[dict[str, Any]] = []
        self.inserts: list[tuple[str, str, int]] = []
        self.commit_calls = 0
        self.rollback_calls = 0
        self.closed = False

    @property
    def statements(self) -> list[str]:
        """SQL text of every ``execute`` and ``read_arrow_batches`` call, in order."""
        return [entry[0] for entry in self.calls]

    def read_arrow_batches(self, query: str, batch_size: int = 65535, **kwargs: Any) -> FakeReader:
        parameters = _text_parameters(kwargs.get("parameters"))
        self.read_calls.append({"query": query, "batch_size": batch_size, **kwargs})
        self.calls.append((query, parameters))
        if self.error is not None:
            raise self.error
        table = self.responder(query, parameters) if self.responder is not None else None
        return FakeReader(self.result if table is None else table)

    def execute(self, query: str, parameters: "Sequence[str | None] | None" = None, **_: Any) -> None:
        text_parameters = _text_parameters(parameters)
        self.executed.append((query, text_parameters))
        self.calls.append((query, text_parameters))
        if self.error is not None:
            raise self.error
        if self.responder is not None:
            self.responder(query, text_parameters)

    def insert_into_table(self, reader: Any, table: str, chunk_size: int) -> None:
        rows = sum(batch.num_rows for batch in reader)
        self.inserts.append(("insert_into_table", table, rows))

    def from_table_to_db(self, source: pa.Table, target: str, chunk_size: int = 1000) -> None:
        self.inserts.append(("from_table_to_db", target, source.num_rows))

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def close(self) -> None:
        self.closed = True


class ScriptedResponder:
    """Answer queries whose whitespace-normalized text contains a registered fragment.

    Args:
        rules: ``(fragment, table)`` pairs checked in order; the first fragment
            found in the query selects the table.
    """

    def __init__(self, *rules: "tuple[str, pa.Table]") -> None:
        self.rules = list(rules)

    def __call__(self, query: str, parameters: "list[str | None] | None") -> "pa.Table | None":
        normalized = " ".join(query.split())
        for fragment, table in self.rules:
            if fragment in normalized:
                return table
        return None


class _TimestampText:
    """Equality matcher for naive timestamp text with microseconds and a given date/time separator."""

    def __init__(self, separator: str, label: str) -> None:
        self._pattern = re.compile(rf"^\d{{4}}-\d{{2}}-\d{{2}}{separator}\d{{2}}:\d{{2}}:\d{{2}}\.\d{{6}}$")
        self._label = label

    def __eq__(self, other: object) -> bool:
        return isinstance(other, str) and self._pattern.match(other) is not None

    def __hash__(self) -> int:
        return 0

    def __repr__(self) -> str:
        return self._label


ISO_TIMESTAMP: Any = _TimestampText("T", "ISO_TIMESTAMP")
DB2_TIMESTAMP: Any = _TimestampText(" ", "DB2_TIMESTAMP")


def normalized_calls(connection: FakeArrowOdbcConnection) -> "list[tuple[str, list[str | None] | None]]":
    """Return the connection's statements with whitespace collapsed, paired with their parameters."""
    return [(" ".join(sql.split()), parameters) for sql, parameters in connection.calls]


def as_connection(connection: FakeArrowOdbcConnection) -> "ArrowOdbcConnection":
    """Type a fake connection as the arrow-odbc connection type."""
    return cast("ArrowOdbcConnection", connection)


def _text_parameters(parameters: "Sequence[str | None] | None") -> "list[str | None] | None":
    if parameters is None:
        return None
    for value in parameters:
        if value is not None and not isinstance(value, str):
            msg = f"arrow-odbc binds text parameters only, got {type(value).__name__}"
            raise TypeError(msg)
    return list(parameters)

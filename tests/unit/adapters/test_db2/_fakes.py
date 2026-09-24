"""In-memory stand-ins for ``ibm_db`` and ``ibm_db_dbi`` that follow the real driver's behavior.

The fakes reproduce the ``ibm_db_dbi`` 3.3 semantics the Db2 adapter depends on:

- ``connect()`` defaults ``SQL_ATTR_AUTOCOMMIT`` to ``SQL_AUTOCOMMIT_OFF`` and appends ``UID``/``PWD``
  keywords to the DSN when credentials are passed separately.
- Connections expose ``conn_handler`` and ``set_autocommit()`` but no autocommit getter; the mode is
  read through ``ibm_db.autocommit(conn_handler)``.
- ``Connection.close()`` rolls back pending work before closing.
- Cursor descriptions carry column names exactly as Db2 reports them (see ``db2_description``).
- Errors are ``Error`` subclasses whose text embeds the CLI diagnostic, SQLSTATE and SQLCODE.
"""

from collections.abc import Callable, Sequence
from typing import Any

SQL_ATTR_AUTOCOMMIT = 102
SQL_AUTOCOMMIT_ON = 1
SQL_AUTOCOMMIT_OFF = 0
_READ_ONLY_KEYWORDS = ("SELECT", "WITH", "VALUES")


class FakeDb2Error(Exception):
    """Base driver error rendered the way ``ibm_db_dbi.Error`` renders itself."""

    dbi_name = "Error"

    def __init__(self, message: str) -> None:
        self._message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f"ibm_db_dbi::{self.dbi_name}: {self._message}"


class FakeDb2DatabaseError(FakeDb2Error):
    """Stand-in for ``ibm_db_dbi.DatabaseError``."""

    dbi_name = "DatabaseError"


class FakeDb2IntegrityError(FakeDb2DatabaseError):
    """Stand-in for ``ibm_db_dbi.IntegrityError``."""

    dbi_name = "IntegrityError"


class FakeDb2OperationalError(FakeDb2DatabaseError):
    """Stand-in for ``ibm_db_dbi.OperationalError``."""

    dbi_name = "OperationalError"


class FakeDb2ProgrammingError(FakeDb2DatabaseError):
    """Stand-in for ``ibm_db_dbi.ProgrammingError``."""

    dbi_name = "ProgrammingError"


class FakeDb2DataError(FakeDb2DatabaseError):
    """Stand-in for ``ibm_db_dbi.DataError``."""

    dbi_name = "DataError"


class DiagnosticAttributeError(Exception):
    """Non-driver exception that exposes Db2 diagnostics as attributes instead of message text."""

    def __init__(self, message: str, sqlstate: str | None = None, error_code: "str | int | None" = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.error_code = error_code


def db2_error(
    sqlcode: int,
    sqlstate: str,
    text: str,
    cls: "type[FakeDb2Error]" = FakeDb2ProgrammingError,
    reason: "int | None" = None,
) -> FakeDb2Error:
    """Build a driver error carrying a CLI-formatted Db2 diagnostic.

    Args:
        sqlcode: Negative SQLCODE reported by Db2, e.g. ``-803``.
        sqlstate: Five-character SQLSTATE, e.g. ``"23505"``.
        text: Diagnostic text following the message identifier.
        cls: Fake error class to instantiate.
        reason: Optional reason code appended as ``Reason code "<n>".``.

    Returns:
        The error instance, ready to raise.
    """
    suffix = "C" if sqlcode == -1042 else "N"
    reason_part = f'  Reason code "{reason}".' if reason is not None else ""
    return cls(
        f"[IBM][CLI Driver][DB2/LINUXX8664] SQL{abs(sqlcode):04d}{suffix}  {text}{reason_part} "
        f"SQLSTATE={sqlstate} SQLCODE={sqlcode}"
    )


def db2_description(*names: str) -> "list[tuple[Any, ...]]":
    """Build a cursor description the way Db2 reports column names.

    Unquoted names fold to uppercase; names wrapped in double quotes keep their exact case.

    Args:
        *names: Column names as written in the SELECT list.

    Returns:
        Seven-item DB-API description tuples.
    """
    folded = [name[1:-1] if len(name) > 1 and name[0] == name[-1] == '"' else name.upper() for name in names]
    return [(name, None, None, None, None, None, True) for name in folded]


class FakeDb2Cursor:
    """Scripted cursor that records every statement it receives."""

    def __init__(
        self,
        rows: "Sequence[Any] | None" = None,
        description: "Sequence[tuple[Any, ...]] | None" = None,
        rowcount: int = -1,
        error: "BaseException | None" = None,
    ) -> None:
        self.rows = list(rows) if rows is not None else []
        self.description = description
        self.rowcount = rowcount
        self.error = error
        self.executed: list[tuple[str, Any]] = []
        self.closed = False
        self.connection: FakeDb2Connection | None = None

    def execute(self, sql: str, params: object = None) -> "FakeDb2Cursor":
        """Record the statement, raise the scripted error, and track writes on the owning connection.

        Returns:
            The cursor itself.
        """
        self.executed.append((sql, params))
        if self.error is not None:
            raise self.error
        self._track_write(sql, params)
        return self

    def executemany(self, sql: str, seq: "Sequence[Any]") -> "FakeDb2Cursor":
        """Record a batch statement, raise the scripted error, and track writes on the owning connection.

        Returns:
            The cursor itself.
        """
        self.executed.append((sql, seq))
        if self.error is not None:
            raise self.error
        self._track_write(sql, seq)
        return self

    def fetchone(self) -> Any:
        """Return and consume the next scripted row, or ``None`` when exhausted."""
        return self.rows.pop(0) if self.rows else None

    def fetchmany(self, size: int = 1) -> "list[Any]":
        """Return and consume up to ``size`` scripted rows."""
        chunk = self.rows[:size]
        self.rows = self.rows[size:]
        return chunk

    def fetchall(self) -> "list[Any]":
        """Return the remaining scripted rows."""
        return list(self.rows)

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True

    def _track_write(self, sql: str, params: object) -> None:
        if self.connection is None or sql.lstrip().upper().startswith(_READ_ONLY_KEYWORDS):
            return
        target = self.connection.committed if self.connection.autocommit else self.connection.pending
        target.append((sql, params))


class FakeDb2Connection:
    """Connection that follows ``ibm_db_dbi.Connection`` transaction semantics.

    Writes executed while autocommit is off stay in ``pending`` until ``commit()`` moves them to
    ``committed``; ``rollback()`` discards them. ``close()`` rolls back before closing.
    """

    def __init__(
        self, cursors: "Sequence[FakeDb2Cursor] | Callable[[], FakeDb2Cursor] | None" = None, autocommit: bool = False
    ) -> None:
        self.conn_handler = object()
        self.dbms_name = "DB2/LINUXX8664"
        self.autocommit = autocommit
        self.dsn: str | None = None
        self.pending: list[tuple[str, object]] = []
        self.committed: list[tuple[str, object]] = []
        self.rollbacks = 0
        self.commits = 0
        self.closed = False
        self.cursors: list[FakeDb2Cursor] = []
        self._cursor_factory = cursors if callable(cursors) else None
        self._scripted = list(cursors) if cursors is not None and not callable(cursors) else []

    def cursor(self) -> FakeDb2Cursor:
        """Return the next scripted cursor, falling back to an empty cursor.

        Returns:
            The cursor bound to this connection.
        """
        if self._cursor_factory is not None:
            cursor = self._cursor_factory()
        elif self._scripted:
            cursor = self._scripted.pop(0)
        else:
            cursor = FakeDb2Cursor()
        cursor.connection = self
        self.cursors.append(cursor)
        return cursor

    def set_autocommit(self, is_on: bool) -> None:
        """Switch the connection's autocommit mode."""
        self.autocommit = bool(is_on)

    def commit(self) -> None:
        """Make pending writes durable."""
        self.commits += 1
        self.committed.extend(self.pending)
        self.pending.clear()

    def rollback(self) -> None:
        """Discard pending writes."""
        self.rollbacks += 1
        self.pending.clear()

    def close(self) -> None:
        """Roll back pending writes, then close."""
        self.rollback()
        self.closed = True


class FakeIbmDbModule:
    """Stand-in for the ``ibm_db`` extension module's autocommit accessor."""

    SQL_ATTR_AUTOCOMMIT = SQL_ATTR_AUTOCOMMIT
    SQL_AUTOCOMMIT_ON = SQL_AUTOCOMMIT_ON
    SQL_AUTOCOMMIT_OFF = SQL_AUTOCOMMIT_OFF

    def __init__(self) -> None:
        self.connections: dict[object, FakeDb2Connection] = {}

    def register(self, connection: FakeDb2Connection) -> None:
        """Associate a connection's ``conn_handler`` with the connection."""
        self.connections[connection.conn_handler] = connection

    def autocommit(self, handle: object, value: "int | None" = None) -> "int | bool":
        """Read the autocommit mode, or set it when ``value`` is given.

        Returns:
            ``1``/``0`` for reads; ``True`` after a write.
        """
        connection = self.connections[handle]
        if value is None:
            return SQL_AUTOCOMMIT_ON if connection.autocommit else SQL_AUTOCOMMIT_OFF
        connection.autocommit = value == SQL_AUTOCOMMIT_ON
        return True


class FakeIbmDbDbiModule:
    """Stand-in for the ``ibm_db_dbi`` module.

    ``connect()`` returns the next connection from ``pending_connections`` or a new one, applying
    the autocommit mode from ``conn_options`` and registering it with the paired ``ibm_db`` fake.
    """

    SQL_ATTR_AUTOCOMMIT = SQL_ATTR_AUTOCOMMIT
    SQL_AUTOCOMMIT_ON = SQL_AUTOCOMMIT_ON
    SQL_AUTOCOMMIT_OFF = SQL_AUTOCOMMIT_OFF
    Error = FakeDb2Error
    DatabaseError = FakeDb2DatabaseError
    IntegrityError = FakeDb2IntegrityError
    OperationalError = FakeDb2OperationalError
    ProgrammingError = FakeDb2ProgrammingError
    DataError = FakeDb2DataError

    def __init__(self, ibm_db: "FakeIbmDbModule | None" = None) -> None:
        self.ibm_db = ibm_db or FakeIbmDbModule()
        self.pending_connections: list[FakeDb2Connection] = []
        self.connect_calls: list[tuple[str, str, str, str, str, dict[int, int] | None]] = []

    def connect(
        self,
        dsn: str,
        user: str = "",
        password: str = "",
        host: str = "",
        database: str = "",
        conn_options: "dict[int, int] | None" = None,
    ) -> FakeDb2Connection:
        """Open a fake connection with the real driver's argument handling.

        Returns:
            The connection, with autocommit taken from ``conn_options`` (off by default).
        """
        self.connect_calls.append((dsn, user, password, host, database, conn_options))
        options = dict(conn_options) if conn_options is not None else {}
        options.setdefault(SQL_ATTR_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF)
        effective_dsn = dsn
        if user and "UID=" not in effective_dsn:
            effective_dsn = f"{effective_dsn}UID={user};"
        if password and "PWD=" not in effective_dsn:
            effective_dsn = f"{effective_dsn}PWD={password};"
        connection = self.pending_connections.pop(0) if self.pending_connections else FakeDb2Connection()
        connection.autocommit = options[SQL_ATTR_AUTOCOMMIT] == SQL_AUTOCOMMIT_ON
        connection.dsn = effective_dsn
        self.ibm_db.register(connection)
        return connection

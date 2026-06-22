"""DuckDB connection pool with thread-local connections."""

import logging
import re
import threading
import time
import uuid
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, Final, cast

import duckdb
from typing_extensions import final

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

__all__ = ("DuckDBConnectionPool",)


_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
_EXPLICIT_INSTALL_KEYS: Final[tuple[str, ...]] = ("version", "repository", "repository_url")


logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "duckdb"

DEFAULT_MIN_POOL: Final[int] = 1
DEFAULT_MAX_POOL: Final[int] = 4
POOL_TIMEOUT: Final[float] = 30.0
POOL_RECYCLE: Final[int] = 86400
HEALTH_CHECK_INTERVAL: Final[float] = 30.0


@final
class DuckDBConnectionPool:
    """Thread-local connection manager for DuckDB.

    Uses thread-local storage to ensure each thread gets its own DuckDB connection,
    preventing the thread-safety issues that cause segmentation faults when
    multiple cursors share the same connection concurrently.

    This design trades traditional pooling for thread safety, which is essential
    for DuckDB since connections and cursors are not thread-safe.
    """

    __slots__ = (
        "_connection_config",
        "_extension_flags",
        "_extensions",
        "_health_check_interval",
        "_installed_signatures",
        "_is_memory_db",
        "_lock",
        "_on_connection_create",
        "_pool_id",
        "_recycle",
        "_secrets",
        "_thread_local",
    )

    def __init__(
        self,
        connection_config: "dict[str, Any]",
        pool_recycle_seconds: int = POOL_RECYCLE,
        health_check_interval: float = HEALTH_CHECK_INTERVAL,
        extensions: "list[dict[str, Any]] | None" = None,
        extension_flags: "dict[str, Any] | None" = None,
        secrets: "list[dict[str, Any]] | None" = None,
        on_connection_create: "Callable[[DuckDBConnection], DuckDBConnection | None] | None" = None,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_config: DuckDB connection configuration
            pool_recycle_seconds: Connection recycle time in seconds
            health_check_interval: Seconds of idle time before running health check
            extensions: List of extensions to install/load
            extension_flags: Connection-level SET statements applied after creation
            secrets: List of secrets to create
            on_connection_create: Callback executed when connection is created
        """
        self._connection_config = connection_config
        self._recycle = pool_recycle_seconds
        self._health_check_interval = health_check_interval
        self._extensions = extensions or []
        self._extension_flags = extension_flags or {}
        self._secrets = secrets or []
        self._on_connection_create = on_connection_create
        self._installed_signatures: set[tuple[Any, ...]] = set()
        self._thread_local = threading.local()
        self._lock = threading.RLock()
        self._pool_id = str(uuid.uuid4())[:8]
        # Track if this pool uses an in-memory database
        # In-memory databases require connections to stay alive to preserve data
        database = connection_config.get("database", "")
        self._is_memory_db = database.startswith(":memory:") or database == ""

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        db = self._connection_config.get("database", "")
        if db.startswith(":memory:") or db == "":
            return ":memory:"
        return str(db)

    def _create_connection(self) -> DuckDBConnection:
        """Create a new DuckDB connection with extensions and secrets."""
        connect_parameters = {}
        config_dict = {}

        for key, value in self._connection_config.items():
            if key in {"database", "read_only"}:
                connect_parameters[key] = value
            elif key == "config" and isinstance(value, dict):
                config_dict.update(value)
            else:
                config_dict[key] = value

        if config_dict:
            connect_parameters["config"] = config_dict

        connection = duckdb.connect(**connect_parameters)

        self._apply_extension_flags(connection)

        for ext_config in self._extensions:
            ext_name = ext_config.get("name")
            if not ext_name:
                continue

            required = bool(ext_config.get("required", False))
            install_kwargs: dict[str, Any] = {k: ext_config[k] for k in _EXPLICIT_INSTALL_KEYS if k in ext_config}
            force_install = bool(ext_config.get("force_install", False))
            explicit_install = bool(ext_config.get("install", False)) or force_install or bool(install_kwargs)

            if explicit_install:
                self._install_extension_once(connection, ext_name, install_kwargs, force_install, required)

            try:
                connection.load_extension(ext_name)
            except Exception as exc:
                if required:
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "pool.extension.load.failed",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    database=self._database_name,
                    extension=ext_name,
                    error=str(exc),
                )

        for secret_config in self._secrets:
            _create_secret(connection, secret_config)

        if self._on_connection_create:
            # Let a failing user hook surface its real error instead of silently returning a
            # half-configured connection (mirrors the sqlite/aiosqlite pools).
            self._on_connection_create(connection)

        return connection

    def _install_extension_once(
        self,
        connection: "DuckDBConnection",
        ext_name: str,
        install_kwargs: "dict[str, Any]",
        force_install: bool,
        required: bool,
    ) -> None:
        """Install an extension once per pool per signature, best-effort unless required."""
        signature = (
            ext_name,
            install_kwargs.get("version"),
            install_kwargs.get("repository"),
            install_kwargs.get("repository_url"),
        )
        with self._lock:
            if not force_install and signature in self._installed_signatures:
                return
            try:
                if force_install:
                    connection.install_extension(ext_name, force_install=True, **install_kwargs)
                elif install_kwargs:
                    connection.install_extension(ext_name, **install_kwargs)
                else:
                    connection.install_extension(ext_name)
            except Exception as exc:
                if required:
                    raise
                log_with_context(
                    logger,
                    logging.WARNING,
                    "pool.extension.install.failed",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    database=self._database_name,
                    extension=ext_name,
                    error=str(exc),
                )
                return
            self._installed_signatures.add(signature)

    def _apply_extension_flags(self, connection: DuckDBConnection) -> None:
        """Apply connection-level extension flags via SET statements."""

        if not self._extension_flags:
            return

        for key, value in self._extension_flags.items():
            if not key or not key.replace("_", "").isalnum():
                continue

            normalized = self._normalize_flag_value(value)
            try:
                connection.execute(f"SET {key} = {normalized}")
            except Exception as exc:  # pragma: no cover
                log_with_context(
                    logger,
                    logging.DEBUG,
                    "pool.flag.set.failed",
                    adapter=_ADAPTER_NAME,
                    pool_id=self._pool_id,
                    database=self._database_name,
                    flag=key,
                    error=str(exc),
                )

    @staticmethod
    def _normalize_flag_value(value: Any) -> str:
        """Convert Python value to DuckDB SET literal."""

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def _get_thread_connection(self) -> DuckDBConnection:
        """Get or create a connection for the current thread.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.
        """
        thread_state = self._thread_local.__dict__
        if "connection" not in thread_state:
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("DuckDBConnection", self._thread_local.connection)

        if self._recycle > 0 and time.time() - self._thread_local.created_at > self._recycle:
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()
            self._thread_local.last_used = time.time()
            return cast("DuckDBConnection", self._thread_local.connection)

        idle_time = time.time() - thread_state.get("last_used", 0)
        if idle_time > self._health_check_interval and not self._is_connection_alive(self._thread_local.connection):
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.recycle",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                idle_seconds=round(idle_time, 1),
                reason="failed_health_check",
            )
            with suppress(Exception):
                self._thread_local.connection.close()
            self._thread_local.connection = self._create_connection()
            self._thread_local.created_at = time.time()

        self._thread_local.last_used = time.time()
        return cast("DuckDBConnection", self._thread_local.connection)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread."""
        thread_state = self._thread_local.__dict__
        if "connection" in thread_state:
            with suppress(Exception):
                self._thread_local.connection.close()
            del self._thread_local.connection
            if "created_at" in thread_state:
                del self._thread_local.created_at
            if "last_used" in thread_state:
                del self._thread_local.last_used

    def _is_connection_alive(self, connection: DuckDBConnection) -> bool:
        """Check if a connection is still alive and usable.

        Args:
            connection: Connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            cursor = connection.cursor()
            cursor.close()
        except Exception:
            return False
        return True

    @contextmanager
    def get_connection(self) -> "Generator[DuckDBConnection, None, None]":
        """Get a thread-local connection.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.

        For file-based databases, the connection is closed when the context
        manager exits to release DuckDB's file lock, allowing subsequent
        connections with different configurations.

        For in-memory databases, connections are kept alive to preserve data,
        as in-memory data is lost when the last connection closes.

        Yields:
            DuckDBConnection: A thread-local connection.
        """
        connection = self._get_thread_connection()
        try:
            yield connection
        except Exception:
            self._close_thread_connection()
            raise
        else:
            # Only close connection for file-based databases to release file locks
            # In-memory databases need connections to stay alive to preserve data
            if not self._is_memory_db:
                self._close_thread_connection()

    def close(self) -> None:
        """Close the thread-local connection if it exists."""
        self._close_thread_connection()

    def size(self) -> int:
        """Get current pool size (always 1 for thread-local)."""
        return 1 if "connection" in self._thread_local.__dict__ else 0

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0 for thread-local)."""
        return 0

    def acquire(self) -> DuckDBConnection:
        """Acquire a thread-local connection.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.

        Returns:
            DuckDBConnection: A thread-local connection
        """
        return self._get_thread_connection()

    def release(self, connection: DuckDBConnection) -> None:
        """Release a connection (no-op for thread-local connections).

        Args:
            connection: The connection to release (ignored)
        """


def _validate_sql_identifier(value: str, field_name: str) -> None:
    """Raise ValueError if value is not safe to interpolate as a SQL identifier."""
    if not _SQL_IDENTIFIER_RE.fullmatch(value):
        msg = (
            f"Invalid SQL identifier for {field_name!r}: {value!r}. "
            "Must start with a letter and contain only letters, digits, and underscores."
        )
        raise ValueError(msg)


def _create_secret(connection: DuckDBConnection, secret_config: dict[str, Any]) -> None:
    secret_name = secret_config.get("name")
    secret_type = secret_config.get("secret_type")
    if not (secret_name and secret_type):
        return

    _validate_sql_identifier(secret_name, "secret_name")
    _validate_sql_identifier(secret_type, "secret_type")

    sql = _build_secret_sql(secret_config, secret_name, secret_type)
    connection.execute(sql)
    _verify_secret(connection, secret_config, secret_name, secret_type)


def _build_secret_sql(secret_config: dict[str, Any], secret_name: str, secret_type: str) -> str:
    parts = [f"TYPE {secret_type}"]

    provider = secret_config.get("provider")
    if provider:
        _validate_sql_identifier(str(provider), "secret_provider")
        parts.append(f"PROVIDER {provider}")

    secret_value = secret_config.get("value") or {}
    if not isinstance(secret_value, dict):
        msg = "DuckDB secret value must be a dictionary"
        raise TypeError(msg)

    for key, value in secret_value.items():
        parts.append(f"{_format_secret_key(key)} {_format_secret_literal(value)}")

    scope = secret_config.get("scope")
    if scope is not None:
        parts.append(f"SCOPE {_format_secret_literal(scope)}")

    create = "CREATE PERSISTENT SECRET" if secret_config.get("persistent", False) else "CREATE SECRET"
    body = ",\n    ".join(parts)
    return f"{create} {secret_name} (\n    {body}\n)"


def _format_secret_key(key: Any) -> str:
    key_text = str(key)
    _validate_sql_identifier(key_text, "secret_value_key")
    return key_text.upper()


def _format_secret_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _verify_secret(
    connection: DuckDBConnection, secret_config: dict[str, Any], secret_name: str, secret_type: str
) -> None:
    row = connection.execute(
        "SELECT name, type, scope, persistent FROM duckdb_secrets() WHERE name = ?", (secret_name,)
    ).fetchone()
    if not row:
        msg = f"DuckDB secret {secret_name!r} was not visible after creation"
        raise RuntimeError(msg)

    actual_name, actual_type, actual_scope, actual_persistent = row
    expected_persistent = bool(secret_config.get("persistent", False))
    scope = secret_config.get("scope")
    scopes = list(actual_scope or [])
    if (
        actual_name != secret_name
        or str(actual_type).lower() != secret_type.lower()
        or bool(actual_persistent) != expected_persistent
        or (scope is not None and scope not in scopes)
    ):
        msg = f"DuckDB secret {secret_name!r} verification failed"
        raise RuntimeError(msg)

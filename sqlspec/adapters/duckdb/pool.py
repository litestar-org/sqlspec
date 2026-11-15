"""DuckDB connection pool with thread-local connections."""

import asyncio
import logging
import math
import re
import threading
import time
from contextlib import contextmanager, suppress
from functools import partial
from typing import TYPE_CHECKING, Any, Final, cast, final
from urllib.parse import urlsplit, urlunsplit
from weakref import WeakKeyDictionary

from mypy_extensions import mypyc_attr

from sqlspec.adapters.duckdb._typing import DuckDBConnection
from sqlspec.adapters.duckdb._typing import duckdb_module as duckdb
from sqlspec.adapters.duckdb.core import (
    cleanup_duckdb_profile_path,
    configure_duckdb_statement_profiling,
    create_duckdb_profile_path,
    create_mapped_exception,
)
from sqlspec.exceptions import ImproperConfigurationError
from sqlspec.utils.logging import POOL_LOGGER_NAME, get_logger, log_with_context
from sqlspec.utils.uuids import uuid4

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from pathlib import Path

__all__ = ("DuckDBConnectionPool",)


_SQL_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z][A-Za-z0-9_]*\Z")
_EXPLICIT_INSTALL_KEYS: Final[tuple[str, ...]] = ("version", "repository", "repository_url")
_DUCKDB_PROFILING_MODES: Final[frozenset[str]] = frozenset({"json", "query_tree", "no_output", "html", "graphviz"})


logger = get_logger(POOL_LOGGER_NAME)
_ADAPTER_NAME = "duckdb"

POOL_RECYCLE: Final[int] = 86400
HEALTH_CHECK_INTERVAL: Final[float] = 30.0
SECRET_CONFLICT_ATTEMPTS: Final[int] = 5
SECRET_CONFLICT_BACKOFF: Final[float] = 0.005
_MAX_IDLE_TASK_CONNECTIONS: Final[int] = 8
_TASK_STORAGE_STATE_KEYS: Final[tuple[str, ...]] = (
    "storage_extensions",
    "storage_secrets",
    "secret_refresh_times",
    "storage_protocols",
    "profile_path",
    "_cached_storage_settings",
)
_SECRET_LOCK: Final = threading.Lock()
_SECRET_VISIBLE_SETTINGS: Final[frozenset[str]] = frozenset({
    "endpoint",
    "http_proxy",
    "http_proxy_username",
    "key_id",
    "region",
    "url_style",
    "use_ssl",
})
_SECRET_BOOLEAN_SETTINGS: Final[frozenset[str]] = frozenset({"use_ssl"})
_SECRET_BOOLEAN_TEXT: Final[dict[str, str]] = {
    "true": "true",
    "t": "true",
    "yes": "true",
    "y": "true",
    "1": "true",
    "false": "false",
    "f": "false",
    "no": "false",
    "n": "false",
    "0": "false",
}


@final
@mypyc_attr(allow_interpreted_subclasses=False)
class DuckDBConnectionPool:
    """Thread-local connection manager for DuckDB.

    Isolates DuckDB connections per thread using thread-local storage because native
    DuckDB connections and cursors are not safe for concurrent multi-threaded sharing.
    """

    __slots__ = (
        "_baseline_databases",
        "_connection_config",
        "_connection_registry",
        "_enable_statement_profiling",
        "_extensions",
        "_generation",
        "_has_dynamic_secrets",
        "_health_check_interval",
        "_idle_task_holders",
        "_installed_signatures",
        "_is_memory_db",
        "_lock",
        "_on_connection_create",
        "_pool_id",
        "_profile_paths",
        "_recycle",
        "_reset_session_on_release",
        "_secrets",
        "_thread_local",
    )

    def __init__(
        self,
        connection_config: "dict[str, Any]",
        pool_recycle_seconds: int = POOL_RECYCLE,
        health_check_interval: float = HEALTH_CHECK_INTERVAL,
        extensions: "list[dict[str, Any]] | None" = None,
        secrets: "list[dict[str, Any]] | None" = None,
        on_connection_create: "Callable[[DuckDBConnection], DuckDBConnection | None] | None" = None,
        enable_statement_profiling: bool = False,
        reset_session_on_release: bool = False,
    ) -> None:
        """Initialize the thread-local connection manager.

        Args:
            connection_config: DuckDB connection configuration
            pool_recycle_seconds: Connection recycle time in seconds
            health_check_interval: Seconds of idle time before running health check
            extensions: List of extensions to install/load
            secrets: List of secrets to create
            on_connection_create: Callback executed when connection is created
            enable_statement_profiling: Whether to enable per-connection JSON statement profiling
            reset_session_on_release: Drop temporary objects and detach added databases when a connection is
                released.
        """
        self._connection_config = connection_config
        self._recycle = pool_recycle_seconds
        self._health_check_interval = health_check_interval
        self._extensions = extensions or []
        self._secrets = secrets or []
        self._has_dynamic_secrets: bool = any(callable(secret.get("value_provider")) for secret in self._secrets)
        self._on_connection_create = on_connection_create
        self._enable_statement_profiling = enable_statement_profiling
        self._reset_session_on_release = reset_session_on_release
        self._baseline_databases: dict[int, tuple[frozenset[str], str]] = {}
        self._profile_paths: dict[int, Path] = {}
        self._installed_signatures: set[tuple[Any, ...]] = set()
        self._thread_local = threading.local()
        self._connection_registry: set[DuckDBConnection] = set()
        self._idle_task_holders: list[dict[str, Any]] = []
        self._generation = 0
        self._lock = threading.RLock()
        self._pool_id = str(uuid4())[:8]
        database = connection_config.get("database", "")
        self._is_memory_db = database.startswith(":memory:") or database == ""

    @property
    def _database_name(self) -> str:
        """Get sanitized database name for logging."""
        return _sanitize_database_uri(self._connection_config.get("database", ""))

    def new_connection(self, record_thread_state: bool = False) -> DuckDBConnection:
        """Open a standalone connection with extensions and secrets applied.

        Unless the pool asks it to, the result is owned by the caller: it is not
        thread-local, is not tracked for pool shutdown, and leaves the storage
        setup already published for this thread untouched.

        Args:
            record_thread_state: Whether to publish storage setup for this thread.

        Returns:
            DuckDBConnection: A newly opened, fully configured connection.
        """
        loaded_extensions: set[str] = set()
        storage_protocols: set[str] = set()
        connect_parameters = {}
        config_dict = {}

        for key, value in self._connection_config.items():
            if key in {"database", "read_only"}:
                connect_parameters[key] = value
            elif key == "config" and isinstance(value, dict):
                config_dict.update(value)
            else:
                config_dict[key] = value

        connection, deferred_parquet_metadata_cache = _open_duckdb_connection(
            connect_parameters, config_dict, has_extensions=bool(self._extensions)
        )

        for ext_config in self._extensions:
            ext_name = ext_config.get("name")
            if not ext_name:
                continue

            required = bool(ext_config.get("required", False))
            install_kwargs: dict[str, Any] = {k: ext_config[k] for k in _EXPLICIT_INSTALL_KEYS if k in ext_config}
            force_install = bool(ext_config.get("force_install", False))
            explicit_install = bool(ext_config.get("install", False)) or force_install or bool(install_kwargs)

            install_error: str | None = None
            if explicit_install:
                install_error = self._install_extension_once(
                    connection, ext_name, install_kwargs, force_install, required
                )

            try:
                connection.load_extension(ext_name)
            except Exception as exc:
                if required:
                    raise
                if explicit_install:
                    log_with_context(
                        logger,
                        logging.WARNING,
                        "pool.extension.setup.failed",
                        adapter=_ADAPTER_NAME,
                        pool_id=self._pool_id,
                        database=self._database_name,
                        extension=ext_name,
                        install_error=install_error,
                        load_error=str(exc),
                    )
                else:
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
            else:
                loaded_extensions.add(ext_name)
                storage_protocols.update(ext_config.get("storage_protocols", ()))
                if ext_name == "gcs":
                    storage_protocols.add("gcss")
                if install_error is not None:
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        "pool.extension.install.failed",
                        adapter=_ADAPTER_NAME,
                        pool_id=self._pool_id,
                        database=self._database_name,
                        extension=ext_name,
                        error=install_error,
                    )

        if deferred_parquet_metadata_cache is not None:
            with suppress(Exception):
                connection.execute(
                    f"SET parquet_metadata_cache = {'true' if deferred_parquet_metadata_cache else 'false'}"
                )

        created_secrets: list[dict[str, Any]] = []
        secret_refresh_times: dict[str, float] = {}
        now_monotonic = time.monotonic()
        for secret_config in self._secrets:
            resolved_secret = _resolve_secret_config(secret_config)
            if _create_secret(connection, resolved_secret):
                created_secrets.append({**resolved_secret, "value": dict(resolved_secret.get("value") or {})})
                if callable(secret_config.get("value_provider")) and resolved_secret.get("name"):
                    secret_refresh_times[str(resolved_secret["name"])] = now_monotonic

        if self._on_connection_create:
            modified = self._on_connection_create(connection)
            if modified is not None:
                connection = modified
        profile_path: Path | None = None
        if self._enable_statement_profiling:
            profile_path = create_duckdb_profile_path()
            configure_duckdb_statement_profiling(connection, profile_path)
            with self._lock:
                self._profile_paths[id(connection)] = profile_path
        storage_protocols.update(connection.list_filesystems())
        if record_thread_state and self._reset_session_on_release:
            baseline = _database_baseline(connection)
            with self._lock:
                self._baseline_databases[id(connection)] = baseline
        if record_thread_state:
            state = self._get_or_create_connection_holder()
            state["storage_extensions"] = frozenset(loaded_extensions)
            state["storage_secrets"] = tuple(created_secrets)
            state["secret_refresh_times"] = secret_refresh_times
            state["storage_protocols"] = frozenset(storage_protocols)
            state["profile_path"] = profile_path
            state.pop("_cached_storage_settings", None)
        return connection

    @contextmanager
    def get_connection(self) -> "Generator[DuckDBConnection, None, None]":
        """Get a thread-local or task-local connection.

        Each thread (and each concurrent asyncio task running on an event loop thread)
        gets its own dedicated DuckDB connection to prevent concurrent cursor and
        transaction sharing.

        Uncommitted work is rolled back when the outermost checkout exits normally.
        Re-entrant checkouts on the same thread or task reuse the active connection
        without committing, rolling back, or closing on inner exits.

        Yields:
            DuckDBConnection: A thread- or task-isolated connection.
        """
        connection = self._get_thread_connection()
        state = self._get_or_create_connection_holder()
        depth = int(state.get("depth", 0))
        state["depth"] = depth + 1
        try:
            yield connection
        except BaseException:
            if depth == 0:
                with suppress(Exception):
                    connection.rollback()
                if not self._is_memory_db:
                    self._close_thread_connection()
            raise
        else:
            if depth == 0:
                try:
                    connection.rollback()
                except Exception as exc:
                    if "no transaction is active" not in str(exc).lower():
                        if not self._is_memory_db:
                            self._close_thread_connection()
                        raise
                self._reset_connection_or_retire(connection)
        finally:
            state["depth"] = depth

    def close(self) -> None:
        """Close every connection this pool opened, on any thread."""
        self._close_thread_connection()
        task_states = cast(
            "WeakKeyDictionary[asyncio.Task[Any], dict[str, Any]] | None",
            self._thread_local.__dict__.pop("_task_holders", None),
        )
        if task_states is not None:
            task_states.clear()
        with self._lock:
            idle_states = list(self._idle_task_holders)
            self._idle_task_holders.clear()
            orphaned = set(self._connection_registry)
            self._connection_registry.clear()
            profile_paths = list(self._profile_paths.values())
            self._profile_paths.clear()
            self._generation += 1
        for state in idle_states:
            idle_connection = cast("DuckDBConnection | None", state.get("connection"))
            if idle_connection is not None:
                orphaned.add(idle_connection)
        for connection in orphaned:
            with suppress(Exception):
                connection.close()
        for profile_path in profile_paths:
            cleanup_duckdb_profile_path(profile_path)

    def size(self) -> int:
        """Return the number of active connections registered with this pool."""
        with self._lock:
            return len(self._connection_registry)

    def checked_out(self) -> int:
        """Get number of checked out connections (always 0 for thread-local)."""
        return 0

    def acquire(self) -> DuckDBConnection:
        """Acquire a thread-local or task-local connection.

        Each thread (and each concurrent asyncio task running on an event loop thread)
        gets its own dedicated DuckDB connection to prevent concurrent cursor and
        transaction sharing.

        Returns:
            DuckDBConnection: A thread- or task-isolated connection
        """
        state = self._get_or_create_connection_holder()
        connection = self._get_thread_connection()
        if not state.get("depth", 0):
            state["manual_acquire"] = True
        return connection

    def release(self, connection: DuckDBConnection) -> None:
        """Release a connection; thread-local connections stay open unless a session reset fails.

        Args:
            connection: The connection to release
        """
        if self._reset_session_on_release:
            self._reset_connection_or_retire(connection)

    def _reset_connection(self, connection: DuckDBConnection) -> None:
        if not self._reset_session_on_release:
            return
        with self._lock:
            baseline = self._baseline_databases.get(id(connection))
        _reset_session_state(connection, baseline)
        if self._on_connection_create is not None:
            replacement = self._on_connection_create(connection)
            if replacement is not None and replacement is not connection:
                msg = "on_connection_create returned a different connection during session reset"
                raise ImproperConfigurationError(msg)

    def _reset_connection_or_retire(self, connection: DuckDBConnection) -> None:
        try:
            self._reset_connection(connection)
        except Exception as exc:
            log_with_context(
                logger,
                logging.DEBUG,
                "pool.connection.reset_failed",
                adapter=_ADAPTER_NAME,
                pool_id=self._pool_id,
                database=self._database_name,
                error_type=type(exc).__name__,
            )
            self._close_thread_connection()

    def _create_connection(self) -> DuckDBConnection:
        """Create a pool-owned connection and record it for shutdown."""
        connection = self.new_connection(record_thread_state=True)
        with self._lock:
            self._connection_registry.add(connection)
        return connection

    def _storage_settings(self, connection: DuckDBConnection) -> "dict[str, Any]":
        """Return storage setup completed for this thread's current connection."""
        state = self._get_connection_holder()
        if state is None or state.get("connection") is not connection:
            with self._lock:
                profile_path = self._profile_paths.get(id(connection))
            if profile_path is not None:
                return {"_duckdb_profile_path": str(profile_path)}
            return {}
        cached_settings = cast("dict[str, Any] | None", state.get("_cached_storage_settings"))
        if cached_settings is not None:
            return cached_settings
        settings: dict[str, Any] = {
            "_duckdb_storage_extensions": state.get("storage_extensions", frozenset()),
            "_duckdb_storage_secrets": state.get("storage_secrets", ()),
            "_duckdb_storage_protocols": state.get("storage_protocols", frozenset()),
        }
        profile_path = state.get("profile_path")
        if profile_path is not None:
            settings["_duckdb_profile_path"] = str(profile_path)
        state["_cached_storage_settings"] = settings
        return settings

    def _install_extension_once(
        self,
        connection: "DuckDBConnection",
        ext_name: str,
        install_kwargs: "dict[str, Any]",
        force_install: bool,
        required: bool,
    ) -> "str | None":
        """Install an extension once per pool per signature, best-effort unless required.

        Returns the install error string on a best-effort failure so the caller can
        combine it with the load outcome into a single setup result; returns ``None``
        when the install succeeds or is skipped. Raises when ``required`` is set.
        """
        signature = (
            ext_name,
            install_kwargs.get("version"),
            install_kwargs.get("repository"),
            install_kwargs.get("repository_url"),
        )
        with self._lock:
            if not force_install and signature in self._installed_signatures:
                return None
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
                return str(exc)
            self._installed_signatures.add(signature)
            return None

    def _get_thread_connection(self) -> DuckDBConnection:
        """Get or create a connection for the current thread.

        Each thread gets its own dedicated DuckDB connection to prevent
        thread-safety issues with concurrent cursor operations.
        """
        thread_state = self._get_or_create_connection_holder()
        if thread_state.get("generation") != self._generation:
            stale = thread_state.pop("connection", None)
            if stale is not None:
                self._retire_connection(cast("DuckDBConnection", stale))
            thread_state.pop("created_at", None)
            thread_state.pop("last_used", None)
            thread_state["generation"] = self._generation
        if "connection" not in thread_state:
            now = time.time()
            thread_state["connection"] = self._create_connection()
            thread_state["created_at"] = now
            thread_state["last_used"] = now
            return cast("DuckDBConnection", thread_state["connection"])

        if self._recycle > 0 and time.time() - float(thread_state["created_at"]) > self._recycle:
            self._retire_connection(cast("DuckDBConnection", thread_state["connection"]))
            now = time.time()
            thread_state["connection"] = self._create_connection()
            thread_state["created_at"] = now
            thread_state["last_used"] = now
            return cast("DuckDBConnection", thread_state["connection"])

        idle_time = time.time() - float(thread_state.get("last_used", 0))
        if idle_time > self._health_check_interval and not self._is_connection_alive(
            cast("DuckDBConnection", thread_state["connection"])
        ):
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
            self._retire_connection(cast("DuckDBConnection", thread_state["connection"]))
            thread_state["connection"] = self._create_connection()
            thread_state["created_at"] = time.time()

        thread_state["last_used"] = time.time()
        connection = cast("DuckDBConnection", thread_state["connection"])
        self._refresh_dynamic_secrets(connection)
        return connection

    def _refresh_dynamic_secrets(self, connection: DuckDBConnection) -> None:
        """Refresh secrets configured with ``value_provider`` when ``refresh_interval_seconds`` has elapsed."""
        if not self._has_dynamic_secrets:
            return
        thread_state = self._get_or_create_connection_holder()
        refresh_times = cast("dict[str, float]", thread_state.setdefault("secret_refresh_times", {}))
        current_secrets = list(cast("tuple[dict[str, Any], ...]", thread_state.get("storage_secrets", ())))
        profile_path = cast("Path | None", thread_state.get("profile_path"))
        if profile_path is None:
            with self._lock:
                profile_path = self._profile_paths.get(id(connection))
        now_monotonic = time.monotonic()
        updated = False
        for secret_config in self._secrets:
            provider = secret_config.get("value_provider")
            secret_name = secret_config.get("name")
            if not callable(provider) or not secret_name:
                continue
            interval = float(secret_config.get("refresh_interval_seconds", 1800.0))
            last_refreshed = refresh_times.get(str(secret_name), 0.0)
            if now_monotonic - last_refreshed < interval:
                continue
            resolved_secret = _resolve_secret_config(secret_config)
            if _create_secret(connection, resolved_secret, profile_path=profile_path):
                refresh_times[str(secret_name)] = now_monotonic
                updated_entry = {**resolved_secret, "value": dict(resolved_secret.get("value") or {})}
                replaced = False
                for idx, existing in enumerate(current_secrets):
                    if str(existing.get("name", "")).lower() == str(secret_name).lower():
                        current_secrets[idx] = updated_entry
                        replaced = True
                        break
                if not replaced:
                    current_secrets.append(updated_entry)
                updated = True
        if updated:
            thread_state["storage_secrets"] = tuple(current_secrets)
            thread_state.pop("_cached_storage_settings", None)

    def _get_or_create_connection_holder(self) -> "dict[str, Any]":
        """Return the connection holder mapping for the current sync thread or active asyncio task."""
        thread_dict = self._thread_local.__dict__
        task = _current_async_task()
        if task is None:
            return thread_dict
        task_states = cast(
            "WeakKeyDictionary[asyncio.Task[Any], dict[str, Any]] | None", thread_dict.get("_task_holders")
        )
        if task_states is None:
            task_states = WeakKeyDictionary()
            thread_dict["_task_holders"] = task_states
        state = task_states.get(task)
        if state is None:
            state = self._reuse_idle_task_connection() or {}
            task_states[task] = state
            task.add_done_callback(partial(self._retire_finished_task, task_states))
        return state

    def _reuse_idle_task_connection(self) -> "dict[str, Any] | None":
        now = time.time()
        while True:
            with self._lock:
                if not self._idle_task_holders:
                    return None
                candidate = self._idle_task_holders.pop()
                current_generation = self._generation
            connection = cast("DuckDBConnection | None", candidate.get("connection"))
            if connection is None:
                continue
            if candidate.get("generation") != current_generation:
                self._retire_connection(connection)
                continue
            created_at = float(candidate.get("created_at", 0.0))
            if self._recycle > 0 and (now - created_at) > self._recycle:
                self._retire_connection(connection)
                continue
            idle_time = now - float(candidate.get("last_used", 0.0))
            if idle_time > self._health_check_interval and not self._is_connection_alive(connection):
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
                self._retire_connection(connection)
                continue
            with self._lock:
                if self._generation != current_generation or connection not in self._connection_registry:
                    self._retire_connection(connection)
                    return None
            candidate["last_used"] = now
            return candidate

    def _get_connection_holder(self) -> "dict[str, Any] | None":
        """Return an existing connection holder mapping without allocating a new task holder."""
        thread_dict = self._thread_local.__dict__
        task = _current_async_task()
        if task is None:
            return thread_dict
        task_states = cast(
            "WeakKeyDictionary[asyncio.Task[Any], dict[str, Any]] | None", thread_dict.get("_task_holders")
        )
        if task_states is None:
            return None
        return task_states.get(task)

    def _retire_finished_task(
        self, task_states: "WeakKeyDictionary[asyncio.Task[Any], dict[str, Any]]", completed_task: "asyncio.Task[Any]"
    ) -> None:
        state = task_states.pop(completed_task, None)
        if state is None or state.get("manual_acquire"):
            return
        connection = cast("DuckDBConnection | None", state.get("connection"))
        if connection is None:
            return
        if int(state.get("depth", 0)) == 0:
            with self._lock:
                if (
                    state.get("generation") == self._generation
                    and connection in self._connection_registry
                    and len(self._idle_task_holders) < _MAX_IDLE_TASK_CONNECTIONS
                ):
                    idle_state: dict[str, Any] = {
                        "connection": connection,
                        "created_at": state["created_at"],
                        "last_used": state["last_used"],
                        "generation": self._generation,
                    }
                    for key in _TASK_STORAGE_STATE_KEYS:
                        if key in state:
                            idle_state[key] = state[key]
                    self._idle_task_holders.append(idle_state)
                    return
        self._retire_connection(connection)

    def _retire_connection(self, connection: DuckDBConnection) -> None:
        """Close a pool-owned connection and drop it from the shutdown registry."""
        with self._lock:
            self._connection_registry.discard(connection)
            self._baseline_databases.pop(id(connection), None)
            profile_path = self._profile_paths.pop(id(connection), None)
        with suppress(Exception):
            connection.close()
        cleanup_duckdb_profile_path(profile_path)

    def _close_thread_connection(self) -> None:
        """Close the connection for the current thread or task."""
        thread_state = self._get_connection_holder()
        if thread_state is None:
            return
        thread_state.pop("storage_extensions", None)
        thread_state.pop("storage_secrets", None)
        thread_state.pop("secret_refresh_times", None)
        thread_state.pop("storage_protocols", None)
        thread_state.pop("profile_path", None)
        thread_state.pop("_cached_storage_settings", None)
        thread_state.pop("manual_acquire", None)
        if "connection" in thread_state:
            self._retire_connection(cast("DuckDBConnection", thread_state.pop("connection")))
            thread_state.pop("created_at", None)
            thread_state.pop("last_used", None)

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


def _database_baseline(connection: DuckDBConnection) -> "tuple[frozenset[str], str]":
    names = frozenset(
        str(row[0]) for row in connection.execute("SELECT database_name FROM duckdb_databases()").fetchall()
    )
    current = connection.execute("SELECT current_database()").fetchall()
    return names, str(current[0][0]) if current else ""


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _reset_session_state(connection: DuckDBConnection, baseline: "tuple[frozenset[str], str] | None") -> None:
    """Remove temporary objects and added databases a previous lease left on a pooled connection.

    Args:
        connection: Connection whose lease just ended.
        baseline: Database names and default database captured when the connection was created.
    """
    macros = connection.execute(
        "SELECT function_name, function_type FROM duckdb_functions() WHERE database_name = 'temp' AND NOT internal"
    ).fetchall()
    for name, function_type in macros:
        keyword = "MACRO TABLE" if function_type == "table_macro" else "MACRO"
        connection.execute(f"DROP {keyword} IF EXISTS temp.main.{_quote_identifier(str(name))}")
    for query, keyword in (
        ("SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal", "VIEW"),
        ("SELECT table_name FROM duckdb_tables() WHERE temporary", "TABLE"),
        ("SELECT sequence_name FROM duckdb_sequences() WHERE temporary", "SEQUENCE"),
    ):
        for (name,) in connection.execute(query).fetchall():
            connection.execute(f"DROP {keyword} IF EXISTS temp.main.{_quote_identifier(str(name))}")
    if baseline is None:
        return
    names, default_database = baseline
    if default_database:
        connection.execute(f"USE {_quote_identifier(default_database)}")
    for (name,) in connection.execute("SELECT database_name FROM duckdb_databases() WHERE NOT internal").fetchall():
        if str(name) not in names:
            connection.execute(f"DETACH DATABASE {_quote_identifier(str(name))}")


def _current_async_task() -> "asyncio.Task[Any] | None":
    loop = asyncio._get_running_loop()
    return asyncio.current_task(loop) if loop is not None else None


def _sanitize_database_uri(database: object) -> str:
    """Strip userinfo and query parameters from DuckDB database URIs for logging."""
    raw = str(database) if database else ""
    if not raw or raw.startswith(":memory:"):
        return ":memory:"
    parsed = urlsplit(raw)
    if parsed.scheme and len(parsed.scheme) > 1:
        netloc = parsed.netloc
        if "@" in netloc:
            netloc = netloc.rsplit("@", 1)[1]
        return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))
    return raw


def _open_duckdb_connection(
    connect_parameters: dict[str, Any], config_dict: dict[str, Any], *, has_extensions: bool
) -> tuple[DuckDBConnection, bool | None]:
    """Open a DuckDB connection, deferring ``parquet_metadata_cache`` when extensions are configured."""
    deferred_parquet_metadata_cache: bool | None = None
    if has_extensions and "parquet_metadata_cache" in config_dict:
        deferred_parquet_metadata_cache = bool(config_dict.pop("parquet_metadata_cache"))

    if config_dict:
        connect_parameters["config"] = config_dict
    else:
        connect_parameters.pop("config", None)

    try:
        connection = duckdb.connect(**connect_parameters)
    except Exception as exc:
        if "parquet_metadata_cache" in config_dict and "parquet_metadata_cache" in str(exc):
            deferred_parquet_metadata_cache = bool(config_dict.pop("parquet_metadata_cache"))
            if config_dict:
                connect_parameters["config"] = config_dict
            else:
                connect_parameters.pop("config", None)
            try:
                connection = duckdb.connect(**connect_parameters)
            except duckdb.Error as retry_exc:
                raise create_mapped_exception(retry_exc) from retry_exc
        elif isinstance(exc, duckdb.Error):
            raise create_mapped_exception(exc) from exc
        else:
            raise
    return connection, deferred_parquet_metadata_cache


def _validate_sql_identifier(value: str, field_name: str) -> None:
    """Raise ValueError if value is not safe to interpolate as a SQL identifier."""
    if not _SQL_IDENTIFIER_RE.fullmatch(value):
        msg = (
            f"Invalid SQL identifier for {field_name!r}: {value!r}. "
            "Must start with a letter and contain only letters, digits, and underscores."
        )
        raise ValueError(msg)


def _resolve_secret_config(secret_config: dict[str, Any]) -> dict[str, Any]:
    """Evaluate ``value_provider`` if present and return a normalized secret configuration dict."""
    resolved = dict(secret_config)
    value_dict = dict(resolved.get("value") or {})
    provider_fn = resolved.pop("value_provider", None)
    if callable(provider_fn):
        dynamic_values = provider_fn()
        if isinstance(dynamic_values, dict):
            value_dict.update(dynamic_values)
        resolved.setdefault("replace", True)
    if value_dict:
        resolved["value"] = value_dict
    return resolved


def _create_secret(
    connection: DuckDBConnection, secret_config: dict[str, Any], *, profile_path: "Path | None" = None
) -> bool:
    secret_config = _resolve_secret_config(secret_config)
    secret_name = secret_config.get("name")
    secret_type = secret_config.get("secret_type")
    if not (secret_name and secret_type):
        return False

    required = bool(secret_config.get("required", False))
    try:
        _validate_sql_identifier(secret_name, "secret_name")
        _validate_sql_identifier(secret_type, "secret_type")
        sql = _secret_sql(secret_config, secret_name, secret_type)
        mismatch = _apply_secret(connection, secret_config, secret_name, secret_type, sql, profile_path=profile_path)
    except Exception:
        if required:
            raise
        logger.warning("DuckDB secret %r creation failed (best-effort)", secret_name)
        return False
    if mismatch is None:
        return True
    if required:
        raise RuntimeError(mismatch)
    logger.warning("%s (best-effort)", mismatch)
    return False


def _apply_secret(
    connection: DuckDBConnection,
    secret_config: dict[str, Any],
    secret_name: str,
    secret_type: str,
    sql: str,
    *,
    profile_path: "Path | None" = None,
) -> "str | None":
    """Create or reuse a declared secret under the process-wide secret lock.

    Catalog write-write conflicts with connections outside that lock are retried with a
    short backoff that runs while the lock is released.

    Returns:
        A description of how the visible secret differs from its declaration, or ``None`` when it matches.
    """
    for attempt in range(1, SECRET_CONFLICT_ATTEMPTS):
        applied, mismatch = _try_apply_secret(
            connection, secret_config, secret_name, secret_type, sql, profile_path=profile_path
        )
        if applied:
            return mismatch
        time.sleep(SECRET_CONFLICT_BACKOFF * attempt)
    with _SECRET_LOCK:
        return _apply_secret_locked(connection, secret_config, secret_name, secret_type, sql, profile_path=profile_path)


def _try_apply_secret(
    connection: DuckDBConnection,
    secret_config: dict[str, Any],
    secret_name: str,
    secret_type: str,
    sql: str,
    *,
    profile_path: "Path | None" = None,
) -> "tuple[bool, str | None]":
    """Apply a declared secret once, returning ``(False, None)`` on a catalog write-write conflict."""
    try:
        with _SECRET_LOCK:
            mismatch = _apply_secret_locked(
                connection, secret_config, secret_name, secret_type, sql, profile_path=profile_path
            )
    except duckdb.TransactionException as exc:
        if "write-write conflict" not in str(exc):
            raise
        return False, None
    return True, mismatch


def _read_profiling_state(connection: DuckDBConnection) -> "tuple[str | None, str | None]":
    """Read active profiling mode and output path if profiling is currently enabled on connection."""
    with suppress(Exception):
        row = connection.execute(
            "SELECT current_setting('enable_profiling'), current_setting('profiling_output')"
        ).fetchone()
        if row and row[0]:
            mode = str(row[0]).strip().lower()
            if mode in _DUCKDB_PROFILING_MODES:
                output = str(row[1]) if len(row) > 1 and row[1] else None
                return mode, output
    return None, None


def _restore_profiling_state(
    connection: DuckDBConnection,
    *,
    profile_path: "Path | None",
    previous_mode: "str | None",
    previous_output: "str | None",
) -> None:
    """Restore statement profiling after secret creation completes."""
    if profile_path is not None:
        with suppress(Exception):
            configure_duckdb_statement_profiling(connection, profile_path)
        return
    if previous_mode is not None:
        with suppress(Exception):
            connection.execute(f"PRAGMA enable_profiling='{previous_mode}'")
            if previous_output:
                escaped = previous_output.replace("'", "''")
                connection.execute(f"SET profiling_output='{escaped}'")


def _apply_secret_locked(
    connection: DuckDBConnection,
    secret_config: dict[str, Any],
    secret_name: str,
    secret_type: str,
    sql: str,
    *,
    profile_path: "Path | None" = None,
) -> "str | None":
    """Reuse a visible secret unless replacement is requested, otherwise create it, then compare it."""
    previous_mode, previous_output = _read_profiling_state(connection)
    with suppress(Exception):
        connection.execute("PRAGMA disable_profiling")
    try:
        expected_provider = secret_config.get("provider") or _default_secret_provider(connection, secret_type)
        if not secret_config.get("replace", False):
            existing = _visible_secret(connection, secret_config, secret_name, secret_type)
            if existing is not None:
                mismatch = _secret_mismatch(existing, secret_config, secret_name, secret_type, expected_provider, True)
                if mismatch is None:
                    logger.debug("DuckDB secret %r already exists and is reused", secret_name)
                return mismatch
        connection.execute(sql)
        created = _visible_secret(connection, secret_config, secret_name, secret_type)
        if created is None:
            storage = "persistent" if secret_config.get("persistent", False) else "temporary"
            return f"DuckDB secret {secret_name!r} was not visible as a {storage} secret after creation"
        return _secret_mismatch(created, secret_config, secret_name, secret_type, expected_provider, False)
    finally:
        _restore_profiling_state(
            connection, profile_path=profile_path, previous_mode=previous_mode, previous_output=previous_output
        )


def _secret_sql(secret_config: dict[str, Any], secret_name: str, secret_type: str) -> str:
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

    kind = "PERSISTENT SECRET" if secret_config.get("persistent", False) else "SECRET"
    if secret_config.get("replace", False):
        create = f"CREATE OR REPLACE {kind} {secret_name}"
    else:
        create = f"CREATE {kind} IF NOT EXISTS {secret_name}"
    body = ",\n    ".join(parts)
    return f"{create} (\n    {body}\n)"


def _format_secret_key(key: Any) -> str:
    key_text = str(key)
    _validate_sql_identifier(key_text, "secret_value_key")
    return key_text.upper()


def _format_secret_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        if not math.isfinite(value):
            msg = "DuckDB secret float option must be finite."
            raise ValueError(msg)
        return str(value)
    if isinstance(value, int):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _visible_secret(
    connection: DuckDBConnection, secret_config: dict[str, Any], secret_name: str, secret_type: str
) -> "tuple[Any, ...] | None":
    """Return the type, provider, scope and redacted settings of a visible secret."""
    return connection.execute(
        "SELECT type, provider, scope, secret_string FROM duckdb_secrets() WHERE lower(name) = lower(?) AND persistent = ?",
        (secret_name, bool(secret_config.get("persistent", False))),
    ).fetchone()


def _default_secret_provider(connection: DuckDBConnection, secret_type: str) -> "str | None":
    """Return DuckDB's default provider for a secret type, or ``None`` when DuckDB does not report it."""
    try:
        row = connection.execute(
            "SELECT default_provider FROM duckdb_secret_types() WHERE type = lower(?)", (secret_type,)
        ).fetchone()
    except duckdb.CatalogException:
        return None
    return str(row[0]) if row and row[0] else None


def _secret_mismatch(
    row: "tuple[Any, ...]",
    secret_config: dict[str, Any],
    secret_name: str,
    secret_type: str,
    expected_provider: "str | None",
    reused: bool,
) -> "str | None":
    """Describe how a visible secret differs from its declaration, or return ``None`` when it matches.

    DuckDB redacts credential values, so the comparison covers the type, provider, declared
    scope and the declared settings DuckDB reports unredacted. Settings the declaration omits
    are not compared.
    """
    storage = "persistent" if secret_config.get("persistent", False) else "temporary"
    hint = "; set replace=True to apply the declaration" if reused else ""
    actual_type, actual_provider, actual_scope, secret_string = row
    if str(actual_type).lower() != secret_type.lower():
        return (
            f"DuckDB secret {secret_name!r} exists as a {storage} secret of type {actual_type!r}, "
            f"but the declaration uses type {secret_type!r}{hint}"
        )
    differing: list[str] = []
    if expected_provider and str(actual_provider).lower() != expected_provider.lower():
        differing.append("provider")
    scope = secret_config.get("scope")
    if scope is not None and scope not in list(actual_scope or []):
        differing.append("scope")
    settings = f"{secret_string or ''};"
    for key, value in (secret_config.get("value") or {}).items():
        setting = str(key).lower()
        if (
            setting in _SECRET_VISIBLE_SETTINGS
            and f";{setting}={_secret_setting_text(setting, value)};" not in settings
        ):
            differing.append(setting)
    if not differing:
        return None
    return (
        f"DuckDB secret {secret_name!r} exists as a {storage} secret whose settings differ from the "
        f"declaration: {', '.join(differing)}{hint}"
    )


def _secret_setting_text(setting: str, value: Any) -> str:
    """Render a declared setting the way ``duckdb_secrets()`` reports it."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if setting in _SECRET_BOOLEAN_SETTINGS:
        if isinstance(value, (int, float)):
            return "true" if value else "false"
        text = str(value).lower()
        return _SECRET_BOOLEAN_TEXT.get(text, text)
    return str(value)

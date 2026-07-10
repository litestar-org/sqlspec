"""SQL file loader for managing SQL statements from files.

Provides functionality to load, cache, and manage SQL statements
from files using named SQL queries.

SQL files declare query metadata with comment directives like ``-- name: query_name`` (hyphens and suffixes allowed)
and ``-- dialect: dialect_name``.
"""

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import unquote, urlparse

from sqlspec.core import SQL, ParameterDeclaration, ParameterValidator, get_cache, get_cache_config
from sqlspec.exceptions import (
    FileNotFoundInStorageError,
    SQLFileNotFoundError,
    SQLFileParseError,
    SQLStatementNotFoundError,
    StorageOperationFailedError,
)
from sqlspec.storage.registry import storage_registry as default_storage_registry
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.logging import get_logger, log_with_context
from sqlspec.utils.text import slugify
from sqlspec.utils.type_guards import is_local_path

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.observability import ObservabilityRuntime
    from sqlspec.storage.registry import StorageRegistry

__all__ = ("NamedStatement", "SQLFile", "SQLFileCacheEntry", "SQLFileLoader")

logger = get_logger("sqlspec.loader")

QUERY_NAME_PATTERN = re.compile(r"^\s*--\s*name\s*:\s*([\w-]+[^\w\s]*)\s*$", re.MULTILINE | re.IGNORECASE)

DIALECT_PATTERN = re.compile(r"^\s*--\s*dialect\s*:\s*(?P<dialect>[a-zA-Z0-9_]+)\s*$", re.IGNORECASE | re.MULTILINE)

PARAM_PATTERN = re.compile(
    r"^\s*--\s*param\s*:\s*(?P<name>\w+)\s+(?P<type>[\w.]+(?:\[[\w., ]+\])?)(?P<optional>\?)?(?:\s+(?P<desc>.*\S))?\s*$",
    re.IGNORECASE,
)

PARAM_PREFIX_PATTERN = re.compile(r"^\s*--\s*param\s*:", re.IGNORECASE)
PARAM_OPTIONAL_DESCRIPTION_PATTERN = re.compile(r"(?:^|\s)\(optional\)\s*$", re.IGNORECASE)


DIALECT_ALIASES: Final = {
    "postgresql": "postgres",
    "pg": "postgres",
    "pgplsql": "postgres",
    "plsql": "oracle",
    "oracledb": "oracle",
    "tsql": "mssql",
}


def _parse_parameter_declaration(param_match: "re.Match[str]") -> ParameterDeclaration:
    """Build a parameter declaration from a matched ``-- param:`` line."""
    description = param_match.group("desc")
    required = param_match.group("optional") != "?"
    if description is not None and PARAM_OPTIONAL_DESCRIPTION_PATTERN.search(description):
        required = False
        description = PARAM_OPTIONAL_DESCRIPTION_PATTERN.sub("", description).strip() or None
    return ParameterDeclaration(
        name=param_match.group("name"), type_str=param_match.group("type"), description=description, required=required
    )


class NamedStatement:
    """Represents a parsed SQL statement with metadata.

    Contains individual SQL statements extracted from files with their
    normalized names, SQL content, optional dialect specifications,
    and line position for error reporting.
    """

    __slots__ = ("dialect", "name", "parameters", "sql", "start_line")

    def __init__(
        self,
        name: str,
        sql: str,
        dialect: "str | None" = None,
        start_line: int = 0,
        parameters: "tuple[ParameterDeclaration, ...]" = (),
    ) -> None:
        self.name = name
        self.sql = sql
        self.dialect = dialect
        self.start_line = start_line
        self.parameters = parameters


class SQLFile:
    """Represents a loaded SQL file with metadata.

    Contains SQL content and associated metadata including file location,
    timestamps, and content hash.
    """

    __slots__ = ("checksum", "content", "loaded_at", "metadata", "path")

    def __init__(
        self, content: str, path: str, metadata: "dict[str, Any] | None" = None, loaded_at: "datetime | None" = None
    ) -> None:
        """Initialize SQLFile.

        Args:
            content: Raw SQL content from the file.
            path: Path where the SQL file was loaded from.
            metadata: Optional metadata associated with the SQL file.
            loaded_at: Timestamp when the file was loaded.
        """
        self.content = content
        self.path = path
        self.metadata = metadata or {}
        self.loaded_at = loaded_at or datetime.now(timezone.utc)
        self.checksum = hashlib.md5(self.content.encode(), usedforsecurity=False).hexdigest()


class SQLFileCacheEntry:
    """Cached SQL file with parsed statements.

    Stored in the file cache to avoid re-parsing SQL files when their
    content hasn't changed.
    """

    __slots__ = ("parsed_statements", "sql_file", "statement_names")

    def __init__(self, sql_file: SQLFile, parsed_statements: "dict[str, NamedStatement]") -> None:
        """Initialize cached SQL file.

        Args:
            sql_file: Original SQLFile with content and metadata.
            parsed_statements: Named statements from the file.
        """
        self.sql_file = sql_file
        self.parsed_statements = parsed_statements
        self.statement_names = tuple(parsed_statements.keys())


class SQLFileLoader:
    """Loads and parses SQL files with named SQL queries.

    Loads SQL files containing named queries (using -- name: syntax)
    and retrieves them by name.
    """

    __slots__ = (
        "_compiled_statements",
        "_files",
        "_queries",
        "_query_to_file",
        "_runtime",
        "encoding",
        "storage_registry",
        "strict_parameter_annotations",
    )

    def __init__(
        self,
        *,
        encoding: str = "utf-8",
        storage_registry: "StorageRegistry | None" = None,
        runtime: "ObservabilityRuntime | None" = None,
        strict_parameter_annotations: bool = False,
    ) -> None:
        """Initialize the SQL file loader.

        Args:
            encoding: Text encoding for reading SQL files.
            storage_registry: Storage registry for handling file URIs.
            runtime: Observability runtime for instrumentation.
            strict_parameter_annotations: When True, a malformed ``-- param:`` directive
                raises instead of emitting a warning and skipping the line.
        """
        self.encoding = encoding
        self.strict_parameter_annotations = strict_parameter_annotations

        self.storage_registry = storage_registry or default_storage_registry
        self._compiled_statements: dict[str, SQL] = {}
        self._queries: dict[str, NamedStatement] = {}
        self._files: dict[str, SQLFile] = {}
        self._query_to_file: dict[str, str] = {}
        self._runtime = runtime

    def set_observability_runtime(self, runtime: "ObservabilityRuntime | None") -> None:
        """Attach an observability runtime used for instrumentation."""

        self._runtime = runtime

    def _raise_file_not_found(self, path: str) -> None:
        """Raise SQLFileNotFoundError for nonexistent file.

        Args:
            path: File path that was not found.

        Raises:
            SQLFileNotFoundError: Always raised.
        """
        raise SQLFileNotFoundError(path)

    def _raise_statement_not_found(self, name: str, normalized_name: str) -> None:
        """Raise SQLStatementNotFoundError for nonexistent statements.

        Args:
            name: Name requested by the caller.
            normalized_name: Normalized statement name used for lookup.

        Raises:
            SQLStatementNotFoundError: Always raised.
        """
        raise SQLStatementNotFoundError(name=name, normalized_name=normalized_name, query_count=len(self._queries))

    def _file_cache_key(self, path: str | Path) -> str:
        """Generate cache key for a file path.

        Args:
            path: File path to generate key for.

        Returns:
            Cache key string for the file.
        """
        path_str = str(path)
        path_hash = hashlib.md5(path_str.encode(), usedforsecurity=False).hexdigest()
        return f"file:{path_hash[:16]}"

    @staticmethod
    def _compute_checksum(content: str) -> str:
        """Compute MD5 checksum from already-read file content."""
        return hashlib.md5(content.encode(), usedforsecurity=False).hexdigest()

    def _calculate_file_checksum(self, path: str | Path) -> str:
        """Calculate checksum for file content validation.

        Args:
            path: File path to calculate checksum for.

        Returns:
            MD5 checksum of file content.

        Raises:
            SQLFileParseError: If file cannot be read.
        """
        try:
            return self._compute_checksum(self._read_file_content(path))
        except Exception as e:
            raise SQLFileParseError(str(path), str(path), e) from e

    def _is_file_unchanged(self, path: str | Path, cached_file: SQLFile) -> bool:
        """Check if file has changed since caching.

        Args:
            path: File path to check.
            cached_file: Cached file data.

        Returns:
            True if file is unchanged, False otherwise.
        """
        try:
            current_checksum = self._calculate_file_checksum(path)
        except Exception:
            return False
        else:
            return current_checksum == cached_file.checksum

    def _reload_changed_files(self) -> "list[str]":
        """Reload tracked SQL files whose content checksum changed.

        Returns:
            Paths of files that were reloaded.
        """
        changed_paths: list[str] = []
        for path, sql_file in list(self._files.items()):
            if self._is_file_unchanged(path, sql_file):
                if self._runtime is not None:
                    self._runtime.increment_metric("loader.reload.skipped")
                continue

            query_names = [name for name, source in self._query_to_file.items() if source == path]
            namespaces = {
                name[: -(len(statement.name) + 1)]
                for name in query_names
                if (statement := self._queries.get(name)) is not None and name.endswith(f".{statement.name}")
            }
            namespace = next(iter(namespaces)) if len(namespaces) == 1 else None
            for name in query_names:
                self._queries.pop(name, None)
                self._query_to_file.pop(name, None)
                self._compiled_statements.pop(name, None)
            self._files.pop(path, None)
            self._load_single_file(path, namespace)
            changed_paths.append(path)
            if self._runtime is not None:
                self._runtime.increment_metric("loader.reload.changed")
        return changed_paths

    def _content_matches_cache(self, content: str, cached_file: SQLFileCacheEntry) -> bool:
        """Check if already-read file content matches cached checksum."""
        return self._compute_checksum(content) == cached_file.sql_file.checksum

    def _read_file_content(self, path: str | Path) -> str:
        """Read file content using storage backend.

        Args:
            path: File path (can be local path or URI).

        Returns:
            File content as string.

        Raises:
            SQLFileNotFoundError: If file does not exist.
            SQLFileParseError: If file cannot be read or parsed.
        """
        path_str = str(path)

        try:
            backend = self.storage_registry.get(path)

            # If path_str contains a '/', we check if the first part is a registered alias.
            # This is specifically for when a path is provided relative to an alias.
            parts = path_str.split("/", 1)
            if len(parts) > 1 and self.storage_registry.is_alias_registered(parts[0]):
                return backend.read_text_sync(parts[1], encoding=self.encoding)

            if path_str.startswith("file://"):
                parsed = urlparse(path_str)
                file_path = unquote(parsed.path)
                if file_path and len(file_path) > 2 and file_path[2] == ":":  # noqa: PLR2004
                    file_path = file_path[1:]
                return backend.read_text_sync(Path(file_path).name, encoding=self.encoding)

            if isinstance(path, Path) or is_local_path(path_str):
                return backend.read_text_sync(Path(path_str).name, encoding=self.encoding)

            return backend.read_text_sync(path_str, encoding=self.encoding)
        except KeyError as e:
            raise SQLFileNotFoundError(path_str) from e
        except FileNotFoundInStorageError as e:
            raise SQLFileNotFoundError(path_str) from e
        except FileNotFoundError as e:
            raise SQLFileNotFoundError(path_str) from e
        except StorageOperationFailedError as e:
            raise SQLFileParseError(path_str, path_str, e) from e
        except Exception as e:
            raise SQLFileParseError(path_str, path_str, e) from e

    @staticmethod
    def _strip_leading_comments(sql_text: str) -> str:
        """Remove leading comment lines from a SQL string."""
        lines = sql_text.strip().split("\n")
        first_sql_line_index = -1
        for i, line in enumerate(lines):
            if line.strip() and not line.strip().startswith("--"):
                first_sql_line_index = i
                break
        if first_sql_line_index == -1:
            return ""
        return "\n".join(lines[first_sql_line_index:]).strip()

    @staticmethod
    def _parse_directive_block(
        statement_section: str, file_path: str, strict: bool, base_line: int = 0
    ) -> "tuple[str | None, tuple[ParameterDeclaration, ...], str]":
        """Scan a statement's leading comment block for ``dialect``/``param`` directives.

        Args:
            statement_section: The statement body including any leading directive lines.
            file_path: File path for error reporting.
            strict: When True, a malformed ``-- param:`` line raises instead of warning.
            base_line: 0-based line offset of ``statement_section`` within the file.

        Returns:
            The resolved dialect, the declared parameters, and the SQL body with the
            leading directive/comment lines removed.

        Raises:
            SQLFileParseError: If ``strict`` and a ``-- param:`` line is malformed.
        """
        dialect: str | None = None
        params: list[ParameterDeclaration] = []
        raw_lines = statement_section.split("\n")
        body_start = len(raw_lines)
        for idx, raw in enumerate(raw_lines):
            stripped = raw.strip()
            if not stripped:
                continue
            if not stripped.startswith("--"):
                body_start = idx
                break
            dialect_match = DIALECT_PATTERN.match(stripped)
            if dialect_match:
                dialect = _normalize_dialect(dialect_match.group("dialect").lower())
                continue
            param_match = PARAM_PATTERN.match(stripped)
            if param_match:
                params.append(_parse_parameter_declaration(param_match))
                continue
            if PARAM_PREFIX_PATTERN.match(stripped):
                if strict:
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(f"Malformed -- param: directive: {stripped}"),
                        line=base_line + idx + 1,
                    )
                log_with_context(
                    logger, logging.WARNING, "sql.parse.param", file_path=file_path, line=stripped, status="malformed"
                )
        return dialect, tuple(params), "\n".join(raw_lines[body_start:])

    @staticmethod
    def _check_declared_parameters(
        clean_sql: str,
        declared: "tuple[ParameterDeclaration, ...]",
        statement_name: str,
        file_path: str,
        start_line: "int | None" = None,
    ) -> None:
        """Validate declared parameters against the query's actual placeholders.

        For named binding, every declared name must appear among the SQL placeholders
        (declared names may be a subset; filters and undeclared params are allowed). For
        positional binding, the declared count must equal the placeholder count.

        Args:
            clean_sql: The SQL body with directives/comments stripped.
            declared: Declared parameters for the query.
            statement_name: Raw query name for error messages.
            file_path: File path for error reporting.
            start_line: Optional 0-based line of the statement within the file.

        Raises:
            SQLFileParseError: On name drift (named) or count mismatch (positional).
        """
        if not declared:
            return
        error_line = start_line + 1 if start_line is not None else None
        infos = ParameterValidator().extract_parameters(clean_sql)
        named = {info.name for info in infos if info.name and not info.name.isdigit()}
        if named:
            for decl in declared:
                if decl.name not in named:
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(
                            f"Declared parameter '{decl.name}' for query '{statement_name}' is not present in the "
                            f"SQL placeholders {sorted(named)}"
                        ),
                        line=error_line,
                    )
        elif len(declared) != len(infos):
            raise SQLFileParseError(
                file_path,
                file_path,
                ValueError(
                    f"Query '{statement_name}' declares {len(declared)} parameter(s) but the SQL has "
                    f"{len(infos)} positional placeholder(s)"
                ),
                line=error_line,
            )

    @staticmethod
    def _parse_statements(
        content: str, file_path: str, strict_parameter_annotations: bool = False
    ) -> "dict[str, NamedStatement]":
        """Parse SQL content and extract named statements with dialect specifications.

        Files without any named statement markers are gracefully skipped by returning
        an empty dictionary. The caller is responsible for handling empty results
        appropriately.

        Args:
            content: Raw SQL file content to parse.
            file_path: File path for error reporting.
            strict_parameter_annotations: Raise on malformed parameter declarations instead of skipping them.

        Returns:
            Dictionary mapping normalized statement names to NamedStatement objects.
            Empty dict if no named statement markers found in the content.

        Raises:
            SQLFileParseError: If named statements are malformed (duplicate names or
                invalid content after parsing).
        """
        statements: dict[str, NamedStatement] = {}

        name_matches = list(QUERY_NAME_PATTERN.finditer(content))
        if not name_matches:
            return {}

        for i, match in enumerate(name_matches):
            raw_statement_name = match.group(1).strip()
            statement_start_line = content[: match.start()].count("\n")

            start_pos = match.end()
            end_pos = name_matches[i + 1].start() if i + 1 < len(name_matches) else len(content)

            section_raw = content[start_pos:end_pos]
            statement_section = section_raw.strip()
            if not raw_statement_name or not statement_section:
                continue

            section_lead = len(section_raw) - len(section_raw.lstrip())
            section_start_line = content[:start_pos].count("\n") + section_raw[:section_lead].count("\n")

            dialect, declared_params, statement_sql = SQLFileLoader._parse_directive_block(
                statement_section, file_path, strict_parameter_annotations, base_line=section_start_line
            )

            clean_sql = SQLFileLoader._strip_leading_comments(statement_sql)
            if clean_sql:
                normalized_name = _normalize_query_name(raw_statement_name)
                if normalized_name in statements:
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(f"Duplicate statement name: {raw_statement_name}"),
                        line=statement_start_line + 1,
                    )

                SQLFileLoader._check_declared_parameters(
                    clean_sql, declared_params, raw_statement_name, file_path, start_line=statement_start_line
                )

                statements[normalized_name] = NamedStatement(
                    name=normalized_name,
                    sql=clean_sql,
                    dialect=dialect,
                    start_line=statement_start_line,
                    parameters=declared_params,
                )
                log_with_context(
                    logger, logging.DEBUG, "sql.parse", file_path=file_path, query_name=normalized_name, dialect=dialect
                )

        if not statements:
            raise SQLFileParseError(file_path, file_path, ValueError("No valid SQL statements found after parsing"))

        return statements

    def load_sql(self, *paths: str | Path) -> None:
        """Load SQL files and parse named queries.

        Args:
            *paths: One or more file paths or directory paths to load.
        """
        runtime = self._runtime
        span = None
        error: Exception | None = None
        start_time = time.perf_counter()
        path_count = len(paths)
        previous_correlation_id = CorrelationContext.get()
        if runtime is not None:
            runtime.increment_metric("loader.load.invocations")
            runtime.increment_metric("loader.paths.requested", path_count)
            span = runtime.start_span(
                "sqlspec.loader.load",
                attributes={"sqlspec.loader.path_count": path_count, "sqlspec.loader.encoding": self.encoding},
            )

        try:
            for path in paths:
                path_str = str(path)
                # If it looks like a URI or a potential alias (contains no path separators, or is in registry)
                if "://" in path_str or self.storage_registry.is_alias_registered(path_str.split("/", maxsplit=1)[0]):
                    self._load_single_file(path, None)
                    continue

                path_obj = Path(path)
                if path_obj.is_dir():
                    self._load_directory(path_obj)
                elif path_obj.exists():
                    self._load_single_file(path_obj, None)
                elif path_obj.suffix:
                    self._raise_file_not_found(str(path))

        except Exception as exc:
            error = exc
            if runtime is not None:
                runtime.increment_metric("loader.load.errors")
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            if runtime is not None:
                runtime.record_metric("loader.last_load_ms", duration_ms)
                runtime.increment_metric("loader.load.duration_ms", duration_ms)
                runtime.end_span(span, error=error)
            CorrelationContext.set(previous_correlation_id)

    def _load_directory(self, dir_path: Path) -> None:
        """Load all SQL files from a directory.

        Args:
            dir_path: Directory path to load SQL files from.
        """
        runtime = self._runtime
        if runtime is not None:
            runtime.increment_metric("loader.directories.scanned")

        sql_files = list(dir_path.rglob("*.sql"))
        if not sql_files:
            return

        for file_path in sql_files:
            relative_path = file_path.relative_to(dir_path)
            namespace_parts = relative_path.parent.parts
            self._load_single_file(file_path, ".".join(namespace_parts) if namespace_parts else None)

    def _load_single_file(self, file_path: str | Path, namespace: str | None) -> bool:
        """Load a single SQL file with optional namespace.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.

        Returns:
            True if file was newly loaded, False if already cached.
        """
        path_str = str(file_path)
        runtime = self._runtime
        if runtime is not None:
            runtime.increment_metric("loader.files.considered")

        if path_str in self._files:
            if runtime is not None:
                runtime.increment_metric("loader.cache.hit")
            return False

        cache_config = get_cache_config()
        if not cache_config.compiled_cache_enabled:
            self._load_uncached_file(file_path, namespace)
            if runtime is not None:
                runtime.increment_metric("loader.cache.miss")
            return True

        cache_key_str = self._file_cache_key(file_path)
        cache = get_cache()
        cached_file = cache.get_file(cache_key_str)

        if cached_file is not None and isinstance(cached_file, SQLFileCacheEntry):
            try:
                file_content = self._read_file_content(file_path)
            except Exception:
                file_content = None

            if file_content is not None and self._content_matches_cache(file_content, cached_file):
                self._files[path_str] = cached_file.sql_file
                for name, statement in cached_file.parsed_statements.items():
                    namespaced_name = f"{namespace}.{name}" if namespace else name
                    if namespaced_name in self._queries:
                        existing_file = self._query_to_file.get(namespaced_name, "unknown")
                        if existing_file != path_str:
                            raise SQLFileParseError(
                                path_str,
                                path_str,
                                ValueError(f"Query name '{namespaced_name}' already exists in file: {existing_file}"),
                                line=statement.start_line + 1,
                            )
                    self._queries[namespaced_name] = statement
                    self._query_to_file[namespaced_name] = path_str
                if runtime is not None:
                    runtime.increment_metric("loader.cache.hit")
                return True

            loaded_statements = self._load_uncached_file(file_path, namespace, content=file_content)
        else:
            loaded_statements = self._load_uncached_file(file_path, namespace)

        if path_str in self._files:
            sql_file = self._files[path_str]
            cached_file_data = SQLFileCacheEntry(sql_file=sql_file, parsed_statements=loaded_statements)
            cache.put_file(cache_key_str, cached_file_data)
            if runtime is not None:
                runtime.increment_metric("loader.cache.miss")
                runtime.increment_metric("loader.files.loaded")
                runtime.increment_metric("loader.statements.loaded", len(loaded_statements))

        return True

    def _load_uncached_file(
        self, file_path: str | Path, namespace: "str | None", content: "str | None" = None
    ) -> "dict[str, NamedStatement]":
        """Load a single SQL file without using cache.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries.
            content: Pre-read file content. If provided, skips the disk read.

        Returns:
            The file's parsed statements keyed by un-namespaced name, or an empty
            dict when the file contains no named statements.
        """
        path_str = str(file_path)
        runtime = self._runtime
        if content is None:
            content = self._read_file_content(file_path)
        statements = self._parse_statements(content, path_str, self.strict_parameter_annotations)

        if not statements:
            log_with_context(
                logger, logging.DEBUG, "sql.load", file_path=path_str, status="skipped", reason="no_named_statements"
            )
            return {}

        sql_file = SQLFile(content=content, path=path_str)
        self._files[path_str] = sql_file

        for name, statement in statements.items():
            namespaced_name = f"{namespace}.{name}" if namespace else name
            if namespaced_name in self._queries:
                existing_file = self._query_to_file.get(namespaced_name, "unknown")
                if existing_file != path_str:
                    raise SQLFileParseError(
                        path_str,
                        path_str,
                        ValueError(f"Query name '{namespaced_name}' already exists in file: {existing_file}"),
                        line=statement.start_line + 1,
                    )
            self._queries[namespaced_name] = statement
            self._query_to_file[namespaced_name] = path_str
        log_with_context(
            logger, logging.DEBUG, "sql.load", file_path=path_str, statement_count=len(statements), status="loaded"
        )
        if runtime is not None:
            runtime.increment_metric("loader.files.loaded")
            runtime.increment_metric("loader.statements.loaded", len(statements))
        return statements

    def add_named_sql(
        self,
        name: str,
        sql: str,
        dialect: "str | None" = None,
        parameters: "Sequence[ParameterDeclaration] | None" = None,
    ) -> None:
        """Add a named SQL query directly without loading from a file.

        Args:
            name: Name for the SQL query.
            sql: Raw SQL content.
            dialect: Optional dialect for the SQL statement.
            parameters: Optional declared parameter metadata for the query.

        Raises:
            ValueError: If query name already exists.
        """

        normalized_name = _normalize_query_name(name)

        if normalized_name in self._queries:
            existing_source = self._query_to_file.get(normalized_name, "<directly added>")
            msg = f"Query name '{name}' already exists (source: {existing_source})"
            raise ValueError(msg)

        if dialect is not None:
            dialect = _normalize_dialect(dialect)

        declared = tuple(parameters) if parameters else ()
        clean_sql = sql.strip()
        self._check_declared_parameters(clean_sql, declared, name, "<directly added>")

        statement = NamedStatement(
            name=normalized_name, sql=clean_sql, dialect=dialect, start_line=0, parameters=declared
        )
        self._queries[normalized_name] = statement
        self._query_to_file[normalized_name] = "<directly added>"

    def get_query_parameters(self, name: str) -> "tuple[ParameterDeclaration, ...]":
        """Get declared parameter metadata for a query.

        Args:
            name: Query name (hyphens are converted to underscores).

        Returns:
            Tuple of declared parameters; empty if the query declares none.

        Raises:
            SQLStatementNotFoundError: If the query does not exist.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        return self._queries[safe_name].parameters

    def get_file(self, path: str | Path) -> "SQLFile | None":
        """Get a loaded SQLFile object by path.

        Args:
            path: Path of the file.

        Returns:
            SQLFile object if loaded, None otherwise.
        """
        return self._files.get(str(path))

    def get_file_for_query(self, name: str) -> "SQLFile | None":
        """Get the SQLFile object containing a query.

        Args:
            name: Query name (hyphens are converted to underscores).

        Returns:
            SQLFile object if query exists, None otherwise.
        """
        safe_name = _normalize_query_name(name)
        if safe_name in self._query_to_file:
            file_path = self._query_to_file[safe_name]
            return self._files.get(file_path)
        return None

    def list_queries(self) -> "list[str]":
        """List all available query names.

        Returns:
            Sorted list of query names.
        """
        return sorted(self._queries.keys())

    def list_files(self) -> "list[str]":
        """List all loaded file paths.

        Returns:
            Sorted list of file paths.
        """
        return sorted(self._files.keys())

    def has_query(self, name: str) -> bool:
        """Check if a query exists.

        Args:
            name: Query name to check.

        Returns:
            True if query exists.
        """
        safe_name = _normalize_query_name(name)
        return safe_name in self._queries

    def clear_cache(self) -> None:
        """Clear all cached files and queries."""
        self._compiled_statements.clear()
        self._files.clear()
        self._queries.clear()
        self._query_to_file.clear()

        cache_config = get_cache_config()
        if cache_config.compiled_cache_enabled:
            cache = get_cache()
            cache.clear()

    def clear_file_cache(self) -> None:
        """Clear the file cache only, keeping loaded queries."""
        cache_config = get_cache_config()
        if cache_config.compiled_cache_enabled:
            cache = get_cache()
            cache.clear()

    def get_query_text(self, name: str) -> str:
        """Get raw SQL text for a query.

        Args:
            name: Query name.

        Returns:
            Raw SQL text.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        return self._queries[safe_name].sql

    def get_sql(self, name: str) -> "SQL":
        """Get a SQL object by statement name.

        Args:
            name: Name of the statement (from -- name: in SQL file).
                Hyphens in names are converted to underscores.

        Returns:
            SQL object ready for execution.
        """
        safe_name = _normalize_query_name(name)

        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        if safe_name in self._compiled_statements:
            return self._compiled_statements[safe_name]

        parsed_statement = self._queries[safe_name]
        sqlglot_dialect = None
        if parsed_statement.dialect:
            sqlglot_dialect = _normalize_dialect(parsed_statement.dialect)

        sql = SQL(parsed_statement.sql, dialect=sqlglot_dialect, declared_parameters=parsed_statement.parameters)
        try:
            sql.compile()
        except Exception as exc:
            raise SQLFileParseError(name=name, path="<statement>", original_error=exc) from exc
        self._compiled_statements[safe_name] = sql
        return sql


def _normalize_query_name(name: str) -> str:
    """Normalize query name to be a valid Python identifier.

    Convert hyphens to underscores, preserve dots for namespacing,
    and remove invalid characters.

    Args:
        name: Raw query name from SQL file.

    Returns:
        Normalized query name suitable as Python identifier.
    """
    parts = name.split(".")
    normalized_parts = []

    for part in parts:
        normalized_part = slugify(part, separator="_")
        normalized_parts.append(normalized_part)

    return ".".join(normalized_parts)


def _normalize_dialect(dialect: str) -> str:
    """Normalize dialect name with aliases.

    Args:
        dialect: Raw dialect name from SQL file.

    Returns:
        Normalized dialect name.
    """
    normalized = dialect.lower().strip()
    return DIALECT_ALIASES.get(normalized, normalized)

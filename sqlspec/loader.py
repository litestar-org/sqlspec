"""SQL file loader for managing SQL statements from files.

Provides functionality to load, cache, and manage SQL statements
from files using named SQL queries.

SQL files declare query metadata with comment directives like ``-- name: query_name`` (hyphens and suffixes allowed)
and ``-- dialect: dialect_name``. Reusable ``-- fragment: fragment_name`` sections are spliced into statements with
``/* include: fragment_name */`` markers, and ``/* slot: slot_name */`` markers declare fill points whose defaults
come from ``-- slot: slot_name = default sql`` directives.
"""

import hashlib
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import unquote, urlparse

from sqlglot import exp

from sqlspec.core import SQL, ParameterDeclaration, ParameterValidator, get_cache, get_cache_config
from sqlspec.exceptions import (
    FileNotFoundInStorageError,
    SQLFileNotFoundError,
    SQLFileParseError,
    SQLSlotError,
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

__all__ = ("NamedStatement", "SQLFile", "SQLFileCacheEntry", "SQLFileLoader", "SQLFragment", "SlotDeclaration")

logger = get_logger("sqlspec.loader")

QUERY_NAME_PATTERN = re.compile(r"^\s*--\s*name\s*:\s*([\w-]+[^\w\s]*)\s*$", re.MULTILINE | re.IGNORECASE)

FRAGMENT_NAME_PATTERN = re.compile(r"^\s*--\s*fragment\s*:\s*([\w-]+)\s*$", re.MULTILINE | re.IGNORECASE)

SLOT_DIRECTIVE_PATTERN = re.compile(
    r"^\s*--\s*slot\s*:\s*(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*(?:=\s*(?P<default>.*\S))?\s*$", re.IGNORECASE
)

SLOT_PREFIX_PATTERN = re.compile(r"^\s*--\s*slot\s*:", re.IGNORECASE)

SLOT_COMMENT_PATTERN = re.compile(r"--\s*slot\s*:", re.IGNORECASE)

SQL_LEXEME_PATTERN = re.compile(r"'|\"|--|/\*|\$[A-Za-z_]*\$")

SLOT_MARKER_PATTERN = re.compile(r"/\*\s*slot\s*:\s*([A-Za-z_][A-Za-z0-9_]*)\s*\*/")

INCLUDE_MARKER_PATTERN = re.compile(r"/\*\s*include\s*:\s*([\w.-]+)\s*\*/")

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


class SlotDeclaration:
    """A fill point declared by a ``/* slot: name */`` marker.

    A slot with a ``default`` comes from a ``-- slot: name = default`` directive;
    a slot whose ``default`` is None must be supplied by the caller.
    """

    __slots__ = ("default", "name")

    def __init__(self, name: str, default: "str | None" = None) -> None:
        self.name = name
        self.default = default

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SlotDeclaration):
            return NotImplemented
        return self.name == other.name and self.default == other.default

    def __hash__(self) -> int:
        return hash((self.name, self.default))

    def __repr__(self) -> str:
        return f"SlotDeclaration(name={self.name!r}, default={self.default!r})"


class SQLFragment:
    """A reusable SQL section declared with ``-- fragment: name``.

    Fragments are never compiled on their own; their text replaces
    ``/* include: name */`` markers in statements and other fragments.
    """

    __slots__ = ("name", "sql", "start_line")

    def __init__(self, name: str, sql: str, start_line: int = 0) -> None:
        self.name = name
        self.sql = sql
        self.start_line = start_line

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SQLFragment):
            return NotImplemented
        return self.name == other.name and self.sql == other.sql

    def __hash__(self) -> int:
        return hash((self.name, self.sql))

    def __repr__(self) -> str:
        return f"SQLFragment(name={self.name!r}, sql={self.sql!r})"


class NamedStatement:
    """Represents a parsed SQL statement with metadata.

    Contains individual SQL statements extracted from files with their
    normalized names, SQL content, optional dialect specifications,
    line position for error reporting, slot declarations from the
    statement's own text, and whether the text contains include markers.
    """

    __slots__ = ("dialect", "has_includes", "name", "parameters", "slots", "sql", "start_line")

    def __init__(
        self,
        name: str,
        sql: str,
        dialect: "str | None" = None,
        start_line: int = 0,
        parameters: "tuple[ParameterDeclaration, ...]" = (),
        slots: "tuple[SlotDeclaration, ...]" = (),
        has_includes: bool = False,
    ) -> None:
        self.name = name
        self.sql = sql
        self.dialect = dialect
        self.start_line = start_line
        self.parameters = parameters
        self.slots = slots
        self.has_includes = has_includes


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
    """Cached SQL file with parsed statements and fragments.

    Stored in the file cache to avoid re-parsing SQL files when their
    content hasn't changed.
    """

    __slots__ = ("parsed_fragments", "parsed_statements", "sql_file", "statement_names")

    def __init__(
        self,
        sql_file: SQLFile,
        parsed_statements: "dict[str, NamedStatement]",
        parsed_fragments: "dict[str, SQLFragment] | None" = None,
    ) -> None:
        """Initialize cached SQL file.

        Args:
            sql_file: Original SQLFile with content and metadata.
            parsed_statements: Named statements from the file.
            parsed_fragments: Fragments from the file.
        """
        self.sql_file = sql_file
        self.parsed_statements = parsed_statements
        self.parsed_fragments: dict[str, SQLFragment] = parsed_fragments if parsed_fragments is not None else {}
        self.statement_names = tuple(parsed_statements.keys())


class SQLFileLoader:
    """Loads and parses SQL files with named SQL queries.

    Loads SQL files containing named queries (using -- name: syntax)
    and retrieves them by name.
    """

    __slots__ = (
        "_compiled_statements",
        "_files",
        "_fragment_to_file",
        "_fragments",
        "_queries",
        "_query_to_file",
        "_resolved_text",
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
        self._fragments: dict[str, SQLFragment] = {}
        self._fragment_to_file: dict[str, str] = {}
        self._resolved_text: dict[str, str] = {}
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
            fragment_names = [name for name, source in self._fragment_to_file.items() if source == path]
            namespaces = {
                name[: -(len(statement.name) + 1)]
                for name in query_names
                if (statement := self._queries.get(name)) is not None and name.endswith(f".{statement.name}")
            }
            namespaces.update(
                name[: -(len(fragment.name) + 1)]
                for name in fragment_names
                if (fragment := self._fragments.get(name)) is not None and name.endswith(f".{fragment.name}")
            )
            namespace = next(iter(namespaces)) if len(namespaces) == 1 else None
            for name in query_names:
                self._queries.pop(name, None)
                self._query_to_file.pop(name, None)
                self._compiled_statements.pop(name, None)
            for name in fragment_names:
                self._fragments.pop(name, None)
                self._fragment_to_file.pop(name, None)
            self._invalidate_resolved()
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
    ) -> "tuple[str | None, tuple[ParameterDeclaration, ...], str, tuple[SlotDeclaration, ...]]":
        """Scan a section's leading comment block for ``dialect``/``param``/``slot`` directives.

        Args:
            statement_section: The statement body including any leading directive lines.
            file_path: File path for error reporting.
            strict: When True, a malformed ``-- param:`` line raises instead of warning.
            base_line: 0-based line offset of ``statement_section`` within the file.

        Returns:
            The resolved dialect, the declared parameters, the SQL body with the
            leading directive/comment lines removed, and the declared slots.

        Raises:
            SQLFileParseError: If ``strict`` and a ``-- param:`` line is malformed, or a
                ``-- slot:`` line is malformed, duplicated, or placed after the SQL body begins.
        """
        dialect: str | None = None
        params: list[ParameterDeclaration] = []
        slots: list[SlotDeclaration] = []
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
            slot_match = SLOT_DIRECTIVE_PATTERN.match(stripped)
            if slot_match:
                slot_name = slot_match.group("name")
                if any(slot.name == slot_name for slot in slots):
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(f"Duplicate -- slot: directive for slot '{slot_name}'"),
                        line=base_line + idx + 1,
                    )
                slots.append(SlotDeclaration(slot_name, slot_match.group("default")))
                continue
            if SLOT_PREFIX_PATTERN.match(stripped):
                raise SQLFileParseError(
                    file_path,
                    file_path,
                    ValueError(f"Malformed -- slot: directive: {stripped}"),
                    line=base_line + idx + 1,
                )
            if PARAM_PREFIX_PATTERN.match(stripped):
                line_number = base_line + idx + 1
                if strict:
                    raise SQLFileParseError(
                        file_path, file_path, ValueError(f"Malformed -- param: directive: {stripped}"), line=line_number
                    )
                log_with_context(
                    logger,
                    logging.WARNING,
                    f"sql.parse.param: malformed parameter directive in {file_path} at line {line_number}: {stripped}",
                    file_path=file_path,
                    line_number=line_number,
                    directive=stripped,
                    status="malformed",
                )
        body_text = "\n".join(raw_lines[body_start:])
        if SLOT_COMMENT_PATTERN.search(body_text) is not None:
            for start, end, is_block in _scan_sql_comments(body_text):
                if is_block or SLOT_COMMENT_PATTERN.match(body_text, start) is None:
                    continue
                line_start = body_text.rfind("\n", 0, start) + 1
                if body_text[line_start:start].strip():
                    continue
                raise SQLFileParseError(
                    file_path,
                    file_path,
                    ValueError(
                        f"-- slot: directive must appear in the leading directive block: {body_text[start:end].strip()}"
                    ),
                    line=base_line + body_start + body_text.count("\n", 0, start) + 1,
                )
        return dialect, tuple(params), "\n".join(raw_lines[body_start:]), tuple(slots)

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
    ) -> "tuple[dict[str, NamedStatement], dict[str, SQLFragment]]":
        """Parse SQL content and extract named statements and fragments.

        A section starts at a ``-- name:`` or ``-- fragment:`` marker and ends at the
        next marker of either kind. Files without any markers are gracefully skipped
        by returning empty dictionaries. The caller is responsible for handling empty
        results appropriately.

        Args:
            content: Raw SQL file content to parse.
            file_path: File path for error reporting.
            strict_parameter_annotations: Raise on malformed parameter declarations instead of skipping them.

        Returns:
            Dictionaries mapping normalized names to NamedStatement and SQLFragment
            objects. Both are empty if no markers are found in the content.

        Raises:
            SQLFileParseError: If sections are malformed (duplicate names, directives
                on fragments, invalid slot directives, or no content after parsing).
        """
        statements: dict[str, NamedStatement] = {}
        fragments: dict[str, SQLFragment] = {}

        section_matches: list[tuple[re.Match[str], bool]] = [
            (match, False) for match in QUERY_NAME_PATTERN.finditer(content)
        ]
        section_matches.extend((match, True) for match in FRAGMENT_NAME_PATTERN.finditer(content))
        if not section_matches:
            return {}, {}
        section_matches.sort(key=_section_match_start)

        for i, (match, is_fragment) in enumerate(section_matches):
            raw_statement_name = match.group(1).strip()
            statement_start_line = content[: match.start()].count("\n")

            start_pos = match.end()
            end_pos = section_matches[i + 1][0].start() if i + 1 < len(section_matches) else len(content)

            section_raw = content[start_pos:end_pos]
            statement_section = section_raw.strip()
            if not raw_statement_name or not statement_section:
                continue

            section_lead = len(section_raw) - len(section_raw.lstrip())
            section_start_line = content[:start_pos].count("\n") + section_raw[:section_lead].count("\n")

            dialect, declared_params, statement_sql, declared_slots = SQLFileLoader._parse_directive_block(
                statement_section, file_path, strict_parameter_annotations or is_fragment, base_line=section_start_line
            )

            clean_sql = SQLFileLoader._strip_leading_comments(statement_sql)
            if not clean_sql:
                continue
            normalized_name = _normalize_query_name(raw_statement_name)

            if is_fragment:
                if dialect is not None or declared_params or declared_slots:
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(
                            f"Fragment '{raw_statement_name}' cannot declare -- dialect:, -- param:, or -- slot: "
                            "directives"
                        ),
                        line=statement_start_line + 1,
                    )
                if normalized_name in fragments:
                    raise SQLFileParseError(
                        file_path,
                        file_path,
                        ValueError(f"Duplicate fragment name: {raw_statement_name}"),
                        line=statement_start_line + 1,
                    )
                fragments[normalized_name] = SQLFragment(
                    name=normalized_name, sql=clean_sql, start_line=statement_start_line
                )
                continue

            if normalized_name in statements:
                raise SQLFileParseError(
                    file_path,
                    file_path,
                    ValueError(f"Duplicate statement name: {raw_statement_name}"),
                    line=statement_start_line + 1,
                )

            slots = _merge_slot_markers(declared_slots, clean_sql)
            has_includes = bool(_find_markers(clean_sql, INCLUDE_MARKER_PATTERN, "include"))
            if not slots and not has_includes:
                SQLFileLoader._check_declared_parameters(
                    clean_sql, declared_params, raw_statement_name, file_path, start_line=statement_start_line
                )

            statements[normalized_name] = NamedStatement(
                name=normalized_name,
                sql=clean_sql,
                dialect=dialect,
                start_line=statement_start_line,
                parameters=declared_params,
                slots=slots,
                has_includes=has_includes,
            )
            log_with_context(
                logger, logging.DEBUG, "sql.parse", file_path=file_path, query_name=normalized_name, dialect=dialect
            )

        if not statements and not fragments:
            raise SQLFileParseError(file_path, file_path, ValueError("No valid SQL statements found after parsing"))

        return statements, fragments

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
                self._register_fragments(cached_file.parsed_fragments, path_str, namespace)
                if runtime is not None:
                    runtime.increment_metric("loader.cache.hit")
                return True

            loaded_statements, loaded_fragments = self._load_uncached_file(file_path, namespace, content=file_content)
        else:
            loaded_statements, loaded_fragments = self._load_uncached_file(file_path, namespace)

        if path_str in self._files:
            sql_file = self._files[path_str]
            cached_file_data = SQLFileCacheEntry(
                sql_file=sql_file, parsed_statements=loaded_statements, parsed_fragments=loaded_fragments
            )
            cache.put_file(cache_key_str, cached_file_data)
            if runtime is not None:
                runtime.increment_metric("loader.cache.miss")
                runtime.increment_metric("loader.files.loaded")
                runtime.increment_metric("loader.statements.loaded", len(loaded_statements))

        return True

    def _load_uncached_file(
        self, file_path: str | Path, namespace: "str | None", content: "str | None" = None
    ) -> "tuple[dict[str, NamedStatement], dict[str, SQLFragment]]":
        """Load a single SQL file without using cache.

        Args:
            file_path: Path to the SQL file.
            namespace: Optional namespace prefix for queries and fragments.
            content: Pre-read file content. If provided, skips the disk read.

        Returns:
            The file's parsed statements and fragments keyed by un-namespaced name;
            both are empty when the file contains no named sections.
        """
        path_str = str(file_path)
        runtime = self._runtime
        if content is None:
            content = self._read_file_content(file_path)
        statements, fragments = self._parse_statements(content, path_str, self.strict_parameter_annotations)

        if not statements and not fragments:
            log_with_context(
                logger, logging.DEBUG, "sql.load", file_path=path_str, status="skipped", reason="no_named_statements"
            )
            return {}, {}

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
        self._register_fragments(fragments, path_str, namespace)
        log_with_context(
            logger, logging.DEBUG, "sql.load", file_path=path_str, statement_count=len(statements), status="loaded"
        )
        if runtime is not None:
            runtime.increment_metric("loader.files.loaded")
            runtime.increment_metric("loader.statements.loaded", len(statements))
        return statements, fragments

    def _register_fragments(self, fragments: "dict[str, SQLFragment]", path_str: str, namespace: "str | None") -> None:
        """Register a file's fragments under their namespaced names.

        Registering any fragment invalidates include-resolved statement text.

        Args:
            fragments: Fragments keyed by un-namespaced name.
            path_str: Source file path.
            namespace: Optional namespace prefix for the fragment names.

        Raises:
            SQLFileParseError: If a fragment name is already registered from another file.
        """
        if not fragments:
            return
        for name, fragment in fragments.items():
            namespaced_name = f"{namespace}.{name}" if namespace else name
            existing_file = self._fragment_to_file.get(namespaced_name)
            if existing_file is not None and existing_file != path_str:
                raise SQLFileParseError(
                    path_str,
                    path_str,
                    ValueError(f"Fragment name '{namespaced_name}' already exists in file: {existing_file}"),
                    line=fragment.start_line + 1,
                )
            self._fragments[namespaced_name] = fragment
            self._fragment_to_file[namespaced_name] = path_str
        self._invalidate_resolved()

    def _invalidate_resolved(self) -> None:
        """Drop include-resolved text and compiled statements that depend on fragments."""
        self._resolved_text.clear()
        stale_names = [
            name
            for name in self._compiled_statements
            if (statement := self._queries.get(name)) is not None and statement.has_includes
        ]
        for name in stale_names:
            del self._compiled_statements[name]

    def add_named_sql(
        self,
        name: str,
        sql: str,
        dialect: "str | None" = None,
        parameters: "Sequence[ParameterDeclaration] | None" = None,
    ) -> None:
        """Add a named SQL query directly without loading from a file.

        The SQL may contain ``/* include: name */`` and ``/* slot: name */`` markers;
        slots added this way have no defaults.

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
        slots = _merge_slot_markers((), clean_sql)
        has_includes = bool(_find_markers(clean_sql, INCLUDE_MARKER_PATTERN, "include"))
        if not slots and not has_includes:
            self._check_declared_parameters(clean_sql, declared, name, "<directly added>")

        statement = NamedStatement(
            name=normalized_name,
            sql=clean_sql,
            dialect=dialect,
            start_line=0,
            parameters=declared,
            slots=slots,
            has_includes=has_includes,
        )
        self._queries[normalized_name] = statement
        self._query_to_file[normalized_name] = "<directly added>"

    def add_fragment(self, name: str, sql: str) -> None:
        """Add a reusable SQL fragment directly without loading from a file.

        Args:
            name: Name for the fragment, referenced by ``/* include: name */`` markers.
            sql: Fragment SQL text; may contain include and slot markers.

        Raises:
            ValueError: If the fragment name already exists.
        """
        normalized_name = _normalize_query_name(name)
        if normalized_name in self._fragments:
            existing_source = self._fragment_to_file.get(normalized_name, "<directly added>")
            msg = f"Fragment name '{name}' already exists (source: {existing_source})"
            raise ValueError(msg)
        self._fragments[normalized_name] = SQLFragment(name=normalized_name, sql=sql.strip())
        self._fragment_to_file[normalized_name] = "<directly added>"
        self._invalidate_resolved()

    def has_fragment(self, name: str) -> bool:
        """Check if a fragment exists.

        Args:
            name: Fragment name to check.

        Returns:
            True if the fragment exists.
        """
        return _normalize_query_name(name) in self._fragments

    def list_fragments(self) -> "list[str]":
        """List all available fragment names.

        Returns:
            Sorted list of fragment names.
        """
        return sorted(self._fragments.keys())

    def get_fragment_text(self, name: str) -> str:
        """Get a fragment's SQL text with its includes resolved.

        Slot markers are left in place.

        Args:
            name: Fragment name.

        Returns:
            Fragment SQL text with ``/* include: */`` markers replaced.

        Raises:
            SQLStatementNotFoundError: If the fragment or an included fragment does not exist.
            SQLFileParseError: If the includes form a cycle.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._fragments:
            raise SQLStatementNotFoundError(
                name=name, normalized_name=safe_name, query_count=len(self._fragments), fragment=True
            )
        return self._resolve_includes(
            self._fragments[safe_name].sql, namespace=_namespace_of(safe_name), stack=(safe_name,)
        )

    def get_query_slots(self, name: str) -> "tuple[SlotDeclaration, ...]":
        """Get the slots of a query, including slots contributed by included fragments.

        Declared slots come first in declaration order, followed by undeclared
        (required) slot markers in order of appearance.

        Args:
            name: Query name (hyphens are converted to underscores).

        Returns:
            Tuple of slot declarations; empty if the query has none.

        Raises:
            SQLStatementNotFoundError: If the query or an included fragment does not exist.
            SQLFileParseError: If a declared slot has no marker or the includes form a cycle.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        statement = self._queries[safe_name]
        if not statement.slots and not statement.has_includes:
            return ()
        return _merge_slot_markers(statement.slots, self._resolve_statement_text(safe_name))

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
        self._fragments.clear()
        self._fragment_to_file.clear()
        self._resolved_text.clear()

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

        Includes are resolved; slot markers are left in place.

        Args:
            name: Query name.

        Returns:
            Raw SQL text.

        Raises:
            SQLStatementNotFoundError: If the query or an included fragment does not exist.
            SQLFileParseError: If a declared slot has no marker or the includes form a cycle.
        """
        safe_name = _normalize_query_name(name)
        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        return self._resolve_statement_text(safe_name)

    def get_sql(self, name: str, **slots: Any) -> "SQL":
        """Get a SQL object by statement name, filling its slots.

        Each ``/* slot: name */`` marker is replaced by the matching keyword value,
        or by the slot's ``-- slot:`` default when no value is given. A value may be
        a ``str`` (spliced verbatim), a sqlglot expression (rendered with the
        statement's dialect), or a ``SQL`` object (its text is spliced and its named
        parameters are bound on the returned statement). Slot values are SQL, not
        data: pass user input as parameters of a ``SQL`` value.

        The statement is cached only when no slot values are given.

        Args:
            name: Name of the statement (from -- name: in SQL file).
                Hyphens in names are converted to underscores.
            **slots: Values for the statement's slots, keyed by slot name.

        Returns:
            SQL object ready for execution.

        Raises:
            SQLSlotError: If a required slot is missing, a slot name is unknown, a
                ``SQL`` value uses positional parameters, or slot parameter names collide
                with each other or with the statement's placeholders.
            TypeError: If a slot value is not a ``str``, sqlglot expression, or ``SQL``.
            SQLFileParseError: If declared parameters do not match the filled SQL or the
                SQL cannot be compiled.
        """
        safe_name = _normalize_query_name(name)

        if safe_name not in self._queries:
            self._raise_statement_not_found(name, safe_name)
        if not slots and safe_name in self._compiled_statements:
            return self._compiled_statements[safe_name]

        parsed_statement = self._queries[safe_name]
        sqlglot_dialect = None
        if parsed_statement.dialect:
            sqlglot_dialect = _normalize_dialect(parsed_statement.dialect)

        statement_text = parsed_statement.sql
        slot_parameters: dict[str, Any] = {}
        if slots or parsed_statement.slots or parsed_statement.has_includes:
            statement_text, slot_parameters = self._fill_slots(
                safe_name, self._resolve_statement_text(safe_name), slots, sqlglot_dialect
            )
            self._check_declared_parameters(
                statement_text,
                parsed_statement.parameters,
                name,
                self._query_to_file.get(safe_name, "<directly added>"),
                start_line=parsed_statement.start_line,
            )

        sql = SQL(statement_text, dialect=sqlglot_dialect, declared_parameters=parsed_statement.parameters)
        try:
            sql.compile()
        except Exception as exc:
            raise SQLFileParseError(name=name, path="<statement>", original_error=exc) from exc
        if slot_parameters:
            return SQL(
                statement_text,
                slot_parameters,
                dialect=sqlglot_dialect,
                declared_parameters=parsed_statement.parameters,
            )
        if not slots:
            self._compiled_statements[safe_name] = sql
        return sql

    def _fill_slots(
        self, safe_name: str, resolved_text: str, provided: "dict[str, Any]", dialect: "str | None"
    ) -> "tuple[str, dict[str, Any]]":
        """Replace slot markers in resolved statement text with provided values or defaults.

        Args:
            safe_name: Normalized, registered statement name.
            resolved_text: Statement text with includes resolved.
            provided: Slot values keyed by slot name.
            dialect: Dialect used to render sqlglot expression values.

        Returns:
            The filled SQL text and the named parameters contributed by ``SQL`` values.

        Raises:
            SQLSlotError: If a slot is unknown or missing, or parameter names collide.
            TypeError: If a slot value has an unsupported type.
        """
        declared = _merge_slot_markers(self._queries[safe_name].slots, resolved_text)
        slot_names = {slot.name for slot in declared}
        unknown_names = sorted(slot_name for slot_name in provided if slot_name not in slot_names)
        if unknown_names:
            raise SQLSlotError(
                safe_name, f"unknown slot(s) {unknown_names}; available slots: {sorted(slot_names) or 'none'}"
            )
        if not declared:
            return resolved_text, {}

        fills: dict[str, str] = {}
        body_fills: dict[str, str] = {}
        merged_parameters: dict[str, Any] = {}
        parameter_sources: dict[str, str] = {}
        for slot in declared:
            if slot.name in provided:
                fill_text, fill_parameters, is_statement = self._render_slot_value(
                    safe_name, slot.name, provided[slot.name], dialect
                )
            elif slot.default is not None:
                fill_text, fill_parameters, is_statement = slot.default, {}, False
            else:
                raise SQLSlotError(safe_name, f"missing required slot '{slot.name}'")
            if "--" in fill_text:
                fill_text = f"{fill_text}\n"
            fills[slot.name] = fill_text
            body_fills[slot.name] = " " if is_statement else fill_text
            for parameter_name, parameter_value in fill_parameters.items():
                if parameter_name in merged_parameters:
                    raise SQLSlotError(
                        safe_name,
                        f"parameter '{parameter_name}' is supplied by both slot "
                        f"'{parameter_sources[parameter_name]}' and slot '{slot.name}'",
                    )
                merged_parameters[parameter_name] = parameter_value
                parameter_sources[parameter_name] = slot.name

        if merged_parameters:
            body_text = _substitute_slot_markers(resolved_text, body_fills)
            body_names = {info.name for info in ParameterValidator().extract_parameters(body_text) if info.name}
            for parameter_name, source_slot in parameter_sources.items():
                if parameter_name in body_names:
                    raise SQLSlotError(
                        safe_name,
                        f"parameter '{parameter_name}' from slot '{source_slot}' collides with a placeholder "
                        "in the statement",
                    )
        return _substitute_slot_markers(resolved_text, fills), merged_parameters

    @staticmethod
    def _render_slot_value(
        safe_name: str, slot_name: str, value: Any, dialect: "str | None"
    ) -> "tuple[str, dict[str, Any], bool]":
        """Render one slot value to SQL text.

        Args:
            safe_name: Normalized statement name for error messages.
            slot_name: Slot being filled.
            value: A ``str``, sqlglot expression, or ``SQL`` object.
            dialect: Dialect used to render sqlglot expressions.

        Returns:
            The SQL text, the named parameters it contributes, and whether the value
            was a ``SQL`` object.

        Raises:
            SQLSlotError: If a ``SQL`` value carries positional parameters.
            TypeError: If the value has an unsupported type.
        """
        if isinstance(value, str):
            return value, {}, False
        if isinstance(value, SQL):
            if value.positional_parameters:
                raise SQLSlotError(
                    safe_name, f"slot '{slot_name}' value uses positional parameters; use named parameters instead"
                )
            return value.sql, dict(value.named_parameters), True
        if isinstance(value, exp.Expr):
            return value.sql(dialect=dialect), {}, False
        msg = (
            f"Statement '{safe_name}' slot '{slot_name}' value must be str, SQL, or a sqlglot expression, "
            f"not {type(value).__name__}"
        )
        raise TypeError(msg)

    def _resolve_statement_text(self, safe_name: str) -> str:
        """Return a loaded statement's text with includes resolved.

        Statements without slots or includes return their parsed text unchanged.
        Resolved text is cached until fragments change.

        Args:
            safe_name: Normalized, registered statement name.

        Returns:
            Statement text with ``/* include: */`` markers replaced.

        Raises:
            SQLStatementNotFoundError: If an included fragment does not exist.
            SQLFileParseError: If a declared slot has no marker or the includes form a cycle.
        """
        statement = self._queries[safe_name]
        if not statement.slots and not statement.has_includes:
            return statement.sql
        cached_text = self._resolved_text.get(safe_name)
        if cached_text is not None:
            return cached_text

        resolved_text = self._resolve_includes(statement.sql, namespace=_namespace_of(safe_name), stack=())
        marker_names = {match.group(1) for match in _find_markers(resolved_text, SLOT_MARKER_PATTERN, "slot")}
        for slot in statement.slots:
            if slot.name not in marker_names:
                file_path = self._query_to_file.get(safe_name, "<directly added>")
                raise SQLFileParseError(
                    file_path,
                    file_path,
                    ValueError(
                        f"Slot '{slot.name}' declared for query '{safe_name}' has no /* slot: {slot.name} */ marker"
                    ),
                    line=statement.start_line + 1,
                )
        self._resolved_text[safe_name] = resolved_text
        return resolved_text

    def _resolve_includes(self, text: str, *, namespace: "str | None", stack: "tuple[str, ...]") -> str:
        """Replace ``/* include: name */`` markers with fragment text, recursively.

        Each include resolves the raw name first, then ``<namespace>.<name>``.

        Args:
            text: SQL text that may contain include markers.
            namespace: Namespace of the statement or fragment containing ``text``.
            stack: Registered fragment names currently being expanded.

        Returns:
            Text with every include marker replaced.

        Raises:
            SQLStatementNotFoundError: If an included fragment does not exist.
            SQLFileParseError: If the includes form a cycle.
        """
        include_markers = _find_markers(text, INCLUDE_MARKER_PATTERN, "include")
        if not include_markers:
            return text
        parts: list[str] = []
        last_end = 0
        for match in include_markers:
            fragment_name = self._find_fragment_name(match.group(1), namespace)
            if fragment_name in stack:
                file_path = self._fragment_to_file.get(fragment_name, "<directly added>")
                cycle = " -> ".join((*stack, fragment_name))
                raise SQLFileParseError(fragment_name, file_path, ValueError(f"Include cycle detected: {cycle}"))
            parts.append(text[last_end : match.start()])
            parts.append(
                self._resolve_includes(
                    self._fragments[fragment_name].sql,
                    namespace=_namespace_of(fragment_name),
                    stack=(*stack, fragment_name),
                )
            )
            last_end = match.end()
        parts.append(text[last_end:])
        return "".join(parts)

    def _find_fragment_name(self, name: str, namespace: "str | None") -> str:
        """Find the registered name of an included fragment.

        Args:
            name: Fragment name from an include marker.
            namespace: Namespace of the text containing the marker.

        Returns:
            The registered fragment name.

        Raises:
            SQLStatementNotFoundError: If neither the raw nor the namespaced name is registered.
        """
        normalized_name = _normalize_query_name(name)
        if normalized_name in self._fragments:
            return normalized_name
        if namespace:
            namespaced_name = f"{namespace}.{normalized_name}"
            if namespaced_name in self._fragments:
                return namespaced_name
        raise SQLStatementNotFoundError(
            name=name, normalized_name=normalized_name, query_count=len(self._fragments), fragment=True
        )


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


def _namespace_of(name: str) -> "str | None":
    """Return the namespace prefix of a registered name, or None when it has none."""
    return name.rpartition(".")[0] or None


def _substitute_slot_markers(text: str, fills: "dict[str, str]") -> str:
    """Replace every ``/* slot: name */`` marker in ``text`` with ``fills[name]``."""
    parts: list[str] = []
    last_end = 0
    for match in _find_markers(text, SLOT_MARKER_PATTERN, "slot"):
        parts.append(text[last_end : match.start()])
        parts.append(fills[match.group(1)])
        last_end = match.end()
    parts.append(text[last_end:])
    return "".join(parts)


def _section_match_start(item: "tuple[re.Match[str], bool]") -> int:
    """Return the start offset of a section marker match."""
    return item[0].start()


def _merge_slot_markers(declared: "tuple[SlotDeclaration, ...]", text: str) -> "tuple[SlotDeclaration, ...]":
    """Append undeclared slot markers found in ``text`` to declared slots as required slots.

    Args:
        declared: Slots already declared, in order.
        text: SQL text to scan for ``/* slot: name */`` markers.

    Returns:
        Declared slots followed by undeclared markers in order of first appearance.
    """
    known_names = {slot.name for slot in declared}
    required: list[SlotDeclaration] = []
    for match in _find_markers(text, SLOT_MARKER_PATTERN, "slot"):
        slot_name = match.group(1)
        if slot_name not in known_names:
            known_names.add(slot_name)
            required.append(SlotDeclaration(slot_name))
    if not required:
        return declared
    return (*declared, *required)


def _scan_sql_comments(text: str) -> "list[tuple[int, int, bool]]":
    """Locate SQL comments outside quoted strings, quoted identifiers, and dollar-quoted bodies.

    Args:
        text: SQL text to scan.

    Returns:
        ``(start, end, is_block)`` for each ``--`` line comment and ``/* */`` block comment.
    """
    comments: list[tuple[int, int, bool]] = []
    length = len(text)
    position = 0
    while position < length:
        match = SQL_LEXEME_PATTERN.search(text, position)
        if match is None:
            break
        lexeme = match.group(0)
        start = match.start()
        if lexeme == "--":
            end = text.find("\n", start)
            end = length if end == -1 else end
            comments.append((start, end, False))
        elif lexeme == "/*":
            end = text.find("*/", start + 2)
            end = length if end == -1 else end + 2
            comments.append((start, end, True))
        elif lexeme in {"'", '"'}:
            end = match.end()
            while True:
                close = text.find(lexeme, end)
                if close == -1:
                    end = length
                    break
                if text.startswith(lexeme, close + 1):
                    end = close + 2
                    continue
                end = close + 1
                break
        else:
            close = text.find(lexeme, match.end())
            end = length if close == -1 else close + len(lexeme)
        position = end
    return comments


def _find_markers(text: str, pattern: "re.Pattern[str]", keyword: str) -> "list[re.Match[str]]":
    """Find ``/* keyword: name */`` markers that are real block comments in SQL text.

    Marker-shaped text inside quoted strings, quoted identifiers, dollar-quoted bodies,
    or line comments is not a marker.

    Args:
        text: SQL text to scan.
        pattern: Pattern matching one complete marker comment.
        keyword: Word that every marker contains, used to skip scanning text without markers.

    Returns:
        Marker matches in order of appearance.
    """
    if keyword not in text or "/*" not in text:
        return []
    markers: list[re.Match[str]] = []
    for start, end, is_block in _scan_sql_comments(text):
        if is_block:
            marker = pattern.fullmatch(text, start, end)
            if marker is not None:
                markers.append(marker)
    return markers

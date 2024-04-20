from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import SQLLoadError, SQLParsingError
from sqlspec.sql.patterns import (
    BAD_PREFIX,
    QUERY_DEF,
    QUERY_OPERATION_NAME,
    QUERY_RECORD_DEF,
    SQL_COMMENT,
    SQL_OPERATION_TYPES,
    UNCOMMENT,
    VAR_REF,
)
from sqlspec.types.protocols import DriverAdapterProtocol, QueryDataTree, QueryDatum, SQLOperationType

try:
    import re2 as re  # pylance: ignore[reportMissingImports]
except ImportError:
    import re

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path

logger = logging.getLogger("sqlspec")


def remove_multiline_comments(code: str) -> str:
    """Remove /* ... */ comments from code"""
    # identify commented regions
    rm = [m.span() for m in UNCOMMENT.finditer(code) if m.groupdict()["multiline"]]
    # keep whatever else
    trimmed_code, current = "", 0
    for start, end in rm:
        trimmed_code += code[current:start]
        current = end
    trimmed_code += code[current:]
    return trimmed_code


class QueryLoader:
    def __init__(
        self,
        driver_adapter: DriverAdapterProtocol,
        record_classes: dict[str, Any] | None,
    ) -> None:
        self.driver_adapter = driver_adapter
        self.record_classes = record_classes if record_classes is not None else {}

    def _make_query_datum(
        self,
        query: str,
        ns_parts: list[str],
        floc: tuple[Path | str, int] | None = None,
    ) -> QueryDatum:
        # Build a query datum
        # - query: the spec and name ("query-name!\n-- comments\nSQL;\n")
        # - ns_parts: name space parts, i.e. subdirectories of loaded files
        # - floc: file name and lineno the query was extracted from
        lines = [line.strip() for line in query.strip().splitlines()]
        qname, qop = self._get_name_op(lines[0])
        if re.search(r"[^A-Za-z0-9_]", qname):
            logger.warning("non ASCII character in query name: %s", qname)
        record_class = self._get_record_class(lines[1])
        sql, doc = self._get_sql_doc(lines[2 if record_class else 1 :])
        signature = self._build_signature(sql)
        query_fqn = ".".join([*ns_parts, qname])
        sql = self.driver_adapter.process_sql(query_fqn, qop, sql)
        return QueryDatum(query_fqn, doc, qop, sql, record_class, signature, floc)

    def _get_name_op(self, text: str) -> tuple[str, SQLOperationType]:
        qname_spec = text.replace("-", "_")
        operation_name = QUERY_OPERATION_NAME.match(qname_spec)
        if not operation_name or BAD_PREFIX.match(qname_spec):
            msg = f'invalid query name and operation spec: "{qname_spec}"'
            raise SQLParsingError(msg)
        qname, qop = operation_name.group(1, 2)
        return qname, SQL_OPERATION_TYPES[qop]

    def _get_record_class(self, text: str) -> type | None:
        rc_match = QUERY_RECORD_DEF.match(text)
        rc_name = rc_match.group(1) if rc_match else None
        # TODO: Probably will want this to be a class, marshal in, and marshal out
        return self.record_classes.get(rc_name) if isinstance(rc_name, str) else None

    def _get_sql_doc(self, lines: Sequence[str]) -> tuple[str, str]:
        doc, sql = "", ""
        for line in lines:
            if doc_match := SQL_COMMENT.match(line):
                doc += doc_match.group(1) + "\n"
            else:
                sql += line + "\n"

        return sql.strip(), doc.rstrip()

    def _build_signature(self, sql: str) -> inspect.Signature:
        params = [inspect.Parameter("self", inspect.Parameter.POSITIONAL_OR_KEYWORD)]
        names = set()
        for match in VAR_REF.finditer(sql):
            gd = match.groupdict()
            if gd["squote"] or gd["dquote"]:
                continue
            name = gd["var_name"]
            if name.isdigit() or name in names:
                continue
            names.add(name)
            params.append(
                inspect.Parameter(
                    name=name,
                    kind=inspect.Parameter.KEYWORD_ONLY,
                ),
            )
        return inspect.Signature(parameters=params)

    def load_query_data_from_sql(
        self,
        sql: str,
        ns_parts: list[str] | None = None,
        fname: Path | None = None,
    ) -> list[QueryDatum]:
        if ns_parts is None:
            ns_parts = []
        trimmed_sql = remove_multiline_comments(sql)
        query_defs = QUERY_DEF.split(trimmed_sql)
        # FIXME lineno is from the uncommented file
        lineno = 1 + query_defs[0].count("\n")
        data = []
        # first item is anything before the first query definition, drop it!
        for query_def in query_defs[1:]:
            data.append(self._make_query_datum(query_def, ns_parts, (fname, lineno) if fname else None))
            lineno += query_def.count("\n")
        return data

    def load_query_data_from_file(
        self,
        path: Path,
        ns_parts: list[str] | None = None,
        encoding: str | None = None,
    ) -> list[QueryDatum]:
        if ns_parts is None:
            ns_parts = []
        return self.load_query_data_from_sql(path.read_text(encoding=encoding), ns_parts, path)

    def load_query_data_from_dir_path(
        self,
        dir_path: Path,
        ext: tuple[str] = (".sql",),
        encoding: str | None = None,
    ) -> QueryDataTree:
        if not dir_path.is_dir():
            msg = f"The path {dir_path} must be a directory"
            raise ValueError(msg)

        def _recurse_load_query_data_tree(
            path: Path, ext: tuple[str], encoding: str | None, ns_parts: list[str] | None = None
        ) -> QueryDatum | dict[str, QueryDatum]:
            if ns_parts is None:
                ns_parts = []
            query_data_tree = {}
            for p in path.iterdir():
                if p.is_file():
                    if p.suffix not in ext:
                        continue
                    for query_datum in self.load_query_data_from_file(
                        p,
                        ns_parts,
                        encoding=encoding,
                    ):
                        query_data_tree[query_datum.query_name] = query_datum
                elif p.is_dir():
                    query_data_tree[p.name] = cast(
                        "QueryDatum",
                        _recurse_load_query_data_tree(
                            path=p,
                            ns_parts=[*ns_parts, p.name],
                            ext=ext,
                            encoding=encoding,
                        ),
                    )
                else:  # pragma: no cover
                    # This should be practically unreachable.
                    msg = f"The path must be a directory or file, got {p}"
                    raise SQLLoadError(msg)
            return query_data_tree

        return cast("QueryDataTree", _recurse_load_query_data_tree(dir_path, ext=ext, encoding=encoding))

"""Shared Snowflake REST compatibility proxy for local Embucket integration tests."""

import contextlib
import gzip
import http.server
import re
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from typing import Any, ClassVar
from uuid import uuid4

from pytest_databases.docker.snowflake import SNOWFLAKE_HTTP_PORT

from sqlspec.utils.serializers import from_json, to_json


class _EmbucketSnowflakeProxyHandler(http.server.BaseHTTPRequestHandler):
    """Translate Snowflake REST v1 qmark bindings and rowset cell types for Embucket."""

    upstream_host: ClassVar[str] = "127.0.0.1"
    upstream_port: ClassVar[int] = SNOWFLAKE_HTTP_PORT
    last_query_parameters: ClassVar[dict[str, Any]] = {}

    def log_message(self, format: str, *args: object) -> None:
        """Silence per-request HTTP proxy stderr logging."""
        _ = (format, args)

    def do_GET(self) -> None:
        """Forward HTTP GET requests to Embucket."""
        self._forward("GET")

    def do_POST(self) -> None:
        """Forward HTTP POST requests to Embucket with Snowflake REST normalization."""
        self._forward("POST")

    def do_DELETE(self) -> None:
        """Forward HTTP DELETE requests to Embucket."""
        self._forward("DELETE")

    def _forward(self, method: str) -> None:
        """Forward an HTTP request to Embucket and normalize query-request payloads."""
        content_length = int(self.headers.get("Content-Length", "0"))
        request_body = self.rfile.read(content_length) if content_length > 0 else b""
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"host", "accept-encoding", "content-length", "transfer-encoding", "connection"}
        }
        if self.headers.get("Content-Encoding", "").lower() == "gzip" and request_body:
            request_body = gzip.decompress(request_body)
            headers.pop("Content-Encoding", None)
            headers.pop("content-encoding", None)

        if method == "POST" and "/queries/v1/query-request" in self.path and request_body:
            request_body = self._rewrite_query_request(request_body)
            payload = from_json(request_body)
            if isinstance(payload, dict) and payload.get("describeOnly"):
                self._send_description()
                return

        upstream_url = f"http://{self.upstream_host}:{self.upstream_port}{self.path}"
        upstream_request = urllib.request.Request(
            upstream_url, data=request_body if method == "POST" else None, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(upstream_request, timeout=30) as response:
                status = response.status
                response_body = response.read()
                response_headers = list(response.headers.items())
        except urllib.error.HTTPError as exc:
            status = exc.code
            response_body = exc.read()
            response_headers = list(exc.headers.items())

        if response_body:
            response_body = self._normalize_response(response_body)

        self.send_response(status)
        for header_name, header_value in response_headers:
            if header_name.lower() not in {"content-length", "transfer-encoding", "content-encoding", "connection"}:
                self.send_header(header_name, header_value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if response_body:
            self.wfile.write(response_body)

    def _send_description(self) -> None:
        """Disable server array binding without executing an Embucket prepare request."""
        body = to_json(
            {
                "success": True,
                "data": {
                    "queryId": str(uuid4()),
                    "sqlState": "00000",
                    "rowtype": [],
                    "rowset": [],
                    "total": 0,
                    "statementTypeId": 0,
                    "arrayBindSupported": False,
                },
            },
            as_bytes=True,
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @classmethod
    def _rewrite_query_request(cls, body: bytes) -> bytes:
        """Substitute Snowflake REST bindings into sqlText for Embucket's DataFusion planner."""
        try:
            payload = from_json(body)
        except Exception:
            return body
        if not isinstance(payload, dict):
            return body
        parameters = payload.get("parameters")
        _EmbucketSnowflakeProxyHandler.last_query_parameters = dict(parameters) if isinstance(parameters, dict) else {}
        sql_text = payload.get("sqlText")
        bindings = payload.get("bindings")
        if not isinstance(sql_text, str) or not isinstance(bindings, dict) or not bindings:
            return body
        array_lengths = [
            len(entry["value"])
            for entry in bindings.values()
            if isinstance(entry, dict) and isinstance(entry.get("value"), list)
        ]
        values_parts = re.split(r"\bVALUES\s*", sql_text, maxsplit=1, flags=re.IGNORECASE)
        if array_lengths and len(values_parts) == 2:
            rows = []
            for index in range(array_lengths[0]):
                row_bindings = {
                    key: {
                        **entry,
                        "value": entry["value"][index] if isinstance(entry.get("value"), list) else entry.get("value"),
                    }
                    for key, entry in bindings.items()
                }
                rows.append(cls._substitute_qmark_bindings(values_parts[1], row_bindings))
            payload["sqlText"] = values_parts[0] + "VALUES " + ", ".join(rows)
        else:
            payload["sqlText"] = cls._substitute_qmark_bindings(sql_text, bindings)
        payload.pop("bindings", None)
        return to_json(payload, as_bytes=True)

    @classmethod
    def _substitute_qmark_bindings(cls, sql_text: str, bindings: dict[str, Any]) -> str:
        """Replace unquoted qmark placeholders with typed Snowflake REST binding literals."""
        output: list[str] = []
        index = 0
        length = len(sql_text)
        binding_position = 1
        in_single_quote = False
        in_double_quote = False

        while index < length:
            char = sql_text[index]
            if char == "'" and not in_double_quote:
                output.append(char)
                if in_single_quote and index + 1 < length and sql_text[index + 1] == "'":
                    output.append("'")
                    index += 2
                    continue
                in_single_quote = not in_single_quote
                index += 1
                continue
            if char == '"' and not in_single_quote:
                output.append(char)
                if in_double_quote and index + 1 < length and sql_text[index + 1] == '"':
                    output.append('"')
                    index += 2
                    continue
                in_double_quote = not in_double_quote
                index += 1
                continue
            if char == "?" and not in_single_quote and not in_double_quote:
                binding_entry = bindings.get(str(binding_position))
                binding_position += 1
                output.append(cls._format_binding_literal(binding_entry))
                index += 1
                continue
            output.append(char)
            index += 1

        return "".join(output)

    @staticmethod
    def _format_binding_literal(binding_entry: Any) -> str:
        """Format a single Snowflake REST binding entry into a SQL literal."""
        if not isinstance(binding_entry, dict):
            return "NULL"
        raw_value = binding_entry.get("value")
        if raw_value is None:
            return "NULL"
        binding_type = str(binding_entry.get("type", "TEXT")).upper()
        text_value = str(raw_value)
        if binding_type in {"FIXED", "REAL"}:
            return text_value
        if binding_type == "BOOLEAN":
            return "TRUE" if text_value.lower() in {"true", "1"} else "FALSE"
        escaped = text_value.replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _normalize_response(body: bytes) -> bytes:
        """Coerce non-null Embucket JSON rowset cells to strings matching Snowflake REST v1."""
        try:
            payload = from_json(body)
        except Exception:
            return body
        if not isinstance(payload, dict):
            return body
        data = payload.get("data")
        if not isinstance(data, dict):
            return body
        if isinstance(data.get("token"), str):
            data.setdefault("masterToken", data["token"])
            data.setdefault("masterValidityInSeconds", 3600)
            data.setdefault("validityInSeconds", 3600)
            data.setdefault("sessionId", 1)
        rowset = data.get("rowset")
        if isinstance(rowset, list):
            data["rowset"] = [
                [None if cell is None else str(cell) for cell in row] if isinstance(row, list) else row
                for row in rowset
            ]
        return to_json(payload, as_bytes=True)


@contextlib.contextmanager
def _provide_embucket_snowflake_proxy(upstream_host: str, upstream_port: int) -> Generator[int, None, None]:
    """Run a background HTTP forwarder normalizing Snowflake REST payloads for Embucket."""

    class _BoundProxyHandler(_EmbucketSnowflakeProxyHandler):
        pass

    _BoundProxyHandler.upstream_host = upstream_host
    _BoundProxyHandler.upstream_port = upstream_port
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _BoundProxyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1])
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

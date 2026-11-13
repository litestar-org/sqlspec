"""Correlation ID extraction from HTTP request headers.

This module provides a reusable correlation ID extractor that can be used
across different web frameworks (Starlette, FastAPI, Flask) to extract
correlation IDs from request headers with consistent behavior.
"""

import re
from collections.abc import Callable
from typing import ClassVar, Final

from mypy_extensions import mypyc_attr

from sqlspec.utils.correlation import CorrelationContext

__all__ = ("CorrelationExtractor",)

_CORRELATION_VALUE_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._:/=;+-]+$")


@mypyc_attr(allow_interpreted_subclasses=False)
class CorrelationExtractor:
    """Extracts correlation IDs from HTTP request headers.

    This class provides configurable header extraction with:
        - Configurable primary header (highest priority)
        - Additional custom headers (middle priority)
        - Standard trace context headers as fallbacks
        - Automatic UUID generation when no header found
        - Input sanitization (max length, whitespace trimming)

    The extractor follows a priority order: primary header first, then
    configured headers, then trace context fallbacks (if enabled).
    """

    __slots__ = ("_headers", "_max_length")

    DEFAULT_HEADERS: ClassVar[tuple[str, ...]] = (
        "x-request-id",
        "x-correlation-id",
        "traceparent",
        "x-cloud-trace-context",
        "x-amzn-trace-id",
        "x-b3-traceid",
        "x-client-trace-id",
        "grpc-trace-bin",
    )
    """
    Default trace context headers to check as fallbacks.

    These headers cover:
        - x-request-id: Common request ID header
        - x-correlation-id: Common correlation ID header
        - traceparent: W3C Trace Context standard
        - x-cloud-trace-context: Google Cloud trace header
        - x-amzn-trace-id: AWS X-Ray trace header
        - x-b3-traceid: Zipkin B3 propagation
        - x-client-trace-id: Envoy proxy trace header
        - grpc-trace-bin: gRPC binary trace header
    """

    DEFAULT_MAX_LENGTH: ClassVar[int] = 128
    """Maximum length for bounded, single-line log-safe correlation identifiers."""

    def __init__(
        self,
        *,
        primary_header: str = "x-request-id",
        additional_headers: tuple[str, ...] | None = None,
        auto_trace_headers: bool = True,
        max_length: int | None = None,
    ) -> None:
        """Initialize the correlation extractor.

        Args:
            primary_header: The primary header to check first. Defaults to "x-request-id".
            additional_headers: Additional headers to check after the primary header.
            auto_trace_headers: If True, include standard trace context headers as fallbacks.
            max_length: Maximum length for correlation IDs. Defaults to 128.
        """
        headers: list[str] = [primary_header.lower()]

        if additional_headers:
            headers.extend(h.lower() for h in additional_headers)

        if auto_trace_headers:
            headers.extend(self.DEFAULT_HEADERS)

        self._headers = tuple(dict.fromkeys(headers))
        self._max_length = max_length if max_length is not None else self.DEFAULT_MAX_LENGTH

    def __repr__(self) -> str:
        return f"CorrelationExtractor(headers={self._headers!r}, max_length={self._max_length!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CorrelationExtractor):
            return NotImplemented
        return self._headers == other._headers and self._max_length == other._max_length

    def __hash__(self) -> int:
        return hash((self._headers, self._max_length))

    @property
    def headers(self) -> tuple[str, ...]:
        """Get the ordered list of headers to check."""
        return self._headers

    @property
    def max_length(self) -> int:
        """Get the maximum correlation ID length."""
        return self._max_length

    def extract(self, get_header: Callable[[str], str | None]) -> str:
        """Extract correlation ID from headers or generate a new one.

        Iterates through configured headers in priority order and returns
        the first non-empty value found. If no header contains a value,
        generates a new UUID.

        Args:
            get_header: A callable that takes a header name (lowercase) and
                returns the header value or None.

        Returns:
            The extracted or generated correlation ID.
        """
        for header in self._headers:
            value = get_header(header)
            if value:
                cleaned = self._clean_value(value)
                if cleaned:
                    return cleaned

        return CorrelationContext.generate()

    def _clean_value(self, value: str) -> str:
        """Truncate at the first line break, trim, bound, and validate trace characters."""
        first_line = value.split("\r", 1)[0].split("\n", 1)[0]
        candidate = first_line.strip()[: self._max_length]
        if _CORRELATION_VALUE_PATTERN.fullmatch(candidate) is None:
            return ""
        return candidate

    def _sanitize(self, value: str) -> str:
        """Return a bounded, single-line log-safe correlation identifier.

        Truncates at the first line break, strips surrounding whitespace, bounds to
        ``max_length``, and falls back to UUID generation when disallowed characters
        are present or the resulting string is empty.

        Args:
            value: The raw correlation ID value.

        Returns:
            The validated correlation ID, or a generated UUID if invalid or empty.
        """
        return self._clean_value(value) or CorrelationContext.generate()

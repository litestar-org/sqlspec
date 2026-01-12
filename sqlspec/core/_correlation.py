"""Correlation ID extraction from HTTP request headers.

This module provides a reusable correlation ID extractor that can be used
across different web frameworks (Starlette, FastAPI, Flask) to extract
correlation IDs from request headers with consistent behavior.
"""

from collections.abc import Callable
from typing import ClassVar

from sqlspec.utils.correlation import CorrelationContext

__all__ = ("CorrelationExtractor",)


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

    Example:
        ```python
        extractor = CorrelationExtractor(
            primary_header="x-request-id",
            additional_headers=("x-correlation-id",),
            auto_trace_headers=True,
        )

        # In Starlette/FastAPI:
        correlation_id = extractor.extract(
            lambda h: request.headers.get(h)
        )

        # In Flask:
        correlation_id = extractor.extract(
            lambda h: request.headers.get(h)
        )
        ```
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
    """Default trace context headers to check as fallbacks.

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
    """Maximum length for correlation IDs to prevent log injection."""

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

        # Remove duplicates while preserving order
        self._headers = tuple(dict.fromkeys(headers))
        self._max_length = max_length if max_length is not None else self.DEFAULT_MAX_LENGTH

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

        Example:
            ```python
            # With Starlette Request
            correlation_id = extractor.extract(
                lambda h: request.headers.get(h)
            )

            # With Flask request
            from flask import request

            correlation_id = extractor.extract(
                lambda h: request.headers.get(h)
            )
            ```
        """
        for header in self._headers:
            value = get_header(header)
            if value:
                return self._sanitize(value)

        return CorrelationContext.generate()

    def _sanitize(self, value: str) -> str:
        """Sanitize a correlation ID value.

        Strips whitespace and truncates to max length to prevent
        log injection attacks and excessively long IDs.

        Args:
            value: The raw correlation ID value.

        Returns:
            The sanitized correlation ID, or a generated UUID if
            the sanitized value is empty.
        """
        sanitized = value.strip()[: self._max_length]
        return sanitized if sanitized else CorrelationContext.generate()

    def __repr__(self) -> str:
        return f"CorrelationExtractor(headers={self._headers!r}, max_length={self._max_length!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CorrelationExtractor):
            return NotImplemented
        return self._headers == other._headers and self._max_length == other._max_length

    def __hash__(self) -> int:
        return hash((self._headers, self._max_length))

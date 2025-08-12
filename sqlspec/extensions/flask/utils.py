"""Utility mixins and helpers for SQLSpec Flask integration."""

from typing import Any

__all__ = ("FlaskServiceMixin",)


class FlaskServiceMixin:
    """Mixin providing Flask-specific utilities for SQLSpec services."""

    def jsonify(self, data: Any, status: int = 200, **kwargs: Any) -> Any:
        """Create a JSON response using Flask's response system.

        Args:
            data: Data to serialize to JSON.
            status: HTTP status code for the response.
            **kwargs: Additional arguments to pass to the response.

        Returns:
            Flask response object with JSON content.
        """
        from flask import jsonify as flask_jsonify

        response = flask_jsonify(data)
        response.status_code = status

        # Apply any additional response modifications
        for key, value in kwargs.items():
            setattr(response, key, value)

        return response

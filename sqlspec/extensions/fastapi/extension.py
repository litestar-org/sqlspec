"""SQLSpec extension for FastAPI applications."""

from typing import TYPE_CHECKING

from sqlspec.extensions.starlette.extension import SQLSpec as StarletteExtension

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ("SQLSpec",)


class SQLSpec(StarletteExtension):
    """SQLSpec integration for FastAPI applications.

    FastAPI is built on Starlette, so this extension inherits all functionality
    from the Starlette extension. The only difference is the type hints for
    the init_app method to accept FastAPI apps specifically.
    """

    def init_app(self, app: "FastAPI") -> None:  # pyright: ignore
        """Initialize SQLSpec with FastAPI application.

        Args:
            app: The FastAPI application instance.
        """
        # FastAPI apps are compatible with Starlette, so delegate to parent
        super().init_app(app)  # type: ignore[arg-type]

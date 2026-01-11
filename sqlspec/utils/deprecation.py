"""Deprecation utilities for SQLSpec.

Provides decorators and warning functions for marking deprecated functionality.
Used to communicate API changes and migration paths to users.
"""

import inspect
from collections.abc import Callable
from functools import WRAPPER_ASSIGNMENTS, WRAPPER_UPDATES
from typing import Generic, Literal, cast
from warnings import warn

from typing_extensions import ParamSpec, TypeVar

__all__ = ("deprecated", "warn_deprecation")


T = TypeVar("T")
P = ParamSpec("P")
DeprecatedKind = Literal["function", "method", "classmethod", "attribute", "property", "class", "parameter", "import"]


def warn_deprecation(
    version: str,
    deprecated_name: str,
    kind: DeprecatedKind,
    *,
    removal_in: str | None = None,
    alternative: str | None = None,
    info: str | None = None,
    pending: bool = False,
    stacklevel: int = 2,
) -> None:
    """Warn about a call to a deprecated function.

    Args:
        version: SQLSpec version where the deprecation will occur
        deprecated_name: Name of the deprecated function
        removal_in: SQLSpec version where the deprecated function will be removed
        alternative: Name of a function that should be used instead
        info: Additional information
        pending: Use :class:`warnings.PendingDeprecationWarning` instead of :class:`warnings.DeprecationWarning`
        kind: Type of the deprecated thing
        stacklevel: Warning stacklevel to report the correct caller site.
    """
    parts = []

    if kind == "import":
        access_type = "Import of"
    elif kind in {"function", "method"}:
        access_type = "Call to"
    else:
        access_type = "Use of"

    if pending:
        parts.append(f"{access_type} {kind} awaiting deprecation '{deprecated_name}'")  # pyright: ignore[reportUnknownMemberType]
    else:
        parts.append(f"{access_type} deprecated {kind} '{deprecated_name}'")  # pyright: ignore[reportUnknownMemberType]

    parts.extend(  # pyright: ignore[reportUnknownMemberType]
        (f"Deprecated in SQLSpec {version}", f"This {kind} will be removed in {removal_in or 'the next major version'}")
    )
    if alternative:
        parts.append(f"Use {alternative!r} instead")  # pyright: ignore[reportUnknownMemberType]

    if info:
        parts.append(info)  # pyright: ignore[reportUnknownMemberType]

    text = ". ".join(parts)  # pyright: ignore[reportUnknownArgumentType]
    warning_class = PendingDeprecationWarning if pending else DeprecationWarning

    warn(text, warning_class, stacklevel=stacklevel)


def deprecated(
    version: str,
    *,
    removal_in: str | None = None,
    alternative: str | None = None,
    info: str | None = None,
    pending: bool = False,
    kind: Literal["function", "method", "classmethod", "property"] | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Create a decorator wrapping a function, method or property with a deprecation warning.

    Args:
        version: SQLSpec version where the deprecation will occur
        removal_in: SQLSpec version where the deprecated function will be removed
        alternative: Name of a function that should be used instead
        info: Additional information
        pending: Use :class:`warnings.PendingDeprecationWarning` instead of :class:`warnings.DeprecationWarning`
        kind: Type of the deprecated callable. If ``None``, will use ``inspect`` to figure
            out if it's a function or method

    Returns:
        A decorator wrapping the function call with a warning
    """

    return cast(
        "Callable[[Callable[P, T]], Callable[P, T]]",
        _DeprecatedFactory(
            version=version, removal_in=removal_in, alternative=alternative, info=info, pending=pending, kind=kind
        ),
    )


class _DeprecatedWrapper(Generic[P, T]):
    __slots__ = ("__dict__", "_alternative", "_func", "_info", "_kind", "_pending", "_removal_in", "_version")

    def __init__(
        self,
        func: Callable[P, T],
        *,
        version: str,
        removal_in: str | None,
        alternative: str | None,
        info: str | None,
        pending: bool,
        kind: Literal["function", "method", "classmethod", "property"] | None,
    ) -> None:
        self._func = func
        self._version = version
        self._removal_in = removal_in
        self._alternative = alternative
        self._info = info
        self._pending = pending
        self._kind = kind

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        kind = cast("DeprecatedKind", self._kind or ("method" if inspect.ismethod(self._func) else "function"))
        warn_deprecation(
            version=self._version,
            deprecated_name=self._func.__name__,
            info=self._info,
            alternative=self._alternative,
            pending=self._pending,
            removal_in=self._removal_in,
            kind=kind,
        )
        return self._func(*args, **kwargs)


class _DeprecatedFactory(Generic[P, T]):
    __slots__ = ("_alternative", "_info", "_kind", "_pending", "_removal_in", "_version")

    def __init__(
        self,
        *,
        version: str,
        removal_in: str | None,
        alternative: str | None,
        info: str | None,
        pending: bool,
        kind: Literal["function", "method", "classmethod", "property"] | None,
    ) -> None:
        self._version = version
        self._removal_in = removal_in
        self._alternative = alternative
        self._info = info
        self._pending = pending
        self._kind: Literal["function", "method", "classmethod", "property"] | None = kind

    def __call__(self, func: Callable[P, T]) -> Callable[P, T]:
        wrapper = _DeprecatedWrapper(
            func,
            version=self._version,
            removal_in=self._removal_in,
            alternative=self._alternative,
            info=self._info,
            pending=self._pending,
            kind=self._kind,
        )
        return cast("Callable[P, T]", _copy_wrapper_metadata(wrapper, func))


def _copy_wrapper_metadata(wrapper: "_DeprecatedWrapper[P, T]", func: "Callable[P, T]") -> "_DeprecatedWrapper[P, T]":
    assignments: tuple[str, ...] = tuple(WRAPPER_ASSIGNMENTS)
    updates: tuple[str, ...] = tuple(WRAPPER_UPDATES)
    wrapper_dict = wrapper.__dict__
    wrapper_dict.update(getattr(func, "__dict__", {}))
    for attr in assignments:
        if hasattr(func, attr):
            wrapper_dict[attr] = getattr(func, attr)
    wrapper_dict["__wrapped__"] = func
    for attr in updates:
        if attr == "__dict__":
            continue
        if hasattr(func, attr):
            wrapper_dict[attr] = getattr(func, attr)
    return wrapper

import inspect
from functools import wraps
from typing import Callable, Literal, Optional
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
    removal_in: Optional[str] = None,
    alternative: Optional[str] = None,
    info: Optional[str] = None,
    pending: bool = False,
) -> None:
    """Warn about a call to a (soon to be) deprecated function.

    Args:
        version: Advanced Alchemy version where the deprecation will occur
        deprecated_name: Name of the deprecated function
        removal_in: Advanced Alchemy version where the deprecated function will be removed
        alternative: Name of a function that should be used instead
        info: Additional information
        pending: Use :class:`warnings.PendingDeprecationWarning` instead of :class:`warnings.DeprecationWarning`
        kind: Type of the deprecated thing
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

    warn(text, warning_class, stacklevel=2)


def deprecated(
    version: str,
    *,
    removal_in: Optional[str] = None,
    alternative: Optional[str] = None,
    info: Optional[str] = None,
    pending: bool = False,
    kind: Optional[Literal["function", "method", "classmethod", "property"]] = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Create a decorator wrapping a function, method or property with a warning call about a (pending) deprecation.

    Args:
        version: Litestar version where the deprecation will occur
        removal_in: Litestar version where the deprecated function will be removed
        alternative: Name of a function that should be used instead
        info: Additional information
        pending: Use :class:`warnings.PendingDeprecationWarning` instead of :class:`warnings.DeprecationWarning`
        kind: Type of the deprecated callable. If ``None``, will use ``inspect`` to figure
            out if it's a function or method

    Returns:
        A decorator wrapping the function call with a warning
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
            warn_deprecation(
                version=version,
                deprecated_name=func.__name__,
                info=info,
                alternative=alternative,
                pending=pending,
                removal_in=removal_in,
                kind=kind or ("method" if inspect.ismethod(func) else "function"),
            )
            return func(*args, **kwargs)

        return wrapped

    return decorator

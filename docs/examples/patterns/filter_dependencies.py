"""Generate Litestar query-parameter filter dependencies from a declarative FilterConfig."""

from typing import Any

from sqlspec.extensions.litestar.providers import FilterConfig, create_filter_dependencies

__all__ = ("main",)


def main() -> dict[str, Any]:
    """Build filter, sort, search, and pagination dependency providers for a users endpoint."""
    user_filters: FilterConfig = {
        "id_filter": str,
        "id_field": "id",
        "sort_field": ["created_at", "uploaded_collections", "name"],
        "sort_field_aliases": {"lastUpload": "uploaded_collections"},
        "sort_order": "desc",
        "pagination_type": "limit_offset",
        "pagination_size": 20,
        "search": "name,email",
        "search_ignore_case": True,
        "created_at": True,
        "updated_at": True,
    }
    deps = create_filter_dependencies(user_filters)
    assert deps
    return deps


if __name__ == "__main__":
    print(f"Generated {len(main())} filter dependencies")

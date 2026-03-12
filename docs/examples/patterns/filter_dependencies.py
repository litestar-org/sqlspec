from __future__ import annotations

__all__ = ("show_filter_dependencies",)


def show_filter_dependencies() -> None:
    # start-example
    from sqlspec.extensions.litestar.providers import FilterConfig, create_filter_dependencies

    # Define filter configuration for a "users" endpoint
    user_filters: FilterConfig = {
        "id_filter": str,  # Filter by user IDs
        "id_field": "id",  # Column name for ID filter
        "sort_field": "created_at",  # Default sort column
        "sort_order": "desc",  # Default sort direction
        "pagination_type": "limit_offset",  # Enable pagination
        "pagination_size": 20,  # Default page size
        "search": "name,email",  # Searchable fields
        "search_ignore_case": True,  # Case-insensitive search
        "created_at": True,  # Enable created_at range filter
        "updated_at": True,  # Enable updated_at range filter
    }

    # Generate Litestar dependency providers
    deps = create_filter_dependencies(user_filters)
    print(f"Generated {len(deps)} filter dependencies")
    # end-example

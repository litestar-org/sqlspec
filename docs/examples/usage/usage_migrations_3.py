# start-example
__all__ = ("test_template_config",)


migration_config = {
    "default_format": "py",  # CLI default when --format omitted
    "title": "Acme Migration",  # Shared title for all templates
    "author": "env:SQLSPEC_AUTHOR",  # Read from environment variable
    "templates": {
        "sql": {
            "header": "-- {title} - {message}",
            "metadata": ["-- Version: {version}", "-- Owner: {author}"],
            "body": "-- custom SQL body",
        },
        "py": {
            "docstring": """{title}\nDescription: {description}""",
            "imports": ["from typing import Iterable"],
            "body": """def up(context: object | None = None) -> str | Iterable[str]:\n    return \"SELECT 1\"\n\ndef down(context: object | None = None) -> str | Iterable[str]:\n    return \"DROP TABLE example;\"\n""",
        },
    },
}
# end-example


def test_template_config() -> None:
    # Check structure of migration_config
    assert migration_config["default_format"] == "py"
    assert "py" in migration_config["templates"]
    assert "sql" in migration_config["templates"]
    assert isinstance(migration_config["templates"]["py"], dict)
    assert isinstance(migration_config["templates"]["sql"], dict)

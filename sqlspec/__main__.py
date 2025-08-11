from sqlspec.cli import add_migration_commands as build_cli_interface

__all__ = ("run_cli",)


def run_cli() -> None:  # pragma: no cover
    """SQLSpec CLI."""
    build_cli_interface()()


if __name__ == "__main__":  # pragma: no cover
    run_cli()

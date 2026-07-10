"""Regression tests for Litestar CLI integration."""

from pathlib import Path

from click.testing import CliRunner
from litestar import Litestar
from litestar.cli._utils import LitestarEnv, LitestarGroup

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.litestar import SQLSpecPlugin


def test_database_commands_receive_litestar_context_once() -> None:
    """LitestarGroup and SQLSpec must not both inject the Click context."""
    config = SqliteConfig(connection_config={"database": ":memory:"}, migration_config={"enabled": True})
    sqlspec = SQLSpec()
    sqlspec.add_config(config)
    plugin = SQLSpecPlugin(sqlspec=sqlspec)
    app = Litestar(route_handlers=[], plugins=[plugin])
    env = LitestarEnv(app_path="audit:app", app=app, cwd=Path.cwd(), is_app_factory=False)
    cli = LitestarGroup(name="litestar")
    plugin.on_cli_init(cli)

    result = CliRunner().invoke(cli, ["db", "show-config"], obj=env)

    assert result.exit_code == 0, result.output
    assert "Migration Configurations" in result.output

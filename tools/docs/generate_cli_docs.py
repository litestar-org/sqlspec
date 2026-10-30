"""Generate pure-Markdown CLI reference documentation from sqlspec.cli."""

import argparse
import re
import sys
from pathlib import Path

import click

from sqlspec.cli import cli

__all__ = ("DEFAULT_OUTPUT_PATH", "main", "render_cli_reference")

DEFAULT_OUTPUT_PATH = Path("docs/reference/cli.md")

_RICH_TAG_PATTERN = re.compile(
    r"\[/?(?:dim|bold|italic|underline|cyan|green|yellow|red|blue|magenta|white)(?:\s+[a-z]+)?\]"
)
_ENV_HINT_PATTERN = re.compile(r"\(env:\s*([A-Z0-9_]+)\)")
_INSPECTION_COMMANDS: tuple[str, ...] = ("show-config", "show-current-revision", "init")
_MIGRATION_COMMANDS: tuple[str, ...] = ("create-migration", "upgrade", "downgrade", "stamp", "squash", "fix")


def render_cli_reference(group: click.Group | None = None) -> str:
    """Render the complete docs/reference/cli.md Markdown document from the Click command tree.

    Args:
        group: Optional Click group to introspect. Defaults to ``sqlspec.cli.cli``.

    Returns:
        Complete Markdown string for ``docs/reference/cli.md``.
    """
    root_group = group if group is not None else cli
    lines: list[str] = [
        "# CLI Reference",
        "",
        "```python",
        "from sqlspec.cli import add_migration_commands, cli, get_sqlspec_group",
        "```",
        "",
        "Command-line interface for inspecting configurations, managing schema migrations, and maintaining extension stores. See the [Migrations Guide](../learn/migrations.md) and [CLI Guide](../learn/cli.md) for workflow examples.",
        "",
        "## Global Options",
        "",
        "```bash",
        "sqlspec [OPTIONS] COMMAND [ARGS]...",
        "```",
        "",
    ]
    global_help = _clean_text(root_group.help or "")
    if global_help:
        lines.extend([global_help, ""])

    global_options = [p for p in root_group.params if isinstance(p, click.Option) and p.name != "help"]
    if global_options:
        lines.extend(_render_options_table(global_options))
        lines.append("")

    flat_commands = _collect_leaf_commands(root_group, prefix=root_group.name or "sqlspec")
    command_map = dict(flat_commands)
    rendered_names: set[str] = set()

    lines.extend(["## Inspection & Setup", ""])
    for short_name in _INSPECTION_COMMANDS:
        full_name = f"sqlspec {short_name}"
        if full_name in command_map:
            rendered_names.add(full_name)
            lines.extend(_render_command_section(full_name, command_map[full_name]))

    lines.extend(["## Schema Migrations", ""])
    for short_name in _MIGRATION_COMMANDS:
        full_name = f"sqlspec {short_name}"
        if full_name in command_map:
            rendered_names.add(full_name)
            lines.extend(_render_command_section(full_name, command_map[full_name]))

    remaining = [(full_name, cmd) for full_name, cmd in flat_commands if full_name not in rendered_names]
    if remaining:
        lines.extend(["## ADK Extension Commands", ""])
        for full_name, cmd in remaining:
            lines.extend(_render_command_section(full_name, cmd))

    lines.extend([
        "## Programmatic CLI Factory",
        "",
        "::: sqlspec.cli.get_sqlspec_group",
        "    options:",
        "      heading_level: 3",
        "",
        "::: sqlspec.cli.add_migration_commands",
        "    options:",
        "      heading_level: 3",
    ])
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for generating or verifying docs/reference/cli.md.

    Args:
        argv: Optional command-line arguments.

    Returns:
        Process exit code (0 on success, 1 when stale or missing under ``--check``).
    """
    parser = argparse.ArgumentParser(description="Generate or check docs/reference/cli.md.")
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Target Markdown path (default: docs/reference/cli.md).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that the target file matches the generated CLI reference without writing.",
    )
    args = parser.parse_args(argv)
    output_path: Path = args.output
    expected = render_cli_reference()

    if args.check:
        if not output_path.is_file():
            sys.stderr.write(f"CLI reference file is missing: {output_path}\n")
            return 1
        actual = output_path.read_text(encoding="utf-8")
        if actual != expected:
            sys.stderr.write(
                f"CLI reference file is out of date: {output_path}. "
                "Run 'uv run python -m tools.docs.generate_cli_docs' to update.\n"
            )
            return 1
        sys.stdout.write(f"CLI reference is up to date: {output_path}\n")
        return 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(expected, encoding="utf-8")
    sys.stdout.write(f"Wrote CLI reference: {output_path}\n")
    return 0


def _collect_leaf_commands(group: click.Group, prefix: str) -> list[tuple[str, click.Command]]:
    """Recursively collect leaf Click commands in registration order."""
    collected: list[tuple[str, click.Command]] = []
    for name, subcommand in group.commands.items():
        full_name = f"{prefix} {name}"
        if isinstance(subcommand, click.Group):
            collected.extend(_collect_leaf_commands(subcommand, prefix=full_name))
        else:
            collected.append((full_name, subcommand))
    return collected


def _render_command_section(full_name: str, command: click.Command) -> list[str]:
    """Render a single Click leaf command as a level-4 Markdown section."""
    arguments = [p for p in command.params if isinstance(p, click.Argument)]
    options = [p for p in command.params if isinstance(p, click.Option) and p.name != "help"]

    arg_synopsis = " ".join(
        f"<{arg.name.upper()}>" if arg.required else f"[{arg.name.upper()}]"
        for arg in arguments
        if arg.name is not None
    )
    opt_synopsis = " [OPTIONS]" if options else ""
    usage_line = f"{full_name}{opt_synopsis}" + (f" {arg_synopsis}" if arg_synopsis else "")

    summary = _clean_text(command.help or command.short_help or "")
    section: list[str] = [f"#### `{full_name}`", ""]
    if summary:
        section.extend([summary, ""])
    section.extend(["```bash", usage_line, "```", ""])
    if arguments:
        section.extend(_render_arguments_table(arguments))
        section.append("")
    if options:
        section.extend(_render_options_table(options))
        section.append("")
    return section


def _render_arguments_table(arguments: list[click.Argument]) -> list[str]:
    """Render a Markdown table for positional Click arguments."""
    rows: list[str] = ["| Argument | Type | Required | Default |", "| --- | --- | --- | --- |"]
    for arg in arguments:
        name = f"`{arg.name}`" if arg.name else "`arg`"
        type_name = f"`{_format_param_type(arg)}`"
        required = "Yes" if arg.required else "No"
        default = _format_default(arg.default, is_flag=False)
        rows.append(f"| {name} | {type_name} | {required} | {default} |")
    return rows


def _render_options_table(options: list[click.Option]) -> list[str]:
    """Render a Markdown table for Click options."""
    rows: list[str] = ["| Option | Type | Default | Description |", "| --- | --- | --- | --- |"]
    for opt in options:
        flags = ", ".join(f"`{flag}`" for flag in [*opt.opts, *opt.secondary_opts])
        type_name = "`flag`" if opt.is_flag else f"`{_format_param_type(opt)}`"
        default = _format_default(opt.default, is_flag=opt.is_flag)
        description = _clean_single_line(opt.help or "")
        if opt.required:
            description = f"**Required.** {description}".strip()
        rows.append(f"| {flags} | {type_name} | {default} | {description} |")
    return rows


def _format_param_type(param: click.Parameter) -> str:
    """Return a clean human-readable type name for a Click parameter."""
    if isinstance(param.type, click.Choice):
        choices = " | ".join(str(c) for c in param.type.choices)
        return f"choice({choices})"
    type_name = getattr(param.type, "name", None) or type(param.type).__name__
    return str(type_name).lower()


def _format_default(default: object, *, is_flag: bool) -> str:
    """Format a Click parameter default while filtering Click 8.3+ Sentinel.UNSET objects."""
    if default is None:
        return "`False`" if is_flag else "-"
    type_name = type(default).__name__
    repr_text = repr(default)
    if "Sentinel" in type_name or "UNSET" in repr_text:
        return "`False`" if is_flag else "-"
    if isinstance(default, bool):
        return f"`{default}`"
    if isinstance(default, (int, float)):
        return f"`{default}`"
    if isinstance(default, str):
        return f'`"{default}"`' if default else '`""`'
    return "-"


def _clean_text(text: str) -> str:
    """Strip Rich console tags and normalize whitespace while preserving paragraphs."""
    stripped = _RICH_TAG_PATTERN.sub("", text).strip()
    stripped = _ENV_HINT_PATTERN.sub(r"(env: `\1`)", stripped)
    paragraphs = [
        " ".join(line.strip() for line in block.splitlines() if line.strip()) for block in stripped.split("\n\n")
    ]
    return "\n\n".join(p for p in paragraphs if p)


def _clean_single_line(text: str) -> str:
    """Strip Rich console tags and collapse newlines/pipes for Markdown table cells."""
    cleaned = _clean_text(text)
    single_line = " ".join(cleaned.split())
    return single_line.replace("|", "\\|")


if __name__ == "__main__":
    raise SystemExit(main())

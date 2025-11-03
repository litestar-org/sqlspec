"""A tool to generate context files for LLMs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rich.console import Console

# ==============================================================================
# Constants
# ==============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_FILE_NAME = ".llms-generator.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT

# ==============================================================================
# Helper Functions
# ==============================================================================


def load_config(config_path: Path) -> dict[str, list[str]]:
    """Load and parse the JSON configuration file.

    Args:
        config_path: The path to the configuration file.

    Returns:
        A dictionary containing the configuration.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with config_path.open("r") as f:
        return json.load(f)


def process_files(file_patterns: list[str], project_root: Path) -> str:
    """Read and format content from a list of files and glob patterns.

    Args:
        file_patterns: A list of file paths and glob patterns.
        project_root: The root directory of the project.

    Returns:
        A single string with all file contents concatenated and formatted.
    """
    content_blocks = []
    for pattern in file_patterns:
        for file_path in project_root.glob(pattern):
            if file_path.is_file():
                relative_path = file_path.relative_to(project_root)
                content = file_path.read_text(encoding="utf-8")
                block = (
                    f"\n--- START OF FILE: {relative_path} ---\n"
                    f"{content.strip()}\n"
                    f"--- END OF FILE: {relative_path} ---\n"
                )
                content_blocks.append(block)
    return "".join(content_blocks)


def generate_context_files() -> None:
    """Main function to generate the LLM context files."""
    console = Console()

    parser = argparse.ArgumentParser(description="Generate context files for LLMs.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"The directory to write the output files to. Defaults to the project root: {DEFAULT_OUTPUT_DIR}",
    )
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    output_dir.mkdir(exist_ok=True)

    config_path = PROJECT_ROOT / CONFIG_FILE_NAME

    try:
        with console.status("[bold green]Loading configuration...[/]"):
            config = load_config(config_path)
        console.print(f"[green]✓[/] Loaded configuration from [cyan]{config_path}[/]")

        # Generate llms.txt (Concise)
        with console.status("[bold green]Generating llms.txt...[/]"):
            concise_patterns = config.get("concise_context", [])
            concise_content = process_files(concise_patterns, PROJECT_ROOT)
            concise_output_path = output_dir / "llms.txt"
            concise_output_path.write_text(concise_content, encoding="utf-8")
        console.print(
            f"[green]✓[/] Generated [cyan]llms.txt[/] ({len(concise_content)} bytes) "
            f"from {len(concise_patterns)} patterns."
        )

        # Generate llms-full.txt (Comprehensive)
        with console.status("[bold green]Generating llms-full.txt...[/]"):
            full_patterns = list(set(concise_patterns + config.get("full_context", [])))
            full_content = process_files(full_patterns, PROJECT_ROOT)
            full_output_path = output_dir / "llms-full.txt"
            full_output_path.write_text(full_content, encoding="utf-8")
        console.print(
            f"[green]✓[/] Generated [cyan]llms-full.txt[/] ({len(full_content)} bytes) "
            f"from {len(full_patterns)} patterns."
        )

    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/_] {e}")
    except Exception as e:
        console.print(f"[bold red]An unexpected error occurred:[/_] {e}")


if __name__ == "__main__":
    generate_context_files()

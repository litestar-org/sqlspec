# Implementation Tasks: `llms-txt-generator`

This document provides a granular, step-by-step checklist for the Expert agent to implement the `llms-txt-generator` tool.

## Phase 1: Project Setup

- [x] **Task 1.1**: Create the directory `tools/llms_generator/`.
- [x] **Task 1.2**: Create an empty file `tools/llms_generator/__init__.py`.
- [x] **Task 1.3**: Create the main script file `tools/llms_generator/main.py`.
- [x] **Task 1.4**: Create the configuration file `.llms-generator.json` in the project root.
- [x] **Task 1.5**: Populate `.llms-generator.json` with the initial content:
    ```json
    {
      "concise_context": [
        "AGENTS.md",
        ".gemini/GEMINI.md",
        "README.md",
        "docs/guides/architecture/architecture.md"
      ],
      "full_context": [
        "docs/guides/**/*.md",
        "CONTRIBUTING.rst",
        "pyproject.toml"
      ]
    }
    ```

## Phase 2: Script Implementation (`tools/llms_generator/main.py`)

- [x] **Task 2.1**: Add standard library imports: `argparse`, `json`, `pathlib`.
- [x] **Task 2.2**: Import `rich.console` for user-friendly output.
- [x] **Task 2.3**: Define a main function, e.g., `generate_context_files()`.
- [x] **Task 2.4**: Inside `generate_context_files`, initialize `rich.console.Console`.
- [x] **Task 2.5**: Implement argument parsing using `argparse` to accept an optional `--output-dir` (defaulting to the project root).
- [x] **Task 2.6**: Implement a helper function `load_config(config_path)` that reads and parses the `.llms-generator.json` file.
- [x] **Task 2.7**: Implement a core function `process_files(file_patterns: list[str], project_root: pathlib.Path) -> str`.
    - This function should initialize an empty list to hold content blocks.
    - It should iterate through the `file_patterns`.
    - For each pattern, it should use `project_root.glob(pattern)` to find matching files.
    - For each found file, it should:
        - Read the file's content.
        - Format the content into a block: `\n--- START OF FILE: {relative_path} ---\n{content}\n--- END OF FILE: {relative_path} ---\n`.
        - Append the block to the list.
    - The function should return `"\".join(content_blocks)`.
- [x] **Task 2.8**: In the main function, call `load_config` to get the configuration.
- [x] **Task 2.9**: **Generate `llms.txt`**:
    - Get the `concise_context` list from the config.
    - Call `process_files` with the `concise_context` list.
    - Write the returned string to the output file (`llms.txt`).
    - Print a success message to the console using `rich`.
- [x] **Task 2.10**: **Generate `llms-full.txt`**:
    - Combine the `concise_context` and `full_context` lists. Use a `set` to ensure unique file patterns.
    - Call `process_files` with the combined list.
    - Write the returned string to the output file (`llms-full.txt`).
    - Print a success message to the console using `rich`.
- [x] **Task 2.11**: Add the `if __name__ == "__main__":` block to call the main `generate_context_files` function.

## Phase 3: Project Integration

- [x] **Task 3.1**: Open the root `.gitignore` file.
- [x] **Task 3.2**: Add the following lines to the end of `.gitignore` to exclude the generated files:
    ```
    # LLM Context Files
    llms.txt
    llms-full.txt
    ```
- [x] **Task 3.3**: Open the `Makefile`.
- [x] **Task 3.4**: Add a new target to the `Makefile` for running the generator script. Place it under the "Cleaning and Maintenance" or a new "Development" section.
    ```makefile
    .PHONY: llms-context
    llms-context: ## Generate context files for LLMs
    	@echo "$(INFO) Generating LLM context files..."
    	@uv run python tools/llms_generator/main.py
    	@echo "$(OK) LLM context files generated successfully"
    ```

## Phase 4: Documentation & Verification

- [x] **Task 4.1**: Run the new command: `make llms-context`.
- [x] **Task 4.2**: Manually verify that `llms.txt` and `llms-full.txt` are created in the project root.
- [x] **Task 4.3**: Inspect the contents of both files to ensure they are correctly formatted and contain the expected content.
- [x] **Task 4.4**: Run `git status` to confirm that the generated files are ignored.
- [x] **Task 4.5**: (Optional but recommended) Update `CONTRIBUTING.rst` or `README.md` to mention the new tool and its purpose for developers who wish to improve LLM interactions.

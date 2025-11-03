# Product Requirements Document: `llms-txt-generator`

**Version**: 1.0
**Status**: Proposed
**Author**: PRD Agent

## 1. Overview

This document specifies the requirements for a new development tool, `llms-txt-generator`. This command-line tool will programmatically generate two context files, `llms.txt` (concise) and `llms-full.txt` (comprehensive), in the project root. These files will serve as a single source of truth for Large Language Models (LLMs) and AI agents, providing them with curated, up-to-date, and idiomatic information about the `sqlspec` repository.

The primary goal is to significantly improve the quality, accuracy, and efficiency of LLM-assisted development by grounding the models in the project's specific architectural patterns, mandatory coding standards, and key design principles. By automating the creation of this context, we reduce the burden on developers to manually provide it, minimize API hallucinations, and ensure that AI-generated code adheres to the project's high standards.

## 2. Problem Statement

Developing within the `sqlspec` repository requires adherence to a strict set of conventions and a deep understanding of its sophisticated, protocol-based architecture. When developers use LLMs for assistance, these models lack this critical context, leading to several significant problems:

*   **Non-Idiomatic Code Generation**: LLMs frequently produce code that violates the project's mandatory standards (e.g., incorrect typing, import order, or design patterns), requiring extensive manual correction.
*   **API Hallucination and Misuse**: Models may invent non-existent functions or misuse complex internal APIs (e.g., the driver system, statement pipeline) because they are operating from general knowledge, not project-specific facts.
*   **Developer Inefficiency**: Developers waste considerable time and effort crafting highly detailed prompts to manually inject the necessary context in every interaction with an LLM.
*   **Failure to Leverage Project Patterns**: Key architectural patterns that are crucial for writing good `sqlspec` code, such as the `driver_features` configuration or the `LOB Hydration Pattern`, are unknown to the LLM, resulting in suboptimal solutions.

The absence of a centralized, machine-readable context source is a direct cause of this friction, making LLM assistance less effective and more time-consuming than it should be.

## 3. Acceptance Criteria

The `llms-txt-generator` tool will be considered complete and successful when the following criteria are met:

1.  **A new Makefile command `make llms-context` is available.**
2.  **Running `make llms-context` successfully generates two files in the project root: `llms.txt` and `llms-full.txt`.**
3.  **The generated files (`llms.txt`, `llms-full.txt`) are listed in the `.gitignore` file.**
4.  **A new configuration file, `.llms-generator.json`, exists in the project root and is used to drive the content of the generated files.**
5.  **The `llms.txt` file contains the concatenated content of all files specified in the `concise_context` list in the configuration file.**
6.  **The `llms-full.txt` file contains the concatenated content of all files from both the `concise_context` and `full_context` lists.**
7.  **The content of each aggregated file within the generated outputs is clearly demarcated by `--- START OF FILE: [path] ---` and `--- END OF FILE: [path] ---` markers.**
8.  **The tool is implemented as a standalone Python script in `tools/llms_generator/main.py` and uses only standard libraries plus the existing `rich` dependency.**
9.  **The tool provides clear, user-friendly output to the console upon execution, indicating which files are being generated and reporting success.**
10. **The tool gracefully handles glob patterns (`**/*.md`) in the configuration file.**

## 4. Technical Design

The design of the `llms-txt-generator` prioritizes simplicity, maintainability, and alignment with existing project conventions.

### 4.1. Tool Location and Structure

*   The tool's source code will be placed in a new directory: `tools/llms_generator/`.
*   The main executable script will be `tools/llms_generator/main.py`.
*   This location was chosen to align with the project's established pattern of keeping development and build scripts in the `tools/` directory, separate from the main `sqlspec` application source.

### 4.2. Configuration

A JSON file, `.llms-generator.json`, will be used to configure the tool. This approach was chosen over hardcoding paths in the script to make the context sources explicit, discoverable, and easily modifiable without changing the code.

*   **Location**: Project root.
*   **Schema**:
    ```json
    {
      "concise_context": ["path/to/file1.md", "glob/pattern/**/*.md"],
      "full_context": ["path/to/dir/"]
    }
    ```

### 4.3. Core Logic

The script will perform the following steps:

1.  **Parse Arguments**: Use Python's `argparse` to handle any command-line flags (e.g., `--output-dir`).
2.  **Read Configuration**: Load and parse the `.llms-generator.json` file from the project root.
3.  **Process `concise_context`**:
    *   Iterate through the file paths and glob patterns in the `concise_context` list.
    *   For each entry, find all matching files.
    *   Read the content of each file.
    *   Format the content with the specified header and footer.
    *   Concatenate the formatted content blocks.
    *   Write the result to `llms.txt`.
4.  **Process `full_context`**:
    *   Combine the lists from `concise_context` and `full_context`, ensuring uniqueness.
    *   Repeat the file processing and concatenation logic.
    *   Write the result to `llms-full.txt`.
5.  **Provide Feedback**: Use the `rich` library to print informative messages to the console, indicating progress and the final output locations and sizes.

### 4.4. Invocation

A new target will be added to the `Makefile`:

```makefile
.PHONY: llms-context
llms-context: ## Generate context files for LLMs
	@echo "$(INFO) Generating LLM context files..."
	@uv run python tools/llms_generator/main.py
	@echo "$(OK) LLM context files generated successfully"
```
This provides a simple and consistent entry point for developers.

## 5. Testing Strategy

As this is a non-critical development tool, the testing strategy will be lightweight and focused on manual validation.

*   **Manual Verification**: After implementation, the developer will run `make llms-context` and manually inspect the generated `llms.txt` and `llms-full.txt` files to ensure:
    *   The files are created in the correct location.
    *   The content is correctly concatenated from the source files defined in `.llms-generator.json`.
    *   The `--- START/END OF FILE ---` markers are present and correct.
    *   The files are properly ignored by git.
*   **No Automated Tests**: Formal unit or integration tests are not required for this tool, aligning with the testing approach for other scripts in the `tools/` directory.

## 6. Risks and Mitigation

*   **Risk**: The `llms-full.txt` file becomes too large for an LLM's context window.
    *   **Mitigation**: The initial scope of `full_context` is limited to documentation and configuration files, explicitly excluding the entire Python source code. The configuration file makes it easy to prune content if it becomes too large in the future.
*   **Risk**: The configuration file (`.llms-generator.json`) becomes outdated as new, important documents are added to the project.
    *   **Mitigation**: This is a process risk. The existence and purpose of this tool should be documented in the `CONTRIBUTING.rst` to ensure new developers are aware of it and consider updating the configuration when adding significant new patterns or guides.
*   **Risk**: The tool has a bug (e.g., mishandles a glob pattern).
    *   **Mitigation**: As a development tool, the impact of a bug is low. It would be caught and fixed by a developer during their normal workflow. The simplicity of the design (standard libraries, no complex logic) minimizes this risk.

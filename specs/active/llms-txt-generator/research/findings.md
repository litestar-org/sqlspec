# Research & Findings: `llms-txt-generator` Tool

**Version**: 1.0
**Status**: Completed
**Author**: PRD Agent

## 1. Introduction

This document details the comprehensive research, analysis, and final specification for the `llms-txt-generator` tool. The primary objective of this tool is to enhance the effectiveness of Large Language Models (LLMs) in the `sqlspec` repository by providing them with curated, high-quality context files. This research follows the mandatory CRASH protocol, ensuring a hyper-detailed and exhaustively researched foundation for the subsequent Product Requirements Document (PRD) and implementation phases.

## 2. Deconstruction of the Request

The initial phase involved a rigorous deconstruction of the user's request to understand its full scope and impact.

*   **Core Feature**: Create a command-line tool named `llms-txt-generator`.
*   **Primary Output**: The tool will generate two text files in the project root:
    *   `llms.txt`: A concise context file for general-purpose LLM assistance.
    *   `llms-full.txt`: A comprehensive context file for deep, subsystem-specific tasks.
*   **Stated Goal**: To improve LLM performance by providing accurate, up-to-date, and idiomatic context, thereby reducing code generation errors, API hallucinations, and inefficient developer-LLM interaction.
*   **Proposed Location**: A new `tools/llms_text` module.
*   **Identified Affected Components**:
    *   **New Code**: A new Python script and module in the `tools/` directory.
    *   **Build System**: Integration into the `Makefile` for easy invocation.
    *   **Version Control**: The `.gitignore` file will require updates to exclude the generated files.
    *   **Documentation**: The tool's existence and usage will need to be documented for developers.
    *   **Content Sources**: The tool will need to read from and aggregate numerous project files, primarily markdown (`.md`), reStructuredText (`.rst`), and configuration (`.toml`) files.

## 3. Research Strategy

A multi-faceted research strategy was formulated to gather a complete picture of both the internal project landscape and external best practices.

### 3.1. Internal Project & Codebase Analysis

*   **Objective**: To understand the existing conventions, tooling, and architectural principles of the `sqlspec` project to ensure the new tool is idiomatic.
*   **Tools Used**: `read_many_files`, `read_file`.
*   **Strategy**:
    1.  Read high-level project definition files: `AGENTS.md`, `README.md`, `CONTRIBUTING.rst`, `Makefile`, `pyproject.toml`, and all `GEMINI.md` files.
    2.  Inspect specific implementation files for established patterns: `sqlspec/cli.py`, `sqlspec/__main__.py`, `tools/build_docs.py`, and `tools/pypi_readme.py`.
    3.  Analyze core architectural documentation: `docs/guides/architecture/architecture.md`.

### 3.2. External Research & Best Practices

*   **Objective**: To learn the state-of-the-art for structuring context files for LLMs to maximize their effectiveness in code generation tasks.
*   **Tools Used**: `fetch`, `gh search code`.
*   **Strategy**:
    1.  Attempt to fetch articles and blog posts on LLM context optimization and RAG (Retrieval-Augmented Generation) best practices.
    2.  Use GitHub code search to find examples of similar tools and documentation in other high-quality open-source projects.

### 3.3. Deep Reasoning & Design Formulation

*   **Objective**: To systematically think through the core design trade-offs and define the tool's precise behavior.
*   **Tool Used**: `sequentialthinking`.
*   **Strategy**: Initiate a structured thought process to decide on:
    1.  The optimal content curation strategy for the concise vs. full context files.
    2.  The most effective output format for LLM consumption.
    3.  The precise scope of the "full" context to balance detail with manageability.
    4.  The best approach for configuring the tool (hardcoded vs. config file).
    5.  The ideal user experience for developers.
    6.  The necessary dependencies and implementation details.

## 4. Execution & Analysis of Research

### 4.1. Internal Context Analysis: Key Findings

The internal research revealed a project with a very high degree of structure and extremely strict, well-documented conventions.

1.  **Strict Conventions are Law**: `AGENTS.md` and `.gemini/GEMINI.md` are the most important files. They define non-negotiable rules for typing, imports, code style, and even agent behavior. **Any context provided to an LLM must start with these rules.**
2.  **`Makefile` is the UI**: The `Makefile` serves as the primary entry point for all development tasks. New development tools should be integrated here for consistency.
3.  **Tooling is Distinct**: There is a clear separation between the user-facing `rich-click` CLI (`sqlspec/cli.py`) and internal development scripts (`tools/`). The `llms-txt-generator` clearly belongs in the `tools/` directory.
4.  **Architecture is Sophisticated**: `docs/guides/architecture/architecture.md` revealed a highly principled design based on immutability, a single-pass processing pipeline, multi-tier caching, and a protocol-based driver system. These are the core concepts an LLM needs to understand to generate idiomatic code.
5.  **Dependencies**: `pyproject.toml` confirmed that `rich-click` is used, making `rich` available for enhanced terminal output in our script without adding new dependencies.

### 4.2. External Best Practices Analysis: Key Findings

External research was challenging due to `robots.txt` limitations on search engines. However, analysis of GitHub code search results provided valuable high-level validation.

1.  **Context Window Management is a Universal Problem**: The need to manage the finite LLM context window is a recurring theme. This validates the proposed two-file approach (`concise` and `full`).
2.  **Curated "Best Practices" are High-Value**: Many projects include dedicated sections or files on best practices. This confirms that including `AGENTS.md` and summaries of architectural principles is the correct approach.
3.  **No Single Standard Exists**: There is no universal format for LLM context files. The most effective context is tailored to the specific project's architecture and conventions. This gives us the license to create a format that best serves `sqlspec`.

## 5. Synthesis & Proposed Specification

The following specification was derived from a `sequentialthinking` process that synthesized all the above findings.

### 5.1. Goal

The primary goal is to create a Python script, `llms-txt-generator`, that generates two text files (`llms.txt` and `llms-full.txt`) containing curated context about the `sqlspec` project.

### 5.2. Tool Location and Invocation

*   **Location**: The tool will reside at `tools/llms_generator/main.py`.
*   **Invocation**: It will be executed via a new `Makefile` target: `make llms-context`.
*   **CLI Interface**: The script will use standard libraries (`argparse`, `json`, `pathlib`) and the existing `rich` dependency for a clean CLI experience. It will not be part of the main `sqlspec` application.

### 5.3. Configuration

To ensure maintainability, the tool will be driven by a configuration file, not hardcoded paths.
*   **Configuration File**: A new file, `.llms-generator.json`, will be created in the project root and checked into version control.
*   **Structure**:
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

### 5.4. Content Strategy

*   **`llms.txt` (Concise Version)**: Generated from the `concise_context` list. It is designed for high-frequency, general tasks. It will contain the project's rules, overview, and core architectural principles.
*   **`llms-full.txt` (Comprehensive Version)**: Generated from the combination of `concise_context` and `full_context`. It provides deep context for complex tasks, including all developer guides and the project's dependency manifest. Source code is explicitly excluded to favor curated documentation over raw code.

### 5.5. Output Formatting

To maximize clarity for the LLM, each aggregated file's content will be wrapped in a distinct block.

*   **Header**: `--- START OF FILE: {relative_path} ---`
*   **Footer**: `--- END OF FILE: {relative_path} ---`
*   **Separation**: Blank lines will surround each file block to ensure clean parsing.

### 5.6. Project Integration

*   **Makefile**: A new `llms-context` target will be added.
*   **.gitignore**: `llms.txt` and `llms-full.txt` will be added to `.gitignore`.

## 6. Conclusion

This research phase has produced a hyper-detailed, actionable specification for the `llms-txt-generator` tool. The proposed design is deeply informed by the `sqlspec` project's existing high standards for structure and convention, and is validated by external best practices in LLM context management. This specification provides a solid foundation for the PRD and implementation phases.
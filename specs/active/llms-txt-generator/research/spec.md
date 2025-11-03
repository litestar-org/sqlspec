# `llms.txt` and `llms-full.txt` Specification for SQLSpec

## 1. Overview

This document specifies the content, structure, and generation strategy for the `llms.txt` and `llms-full.txt` files for the SQLSpec project. The format is adopted from the conventions established by the LangGraph project.

## 2. `llms.txt` - The Concise Index

This file serves as a high-level, machine-readable index for an LLM. It must be formatted as a Markdown file (`.md`).

### 2.1. Structure

The file will be organized into logical sections using Markdown headers (`#`, `##`). Each entry within a section will be a bulleted list item containing a Markdown link and a brief, descriptive summary of the linked document's content.

### 2.2. Content Sections

#### `# SQLSpec LLM Context`

- A brief introductory sentence explaining the purpose of the file.

#### `## Core Concepts`

- **`README.md`**: A link to the main project README for a general overview.
- **`AGENTS.md`**: A link to the agent guide, flagged as the **primary source of truth** for LLM collaboration, coding standards, and development processes.
- **`CONTRIBUTING.rst`**: A link to the contribution guide.

#### `## Architecture`

- **`docs/guides/architecture/architecture.md`**: An overview of the protocol-based adapter design, core components, and key design patterns.
- **`docs/guides/architecture/driver-parameter-profiles.md`**: Explanation of the parameter profile registry.

#### `## Key Development Guides`

- **`docs/guides/testing/testing.md`**: Guide to the project's testing strategy, including unit and integration tests.
- **`docs/guides/performance/sqlglot-best-practices.md`**: Best practices for performance optimization.
- **`docs/guides/migrations/hybrid-versioning.md`**: Guide to the database migration system.

#### `## Adapter Guides`

- A dynamically generated list of links to each adapter's guide in `docs/guides/adapters/`, from `adbc.md` to `sqlite.md`.

## 3. `llms-full.txt` - The Comprehensive Context

This file provides the full, concatenated content of the documents indexed in `llms.txt`. It is intended for use with RAG-enabled IDEs and tools.

### 3.1. Structure

- The file will begin with the complete, verbatim content of the generated `llms.txt`.
- Following the index, the content of each file referenced in the `llms.txt` index will be appended.
- Each appended file's content must be preceded by a separator line: `--- FILE: {path/to/file} ---`.

### 3.2. Content Order

The files will be concatenated in the same order they appear in `llms.txt` to maintain logical consistency.

## 4. Generation Tool (`tools/llms_text`)

The generation tool will be responsible for:

1. Identifying the list of canonical source files. This list should be easily configurable within the tool's source code.
2. Reading each source file.
3. For each file, generating a concise, one-sentence description for the `llms.txt` index.
4. Assembling the `llms.txt` Markdown file.
5. Assembling the `llms-full.txt` file by combining the `llms.txt` content and the raw content of the source files.
6. Writing both files to the project root.

# Gemini Agent System: Core Context for sqlspec

**Version**: 4.0
**Last Updated**: Wednesday, October 29, 2025

This document is the **single source of truth** for the agentic workflow in this project. As the Gemini agent, you must load and adhere to these guidelines in every session. Failure to follow these rules is a failure of your core function.

## Section 1: The Philosophy

This system is built on the principle of **"Continuous Knowledge Capture."** The primary goal is not just to write code, but to ensure that the project's documentation and knowledge base evolve in lockstep with the implementation.

## Section 2: Agent Roles & Responsibilities

You are a single agent that adopts one of five roles based on custom slash commands.

| Role | Invocation | Mission |
| :--- | :--- | :--- |
| **PRD** | `/prd "create a PRD for..."` | To translate user requirements into a comprehensive, actionable, and technically-grounded plan. |
| **Expert** | `/implement {slug}` | To implement the planned feature while simultaneously capturing all new knowledge in the project's guides. |
| **Testing** | `/test {slug}` | To validate the implementation against its requirements and ensure its robustness and correctness. |
| **Review** | `/review {slug}` | To act as the final quality gate, verifying both the implementation and the captured knowledge before archival. |
| **Guides** | `/sync-guides` | To perform a comprehensive audit and synchronization of `specs/guides/` against the current codebase, ensuring all documentation is accurate and up-to-date. |

## Section 3: The Workflow (Sequential & MANDATORY)

The development lifecycle follows four strict, sequential phases. You may not skip a phase.

### Section 3.1: Mandate for Astronomical Excellence and Proactive Decomposition

**This is the prime directive and is non-negotiable.** Your performance is measured against this standard. Failure to adhere to it is a failure of your core function.

1.  **Astronomical Excellence Bar**: You must always operate at the highest possible level of detail, thoroughness, and quality. Superficial or incomplete work is never acceptable.
2.  **No Shortcuts**: You will never take a shorter route or reduce the quality/detail of your work. Your process must be exhaustive, every time.
3.  **Proactive Decomposition**: Upon receiving any request, your **first step** is to perform a deep, comprehensive analysis of the relevant codebase and context. If a task is too large or complex, you **MUST** automatically redefine it as a multi-phase project.

### Section 3.2: Mandate for Documentation Integrity and Quality Gate Supremacy

1.  **Guides are the Single Source of Truth**: The `specs/guides/` directory must **only** document the "current way" the system works. It is a live representation of the codebase, not a historical record.
2.  **Quality Gate is Absolute**: You are responsible for fixing **100%** of all linting errors and test failures that arise during your work.

---

1.  **Phase 1: PRD (`/prd`)**: A new workspace is created in `specs/active/{slug}/`.
2.  **Phase 2: Implementation (`/implement`)**: The Expert agent reads the PRD and writes production code, updating `specs/guides/` as it works.
3.  **Phase 3: Testing (`/test`)**: The Testing agent writes a comprehensive test suite.
4.  **Phase 4: Review (`/review`)**: The Review agent verifies documentation, runs the quality gate, and archives the workspace.

## Section 4: Workspace Management

All work **MUST** occur within a requirement-specific directory inside `specs/active/`.

`specs/active/{requirement-slug}/`
├── prd.md
├── tasks.md
├── recovery.md
├── research/
└── tmp/

**RULE**: The `specs/active` and `specs/archive` directories should be added to the project's `.gitignore` file if not already present.

## Section 5: Tool & Research Protocol

You must follow this priority order when seeking information.

1.  **📚 `specs/guides/` (Local Guides) - FIRST**
2.  **📁 Project Codebase - SECOND**
3.  **📖 Context7 MCP - THIRD**
4.  **🤔 Sequential Thinking - FOURTH**
5.  **🌐 WebSearch - FIFTH**
6.  **🧠 Zen MCP - LAST**

## Section 6: Code Quality Standards (Tailored)

These standards are derived from the project analysis and are **non-negotiable**.

-   **Language & Version**: `Python`
-   **Primary Framework**: `SQLSpec (as a library)`
-   **Architectural Pattern**: Adhere to the `Protocol-Based Adapter Architecture` pattern.
-   **Typing**: `Fully typed with stringified hints`.
-   **Style & Formatting**: All code must pass `make lint`.
-   **Testing**: All new logic must be accompanied by tests. The test suite must pass (`make test`).
-   **Error Handling**: Follow the established `Custom Exception Hierarchy`.

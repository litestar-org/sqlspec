# Recovery Instructions: `llms-txt-generator`

## Current Status
**Phase**: PRD (Product Requirements and Design) Complete
**Last Updated**: 2025-11-03

## What's Been Done
- ✓ **Deconstruction**: The initial user request was fully deconstructed to identify all affected components.
- ✓ **Rigorous Research**: A comprehensive research phase was completed, analyzing both internal project conventions (`AGENTS.md`, architecture guides) and external best practices for LLM context generation.
- ✓ **Specification**: A hyper-detailed specification for the `llms-txt-generator` tool was formulated based on the research.
- ✓ **Workspace Artifacts Created**:
    - `research/findings.md`: A comprehensive document detailing the entire research and synthesis process.
    - `prd.md`: A formal Product Requirements Document outlining the tool's purpose, acceptance criteria, and technical design.
    - `tasks.md`: A hyper-granular, step-by-step checklist for the implementation agent.

## Next Steps

The planning phase is complete. The project is now ready for implementation.

1.  **Invoke the Expert Agent**: Run the command `/implement llms-txt-generator` to begin the implementation phase.
2.  **Follow the Plan**: The Expert agent is directed to read the `prd.md` and meticulously follow the checklist in `tasks.md` to build the tool.
3.  **Automatic Handoff**: The implementation process will automatically invoke the Testing and Review agents upon completion.

## Key Decisions Made

- **Standalone Script**: The tool will be a standalone script in `tools/llms_generator/` rather than being integrated into the main `sqlspec` CLI application. This aligns with the project's existing pattern for development tools.
- **Configuration-Driven**: The tool will be driven by a `.llms-generator.json` file in the project root. This was chosen over hardcoding paths to make the context sources explicit, maintainable, and discoverable.
- **Curated Documentation over Code**: The context files will be generated from curated documentation (`.md`, `.rst`) and configuration files, not raw Python source code. This provides a higher signal-to-noise ratio for the LLM.

## Files to Review Before Continuing
- **`prd.md`**: For the "what" and "why" of the feature.
- **`tasks.md`**: For the "how" – the exact implementation steps.
- **`research/findings.md`**: For the full background, research, and rationale behind the design decisions.
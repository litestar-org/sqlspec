Create a comprehensive, research-grounded plan for: {{prompt}}

Invoke the Planner agent to:

1. **Research** - Consult docs/guides/, Context7, and WebSearch
2. **Plan** - Use zen.planner for structured planning
3. **Consensus** - Get multi-model agreement on complex decisions
4. **Workspace** - Create requirements/{requirement-slug}/ with:
   - prd.md (Product Requirements Document)
   - tasks.md (Implementation checklist)
   - research/plan.md (Research findings)
   - recovery.md (Session resume instructions)
   - tmp/ (temporary files directory)

The planner should reference:

- docs/guides/adapters/ for database-specific patterns
- docs/guides/performance/ for optimization strategies
- docs/guides/testing/ for test planning
- docs/guides/architecture/ for design patterns
- docs/guides/quick-reference/ for common patterns
- CLAUDE.md for code quality standards

After planning, the workspace will be ready for implementation.

Next step: Run `/implement` to begin development.

# Agent Role: Review
**Mission**: To act as the final, non-negotiable quality gate, ensuring that no work is archived until both the code and the documentation are of the highest standard.
**Invocation**: `/review {slug}`
### Core Responsibilities (Sequential & MANDATORY)
1.  [ ] **Verify Documentation (BLOCKING)**: Your **first** step is to verify the Expert agent's updates in `specs/guides/`.
2.  [ ] **Quality Gate (BLOCKING)**: Run the full test suite (`make test`) and linter (`make lint`).
3.  [ ] **Cleanup & Archive (MANDATORY)**: If checks pass, delete the `tmp/` directory and move the `specs/active/{slug}` directory to `specs/archive/`.

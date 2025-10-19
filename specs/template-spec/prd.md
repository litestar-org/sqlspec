# Feature: {Feature Name}

## Overview

{1-2 paragraph description of the feature and its value}

## Problem Statement

{What problem does this solve? What pain points does it address?}

## Goals

- Primary goal: {Main objective}
- Secondary goals: {Additional objectives}

## Target Users

- **{User Type 1}**: {How they benefit}
- **{User Type 2}**: {How they benefit}

## Technical Scope

### Technology Stack

**SQLSpec Stack**:
- Language: Python 3.10+
- Framework: Litestar (web integration)
- Databases: PostgreSQL, Oracle, DuckDB, MySQL, SQLite, BigQuery
- Testing: pytest with pytest-databases
- Build: uv + make

### Implementation Details

**Affected Components**:
- `sqlspec/{component}/` - {Description}

**Database Adapters Affected**:
- AsyncPG (PostgreSQL)
- Oracle (oracledb)
- DuckDB
- {Others as needed}

## Acceptance Criteria

### Functional Requirements
- [ ] Feature works as specified
- [ ] All integrations functional
- [ ] Backward compatible
- [ ] Performance acceptable

### Technical Requirements
- [ ] Code follows project standards (see AGENTS.md)
- [ ] Tests comprehensive and passing
- [ ] Error handling proper
- [ ] Documentation complete
- [ ] Type hints stringified for non-builtins
- [ ] No `from __future__ import annotations`

### Testing Requirements
- [ ] Unit tests passing
- [ ] Integration tests passing (all affected adapters)
- [ ] Edge cases covered (empty, None, errors, boundaries)
- [ ] Coverage targets met (>80%)
- [ ] Tests are function-based (NOT class-based)

## Implementation Phases

### Phase 1: Planning & Research ✅
- [x] 1.1 Create requirement workspace
- [x] 1.2 Write comprehensive PRD
- [x] 1.3 Create task breakdown
- [x] 1.4 Identify research questions

### Phase 2: Expert Research
- [ ] 2.1 Research implementation patterns
- [ ] 2.2 Review adapter integration patterns
- [ ] 2.3 Analyze performance implications
- [ ] 2.4 Check mypyc compilation compatibility
- [ ] 2.5 Document findings in `research/`

### Phase 3: Core Implementation (Expert)
- [ ] 3.1 Implement core functionality
- [ ] 3.2 Add adapter-specific logic
- [ ] 3.3 Update type definitions
- [ ] 3.4 Handle edge cases
- [ ] 3.5 Follow AGENTS.md standards

### Phase 4: Integration (Expert)
- [ ] 4.1 Update affected adapters
- [ ] 4.2 Add necessary mixins
- [ ] 4.3 Update configuration
- [ ] 4.4 Test integrations

### Phase 5: Testing (Automatic via Expert)
- [ ] 5.1 Create unit tests
- [ ] 5.2 Create integration tests (all adapters)
- [ ] 5.3 Test edge cases
- [ ] 5.4 Performance validation
- [ ] 5.5 Achieve coverage targets

### Phase 6: Documentation (Automatic via Expert)
- [ ] 6.1 Update API documentation
- [ ] 6.2 Create/update guide in docs/guides/
- [ ] 6.3 Add code examples
- [ ] 6.4 Update CHANGELOG

### Phase 7: Knowledge Capture & Archive (Automatic via Expert)
- [ ] 7.1 Extract new patterns
- [ ] 7.2 Update AGENTS.md
- [ ] 7.3 Update relevant guides
- [ ] 7.4 Re-validate
- [ ] 7.5 Clean and archive

## Dependencies

**Internal Dependencies**:
- {List SQLSpec components this depends on}

**External Dependencies**:
- {New packages needed? Update pyproject.toml}

## Risks & Mitigations

### Risk 1: {Risk Description}
- **Mitigation**: {How to mitigate}

### Risk 2: Mypyc Compilation Compatibility
- **Mitigation**: Avoid `from __future__ import annotations`, use stringified types

## Research Questions for Expert

1. {Question about implementation approach}
2. {Question about adapter-specific handling}
3. {Question about performance optimization}
4. {Question about mypyc compatibility}

## Success Metrics

- Feature functional and tested
- Tests passing with >80% coverage
- Documentation complete
- No performance regression
- Zero breaking changes
- Mypyc compilation succeeds
- New patterns captured in AGENTS.md

## References

**Similar Features**:
- {Link to similar code in SQLSpec}

**External Documentation**:
- {Links to relevant library docs}

**SQLSpec Guides**:
- [Architecture](../../docs/guides/architecture/architecture.md)
- [Testing](../../docs/guides/testing/testing.md)
- [Adapter Patterns](../../docs/guides/adapters/)

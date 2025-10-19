# Tasks: {Feature Name}

## Phase 1: Planning & Research ✅
- [x] 1.1 Create requirement workspace
- [x] 1.2 Write comprehensive PRD
- [x] 1.3 Create task breakdown
- [x] 1.4 Identify research questions

## Phase 2: Expert Research
- [ ] 2.1 Research implementation patterns
- [ ] 2.2 Review adapter integration patterns
- [ ] 2.3 Analyze performance implications
- [ ] 2.4 Check mypyc compilation compatibility
- [ ] 2.5 Document findings in `research/`

## Phase 3: Core Implementation (Expert)
- [ ] 3.1 Implement core functionality
- [ ] 3.2 Add adapter-specific logic
- [ ] 3.3 Update type definitions (stringified, no __future__)
- [ ] 3.4 Handle edge cases
- [ ] 3.5 Follow AGENTS.md standards

## Phase 4: Integration (Expert)
- [ ] 4.1 Update affected adapters
- [ ] 4.2 Add necessary mixins
- [ ] 4.3 Update configuration
- [ ] 4.4 Test integrations

## Phase 5: Testing (Testing Agent - Automatic)
- [ ] 5.1 Create unit tests (function-based)
- [ ] 5.2 Create integration tests (all adapters)
- [ ] 5.3 Test edge cases (empty, None, errors)
- [ ] 5.4 Performance validation
- [ ] 5.5 Coverage target achieved (>80%)

## Phase 6: Documentation (Docs & Vision - Automatic)
- [ ] 6.1 Update API documentation
- [ ] 6.2 Create/update guide in docs/guides/
- [ ] 6.3 Add code examples
- [ ] 6.4 Update CHANGELOG

## Phase 7: Knowledge Capture & Archive (Docs & Vision - Automatic)
- [ ] 7.1 Extract new patterns from implementation
- [ ] 7.2 Update AGENTS.md with patterns
- [ ] 7.3 Update guides with new patterns
- [ ] 7.4 Re-validate (tests, docs, consistency)
- [ ] 7.5 Clean tmp/ and archive

## Handoff Notes

**To Expert**:
- Read PRD thoroughly
- Start with Phase 2 (Research)
- Consult AGENTS.md for SQLSpec-specific patterns
- Document findings before implementation
- Update tasks.md and recovery.md as you progress
- Will auto-invoke Testing and Docs & Vision agents when ready

**To Testing Agent** (Auto-invoked by Expert):
- Follow SQLSpec test patterns (see AGENTS.md)
- Function-based tests ONLY (no class-based)
- Test all affected adapters
- Test edge cases (empty, None, errors)
- Achieve >80% coverage
- All tests must pass before returning control

**To Docs & Vision** (Auto-invoked by Expert):
- Update project documentation
- Run quality gate (must pass)
- **Extract and capture new patterns in AGENTS.md**
- **Update docs/guides/ with new patterns**
- **Re-validate after updates**
- Clean and archive when complete

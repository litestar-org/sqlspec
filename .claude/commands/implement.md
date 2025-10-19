Implement the feature from the active workspace.

Invoke the Expert agent to:

1. **Read Plan** - Load prd.md, tasks.md, research/plan.md from specs/active/{requirement}/
2. **Research** - Consult guides and library docs
3. **Implement** - Write clean, type-safe, performant code following AGENTS.md standards
4. **Test** - Run relevant tests to verify implementation
5. **Auto-invoke Testing agent** - Create comprehensive test suite (automatic)
6. **Auto-invoke Docs & Vision agent** - Full 5-phase workflow (automatic):
   - Phase 1: Update documentation
   - Phase 2: Quality gate validation
   - Phase 3: **Knowledge capture** (update AGENTS.md + guides)
   - Phase 4: **Re-validation** (verify consistency after updates)
   - Phase 5: Clean and archive

The expert should:

- Follow AGENTS.md code quality standards (NO hasattr, NO workaround naming, etc.)
- Reference docs/guides/ for patterns
- Use zen.debug for complex bugs
- Use zen.thinkdeep for architectural decisions
- Use zen.analyze for code analysis
- Update workspace progress continuously

**This ONE command handles:**
✅ Implementation
✅ Testing (automatic via Testing agent)
✅ Documentation (automatic via Docs & Vision)
✅ Quality gate (automatic via Docs & Vision)
✅ **Knowledge capture** (automatic via Docs & Vision)
✅ **Re-validation** (automatic via Docs & Vision)
✅ Archival (automatic via Docs & Vision)

**After implementation:**
Feature is complete, tested, documented, patterns captured, and archived!

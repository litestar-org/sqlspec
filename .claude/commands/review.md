Complete documentation, quality gate, knowledge capture, and archival for the implemented feature.

Invoke the Docs & Vision agent to run the full 5-phase workflow:

**Phase 1: Documentation**
- Update API documentation
- Create/update guides in docs/guides/
- Validate code examples work
- Build documentation without errors

**Phase 2: Quality Gate**
- Verify all PRD acceptance criteria met
- Verify all tests passing
- Check code standards compliance (AGENTS.md)
- BLOCK if any criteria not met

**Phase 3: Knowledge Capture (NEW!)**
- Analyze implementation for new patterns
- Extract best practices and conventions
- **Update AGENTS.md with new patterns**
- **Update relevant guides in docs/guides/**
- Document patterns with working examples

**Phase 4: Re-validation (NEW!)**
- Re-run tests after documentation updates
- Rebuild documentation to verify no errors
- Check pattern consistency across project
- Verify no breaking changes introduced
- BLOCK if re-validation fails

**Phase 5: Cleanup & Archive**
- Remove all tmp/ files
- Move specs/active/{requirement} to specs/archive/
- Generate completion report

**Note:** This command is typically not needed manually because `/implement` automatically invokes Docs & Vision. Use this only if you need to:
- Re-run validation after manual changes
- Regenerate documentation
- Force re-archival
- Update AGENTS.md with new patterns after manual implementation

After review, feature is documented, validated, patterns captured, and archived!

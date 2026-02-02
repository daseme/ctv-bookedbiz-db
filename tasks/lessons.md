# Lessons Learned - Refactoring and Code Cleanup Sessions

## Template and File Deletion Patterns

### Rule 1: Always Verify References Before Deletion
**Context**: The nord_base.html deletion broke 7 templates that still included it  
**Pattern**: Before deleting ANY template file:
```bash
# Check for references in all source files
rg -l "filename.html" src/
# Check for both include and extend patterns
rg "{% include.*filename\.html" src/
rg "{% extends.*filename\.html" src/
```
**Action**: Never delete a template without comprehensive reference checking across the entire codebase.

### Rule 2: Verify ALL Templates After Bulk Changes
**Context**: CSS migration missed sector-analysis.html, causing 500 errors  
**Pattern**: After any bulk template modification (CSS migrations, base template changes):
```bash
# Find ALL templates that might be affected
find src/web/templates -name "*.html" -exec grep -l "old_pattern" {} \;
# Verify each one was properly updated
# Test the application after bulk changes
```
**Action**: Sample verification is insufficient - check every affected file systematically.

## CSS Architecture and Migration Patterns

### Rule 3: Document CSS Dependencies During Migration
**Context**: Complex Nord theme CSS had interdependencies not immediately obvious  
**Pattern**: Before CSS refactoring:
1. Map all CSS files and their dependencies
2. Identify templates using each CSS approach
3. Create migration plan with verification steps
4. Test each template type after changes

**Action**: Create dependency maps before major CSS restructuring.

## Code Cleanup and Redundancy Analysis

### Rule 4: Categorize Files by Reference Risk Level
**Context**: Successfully removed 9,000+ lines by categorizing files by risk  
**Pattern**: For cleanup tasks:
- **ZERO RISK**: Files in 'old/' directories with '-OLD' suffixes
- **LOW RISK**: Unused imports confirmed by static analysis
- **MEDIUM RISK**: Templates requiring reference verification
- **HIGH RISK**: Active code with potential runtime dependencies

**Action**: Always start with zero-risk items, work up the risk ladder with increasing verification.

### Rule 5: Commit Atomically by Risk Level
**Context**: Three separate commits for old directories, orphaned templates, unused imports  
**Pattern**: Each cleanup phase should be a separate commit:
1. Delete confirmed dead code (zero risk)
2. Remove verified orphaned files (low risk)  
3. Clean up unused imports (very low risk)

**Action**: Atomic commits allow easy rollback if any phase causes issues.

## Template Analysis and Reference Checking

### Rule 6: Use Multiple Grep Patterns for Template References
**Context**: Templates can be referenced via routes, includes, extends, or documentation  
**Pattern**: Check for template references using multiple patterns:
```bash
# Route references (Python files)
rg "template_name\.html" --type py
# Template includes/extends (HTML files)  
rg "template_name" --type html
# Documentation references (Markdown files)
rg "template_name" --type md
```
**Action**: Cast a wide net when checking for template references.

## Error Recovery Patterns

### Rule 7: Test Application After Each Major Change
**Context**: 500 errors were caught immediately after CSS migration  
**Pattern**: After any template or CSS changes:
1. Start the application locally if possible
2. Check key routes for 500 errors
3. Verify browser console for missing CSS/JS resources
4. Test Nord theme pages specifically

**Action**: Build testing into the refactoring workflow, not just at the end.

## Git Workflow for Refactoring

### Rule 8: Use Descriptive Commit Messages with Impact Metrics
**Context**: All commits included line counts and specific changes made  
**Pattern**: Commit message format for cleanup:
```
Phase X: Brief description

- Specific action 1 (impact metrics)
- Specific action 2 (impact metrics)
- Total impact: X lines removed

🤖 Generated with [Claude Code](https://claude.ai/code)
Co-Authored-By: Claude <noreply@anthropic.com>
```
**Action**: Include quantified impact in commit messages for better tracking.

## Planning and Execution Patterns

### Rule 9: Write Phase Plans to tasks/ Directory Before Execution
**Context**: tasks/todo.md provided clear roadmap and stopping points  
**Pattern**: For multi-phase refactoring:
1. Document all phases with success criteria
2. Include explicit STOP conditions  
3. Define risk levels and verification steps
4. Track progress with TodoWrite tool

**Action**: Always plan before executing, especially for destructive operations.

### Rule 10: Verify with User Before Proceeding to Higher Risk Phases
**Context**: User explicitly requested "Execute Phase 1 only" with stop condition  
**Pattern**: For cleanup projects:
- Start with lowest risk phases
- Get user confirmation before proceeding to higher risk changes
- Provide clear phase boundaries and impact estimates

**Action**: Respect user-defined boundaries, especially for potentially breaking changes.

---

### Rule 11: Understand Business Context for Data Constants
**Context**: P language code had inconsistent mapping (Portuguese vs Punjabi) during consolidation  
**Pattern**: Asian language television company context:
- All languages are Asian except English
- P = Punjabi (South Asian), not Portuguese
- Language mappings are business logic, not technical data

**Action**: Always verify business context when consolidating data constants.

---

**Last Updated**: 2026-02-02  
**Session Context**: Phase 1 cleanup removing 9,000+ lines of dead code across old directories, orphaned templates, and unused imports. Phase 2 Step 1 consolidating 115 lines of duplicate utilities.
# CSS Consolidation Project

## Phase 1: Audit Complete ✓
Template analysis completed - identified 46 templates with inheritance, 4 standalone, and 15 with additional CSS.

## Phase 2: CSS Consolidation Analysis
- [ ] Extract all unique styles from pricing.css that aren't in base.html
- [ ] Extract all unique styles from customer-sector-manager.css
- [ ] Extract all unique styles from pipeline CSS
- [ ] Document: which styles are truly unique vs duplicates of base.html
- [ ] Propose: single consolidated reports.css with only net-new styles

## Phase 3: Base Template Alignment
- [ ] Analyze _pricing_layout.html - what does it add beyond base.html?
- [ ] Analyze nord_base.html - why does it exist separately?
- [ ] Recommend: consolidate into base.html or document why separation is needed

## Phase 4: CSS Migration
For the 15 templates with extra CSS references:
- [ ] Replace individual CSS file references with consolidated reports.css
- [ ] Verify each template renders correctly after change
- [ ] Git commit after each successful migration

## Phase 5: Cleanup
- [ ] Delete unused CSS files (after confirming no references remain)
- [ ] Update any CSS imports in base.html if consolidation changed structure
- [ ] Final verification: load 5 representative pages, confirm styling

## Progress Log
- Started Phase 2: CSS Consolidation Analysis
- ✅ Completed CSS file analysis

### Phase 2 Analysis Results

**Unique Styles NOT in base.html:**

**From pricing.css (~1,927 lines):**
- Complete Bootstrap-like grid system (container-fluid, row, col-*) 
- Bootstrap-like components: cards, badges, alerts, buttons, forms, tables, breadcrumbs
- Advanced table features: sticky headers, sortable columns, clickable rows
- Specialized pricing components: period selectors, stat cards, charts, filters
- HHI analysis, consistency analysis, concentration analysis components
- Comprehensive responsive design system
- Print styles
- Tab navigation system for pricing pages

**From customer-sector-manager.css (~780 lines):**
- Enhanced customer management UI components
- Specialized table styling for customer data
- Advanced modal system with backdrop blur
- Stats bar with hover animations
- Sector badge system with status indicators
- Notification system with sliding animations
- Loading states with spinners
- Pagination controls
- Form components with Nordic styling

**From pipeline CSS files (~675 lines total):**
- Pipeline-specific decay tracking indicators  
- Modal systems for decay timeline and customer details
- Month cards with decay status visualization
- Revenue metrics with editable pipeline inputs
- Gap analysis components
- Review session panels with calibration controls
- Timeline event visualization
- Specialized Nord color extensions for decay states

**Overlap Analysis:**
- Nord color variables: Defined in all files (redundant)
- Basic button styling: Some overlap but specialized variants
- Modal systems: Different implementations for different use cases
- Table styling: Base styles overlap but specialized features are unique

**Dependencies:**
- customer-sector-manager.css imports 'nord-variables.css' (may not exist)
- All CSS files assume Nord color variables are available

### Consolidation Proposal

**RECOMMENDATION: Create Multiple Domain-Specific CSS Files Instead of One Large File**

**Why Not Single reports.css:**
1. **Size Issue**: Combined CSS would be ~3,400 lines - too large for maintainability
2. **Functional Separation**: Each CSS serves distinct business domains with minimal overlap
3. **Performance**: Loading unused CSS hurts page performance

**Proposed Structure:**
```
/src/web/static/css/
├── shared/
│   ├── components.css      # Bootstrap-like grid, cards, buttons, forms (from pricing.css)
│   └── tables.css          # Advanced table components (from pricing.css)
├── domain/  
│   ├── pricing.css         # Pricing-specific: period selectors, stats, HHI analysis
│   ├── customer.css        # Customer management: modals, stats bars, notifications
│   └── pipeline.css        # Pipeline: decay indicators, timeline, calibration
```

**Benefits:**
- Shared components reusable across domains
- Domain-specific files remain focused
- Easy to maintain and debug
- Better performance (load only what's needed)
- Clear separation of concerns

**Migration Strategy:**
1. Extract shared components first
2. Clean domain-specific files  
3. Update templates to load: shared/components.css + domain/[specific].css
4. Remove redundant Nord variable definitions (keep only in base.html)

## Phase 3: Base Template Alignment - COMPLETED ✓

### _pricing_layout.html Analysis
- **Purpose**: Master layout for pricing reports with unified tab navigation
- **Adds Beyond base.html**:
  - Custom breadcrumb path: Home › Analytics › Pricing › [Page]
  - Tab navigation system (includes `_tabs.html`)  
  - Period selector integration (includes `_period_selector.html`)
  - Pricing-specific page structure with Bootstrap-like grid
  - Chart.js integration for pricing visualizations
  - Specialized block system for pricing pages
- **Dependencies**: Requires pricing.css, uses Bootstrap-like classes
- **Usage**: Extended by 6 pricing analysis templates

### nord_base.html Analysis  
- **Purpose**: Standalone CSS-only module providing Nordic theme overrides
- **Content**: 404 lines of CSS styling (not a full template)
- **Adds Beyond base.html**:
  - Alternative Nordic aesthetic with enhanced styling
  - Advanced gradient effects and backdrop filters
  - Semantic CSS variable system (--bg-primary, --text-secondary, etc.)
  - Enhanced component library (nord-btn, nord-table, nord-stats-grid)
  - Comprehensive form styling system
  - Inter font integration
- **Critical Issue**: Contains duplicate Nord color definitions (redundant with base.html)
- **Usage**: Only used by sector_management.html

### Template Consolidation Recommendations

**_pricing_layout.html**: ✅ **KEEP SEPARATE** 
- Serves legitimate specialized purpose
- Provides essential pricing-specific navigation and structure
- No significant redundancy with base.html
- Clean inheritance model

**nord_base.html**: ⚠️ **CONSOLIDATE RECOMMENDED**
- Primarily CSS styling (should be CSS file, not template)
- Contains redundant Nord color definitions
- Only used by 1 template
- Better as: /src/web/static/css/shared/nord-enhanced.css

**Action Plan:**
1. Convert nord_base.html content to shared/nord-enhanced.css
2. Update sector_management.html to include shared/nord-enhanced.css
3. Remove duplicate Nord variables from nord-enhanced.css
4. Keep _pricing_layout.html as-is (serves valid purpose)
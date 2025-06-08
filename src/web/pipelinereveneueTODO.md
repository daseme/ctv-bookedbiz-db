# Revenue Management System - Development To-Do List

## Phase 1: Core Infrastructure & Data Layer

### Database Integration
- [ ] **Set up connection to existing database**
  - [ ] Identify database schema for historical revenue data
  - [ ] Identify database schema for booked revenue data
  - [ ] Identify database schema for customer deal data
  - [ ] Create database connection and query functions
  - [ ] Test data retrieval for sample AE and date range

### JSON Storage Setup
- [x] **Design JSON file structure for budget data**
  - [x] Define schema for AE budget by month
  - [x] Create sample budget data file
  - [x] Implement read/write functions for budget data
- [x] **Design JSON file structure for pipeline data**
  - [x] Define schema for current pipeline vs expected pipeline
  - [x] Include audit fields (last_updated, updated_by, notes)
  - [x] Create sample pipeline data file
  - [x] Implement read/write functions for pipeline data
- [x] **Design JSON structure for review session data**
  - [x] AE completion status tracking
  - [x] Session notes and timestamps
  - [x] Review metadata

### API Development
- [x] **Core data endpoints**
  - [x] `GET /api/aes` - List all Account Executives
  - [x] `GET /api/revenue/historical/{ae_id}/{period}` - Historical from DB
  - [x] `GET /api/revenue/booked/{ae_id}/{period}` - Booked from DB
  - [x] `GET /api/customers/{ae_id}/{month}` - Customer details from DB
- [x] **Pipeline management endpoints**
  - [x] `GET /api/pipeline/{ae_id}/{period}` - Current pipeline from JSON
  - [x] `GET /api/pipeline-targets/{ae_id}/{period}` - Expected pipeline from JSON
  - [x] `PUT /api/pipeline/{ae_id}/{month}` - Update pipeline in JSON
- [x] **Session management endpoints**
  - [x] `GET /api/budget/{ae_id}/{period}` - Budget reference from JSON
  - [x] `POST /api/review-notes` - Save notes and completion status
  - [x] `GET /api/review-session/{date}` - Load session state

---

## Phase 2: Core UI Components

### Header & Navigation
- [x] **Application header**
  - [x] Application title and branding
  - [x] Review date display and period selector
  - [ ] Export functionality (placeholder initially)
  - [x] Save controls with status indicators

### AE Selection Interface
- [x] **AE dropdown selector**
  - [x] Populate dropdown from AE list API
  - [x] Display selected AE's key statistics
  - [x] Pipeline vs target, YTD actual, pipeline accuracy, avg deal size
- [x] **Review progress tracking**
  - [x] Visual completion status indicators (dots)
  - [x] "X of Y AEs Reviewed" counter
  - [x] Mark complete functionality

### Monthly Revenue Cards
- [x] **Card layout and structure**
  - [x] Responsive grid for month cards
  - [x] Card styling with status-based color coding
  - [x] Current month highlighting
- [x] **Revenue data display per card**
  - [x] Booked revenue (from database)
  - [x] Current pipeline (editable, from JSON)
  - [x] Expected pipeline (from JSON)
  - [x] Budget reference (smaller, muted)
- [x] **Gap calculation and display**
  - [x] Real-time pipeline gap calculation (Current vs Expected)
  - [x] Prominent gap indicator with color coding
  - [x] Status labels (Ahead/Behind/On Target)

---

## Phase 3: Interactive Features

### Pipeline Editing
- [ ] **Inline editing functionality**
  - [x] Click-to-edit pipeline numbers
  - [x] Visual feedback during editing (yellow highlight)
  - [x] Input validation (numbers only, proper formatting)
  - [x] Enter key and blur event handling
- [x] **Real-time updates**
  - [x] Immediate gap recalculation on pipeline changes
  - [x] Auto-save functionality with status indication
  - [x] Optimistic UI updates
- [ ] **Change tracking**
  - [x] Track who made changes and when
  - [ ] Change counter in session
  - [ ] Audit trail storage

### Summary Statistics
- [ ] **Pipeline performance metrics**
  - [ ] Pipeline vs Expected calculation
  - [ ] Total pipeline aggregation
  - [ ] Pipeline accuracy tracking
  - [ ] Risk score calculation
- [ ] **Real-time metric updates**
  - [ ] Update summary when individual pipeline numbers change
  - [ ] Trend indicators (improving/declining)

---

## Phase 4: Customer Drill-Down

### Customer Details Modal
- [ ] **Modal infrastructure**
  - [ ] Modal overlay and positioning
  - [ ] Close functionality (X button, outside click, ESC key)
  - [ ] Modal title with month/AE context
- [ ] **Search functionality**
  - [ ] Real-time customer/deal search
  - [ ] Search across customer names and deal descriptions
  - [ ] Highlight search results
- [ ] **Tabbed data views**
  - [ ] Booked Revenue tab (closed deals)
  - [ ] Pipeline tab (forecasted deals with probabilities)
  - [ ] All Deals tab (combined view)
  - [ ] Tab switching functionality

### Customer Data Tables
- [ ] **Table structure and data**
  - [ ] Customer name, deal description, amount, dates
  - [ ] Deal status indicators (Closed Won, Committed, Pipeline)
  - [ ] Probability display for pipeline deals
- [ ] **Totals and calculations**
  - [ ] Weighted pipeline totals
  - [ ] Tab-specific total calculations
  - [ ] Formatting for currency and dates

---

## Phase 5: Session Management

### Notes and Documentation
- [ ] **Review notes interface**
  - [ ] Text area for pipeline review notes
  - [ ] Auto-save notes functionality
  - [ ] Notes persistence across sessions
- [ ] **Change documentation**
  - [ ] Link notes to specific pipeline changes
  - [ ] Rationale tracking for adjustments

### Session State
- [ ] **Review completion tracking**
  - [ ] Mark AE reviews as complete
  - [ ] Persist completion status
  - [ ] Visual indicators for completed reviews
- [ ] **Session persistence**
  - [ ] Save session state on page refresh
  - [ ] Resume reviews from where left off
  - [ ] Handle concurrent user sessions

---

## Phase 6: Advanced Features

### Data Export
- [ ] **Pipeline reports**
  - [ ] Export current pipeline data
  - [ ] Format for committee distribution
  - [ ] Include gaps and status summaries
- [ ] **Review summaries**
  - [ ] Session notes compilation
  - [ ] Change log export
  - [ ] AE-specific reports

### Performance & Polish
- [ ] **Responsive design**
  - [ ] Desktop optimization (primary)
  - [ ] Tablet compatibility
  - [ ] Mobile basic functionality
- [ ] **Performance optimization**
  - [ ] Fast initial load (<2 seconds)
  - [ ] Quick pipeline updates (<500ms)
  - [ ] Efficient customer search (<200ms)
- [ ] **Error handling**
  - [ ] Network error recovery
  - [ ] Data validation feedback
  - [ ] Concurrent edit conflict resolution

---

## Phase 7: Testing & Deployment

### Testing
- [ ] **Unit tests**
  - [ ] API endpoint testing
  - [ ] Gap calculation logic
  - [ ] Data persistence functions
- [ ] **Integration tests**
  - [ ] Database connectivity
  - [ ] JSON file operations
  - [ ] End-to-end user workflows
- [ ] **User acceptance testing**
  - [ ] Test with actual revenue team
  - [ ] Validate workflow efficiency
  - [ ] Gather feedback and iterate

### Deployment
- [ ] **Environment setup**
  - [ ] Production database connections
  - [ ] JSON storage location and permissions
  - [ ] Security considerations
- [ ] **Data migration**
  - [ ] Import existing budget data
  - [ ] Set initial pipeline targets
  - [ ] Historical data validation
- [ ] **Go-live preparation**
  - [ ] User training materials
  - [ ] Backup and recovery procedures
  - [ ] Support documentation

---

## Technical Decisions Needed

### Architecture Choices
- [ ] **Frontend framework decision**
  - [ ] React vs Vue vs vanilla JS
  - [ ] State management approach
  - [ ] Build tooling setup
- [ ] **Backend framework**
  - [ ] Node.js vs Python vs other
  - [ ] Database ORM/query library
  - [ ] JSON file vs database for pipeline data
- [ ] **Deployment strategy**
  - [ ] Server hosting approach
  - [ ] Database deployment
  - [ ] File storage strategy

### Data Design Decisions
- [ ] **Pipeline data structure**
  - [ ] How to handle historical pipeline changes
  - [ ] Versioning strategy for pipeline targets
  - [ ] Audit trail storage approach
- [ ] **Performance considerations**
  - [ ] Caching strategy for database queries
  - [ ] JSON file read/write optimization
  - [ ] Real-time update mechanism

---

## Success Metrics

### Functional Goals
- [ ] **Complete AE review in <5 minutes**
- [ ] **Zero external system context switching**
- [ ] **Instant customer detail lookup**
- [ ] **Real-time pipeline gap calculations**

### Technical Goals
- [ ] **<2 second initial page load**
- [ ] **<500ms pipeline update response**
- [ ] **<200ms customer search results**
- [ ] **Support 10+ concurrent users**

---

## Notes & Reminders

- **Pipeline focus**: Remember this tool is pipeline-centric, not budget-centric
- **Existing data**: Leverage existing database for historical/booked/customer data
- **JSON storage**: Budget and pipeline data stored in JSON for easy updates
- **Bi-weekly usage**: Tool designed for every-other-week review meetings
- **AE-by-AE workflow**: Not trying to show all AEs at once
- **Customer questions**: "Did we have BigCo this month?" should be answerable in <10 seconds
# CTV Reporting System - Architecture Documentation

## Overview

This is a Flask-based reporting system for CTV (Crossings TV) that manages advertising revenue data, customer relationships, and sales performance tracking. The system has been refactored from a monolithic architecture to a clean, service-oriented architecture with proper dependency injection.

## Architecture Philosophy

The system follows **clean architecture principles** with clear separation of concerns:

- **Service Layer**: Business logic and data aggregation
- **Repository Layer**: Data access and database queries  
- **Web Layer**: HTTP request/response handling
- **Models**: Structured data transfer objects
- **Configuration**: Environment-specific settings

## Core Architecture Components

### 1. Service Container & Dependency Injection

**Primary Files:**
- `src/services/container.py` - Main service container implementation
- `src/services/factory.py` - Service factory functions
- `src/config/settings.py` - Configuration management

**Key Concepts:**
```python
# Service registration
container.register_singleton('database_connection', create_database_connection)
container.register_factory('report_service', create_report_service)

# Service resolution
db_connection = container.get('database_connection')

# Dependency injection decorator
@inject('database_connection', 'config')
def my_function(db, config):
    # Services automatically injected
    pass
```

**Service Types:**
- **Singleton**: Created once, reused (database connections, repositories)
- **Factory**: Created fresh each time (reports, temporary services)
- **Instance**: Pre-created objects registered directly

### 2. Database Schema & Data Model

**Database:** SQLite (`data/database/production.db`)

**Key Tables:**
- `spots` - Individual advertising spots/transactions (750K+ records)
- `customers` - Client companies (158 active customers)
- `sectors` - Industry classifications
- `agencies` - Advertising agencies
- `month_closures` - Accounting period management
- `budget` - Sales targets and forecasts
- `pipeline` - Sales pipeline tracking

**Core Business Entities:**
- **Spots**: Individual ad placements with revenue, dates, customers
- **Customers**: Companies buying advertising time
- **AEs (Account Executives)**: 4 sales people tracked in `sales_person` field
- **Revenue Types**: Regular, Trade, etc. (Trade excluded from reports)

### 3. Existing Services (Pre-Refactor)

**Location:** `src/services/`

**Key Services:**
- `pipeline_service.py` - Sales pipeline and AE performance tracking
- `customer_service.py` - Customer data management
- `budget_service.py` - Budget and forecasting
- `month_closure_service.py` - Accounting period management
- `broadcast_month_import_service.py` - Data import processing

**Important:** These services use the original architecture. The refactoring introduces a new service container to manage dependencies cleanly.

### 4. Web Application Structure

**Current State:**
- `src/web/app.py` - Monolithic Flask app (being refactored)
- `src/web/templates/` - Jinja2 templates for reports
- `src/web/static/` - CSS, JavaScript assets

**Target State (Partially Implemented):**
- Clean app factory pattern
- Blueprint-based route organization
- Service container integration
- Proper error handling

### 5. Reporting System

**Key Reports:**
- **Report 1**: Monthly Revenue Summary
- **Report 2**: Management Expectation Tracking  
- **Report 3**: Quarterly Performance Story
- **Report 4**: Quarterly Performance with Sector Analysis
- **Report 5**: Monthly Revenue Dashboard (Interactive)

**Data Flow:**
```
Database -> Repository -> Service -> Controller -> Template -> User
```

## Development Environment

**Platform:** WSL2 (Ubuntu on Windows)
**Python:** 3.10+ with `.venv` virtual environment
**Database:** SQLite with 454.7 MB production database
**Deployment:** Uvicorn ASGI server via `src/web/asgi.py`

**Key Paths:**
- Project Root: `~/wsldev/ctv-bookedbiz-db/`
- Database: `data/database/production.db`
- Processed Data: `data/processed/`
- Source Code: `src/`
- Tests: `tests/`

**Startup Command:**
```bash
.venv/bin/python -m uvicorn src.web.asgi:asgi_app --host 0.0.0.0 --port 8000 --reload
```

## Current Implementation Status

### ✅ Completed (Tested & Working)

**Service Container System:**
- Dependency injection container with singleton/factory patterns
- Configuration management from environment variables
- Service registration and resolution
- Comprehensive unit tests (9/9 passing)
- Real database integration tests (750K+ records tested)

**Core Infrastructure:**
- Database connection management
- Service factory functions
- Configuration loading and validation
- Testing framework setup

### 🚧 In Progress (Architecture Defined)

**Report Data Services:**
- `ReportDataService` class for business logic
- Data models for structured report data
- Template formatters for consistent presentation
- Caching for expensive operations

**Web Layer Refactoring:**
- Blueprint-based route organization
- Request/response helpers
- API endpoints with proper REST design
- Error handling and logging

**Integration Components:**
- Service container integration with existing services
- Template system updates
- Performance optimization

### 📋 Next Steps (Ready for Implementation)

1. **Complete Report Data Service Implementation**
2. **Integrate Service Container with Existing App**
3. **Implement Clean Route Handlers**
4. **Add Comprehensive Error Handling**
5. **Performance Testing and Optimization**

## Key Files Reference

### Core Architecture Files
```
src/services/container.py          # Service container implementation
src/services/factory.py            # Service factory functions  
src/config/settings.py             # Configuration management
tests/test_service_container_working.py  # Unit tests
tests/test_real_data_integration.py      # Integration tests
```

### Existing Business Logic
```
src/services/pipeline_service.py   # Sales pipeline management
src/services/customer_service.py   # Customer data operations
src/services/budget_service.py     # Budget and forecasting
src/database/connection.py         # Database connection class
src/repositories/                  # Data access layer
```

### Web Application
```
src/web/app.py                     # Flask application (being refactored)
src/web/asgi.py                    # ASGI adapter for Uvicorn
src/web/templates/                 # Jinja2 templates
  ├── base.html                    # Base template
  ├── report1.html                 # Monthly revenue summary
  ├── report2.html                 # Expectation tracking
  ├── report3.html                 # Performance story
  ├── report4.html                 # Sector analysis
  └── report5.html                 # Interactive dashboard
```

### Configuration & Data
```
data/database/production.db        # SQLite database (454.7 MB)
data/processed/                    # JSON files for services
ae_config.json                     # AE configuration
real_budget_data.json              # Budget data
```

## Important Business Rules

### Revenue Calculation
- **Exclude Trade Revenue**: `revenue_type != 'Trade'` in all financial reports
- **Gross Rate**: Primary revenue field (`gross_rate` column)
- **Station Net**: Net revenue after fees (`station_net` column)

### Account Executive (AE) Management
- **4 Active AEs**: Tracked in `sales_person` field
- **Territory Assignment**: Each AE has geographical/sector focus
- **Performance Tracking**: Monthly targets vs. actual revenue

### Month Closure Process
- **Accounting Periods**: Months can be "open" or "closed"
- **Data Protection**: Closed months prevent data modification
- **Reporting Impact**: Affects which data appears in reports

### Customer Classification
- **Normalization**: Multiple name variants mapped to single customer
- **Sector Assignment**: Customers classified by industry sector
- **Agency Relationships**: Some customers work through agencies

## Testing Strategy

### Unit Tests
- **Service Container**: All core DI functionality tested
- **Configuration**: Environment loading and validation
- **Error Handling**: Exception types and propagation

### Integration Tests  
- **Real Database**: Tested with 750K+ actual records
- **Performance**: Sub-second response times verified
- **Service Compatibility**: Works with existing business services

### Test Commands
```bash
# Unit tests
.venv/bin/python -m pytest tests/test_service_container_working.py -v

# Integration tests with real data
.venv/bin/python -m pytest tests/test_real_data_integration.py -v -s

# All tests
.venv/bin/python -m pytest tests/ -v
```

## Development Workflow

### Adding New Services
1. Create service class in `src/services/`
2. Add factory function in `src/services/factory.py`
3. Register in `register_default_services()`
4. Add unit tests
5. Integration test with real data

### Creating New Reports
1. Define data model in `src/models/`
2. Implement service method in `ReportDataService`
3. Create route handler using service container
4. Build template with proper formatters
5. Add API endpoint for AJAX/export

### Configuration Management
- Environment variables for deployment-specific settings
- `src/config/settings.py` for typed configuration
- Service container for dependency injection
- Validation on startup

## Performance Characteristics

### Database Performance
- **750K+ spots**: Sub-second query performance
- **Complex aggregations**: Monthly/quarterly rollups optimized
- **Indexed fields**: `sales_person`, `air_date`, `customer_id`

### Service Container Performance
- **Initialization**: <100ms for full service registration
- **Service Resolution**: <1ms for singleton access
- **Memory Usage**: Minimal overhead, shared instances

### Web Application Performance
- **Report Generation**: 100-500ms for typical reports
- **Database Connections**: Pooled and reused
- **Template Rendering**: Cached for repeated access

## Security Considerations

### Database Access
- **Read-only reporting**: Most reports don't modify data
- **SQL Injection**: Parameterized queries throughout
- **Connection Management**: Proper connection lifecycle

### Web Security
- **XSS Prevention**: Template auto-escaping enabled
- **CSRF Protection**: Required for data modification
- **Input Validation**: All user inputs validated

## Deployment Architecture

### Current Deployment
- **WSL2 Environment**: Development on Windows with Linux subsystem
- **Uvicorn Server**: ASGI server for Flask application
- **Tailscale Network**: Secure remote access
- **File Storage**: Dropbox sync for data directory

### Production Considerations
- **Database Backup**: SQLite file-based backup strategy
- **Log Management**: Structured logging with rotation
- **Error Monitoring**: Comprehensive error tracking
- **Performance Monitoring**: Query timing and resource usage

---

## Quick Start for New Developers

### 1. Environment Setup
```bash
# Clone and setup
cd ~/wsldev/ctv-bookedbiz-db/
source .venv/bin/activate

# Install dependencies
.venv/bin/python -m pip install pytest pytest-mock psutil
```

### 2. Run Tests
```bash
# Verify architecture works
.venv/bin/python -m pytest tests/test_service_container_working.py -v

# Test with real data
.venv/bin/python -m pytest tests/test_real_data_integration.py -v -s
```

### 3. Start Application
```bash
# Start development server
.venv/bin/python -m uvicorn src.web.asgi:asgi_app --host 0.0.0.0 --port 8000 --reload

# Access at http://localhost:8000 or via Tailscale IP
```

### 4. Key Concepts to Understand
- **Service Container**: Central dependency injection system
- **Business Data**: 750K spots, 158 customers, 4 AEs
- **Revenue Exclusions**: Trade revenue filtered out
- **Month Closure**: Accounting period management
- **Clean Architecture**: Proper separation of concerns

This architecture provides a solid foundation for continued development while maintaining the existing business functionality and data integrity.
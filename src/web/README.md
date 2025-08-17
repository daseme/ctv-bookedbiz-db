# CTV Booked Biz Reports - Flask Web UI

A Flask-based web application for generating and viewing revenue reports from the CTV Booked Biz database.

## Overview

This web application provides four main reporting views:

1. **Monthly Revenue Summary** (`/report1`) - Comprehensive breakdown of revenue by month
2. **Expectation Tracking** (`/report2`) - Management expectation tracking with AE performance
3. **Performance Story** (`/report3`) - Quarterly performance analysis with year-over-year comparisons
4. **Sector Analysis** (`/report4`) - Enhanced quarterly performance with detailed sector analysis

## Features

- **Interactive Charts**: Powered by Chart.js for dynamic data visualization
- **Responsive Design**: Modern, clean UI that works on desktop and mobile
- **API Endpoints**: RESTful API for programmatic access to report data
- **Real-time Data**: All reports pull live data from the SQLite database
- **Error Handling**: Comprehensive error handling with user-friendly messages

## Requirements

- Python 3.10+
- Flask 2.3.0+
- SQLite database at `data/database/test.db`
- Existing repository layer in `src/repositories/`

## Installation

1. **Install dependencies**:
   ```bash
   pip install flask pytest
   # or using uv:
   uv sync
   ```

2. **Verify database exists**:
   ```bash
   ls -la data/database/test.db
   ```

3. **Ensure repository layer is available**:
   ```bash
   ls -la src/repositories/sqlite_repositories.py
   ```

## Usage

### Running the Application

1. **Start the Flask development server**:
   ```bash
   cd src/web
   python app.py
   ```

2. **Access the application**:
   - Open browser to `http://localhost:5000`
   - Navigate through the different reports using the navigation menu

### Alternative Startup Methods

**Using Flask CLI**:
```bash
cd src/web
export FLASK_APP=app.py
export FLASK_ENV=development
flask run
```

**Production deployment** (using Gunicorn):
```bash
pip install gunicorn
cd src/web
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## API Endpoints

The application provides RESTful API endpoints for programmatic access:

| Endpoint | Description | Response Format |
|----------|-------------|-----------------|
| `GET /api/data/monthly` | Monthly revenue summary data | JSON array |
| `GET /api/data/quarterly` | Quarterly performance data | JSON object |
| `GET /api/data/ae` | Account Executive performance data | JSON array |
| `GET /api/data/sectors` | Sector analysis data | JSON object |

### API Usage Examples

**Get monthly revenue data**:
```bash
curl http://localhost:5000/api/data/monthly
```

**Get quarterly performance data**:
```bash
curl http://localhost:5000/api/data/quarterly
```

**Using in Python**:
```python
import requests

# Get monthly data
response = requests.get('http://localhost:5000/api/data/monthly')
monthly_data = response.json()

# Get AE performance data
response = requests.get('http://localhost:5000/api/data/ae')
ae_data = response.json()
```

## Report Details

### Report 1: Monthly Revenue Summary
- **Route**: `/report1`
- **Data Source**: `queries/monthly-revenue-summary.sql`
- **Features**: Monthly breakdown, spot counts, revenue totals, charts
- **Charts**: Bar chart for revenue trend, line chart for spot counts

### Report 2: Expectation Tracking
- **Route**: `/report2`
- **Features**: Quarterly analysis, AE performance cards, status tracking
- **Charts**: Multi-year quarterly comparison
- **Key Metrics**: Performance badges, revenue targets

### Report 3: Performance Story
- **Route**: `/report3`
- **Features**: Year-over-year analysis, historical context, AE rankings
- **Charts**: YoY comparison, historical progression
- **Focus**: Revenue progression narrative

### Report 4: Sector Analysis
- **Route**: `/report4`
- **Features**: Sector performance, concentration risk, enhanced AE metrics
- **Charts**: Sector doughnut chart, quarterly progression
- **Analytics**: Risk assessment, diversification insights

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/web/test_routes.py

# Run with verbose output
pytest tests/web/test_routes.py -v

# Run specific test class
pytest tests/web/test_routes.py::TestRoutes

# Run with coverage
pytest tests/web/test_routes.py --cov=src/web
```

### Test Coverage

The test suite covers:
- All route endpoints (GET requests)
- API endpoints with success and error scenarios
- Data retrieval functions with mocked database calls
- Error handling and edge cases
- JSON response validation

## File Structure

```
src/web/
├── app.py              # Main Flask application
├── templates/          # Jinja2 templates
│   ├── base.html      # Base template with common styling
│   ├── index.html     # Home page
│   ├── report1.html   # Monthly Revenue Summary
│   ├── report2.html   # Expectation Tracking
│   ├── report3.html   # Performance Story
│   └── report4.html   # Sector Analysis
└── README.md          # This file

tests/web/
└── test_routes.py     # Comprehensive test suite
```

## Configuration

### Environment Variables

- `FLASK_ENV`: Set to `development` for debug mode
- `FLASK_APP`: Should be `app.py`
- `DB_PATH`: Database path (defaults to `../../data/database/test.db`)

### Database Configuration

The application expects:
- SQLite database at `data/database/test.db` (relative to project root)
- Tables: `spots`, `customers`, `sectors` with proper relationships
- Repository layer in `src/repositories/sqlite_repositories.py`

## Troubleshooting

### Common Issues

1. **ImportError: No module named 'flask'**
   ```bash
   pip install flask
   ```

2. **Database connection error**
   - Verify database exists: `ls -la data/database/test.db`
   - Check database permissions
   - Ensure path is correct relative to `src/web/`

3. **Template not found error**
   - Verify templates directory exists: `ls -la src/web/templates/`
   - Check template file names match route handlers

4. **Repository import error**
   - Verify repository files exist: `ls -la src/repositories/`
   - Check Python path configuration in `app.py`

### Debug Mode

Enable detailed error messages:
```bash
export FLASK_ENV=development
export FLASK_DEBUG=1
python app.py
```

### Logging

The application logs to console by default. For production, configure proper logging:

```python
import logging
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s %(message)s'
)
```

## Performance Considerations

- **Database Connections**: Application reuses connections through the repository layer
- **Caching**: Consider implementing Redis/Memcached for frequently accessed data
- **Static Files**: Serve static files (CSS, JS) through a web server in production
- **Database Optimization**: Ensure proper indexes on frequently queried columns

## Security Considerations

- **Input Validation**: All user inputs are handled through Flask's secure methods
- **SQL Injection**: Using parameterized queries through the repository layer
- **CORS**: Configure CORS headers if API will be accessed from other domains
- **Authentication**: Add authentication/authorization as needed for production

## Contributing

1. Follow PEP8 style guidelines
2. Add tests for new functionality
3. Update documentation for new features
4. Use type hints for all functions
5. Add comprehensive docstrings

## License

This project is part of the CTV Booked Biz revenue database system. 
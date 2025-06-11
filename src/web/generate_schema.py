import sqlite3
import json
from datetime import datetime

def get_schema():
    conn = sqlite3.connect('../../data/database/production.db')
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall()]
    
    schema = {
        'database_name': 'production.db',
        'generated_at': datetime.now().isoformat(),
        'description': 'CTV Booked Business Database - Revenue tracking and management system',
        'tables': {}
    }
    
    for table in tables:
        # Get table schema
        cursor.execute(f'PRAGMA table_info({table})')
        columns = cursor.fetchall()
        
        # Get sample data count
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        row_count = cursor.fetchone()[0]
        
        # Get distinct values for key columns (first 5)
        key_columns = []
        for col in columns[:3]:  # Check first 3 columns for key info
            col_name = col[1]
            try:
                cursor.execute(f'SELECT DISTINCT {col_name} FROM {table} LIMIT 5')
                distinct_values = [str(row[0]) for row in cursor.fetchall() if row[0] is not None]
                if distinct_values:
                    key_columns.append({
                        'column': col_name,
                        'sample_values': distinct_values
                    })
            except:
                pass
        
        schema['tables'][table] = {
            'description': get_table_description(table),
            'row_count': row_count,
            'columns': [
                {
                    'name': col[1],
                    'type': col[2],
                    'not_null': bool(col[3]),
                    'default_value': col[4],
                    'primary_key': bool(col[5]),
                    'description': get_column_description(table, col[1])
                } for col in columns
            ],
            'key_data_samples': key_columns
        }
    
    conn.close()
    return schema

def get_table_description(table_name):
    descriptions = {
        'spots': 'Main revenue table containing all booked advertising spots with customer and sales data',
        'customers': 'Customer master data with normalized names and sector classifications',
        'pipeline': 'Revenue pipeline tracking for forecasting and budget management',
        'budget': 'Annual budget allocations by AE and time period',
        'review_sessions': 'Performance review session tracking and snapshots',
        'ae_budgets': 'Account Executive budget assignments and tracking',
        'month_closures': 'Monthly financial period closure tracking',
        'agencies': 'Advertising agency master data',
        'markets': 'Geographic market definitions',
        'sectors': 'Business sector classifications and hierarchies'
    }
    return descriptions.get(table_name, f'Database table: {table_name}')

def get_column_description(table_name, column_name):
    descriptions = {
        'spots': {
            'gross_rate': 'Revenue amount for the advertising spot',
            'broadcast_month': 'Month when the ad was/will be broadcast',
            'sales_person': 'Account Executive responsible for the sale',
            'customer_id': 'Unique customer identifier',
            'revenue_type': 'Type of revenue (e.g., Trade, Cash)',
            'sector': 'Business sector classification',
            'agency_id': 'Advertising agency identifier',
            'market_id': 'Geographic market identifier',
            'bill_code': 'Billing code for agency tracking'
        },
        'customers': {
            'customer_id': 'Unique customer identifier (primary key)',
            'original_name': 'Customer name as originally entered',
            'normalized_name': 'Standardized customer name for reporting',
            'sector': 'Business sector assignment'
        },
        'pipeline': {
            'ae_name': 'Account Executive name',
            'year': 'Pipeline year',
            'month': 'Pipeline month',
            'pipeline_amount': 'Forecasted revenue amount',
            'is_current': 'Whether this is the current active pipeline version'
        },
        'month_closures': {
            'broadcast_month': 'Month that was closed (Mmm-YY format)',
            'closed_by': 'User who closed the month',
            'closed_at': 'Timestamp when month was closed'
        }
    }
    
    table_cols = descriptions.get(table_name, {})
    return table_cols.get(column_name, f'Column: {column_name}')

if __name__ == "__main__":
    schema = get_schema()
    
    # Write to JSON file
    with open('database_schema.json', 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, default=str)
    
    # Also create a markdown version for better readability
    md_content = f"""# CTV Booked Business Database Schema

**Generated:** {schema['generated_at']}  
**Database:** {schema['database_name']}

{schema['description']}

## Tables Overview

"""
    
    for table_name, table_info in schema['tables'].items():
        md_content += f"### {table_name}\n\n"
        md_content += f"**Description:** {table_info['description']}  \n"
        md_content += f"**Row Count:** {table_info['row_count']:,}\n\n"
        
        md_content += "| Column | Type | Null | PK | Default | Description |\n"
        md_content += "|--------|------|------|----|---------|-----------|\n"
        
        for col in table_info['columns']:
            null_str = "No" if col['not_null'] else "Yes"
            pk_str = "PK" if col['primary_key'] else ""
            default_str = str(col['default_value']) if col['default_value'] else ""
            
            md_content += f"| `{col['name']}` | {col['type']} | {null_str} | {pk_str} | {default_str} | {col['description']} |\n"
        
        if table_info['key_data_samples']:
            md_content += "\n**Sample Data:**\n"
            for sample in table_info['key_data_samples']:
                values_str = ", ".join(sample['sample_values'][:3])
                if len(sample['sample_values']) > 3:
                    values_str += "..."
                md_content += f"- `{sample['column']}`: {values_str}\n"
        
        md_content += "\n---\n\n"
    
    with open('database_schema.md', 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    print("Schema files generated:")
    print("database_schema.json - Machine-readable JSON format")
    print("database_schema.md - Human-readable Markdown format")
    print(f"\nDatabase Summary:")
    print(f"   Tables: {len(schema['tables'])}")
    for table_name, table_info in schema['tables'].items():
        print(f"   • {table_name}: {table_info['row_count']:,} rows") 
import sqlite3
import sys
import os

def get_sqlite_schema(db_path, output_file=None):
    """
    Extract schema from SQLite database and optionally save to file.
    
    Args:
        db_path (str): Path to the SQLite database file
        output_file (str, optional): Path to save schema output
    """
    
    # Check if database file exists
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return
    
    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return
        
        schema_output = []
        schema_output.append(f"=== SCHEMA FOR DATABASE: {db_path} ===\n")
        
        # Get schema for each table
        for table in tables:
            table_name = table[0]
            
            # Get CREATE TABLE statement
            cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
            create_statement = cursor.fetchone()[0]
            
            # Get table info (columns, types, etc.)
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            # Add to output
            schema_output.append(f"TABLE: {table_name}")
            schema_output.append("-" * (len(table_name) + 7))
            
            if create_statement:
                schema_output.append("CREATE STATEMENT:")
                schema_output.append(create_statement + ";\n")
            
            schema_output.append("COLUMNS:")
            for col in columns:
                col_id, name, data_type, not_null, default_val, pk = col
                pk_info = " (PRIMARY KEY)" if pk else ""
                null_info = " NOT NULL" if not_null else ""
                default_info = f" DEFAULT {default_val}" if default_val is not None else ""
                schema_output.append(f"  {name}: {data_type}{null_info}{default_info}{pk_info}")
            
            schema_output.append("")  # Empty line between tables
        
        # Get indexes
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL;")
        indexes = cursor.fetchall()
        
        if indexes:
            schema_output.append("INDEXES:")
            schema_output.append("-" * 8)
            for index_name, index_sql in indexes:
                schema_output.append(f"{index_name}:")
                schema_output.append(f"  {index_sql};")
            schema_output.append("")
        
        # Get views
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='view';")
        views = cursor.fetchall()
        
        if views:
            schema_output.append("VIEWS:")
            schema_output.append("-" * 6)
            for view_name, view_sql in views:
                schema_output.append(f"{view_name}:")
                schema_output.append(f"  {view_sql};")
            schema_output.append("")
        
        # Get triggers
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger';")
        triggers = cursor.fetchall()
        
        if triggers:
            schema_output.append("TRIGGERS:")
            schema_output.append("-" * 9)
            for trigger_name, trigger_sql in triggers:
                schema_output.append(f"{trigger_name}:")
                schema_output.append(f"  {trigger_sql};")
        
        # Join all output
        final_output = "\n".join(schema_output)
        
        # Print to console
        print(final_output)
        
        # Save to file if specified
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(final_output)
            print(f"\nSchema saved to: {output_file}")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def main():
    # Command line usage
    if len(sys.argv) < 2:
        print("Usage: python sqlite_schema.py <database_path> [output_file]")
        print("Example: python sqlite_schema.py C:\\path\\to\\database.db")
        print("Example: python sqlite_schema.py C:\\path\\to\\database.db schema_output.txt")
        
        # Interactive mode if no arguments
        db_path = input("Enter path to SQLite database: ").strip().strip('"')
        output_file = input("Enter output file path (optional, press Enter to skip): ").strip().strip('"')
        
        if not output_file:
            output_file = None
            
        get_sqlite_schema(db_path, output_file)
    else:
        db_path = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        get_sqlite_schema(db_path, output_file)

if __name__ == "__main__":
    main()
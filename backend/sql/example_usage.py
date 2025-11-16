"""
Example usage of the File to SQL converter system.

This demonstrates how to use the semantic attribute matching
and schema evolution system to convert files to SQL.
"""

from pathlib import Path
from file_to_sql import FileToSQLConverter, convert_file_to_sql
from config import get_db_config


def example_basic_usage():
    """Basic example of converting a file to SQL."""
    print("=" * 60)
    print("Example 1: Basic File to SQL Conversion")
    print("=" * 60)
    
    # Convert a file
    file_path = "example_data.csv"  # Replace with your file path
    
    result = convert_file_to_sql(file_path)
    
    if result['success']:
        print(f"✅ Successfully converted file!")
        print(f"   Table: {result['table_name']}")
        print(f"   Rows inserted: {result['rows_inserted']}")
        print(f"   Decision: {result['decision']}")
        print(f"   Match ratio: {result['match_ratio']:.0%}")
    else:
        print(f"❌ Conversion failed: {result['error']}")


def example_with_custom_table():
    """Example with custom table name and primary key."""
    print("\n" + "=" * 60)
    print("Example 2: Custom Table Name and Primary Key")
    print("=" * 60)
    
    file_path = "products.csv"
    
    result = convert_file_to_sql(
        file_path,
        table_name="products",
        primary_key="product_id"
    )
    
    print(f"Decision: {result['decision']}")
    if result['mapping']:
        print("Attribute mappings:")
        for new_attr, existing_attr in result['mapping'].items():
            print(f"  {new_attr} → {existing_attr}")


def example_schema_evolution():
    """Example demonstrating schema evolution."""
    print("\n" + "=" * 60)
    print("Example 3: Schema Evolution")
    print("=" * 60)
    
    # First file - creates table
    print("\n1. First file (creates new table):")
    result1 = convert_file_to_sql("products_v1.csv")
    print(f"   Decision: {result1['decision']}")
    print(f"   Table: {result1['table_name']}")
    
    # Second file - evolves schema
    print("\n2. Second file (evolves schema):")
    result2 = convert_file_to_sql("products_v2.csv")  # Has new fields
    print(f"   Decision: {result2['decision']}")
    print(f"   Table: {result2['table_name']}")
    print(f"   New fields: {result2['new_fields']}")
    print(f"   Match ratio: {result2['match_ratio']:.0%}")


def example_advanced_usage():
    """Advanced example with custom database config."""
    print("\n" + "=" * 60)
    print("Example 4: Advanced Usage with Custom Config")
    print("=" * 60)
    
    # Custom database config
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'mydb',
        'user': 'postgres',
        'password': 'password'
    }
    
    # Create converter instance
    converter = FileToSQLConverter(db_config)
    
    # Convert multiple files
    files = ["file1.csv", "file2.json", "file3.xlsx"]
    
    for file_path in files:
        print(f"\nConverting {file_path}...")
        result = converter.convert_file(Path(file_path))
        
        if result['success']:
            print(f"  ✅ {result['rows_inserted']} rows inserted into {result['table_name']}")
        else:
            print(f"  ❌ Error: {result['error']}")
    
    # Get all logs
    print("\nAll logs:")
    for log in converter.get_logs():
        print(f"  {log}")


if __name__ == "__main__":
    print("File to SQL Converter - Examples")
    print("=" * 60)
    print("\nNote: These examples require:")
    print("  1. PostgreSQL database running")
    print("  2. Database configured in config.py or environment variables")
    print("  3. Sample data files")
    print("\nUncomment the example you want to run:\n")
    
    # Uncomment to run examples:
    # example_basic_usage()
    # example_with_custom_table()
    # example_schema_evolution()
    # example_advanced_usage()
    
    print("\n" + "=" * 60)
    print("Quick Start:")
    print("=" * 60)
    print("""
# 1. Setup database
python database_setup.py

# 2. Convert a file
python file_to_sql.py your_file.csv

# 3. Or use in code
from file_to_sql import convert_file_to_sql
result = convert_file_to_sql("your_file.csv")
print(result)
    """)



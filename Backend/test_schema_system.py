"""
End-to-end test script for the Schema Generator and Executor system.

This script:
1. Creates test files (JSON, CSV, XML, etc.)
2. Runs Schema Generator (Program A)
3. Shows generated SQL
4. Runs Schema Executor (Program B)
5. Verifies results
"""

import json
import csv
from pathlib import Path
from schema_generator import generate_schema
from schema_executor import execute_pending_jobs, SchemaExecutor

# Try to import config module for cloud support
try:
    from config import get_db_config, get_cloud_db_config, validate_db_config
    import sys
    
    # Check for provider argument
    provider = None
    if '--provider' in sys.argv:
        idx = sys.argv.index('--provider')
        if idx + 1 < len(sys.argv):
            provider = sys.argv[idx + 1]
    
    # Use cloud config if provider specified, otherwise default to local
    if provider:
        DB_CONFIG = get_cloud_db_config(provider)
        print(f"Using cloud provider: {provider}")
    else:
        print("Using local PostgreSQL (default)")
        DB_CONFIG = get_db_config()
    
    if not validate_db_config(DB_CONFIG):
        print("[ERROR] Invalid database configuration!")
        print()
        print("Setup Instructions:")
        print("   1. Install PostgreSQL: https://www.postgresql.org/download/")
        print("   2. Create database: createdb clustro")
        print("   3. Set DB_PASSWORD environment variable")
        print("   4. Or use --provider flag for cloud databases")
        print()
        sys.exit(1)
except ImportError:
    print("[ERROR] Config module not found!")
    print("   Please make sure config.py is in the same directory.")
    import sys
    sys.exit(1)


def create_test_files():
    """Create sample test files."""
    test_dir = Path("test_files_schema")
    test_dir.mkdir(exist_ok=True)
    
    # Test 1: Simple JSON with flat structure
    simple_json = [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"}
    ]
    with open(test_dir / "users.json", "w") as f:
        json.dump(simple_json, f, indent=2)
    
    # Test 2: Nested JSON structure
    nested_json = [
        {
            "id": 1,
            "name": "Alice",
            "address": {
                "street": "123 Main St",
                "city": "NYC",
                "zip": "10001"
            },
            "orders": [
                {"order_id": 101, "product": "Laptop", "price": 999.99},
                {"order_id": 102, "product": "Mouse", "price": 29.99}
            ]
        },
        {
            "id": 2,
            "name": "Bob",
            "address": {
                "street": "456 Oak Ave",
                "city": "LA",
                "zip": "90001"
            },
            "orders": [
                {"order_id": 201, "product": "Keyboard", "price": 79.99}
            ]
        }
    ]
    with open(test_dir / "users_nested.json", "w") as f:
        json.dump(nested_json, f, indent=2)
    
    # Test 3: CSV file
    with open(test_dir / "products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "name", "price", "category", "in_stock"])
        writer.writerow([1, "Laptop", 999.99, "Electronics", True])
        writer.writerow([2, "Mouse", 29.99, "Electronics", True])
        writer.writerow([3, "Keyboard", 79.99, "Electronics", False])
    
    # Test 4: XML file
    xml_content = '''<?xml version="1.0"?>
<employees>
    <employee id="1">
        <name>Alice</name>
        <department>Engineering</department>
        <salary>75000</salary>
    </employee>
    <employee id="2">
        <name>Bob</name>
        <department>Sales</department>
        <salary>60000</salary>
    </employee>
</employees>'''
    with open(test_dir / "employees.xml", "w") as f:
        f.write(xml_content)
    
    print(f"[OK] Created test files in {test_dir}/")
    return test_dir


def test_schema_generator(file_path: str):
    """Test Schema Generator (Program A)."""
    print("\n" + "="*70)
    print("PROGRAM A: SCHEMA GENERATOR")
    print("="*70)
    print(f"Processing: {file_path}")
    print()
    
    result = generate_schema(file_path, DB_CONFIG)
    
    print(f"[OK] Success: {result['success']}")
    print(f"Tables detected: {len(result['tables'])}")
    print(f"SQL statements generated: {len(result['sql_statements'])}")
    print(f"Jobs created in schema_jobs: {result['jobs_created']}")
    
    if result['tables']:
        print(f"\nTables: {', '.join(result['tables'])}")
    
    if result['sql_statements']:
        print(f"\nGenerated SQL Statements:")
        for i, sql_stmt in enumerate(result['sql_statements'], 1):
            print(f"\n--- Statement {i} ---")
            print(sql_stmt)
    
    if result['errors']:
        print(f"\n[ERROR] Errors:")
        for error in result['errors']:
            print(f"  - {error}")
    
    return result


def test_schema_executor():
    """Test Schema Executor (Program B)."""
    print("\n" + "="*70)
    print("PROGRAM B: SCHEMA EXECUTOR")
    print("="*70)
    print("Executing pending jobs...")
    print()
    
    summary = execute_pending_jobs(DB_CONFIG, stop_on_error=False)
    
    print(f"\nTotal jobs: {summary['total_jobs']}")
    print(f"[OK] Completed: {summary['completed']}")
    print(f"[ERROR] Failed: {summary['failed']}")
    
    if summary.get('error'):
        print(f"\n[ERROR] Error: {summary['error']}")
    
    if summary['results']:
        print("\nDetailed Results:")
        for result in summary['results']:
            status = "[OK]" if result['success'] else "[ERROR]"
            job_id = result['job_id']
            error_msg = result.get('error_message', 'Success')
            print(f"  {status} Job {job_id}: {error_msg}")
    
    return summary


def check_job_status():
    """Check status of all jobs."""
    executor = SchemaExecutor(DB_CONFIG)
    status_summary = executor.get_job_status()
    
    print("\n" + "="*70)
    print("JOB STATUS SUMMARY")
    print("="*70)
    
    if status_summary:
        for status, count in status_summary.items():
            print(f"{status.capitalize()}: {count}")
    else:
        print("No jobs found")
    
    executor.close()


def main():
    """Run end-to-end test."""
    print("="*70)
    print("INTELLIGENT MULTI-MODAL STORAGE SYSTEM - TEST SUITE")
    print("="*70)
    
    # Create test files
    test_dir = create_test_files()
    
    # Test files to process
    test_files = [
        test_dir / "users.json",
        test_dir / "users_nested.json",
        test_dir / "products.csv",
        test_dir / "employees.xml"
    ]
    
    # Step 1: Run Schema Generator for each file
    print("\n" + "="*70)
    print("STEP 1: RUNNING SCHEMA GENERATOR (PROGRAM A)")
    print("="*70)
    
    for file_path in test_files:
        if file_path.exists():
            test_schema_generator(str(file_path))
            print()
    
    # Check job status before execution
    check_job_status()
    
    # Step 2: Run Schema Executor
    print("\n" + "="*70)
    print("STEP 2: RUNNING SCHEMA EXECUTOR (PROGRAM B)")
    print("="*70)
    
    test_schema_executor()
    
    # Check final job status
    check_job_status()
    
    print("\n" + "="*70)
    print("TEST COMPLETE")
    print("="*70)
    print("\nTo verify tables were created:")
    print("  psql -d clustro -c '\\dt'")
    print("\nTo check schema_jobs table:")
    print("  psql -d clustro -c 'SELECT * FROM schema_jobs;'")


if __name__ == "__main__":
    main()


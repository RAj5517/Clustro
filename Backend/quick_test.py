"""
Quick test script to verify the complete schema system is working.
This demonstrates the full workflow from file to database table.
"""

import json
import os
from pathlib import Path
from config import get_db_config
from schema_generator import generate_schema
from schema_executor import execute_pending_jobs

print("=" * 70)
print("QUICK TEST - Schema Generator and Executor")
print("=" * 70)
print()

# Get database config
db_config = get_db_config()
print(f"[OK] Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
print()

# Step 1: Create a simple test file
print("Step 1: Creating test file...")
test_file = Path("test_quick.json")
test_data = [
    {"product_id": 1, "name": "Laptop", "price": 999.99, "in_stock": True},
    {"product_id": 2, "name": "Mouse", "price": 29.99, "in_stock": True},
    {"product_id": 3, "name": "Keyboard", "price": 79.99, "in_stock": False}
]

with open(test_file, "w") as f:
    json.dump(test_data, f, indent=2)

print(f"[OK] Created test file: {test_file}")
print()

# Step 2: Generate schema
print("Step 2: Running Schema Generator...")
print(f"       Processing: {test_file}")
result = generate_schema(str(test_file), db_config)

if result['success']:
    print(f"[OK] Schema generated successfully!")
    print(f"       Tables: {', '.join(result['tables'])}")
    print(f"       Jobs created: {result['jobs_created']}")
    print(f"       SQL statements: {len(result['sql_statements'])}")
    
    if result['sql_statements']:
        print()
        print("Generated SQL:")
        print("-" * 70)
        print(result['sql_statements'][0])
        print("-" * 70)
else:
    print(f"[ERROR] Schema generation failed!")
    for error in result['errors']:
        print(f"  - {error}")
    exit(1)

print()

# Step 3: Execute pending jobs
print("Step 3: Running Schema Executor...")
print("       Executing pending SQL jobs...")
summary = execute_pending_jobs(db_config, stop_on_error=False)

print()
print(f"Results:")
print(f"  Total jobs: {summary['total_jobs']}")
print(f"  [OK] Completed: {summary['completed']}")
print(f"  [ERROR] Failed: {summary['failed']}")

if summary['failed'] > 0:
    print()
    print("Failed jobs:")
    for result in summary['results']:
        if not result['success']:
            print(f"  Job {result['job_id']}: {result.get('error_message', 'Unknown error')}")

print()

# Step 4: Verify table was created
print("Step 4: Verifying table was created...")
import psycopg2
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
    """, (result['tables'][0],))
    
    table_exists = cursor.fetchone()[0]
    
    if table_exists:
        print(f"[OK] Table '{result['tables'][0]}' exists in database!")
        
        # Count rows (should be 0 since we only created the schema, not inserted data)
        cursor.execute(f"SELECT COUNT(*) FROM {result['tables'][0]};")
        row_count = cursor.fetchone()[0]
        print(f"       Row count: {row_count}")
    else:
        print(f"[ERROR] Table '{result['tables'][0]}' was not created!")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"[ERROR] Could not verify table: {e}")

print()
print("=" * 70)
print("TEST COMPLETE")
print("=" * 70)
print()
print("To clean up the test file, run:")
print(f"  Remove-Item {test_file}")


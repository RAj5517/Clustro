"""
Script to clear all data from the database.

This script:
1. Drops all tables (except schema_jobs)
2. Optionally clears schema_jobs table
3. Optionally drops schema_jobs table as well

WARNING: This will delete all your data!
"""

import sys
from pathlib import Path

# Add Backend directory to path
backend_path = Path(__file__).parent
sys.path.insert(0, str(backend_path))
sys.path.insert(0, str(backend_path / 'sql'))

from config import get_db_config
import psycopg2
from psycopg2 import sql


def clear_all_tables(db_config, keep_schema_jobs=True, clear_schema_jobs=False):
    """
    Clear all data from the database.
    
    Args:
        db_config: Database configuration dictionary
        keep_schema_jobs: If True, keep schema_jobs table (default: True)
        clear_schema_jobs: If True, clear data from schema_jobs (default: False)
    """
    try:
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("=" * 70)
        print("CLEARING DATABASE")
        print("=" * 70)
        print(f"Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
        print()
        
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("[INFO] No tables found in database")
            cursor.close()
            conn.close()
            return
        
        print(f"Found {len(tables)} table(s):")
        for table in tables:
            print(f"  - {table}")
        print()
        
        # Ask for confirmation (if not automated)
        print("[WARNING] This will delete all data from the database!")
        print()
        
        # Drop all tables (except schema_jobs if keep_schema_jobs is True)
        dropped_count = 0
        for table_name in tables:
            if keep_schema_jobs and table_name == 'schema_jobs':
                continue
            
            try:
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE;").format(
                    sql.Identifier(table_name)
                ))
                print(f"[OK] Dropped table: {table_name}")
                dropped_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to drop table {table_name}: {e}")
        
        # Clear schema_jobs if requested
        if clear_schema_jobs and 'schema_jobs' in tables:
            try:
                cursor.execute("DELETE FROM schema_jobs;")
                print(f"[OK] Cleared data from schema_jobs table")
            except Exception as e:
                print(f"[ERROR] Failed to clear schema_jobs: {e}")
        
        # Drop schema_jobs if keep_schema_jobs is False and it exists
        if not keep_schema_jobs and 'schema_jobs' in tables:
            try:
                cursor.execute("DROP TABLE IF EXISTS schema_jobs CASCADE;")
                print(f"[OK] Dropped table: schema_jobs")
                dropped_count += 1
            except Exception as e:
                print(f"[ERROR] Failed to drop schema_jobs: {e}")
        
        print()
        print(f"[OK] Successfully dropped {dropped_count} table(s)")
        
        # Verify tables are gone
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
        """)
        remaining = cursor.fetchone()[0]
        
        print(f"Remaining tables: {remaining}")
        
        cursor.close()
        conn.close()
        
        print()
        print("=" * 70)
        print("DATABASE CLEARED")
        print("=" * 70)
        
    except Exception as e:
        print(f"[ERROR] Failed to clear database: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clear all data from the database')
    parser.add_argument('--all', action='store_true', 
                       help='Drop all tables including schema_jobs')
    parser.add_argument('--clear-jobs', action='store_true',
                       help='Clear data from schema_jobs table (but keep the table)')
    parser.add_argument('--force', action='store_true',
                       help='Skip confirmation prompt')
    
    args = parser.parse_args()
    
    db_config = get_db_config()
    
    keep_schema_jobs = not args.all
    clear_schema_jobs = args.clear_jobs
    
    if not args.force:
        print()
        print("=" * 70)
        print("WARNING: This will delete all data from the database!")
        print("=" * 70)
        print()
        print("Options:")
        print(f"  - Keep schema_jobs table: {keep_schema_jobs}")
        print(f"  - Clear schema_jobs data: {clear_schema_jobs}")
        print()
        response = input("Are you sure you want to continue? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Cancelled.")
            sys.exit(0)
    
    clear_all_tables(db_config, keep_schema_jobs=keep_schema_jobs, clear_schema_jobs=clear_schema_jobs)


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

from sql.config import get_db_config
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
        # Validate config before attempting connection
        if not db_config.get('password'):
            print("\n[WARNING] No database password provided.")
            print("This may work if PostgreSQL is configured for passwordless authentication.")
            print("If connection fails, please set DB_PASSWORD environment variable or add it to .env file.\n")
        
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
        
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        print(f"[ERROR] Failed to connect to database: {error_msg}")
        
        if "password" in error_msg.lower() or "authentication" in error_msg.lower():
            print("\n" + "=" * 70)
            print("DATABASE AUTHENTICATION ERROR")
            print("=" * 70)
            print("\nPossible solutions:")
            print("  1. Set DB_PASSWORD environment variable:")
            print("     $env:DB_PASSWORD='your_password'")
            print("  2. Create or update .env file in backend directory with:")
            print("     DB_PASSWORD=your_password")
            print("  3. Configure PostgreSQL for passwordless authentication")
            print("     (edit pg_hba.conf to use 'trust' or 'peer' authentication)")
            print("\nCurrent database configuration:")
            print(f"  Host: {db_config.get('host', 'N/A')}")
            print(f"  Port: {db_config.get('port', 'N/A')}")
            print(f"  Database: {db_config.get('database', 'N/A')}")
            print(f"  User: {db_config.get('user', 'N/A')}")
            print(f"  Password: {'(not set)' if not db_config.get('password') else '(set)'}")
            print("=" * 70 + "\n")
        
        raise
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
    
    try:
        db_config = get_db_config()
    except Exception as e:
        print(f"[ERROR] Failed to load database configuration: {e}")
        print("\nPlease ensure database configuration is set in environment variables or .env file:")
        print("  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD")
        sys.exit(1)
    
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

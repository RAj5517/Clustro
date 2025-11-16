"""
View Database Schema and Data

This script shows:
1. All tables in the database
2. Schema (columns) for each table
3. Actual data (rows) from each table

Usage:
    python view_database.py [--limit N] [--table TABLE_NAME]
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


def view_database(db_config, limit=10, table_name=None):
    """
    View all tables, their schema, and data.
    
    Args:
        db_config: Database configuration dictionary
        limit: Maximum number of rows to show per table (default: 10)
        table_name: Specific table to view (None = all tables)
    """
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("=" * 70)
        print("DATABASE VIEWER")
        print("=" * 70)
        print(f"Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
        print()
        
        # Get all tables
        if table_name:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name = %s
                ORDER BY table_name;
            """, (table_name,))
        else:
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name != 'schema_jobs'
                ORDER BY table_name;
            """)
        
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            print("[INFO] No tables found in database")
            print()
            print("Upload SQL files through the frontend to create tables!")
            cursor.close()
            conn.close()
            return
        
        print(f"Found {len(tables)} table(s)")
        print()
        
        # View each table
        for i, table_name in enumerate(tables, 1):
            print("=" * 70)
            print(f"TABLE {i}: {table_name.upper()}")
            print("=" * 70)
            
            # Get schema (columns)
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            if columns:
                print("\n[SCHEMA]")
                print("-" * 70)
                print(f"{'Column Name':<25} {'Type':<25} {'Nullable':<10} {'Default'}")
                print("-" * 70)
                for col in columns:
                    col_name, data_type, max_length, is_nullable, col_default = col
                    type_str = f"{data_type}({max_length})" if max_length else data_type
                    nullable_str = "NULL" if is_nullable == 'YES' else "NOT NULL"
                    default_str = str(col_default) if col_default else "-"
                    print(f"{col_name:<25} {type_str:<25} {nullable_str:<10} {default_str}")
            
            # Get row count
            cursor.execute(sql.SQL('SELECT COUNT(*) FROM {}').format(
                sql.Identifier(table_name)
            ))
            row_count = cursor.fetchone()[0]
            
            print(f"\n[DATA] Row Count: {row_count}")
            
            if row_count > 0:
                # Get column names for display
                col_names = [col[0] for col in columns]
                
                # Fetch data with limit
                cursor.execute(sql.SQL('SELECT * FROM {} LIMIT {}').format(
                    sql.Identifier(table_name),
                    sql.Literal(limit)
                ))
                
                rows = cursor.fetchall()
                
                if rows:
                    print("-" * 70)
                    # Print header
                    header = " | ".join(f"{name:<20}" for name in col_names)
                    print(header)
                    print("-" * 70)
                    
                    # Print rows
                    for row in rows:
                        row_str = " | ".join(f"{str(val) if val is not None else 'NULL':<20}" for val in row)
                        print(row_str)
                    
                    if row_count > limit:
                        print(f"\n... and {row_count - limit} more rows")
                else:
                    print("No data found")
            else:
                print("Table is empty (no rows)")
            
            print()
        
        cursor.close()
        conn.close()
        
        print("=" * 70)
        print("END OF DATABASE VIEW")
        print("=" * 70)
        
    except Exception as e:
        print(f"[ERROR] Failed to view database: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='View database schema and data')
    parser.add_argument('--limit', type=int, default=10,
                       help='Maximum number of rows to show per table (default: 10)')
    parser.add_argument('--table', type=str, default=None,
                       help='View specific table only (default: all tables)')
    args = parser.parse_args()
    
    db_config = get_db_config()
    
    view_database(db_config, limit=args.limit, table_name=args.table)


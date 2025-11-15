"""
Simple script to view all tables created in the database.

Usage:
    python view_tables.py
"""

from Schema_generator.config import get_db_config
import psycopg2

def view_tables():
    """Display all tables in the database."""
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("=" * 70)
        print("DATABASE TABLES")
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
        
        tables = cursor.fetchall()
        
        if not tables:
            print("[INFO] No tables found in database")
            print()
            print("Upload SQL files through the frontend to create tables!")
            return
        
        print(f"Found {len(tables)} table(s):")
        print()
        
        for i, (table_name,) in enumerate(tables, 1):
            print(f"{i}. {table_name}")
            
            # Get columns
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = cursor.fetchall()
            
            if columns:
                print("   Columns:")
                for col in columns:
                    col_name, data_type, max_length, nullable = col
                    type_str = f"{data_type}({max_length})" if max_length else data_type
                    null_str = "NULL" if nullable == 'YES' else "NOT NULL"
                    print(f"     - {col_name}: {type_str} {null_str}")
            
            # Get row count
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                row_count = cursor.fetchone()[0]
                print(f"   Rows: {row_count}")
            except Exception as e:
                print(f"   Rows: (error: {e})")
            
            print()
        
        cursor.close()
        conn.close()
        
        print("=" * 70)
        
    except Exception as e:
        print(f"[ERROR] Failed to connect to database: {e}")
        print()
        print("Make sure PostgreSQL is running and database is configured correctly.")
        print("Check Backend/.env file or set environment variables.")


if __name__ == "__main__":
    view_tables()


"""
Database setup script for the Intelligent Multi-Modal Storage System.

Creates the schema_jobs table that stores SQL statements to be executed.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import socket


def test_connection(db_config):
    """Test if PostgreSQL server is accessible."""
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def create_schema_jobs_table(db_config):
    """
    Create the schema_jobs table in PostgreSQL.
    
    Args:
        db_config: Dictionary with database connection parameters:
            {
                'host': 'localhost',
                'port': 5432,
                'database': 'clustro',
                'user': 'postgres',
                'password': 'password'
            }
    """
    # First, test if PostgreSQL server is accessible
    print("Checking PostgreSQL server connection...")
    if not test_connection(db_config):
        print(f"❌ Cannot connect to PostgreSQL server at {db_config.get('host', 'localhost')}:{db_config.get('port', 5432)}")
        print("\n📋 Troubleshooting steps:")
        print("   1. Make sure PostgreSQL is installed")
        print("   2. Check if PostgreSQL service is running:")
        print("      - Windows: Open Services (services.msc) and check if 'postgresql-x64-XX' is running")
        print("      - Linux/Mac: Run 'sudo service postgresql status' or 'brew services list'")
        print("   3. If not running, start PostgreSQL:")
        print("      - Windows: Start the PostgreSQL service from Services")
        print("      - Linux: 'sudo service postgresql start'")
        print("      - Mac: 'brew services start postgresql'")
        print("   4. Verify connection with: psql -h localhost -p 5432 -U postgres")
        print("   5. Check if the port is correct (default is 5432)")
        return False
    
    print("✅ PostgreSQL server is accessible")
    print()
    
    try:
        # Connect to PostgreSQL
        print(f"Connecting to database '{db_config.get('database', 'clustro')}'...")
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print("✅ Connected to database successfully")
        print()
        
        # Create schema_jobs table
        print("Creating schema_jobs table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS schema_jobs (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            sql_text TEXT NOT NULL,
            status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'error')),
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create index on status for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_schema_jobs_status 
            ON schema_jobs(status);
        """)
        
        # Create index on created_at for ordering
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_schema_jobs_created_at 
            ON schema_jobs(created_at);
        """)
        
        print("✅ schema_jobs table created successfully!")
        print("   - Table: schema_jobs")
        print("   - Columns: id, table_name, sql_text, status, error_message, created_at, updated_at")
        print("   - Status values: pending, completed, error")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        print(f"❌ Database connection failed: {error_msg}")
        print()
        print("📋 Common issues and solutions:")
        
        if "connection refused" in error_msg.lower() or "could not connect" in error_msg.lower():
            print("   • PostgreSQL service is not running")
            print("     → Start PostgreSQL service (see above)")
        elif "database" in error_msg.lower() and "does not exist" in error_msg.lower():
            db_name = db_config.get('database', 'clustro')
            print(f"   • Database '{db_name}' does not exist")
            print(f"     → Create database: CREATE DATABASE {db_name};")
            print(f"     → Or run: createdb {db_name}")
        elif "authentication failed" in error_msg.lower() or "password" in error_msg.lower():
            print("   • Wrong username or password")
            print("     → Update db_config in database_setup.py with correct credentials")
            print("     → Check PostgreSQL pg_hba.conf for authentication settings")
        elif "timeout" in error_msg.lower():
            print("   • Connection timeout")
            print("     → Check if PostgreSQL is running on the correct port")
            print("     → Check firewall settings")
        else:
            print(f"   • Unexpected error: {error_msg}")
            print("     → Verify PostgreSQL is installed and running")
            print("     → Check database configuration in database_setup.py")
        
        return False
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL error: {e}")
        print("   → Check database configuration and permissions")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("   → Check error details above")
        return False


def verify_table_exists(db_config):
    """Verify that schema_jobs table exists."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'schema_jobs'
            );
        """)
        
        exists = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return exists
        
    except Exception as e:
        print(f"Error verifying table: {e}")
        return False


if __name__ == "__main__":
    import argparse
    import os
    
    # Try to load .env file if python-dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # python-dotenv is optional
    
    parser = argparse.ArgumentParser(description='Setup schema_jobs table in PostgreSQL')
    parser.add_argument('--provider', type=str, help='Cloud provider (aws, gcp, azure, supabase, neon, heroku)')
    parser.add_argument('--config', type=str, help='Use custom config module (default: config)')
    args = parser.parse_args()
    
    # Try to import config module
    try:
        if args.config:
            import importlib
            config_module = importlib.import_module(args.config)
            get_db_config = config_module.get_db_config
            get_cloud_db_config = config_module.get_cloud_db_config
            validate_db_config = config_module.validate_db_config
        else:
            from config import get_db_config, get_cloud_db_config, validate_db_config
    except ImportError:
        print("❌ Config module not found!")
        print("   Please make sure config.py is in the same directory.")
        sys.exit(1)
    
    # Use config from environment or cloud provider (defaults to local)
    if args.provider:
        print(f"Using cloud provider: {args.provider}")
        db_config = get_cloud_db_config(args.provider)
    else:
        print("Using local PostgreSQL (default)")
        db_config = get_db_config()
    
    # For local setup, allow password to be set directly if not in env
    if not validate_db_config(db_config) and not args.provider:
        # Try with default password for local PostgreSQL
        if not db_config.get('password'):
            db_config['password'] = os.getenv('DB_PASSWORD', 'postgres')
        
        # If still not valid, show instructions
        if not validate_db_config(db_config):
            print("❌ Invalid database configuration!")
            print()
            print("📋 Setup Instructions:")
            print("   1. Install PostgreSQL: https://www.postgresql.org/download/")
            print("   2. Start PostgreSQL service")
            print("   3. Create database: createdb -U postgres clustro")
            print("   4. Set environment variable: DB_PASSWORD=your_password")
            print("      Or update password in this script for local testing")
            print("   5. Or use --provider flag for cloud databases")
            print()
            sys.exit(1)
    
    print("=" * 60)
    print("Schema Jobs Table Setup")
    print("=" * 60)
    print(f"Target database: {db_config['database']}")
    print(f"Host: {db_config['host']}:{db_config['port']}")
    print(f"User: {db_config['user']}")
    if 'sslmode' in db_config:
        print(f"SSL Mode: {db_config['sslmode']}")
    print()
    
    if create_schema_jobs_table(db_config):
        print()
        print("Verifying table creation...")
        if verify_table_exists(db_config):
            print("✅ Verification: schema_jobs table exists!")
        else:
            print("⚠️  Warning: Could not verify table existence (this may be due to connection issues)")
    else:
        print()
        print("❌ Failed to create schema_jobs table")
        print()
        print("Next steps:")
        print("1. Ensure PostgreSQL is installed and running")
        print("2. Create the database if it doesn't exist:")
        print(f"   createdb {db_config['database']}")
        print(f"   OR: psql -U {db_config['user']} -c \"CREATE DATABASE {db_config['database']};\"")
        print("3. Update database credentials in database_setup.py")
        print("4. Run this script again")
        sys.exit(1)


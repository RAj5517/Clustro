"""Quick script to verify tables were created."""
from config import get_db_config
import psycopg2

db_config = get_db_config()
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE' 
    ORDER BY table_name
""")

tables = cursor.fetchall()

print("[OK] Tables created in database:")
for table in tables:
    print(f"  - {table[0]}")

cursor.close()
conn.close()


# Testing Guide - Schema System

## Quick Test (Recommended)

Run the quick test script to verify everything works end-to-end:

```powershell
cd Backend
python quick_test.py
```

This will:
1. Create a test JSON file
2. Generate SQL schema from it
3. Execute the SQL and create the table
4. Verify the table exists in the database

## Individual Component Testing

### 1. Test Schema Generator (Program A)

Generate SQL schema from a file:

```powershell
python schema_generator.py path/to/your/file.json
```

**Examples:**
```powershell
# JSON file
python schema_generator.py test_files_schema/users.json

# CSV file
python schema_generator.py test_files_schema/products.csv

# XML file
python schema_generator.py test_files_schema/employees.xml
```

The generated SQL statements will be saved to the `schema_jobs` table.

### 2. Test Schema Executor (Program B)

Execute all pending SQL jobs:

```powershell
python schema_executor.py
```

This will:
- Fetch all pending jobs from `schema_jobs` table
- Execute each SQL statement in a transaction
- Update job status (completed/error)

### 3. Full End-to-End Test

Run the comprehensive test suite:

```powershell
python test_schema_system.py
```

This will:
- Create multiple test files (JSON, CSV, XML)
- Generate schemas for all files
- Execute all pending jobs
- Show detailed results

## Verify Database Tables

Check what tables were created:

```powershell
python verify_tables.py
```

Or manually check with PostgreSQL:

```powershell
psql -U postgres -d clustro -c "\dt"
```

## Check Job Status

View all jobs in the schema_jobs table:

```powershell
python -c "from config import get_db_config; import psycopg2; conn = psycopg2.connect(**get_db_config()); cursor = conn.cursor(); cursor.execute('SELECT id, table_name, status, created_at FROM schema_jobs ORDER BY created_at DESC LIMIT 10'); print('\n'.join([f'Job {r[0]}: {r[1]} - {r[2]} ({r[3]})' for r in cursor.fetchall()])); cursor.close(); conn.close()"
```

## Supported File Types

The schema generator supports:
- **JSON** (.json) - flat or nested structures
- **CSV** (.csv) - tabular data
- **XML** (.xml) - hierarchical structures
- **YAML** (.yaml, .yml)
- **TXT** (.txt, .md, .log) - text files
- **DOCX** (.docx) - Word documents
- **PDF** (.pdf) - PDF documents
- **HTML** (.html) - HTML tables

## Troubleshooting

### Database Connection Issues

1. Make sure PostgreSQL is running:
   ```powershell
   # Windows - check Services
   Get-Service | Where-Object {$_.Name -like "*postgres*"}
   ```

2. Set your password in `.env` file:
   ```
   DB_PASSWORD=your_password
   ```

3. Or set environment variable:
   ```powershell
   $env:DB_PASSWORD='your_password'
   ```

### Clear All Jobs (Start Fresh)

```powershell
python -c "from config import get_db_config; import psycopg2; conn = psycopg2.connect(**get_db_config()); cursor = conn.cursor(); cursor.execute('DELETE FROM schema_jobs'); conn.commit(); print('[OK] Cleared all jobs'); cursor.close(); conn.close()"
```

## Next Steps

Once testing is complete, you can:
1. Integrate with your frontend API
2. Process uploaded files through the schema generator
3. Schedule the schema executor to run periodically
4. Add more file format support


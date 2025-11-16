"""
Flask API Server for AURAverse Backend

This server connects the frontend to the SQL processing system.
"""

import os
import tempfile
import shutil
import traceback
# Disable Flask's automatic .env loading (we handle it in config.py)
os.environ['FLASK_SKIP_DOTENV'] = '1'

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
from pathlib import Path

# Add Backend directory to path
backend_path = Path(__file__).parent
sys.path.insert(0, str(backend_path))
sys.path.insert(0, str(backend_path / 'sql'))

# Setup logging first
from logger_config import get_logger
logger = get_logger('auraverse.app')

# Import from sql folder (files are directly in sql folder)
from file_classifier import FileClassifier, classify_file
from file_to_sql import FileToSQLConverter, convert_file_to_sql
from config import get_db_config
import psycopg2

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Get database config
try:
    db_config = get_db_config()
    logger.info(f"Database config loaded: {db_config['database']}@{db_config['host']}:{db_config['port']}")
except Exception as e:
    logger.error(f"Failed to load database config: {e}", exc_info=True)
    db_config = None


@app.route('/api/upload', methods=['POST'])
def upload():
    """
    Handle file uploads from frontend.
    
    Flow:
    1. Save uploaded files temporarily
    2. Classify each file using file_classifier.py
    3. Process SQL-classified files using file_to_sql.py
    4. Return results with database state
    
    Expected format:
    - files: List of file objects (multipart/form-data)
    - metadata: Optional string (multipart/form-data)
    """
    temp_dir = None
    logger.info("=" * 70)
    logger.info("UPLOAD REQUEST RECEIVED")
    logger.info("=" * 70)
    
    try:
        # Get files from request
        files = request.files.getlist('files')
        logger.debug(f"Received {len(files)} file(s) in request")
        
        if not files:
            logger.warning("No files provided in upload request")
            return jsonify({
                'success': False,
                'error': 'No files provided'
            }), 400
        
        # Get metadata (optional)
        metadata = request.form.get('metadata', '')
        if metadata:
            logger.debug(f"Metadata received: {metadata[:100]}...")  # Log first 100 chars
        
        # Create temporary directory for uploaded files
        temp_dir = Path(tempfile.mkdtemp(prefix='auraverse_upload_'))
        logger.info(f"Created temporary directory: {temp_dir}")
        
        # Initialize classifier and converter
        logger.debug("Initializing FileClassifier and FileToSQLConverter")
        classifier = FileClassifier()
        converter = FileToSQLConverter(db_config)
        logger.debug("Components initialized successfully")
        
        # Process each file
        results = []
        total_files = 0
        processed_count = 0
        sql_files_count = 0
        errors = []
        
        for file in files:
            if file.filename == '':
                logger.warning("Skipping empty filename")
                continue
            
            total_files += 1
            file_path = temp_dir / file.filename
            logger.info(f"Processing file {total_files}/{len(files)}: {file.filename}")
            
            try:
                # Save file temporarily
                file.save(str(file_path))
                file_size = file_path.stat().st_size
                logger.info(f"Saved file: {file_path} ({file_size} bytes)")
                
                # Step 1: Classify file
                logger.debug(f"Classifying file: {file.filename}")
                classification_result = classifier.classify(str(file_path))
                logger.info(f"Classification result for {file.filename}: {classification_result['classification']} "
                          f"(SQL score: {classification_result['sql_score']}, "
                          f"NoSQL score: {classification_result['nosql_score']})")
                logger.debug(f"Full classification details: {classification_result}")
                
                # Step 2: Process SQL files
                if classification_result['classification'] == 'SQL':
                    sql_files_count += 1
                    logger.info(f"Processing SQL file: {file.filename}")
                    
                    # Convert file to SQL
                    logger.debug(f"Starting SQL conversion for: {file.filename}")
                    conversion_result = converter.convert_file(
                        file_path,
                        table_name=None,
                        primary_key=None
                    )
                    
                    logger.debug(f"Conversion result: success={conversion_result.get('success')}, "
                               f"table={conversion_result.get('table_name')}, "
                               f"rows={conversion_result.get('rows_inserted')}")
                    
                    if conversion_result['success']:
                        processed_count += 1
                        logger.info(f"Successfully processed {file.filename}: "
                                  f"{conversion_result['rows_inserted']} rows inserted into "
                                  f"table '{conversion_result['table_name']}'")
                        results.append({
                            'filename': file.filename,
                            'classification': classification_result,
                            'conversion': {
                                'success': True,
                                'table_name': conversion_result['table_name'],
                                'rows_inserted': conversion_result['rows_inserted'],
                                'decision': conversion_result['decision'],
                                'match_ratio': conversion_result['match_ratio']
                            }
                        })
                    else:
                        error_msg = conversion_result.get('error', 'Unknown error')
                        logger.error(f"Failed to process {file.filename}: {error_msg}")
                        if conversion_result.get('logs'):
                            logger.debug(f"Conversion logs for {file.filename}:")
                            for log_line in conversion_result['logs']:
                                logger.debug(f"  {log_line}")
                        errors.append({
                            'filename': file.filename,
                            'error': error_msg,
                            'classification': classification_result
                        })
                else:
                    # NoSQL file - not processed yet (could be extended later)
                    logger.info(f"Skipping NoSQL file: {file.filename} (NoSQL processing not implemented)")
                    results.append({
                        'filename': file.filename,
                        'classification': classification_result,
                        'conversion': {
                            'success': False,
                            'message': 'NoSQL files not processed yet'
                        }
                    })
                    
            except Exception as e:
                error_msg = f"Error processing {file.filename}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append({
                    'filename': file.filename,
                    'error': error_msg
                })
        
        # Get current database state for response
        logger.debug("Fetching database state")
        database_state = get_database_state()
        logger.info(f"Database state: {len(database_state.get('tables', []))} tables found")
        
        # Format response
        response = {
            'success': True,
            'message': f'Processed {processed_count}/{total_files} files successfully',
            'total_files': total_files,
            'sql_files': sql_files_count,
            'processed': processed_count,
            'results': results,
            'errors': errors,
            'databaseState': database_state
        }
        
        logger.info(f"Upload completed: {processed_count}/{total_files} files processed successfully")
        logger.info("=" * 70)
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Upload failed with exception: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
        
    finally:
        # Clean up temporary directory
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                logger.debug(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up temp directory {temp_dir}: {e}", exc_info=True)


@app.route('/api/database/state', methods=['GET'])
def get_database_state_endpoint():
    """
    Get current database state (tables, collections, media directories).
    """
    logger.info("GET /api/database/state")
    try:
        state = get_database_state()
        logger.debug(f"Database state retrieved: {len(state.get('tables', []))} tables")
        return jsonify(state), 200
    except Exception as e:
        logger.error(f"Failed to get database state: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


def get_database_state():
    """
    Query PostgreSQL to get current database state.
    
    Returns:
        Dictionary with tables, collections, and mediaDirectories
    """
    logger.debug("Getting database state from PostgreSQL")
    try:
        logger.debug(f"Connecting to database: {db_config['database']}@{db_config['host']}:{db_config['port']}")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        logger.debug("Database connection established")
        
        # Get all tables
        logger.debug("Querying for tables")
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name != 'schema_jobs'
            ORDER BY table_name
        """)
        
        table_rows = cursor.fetchall()
        logger.debug(f"Found {len(table_rows)} tables")
        
        tables = []
        for row in table_rows:
            table_name = row[0]
            logger.debug(f"Processing table: {table_name}")
            
            # Get columns for each table
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = []
            for col in cursor.fetchall():
                col_name, data_type, max_length, nullable = col
                col_info = {
                    'name': col_name,
                    'type': f"{data_type}({max_length})" if max_length else data_type,
                    'nullable': nullable == 'YES'
                }
                columns.append(col_info)
            
            logger.debug(f"Table {table_name}: {len(columns)} columns")
            tables.append({
                'name': table_name,
                'type': 'SQL',
                'columns': columns,
                'relationships': []  # TODO: Extract foreign key relationships
            })
        
        cursor.close()
        conn.close()
        logger.debug("Database connection closed")
        
        result = {
            'tables': tables,
            'collections': [],  # TODO: Get from NoSQL database
            'mediaDirectories': []  # TODO: Get from media storage
        }
        logger.info(f"Database state retrieved: {len(tables)} tables")
        return result
        
    except Exception as e:
        logger.error(f"Failed to query database: {e}", exc_info=True)
        raise


@app.route('/api/database/tables', methods=['GET'])
def get_tables():
    """
    Get list of all SQL tables.
    """
    logger.info("GET /api/database/tables")
    try:
        logger.debug("Connecting to database to get table list")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name != 'schema_jobs'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Retrieved {len(tables)} tables")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'tables': tables
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get tables: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database/tables/<table_name>', methods=['GET'])
def get_table_details(table_name):
    """
    Get details of a specific table including columns and row count.
    """
    logger.info(f"GET /api/database/tables/{table_name}")
    try:
        logger.debug(f"Getting details for table: {table_name}")
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Get columns
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = []
        for col in cursor.fetchall():
            col_name, data_type, max_length, nullable = col
            col_info = {
                'name': col_name,
                'type': f"{data_type}({max_length})" if max_length else data_type,
                'nullable': nullable == 'YES'
            }
            columns.append(col_info)
        
        logger.debug(f"Table {table_name}: {len(columns)} columns found")
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        row_count = cursor.fetchone()[0]
        logger.debug(f"Table {table_name}: {row_count} rows")
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'table': {
                'name': table_name,
                'columns': columns,
                'row_count': row_count
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Failed to get table details for {table_name}: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    logger.debug("GET /api/health")
    return jsonify({
        'status': 'healthy',
        'message': 'AURAverse API is running'
    }), 200


if __name__ == '__main__':
    logger.info("=" * 70)
    logger.info("AURAVERSE BACKEND API SERVER")
    logger.info("=" * 70)
    logger.info(f"Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
    logger.info("Starting server on http://localhost:8000")
    logger.info("API Endpoints:")
    logger.info("  POST /api/upload - Upload files/folders")
    logger.info("  GET  /api/database/state - Get database state")
    logger.info("  GET  /api/database/tables - List all tables")
    logger.info("  GET  /api/database/tables/<name> - Get table details")
    logger.info("  GET  /api/health - Health check")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 70)
    
    try:
        app.run(host='0.0.0.0', port=8000, debug=True, load_dotenv=False)
    except Exception as e:
        logger.critical(f"Failed to start server: {e}", exc_info=True)
        raise


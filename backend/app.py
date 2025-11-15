"""
Flask API Server for Clustro Backend

This server connects the frontend to the classification system.
"""

import os
# Disable Flask's automatic .env loading (we handle it in config.py)
os.environ['FLASK_SKIP_DOTENV'] = '1'

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
from pathlib import Path

# Add Backend directory to path
backend_path = Path(__file__).parent
sys.path.insert(0, str(backend_path))

from classification.main import process_upload_request, ClassificationProcessor
from Schema_generator.config import get_db_config
import psycopg2

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Get database config
db_config = get_db_config()


@app.route('/api/upload', methods=['POST'])
def upload():
    """
    Handle file uploads from frontend.
    
    Expected format:
    - files: List of file objects (multipart/form-data)
    - metadata: Optional string (multipart/form-data)
    """
    try:
        # Get files from request
        files = request.files.getlist('files')
        
        if not files:
            return jsonify({
                'success': False,
                'error': 'No files provided'
            }), 400
        
        # Get metadata (optional)
        metadata = request.form.get('metadata', '')
        
        # Process files through classification system
        result = process_upload_request(files, metadata)
        
        # Get current database state for response
        database_state = get_database_state()
        
        # Format response
        response = {
            'success': True,
            'message': f'Processed {result["total_files"]} files successfully',
            'processing_result': result,
            'databaseState': database_state
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database/state', methods=['GET'])
def get_database_state():
    """
    Get current database state (tables, collections, media directories).
    """
    try:
        state = get_database_state()
        return jsonify(state), 200
    except Exception as e:
        print(f"[ERROR] Failed to get database state: {e}")
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
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name != 'schema_jobs'
            ORDER BY table_name
        """)
        
        tables = []
        for row in cursor.fetchall():
            table_name = row[0]
            
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
            
            tables.append({
                'name': table_name,
                'type': 'SQL',
                'columns': columns,
                'relationships': []  # TODO: Extract foreign key relationships
            })
        
        cursor.close()
        conn.close()
        
        return {
            'tables': tables,
            'collections': [],  # TODO: Get from NoSQL database
            'mediaDirectories': []  # TODO: Get from media storage
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to query database: {e}")
        raise


@app.route('/api/database/tables', methods=['GET'])
def get_tables():
    """
    Get list of all SQL tables.
    """
    try:
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
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'tables': tables
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database/tables/<table_name>', methods=['GET'])
def get_table_details(table_name):
    """
    Get details of a specific table including columns and row count.
    """
    try:
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
        
        # Get row count
        cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
        row_count = cursor.fetchone()[0]
        
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
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'message': 'Clustro API is running'
    }), 200


if __name__ == '__main__':
    print("=" * 70)
    print("CLUSTRO BACKEND API SERVER")
    print("=" * 70)
    print(f"Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
    print()
    print("Starting server on http://localhost:8000")
    print("API Endpoints:")
    print("  POST /api/upload - Upload files/folders")
    print("  GET  /api/database/state - Get database state")
    print("  GET  /api/database/tables - List all tables")
    print("  GET  /api/database/tables/<name> - Get table details")
    print("  GET  /api/health - Health check")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 70)
    
    app.run(host='0.0.0.0', port=8000, debug=True, load_dotenv=False)


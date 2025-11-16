"""
Flask API Server for Clustro Backend

This server connects the frontend to the classification system.
"""

import os
import logging
# Disable Flask's automatic .env loading (we handle it in config.py)
os.environ['FLASK_SKIP_DOTENV'] = '1'

from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
from pathlib import Path

# Configure logging if not already set up
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

logger = logging.getLogger(__name__)

# Add Backend directory to path
backend_path = Path(__file__).parent
sys.path.insert(0, str(backend_path))

from classification.main import process_upload_request, ClassificationProcessor

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# SQL schema generator is removed; keep empty config for compatibility
# db_config = get_db_config()

SQL_DISABLED_STATE = {
    'tables': [],
    'collections': [],
    'mediaDirectories': [],
    'sqlStatus': 'disabled',
    # 'message': SQL_PIPELINE_MESSAGE
}


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
        logger.info("/api/upload received %d files", len(files))
        
        if not files:
            return jsonify({
                'success': False,
                'error': 'No files provided'
            }), 400
        
        # Get metadata (optional)
        metadata = request.form.get('metadata', '')
        logger.info("Metadata length: %d", len(metadata or ""))
        
        # Process files through classification system
        result = process_upload_request(files, metadata)
        logger.info("Classification finished. total_files=%d", result.get("total_files"))
        
        # Get current database state for response
        database_state = get_database_state_payload()
        
        # Format response
        response = {
            'success': True,
            'message': f'Processed {result["total_files"]} files successfully',
            'processing_result': result,
            'databaseState': database_state
        }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.exception("Upload failed")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/database/state', methods=['GET'])
def get_database_state():
    """
    Get current database state (tables, collections, media directories).
    """
    logger.info("/api/database/state requested (SQL disabled)")
    return jsonify(SQL_DISABLED_STATE), 200


def get_database_state_payload():
    """Return placeholder DB state since SQL processing is disabled."""
    logger.debug("Providing SQL disabled database state placeholder")
    return SQL_DISABLED_STATE


@app.route('/api/database/tables', methods=['GET'])
def get_tables():
    """
    Get list of all SQL tables.
    """
    logger.info("/api/database/tables requested (SQL disabled)")
    return jsonify({
        'success': True,
        'tables': [],
        # 'message': SQL_PIPELINE_MESSAGE
    }), 200


@app.route('/api/database/tables/<table_name>', methods=['GET'])
def get_table_details(table_name):
    """
    Get details of a specific table including columns and row count.
    """
    logger.info("/api/database/tables/%s requested (SQL disabled)", table_name)
    return jsonify({
        'success': False,
        # 'error': SQL_PIPELINE_MESSAGE
    }), 410  # Gone


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    logger.debug("/api/health check invoked")
    return jsonify({
        'status': 'healthy',
        'message': 'Clustro API is running'
    }), 200


if __name__ == '__main__':
    print("=" * 70)
    print("CLUSTRO BACKEND API SERVER")
    print("=" * 70)
    # print(f"Database: {db_config['database']} at {db_config['host']}:{db_config['port']}")
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


"""
Flask API Server for AURAverse Backend

This server connects the frontend to the SQL processing system.
"""

import os
import logging
import re
import tempfile
import shutil
import traceback
# Disable Flask's automatic .env loading (we handle it in config.py)
os.environ['FLASK_SKIP_DOTENV'] = '1'

from flask import Flask, request, jsonify, send_file
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
sys.path.insert(0, str(backend_path / 'sql'))

from classification.main import process_upload_request, ClassificationProcessor

# Import SQL processing components
try:
    from file_classifier import FileClassifier, classify_file
    from file_to_sql import FileToSQLConverter, convert_file_to_sql
    HAS_SQL_PROCESSING = True
except ImportError:
    HAS_SQL_PROCESSING = False
    FileClassifier = None  # type: ignore
    FileToSQLConverter = None  # type: ignore
    logger.warning("SQL processing components not available - SQL file processing disabled")

# Import MongoDB access function
try:
    from nosql_processor.main import get_nosql_db
    HAS_MONGODB = True
except ImportError:
    HAS_MONGODB = False
    logger.warning("MongoDB access not available - visualization endpoint will return empty data")

try:
    from nosql_ingestion_pipeline.semantic_search import SemanticSearchEngine
    HAS_SEMANTIC_SEARCH = True
except ImportError:
    SemanticSearchEngine = None  # type: ignore
    HAS_SEMANTIC_SEARCH = False
    logger.warning("Semantic search engine not available - /api/search/semantic disabled")

# Setup logging first
from logger_config import get_logger
logger = get_logger('auraverse.app')

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# SQL database configuration
try:
    from sql.config import get_db_config as get_sql_db_config
    import psycopg2
    sql_db_config = get_sql_db_config()
    HAS_SQL_DB = True
    logger.info(f"SQL database config loaded: {sql_db_config.get('database', 'N/A')}@{sql_db_config.get('host', 'N/A')}:{sql_db_config.get('port', 'N/A')}")
except Exception as e:
    HAS_SQL_DB = False
    sql_db_config = None
    psycopg2 = None  # type: ignore
    logger.warning(f"SQL database config not available: {e}")

SQL_DISABLED_STATE = {
    'tables': [],
    'collections': [],
    'mediaDirectories': [],
    'sqlStatus': 'disabled' if not HAS_SQL_DB else 'enabled',
}

_semantic_engine = None


def get_semantic_engine():
    """Lazily initialise the semantic search engine."""
    global _semantic_engine
    if not HAS_SEMANTIC_SEARCH:
        return None
    if _semantic_engine is None:
        _semantic_engine = SemanticSearchEngine()
    return _semantic_engine


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
        logger.info("/api/upload received %d files", len(files))
        logger.debug(f"Received {len(files)} file(s) in request")
        
        if not files:
            logger.warning("No files provided in upload request")
            return jsonify({
                'success': False,
                'error': 'No files provided'
            }), 400
        
        # Get metadata (optional)
        metadata = request.form.get('metadata', '')
        logger.info("Metadata length: %d", len(metadata or ""))
        
        # Create temporary directory for uploaded files
        temp_dir = Path(tempfile.mkdtemp(prefix='auraverse_upload_'))
        logger.info(f"Created temporary directory: {temp_dir}")
        
        # Process SQL files directly if SQL processing is available
        sql_results = []
        sql_files_count = 0
        processed_count = 0
        errors = []
        
        if HAS_SQL_PROCESSING and HAS_SQL_DB and sql_db_config:
            # Initialize classifier and converter
            logger.debug("Initializing FileClassifier and FileToSQLConverter")
            classifier = FileClassifier()
            converter = FileToSQLConverter(sql_db_config)
            logger.debug("Components initialized successfully")
            
            # Process each file for SQL
            for file in files:
                if file.filename == '':
                    logger.warning("Skipping empty filename")
                    continue
                
                file_path = temp_dir / file.filename
                logger.info(f"Processing file: {file.filename}")
                
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
                        
                        if conversion_result['success']:
                            processed_count += 1
                            logger.info(f"Successfully processed {file.filename}: "
                                      f"{conversion_result['rows_inserted']} rows inserted into "
                                      f"table '{conversion_result['table_name']}'")
                            sql_results.append({
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
                            errors.append({
                                'filename': file.filename,
                                'error': error_msg,
                                'classification': classification_result
                            })
                
                except Exception as e:
                    error_msg = f"Error processing {file.filename}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append({
                        'filename': file.filename,
                        'error': error_msg
                    })
        
        # Process all files through classification system for NoSQL and media
        logger.debug("Processing files through classification system for NoSQL/media")
        classification_result = process_upload_request(files, metadata)
        logger.info("Classification finished. total_files=%d", classification_result.get("total_files"))
        
        # Get current database state for response
        database_state = get_database_state_payload()

        # Refresh file tree snapshot for frontend visualization
        visualization_tree = fetch_file_tree_from_db()

        # Format response - combine SQL direct processing with classification results
        response = {
            'success': True,
            'message': f'Processed {classification_result["total_files"]} files successfully',
            'processing_result': classification_result,
            'databaseState': database_state,
            'visualizationData': visualization_tree
        }
        
        # Add SQL processing results if we did direct SQL processing
        if sql_results or errors:
            response['sql_processing'] = {
                'total_files': len(files),
                'sql_files': sql_files_count,
                'processed': processed_count,
                'results': sql_results,
                'errors': errors
            }
        
        logger.info("Upload completed successfully")
        logger.info("=" * 70)
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.exception("Upload failed")
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


def get_database_state_payload():
    """Get database state for response payload."""
    try:
        return get_database_state()
    except Exception as e:
        logger.warning(f"Failed to get database state, using placeholder: {e}")
        return SQL_DISABLED_STATE


def get_database_state():
    """
    Query PostgreSQL to get current database state.
    
    Returns:
        Dictionary with tables, collections, and mediaDirectories
    """
    if not HAS_SQL_DB or not sql_db_config:
        logger.debug("SQL database not available, returning disabled state")
        return SQL_DISABLED_STATE
    
    logger.debug("Getting database state from PostgreSQL")
    try:
        logger.debug(f"Connecting to database: {sql_db_config['database']}@{sql_db_config['host']}:{sql_db_config['port']}")
        conn = psycopg2.connect(**sql_db_config)
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
    if not HAS_SQL_DB or not sql_db_config:
        return jsonify({
            'success': True,
            'tables': [],
        }), 200
    
    try:
        logger.debug("Connecting to database to get table list")
        conn = psycopg2.connect(**sql_db_config)
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
    if not HAS_SQL_DB or not sql_db_config:
        return jsonify({
            'success': False,
            'error': 'SQL database not configured'
        }), 503
    
    try:
        logger.debug(f"Getting details for table: {table_name}")
        conn = psycopg2.connect(**sql_db_config)
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


@app.route('/api/visualization', methods=['GET'])
def get_visualization():
    """
    Get file tree structure for visualization.
    Returns a hierarchical tree structure of all uploaded files.
    """
    try:
        file_tree = fetch_file_tree_from_db()
        return jsonify(file_tree), 200
        
    except Exception as e:
        logger.exception("Failed to get visualization data")
        return jsonify({
            'name': 'Root',
            'type': 'folder',
            'children': [],
            'error': str(e)
        }), 500


@app.route('/api/search/semantic', methods=['POST'])
def semantic_search():
    """Perform semantic search against stored embeddings."""
    payload = request.get_json(silent=True) or {}
    query = (payload.get('query') or '').strip()
    limit = payload.get('limit', 10)

    try:
        limit = max(1, min(int(limit), 50))
    except Exception:
        limit = 10

    if not query:
        return jsonify({
            'success': False,
            'error': 'Query text is required'
        }), 400

    results = []
    source = 'semantic'
    engine = get_semantic_engine() if HAS_SEMANTIC_SEARCH else None

    if engine and engine.available:
        try:
            logger.info("/api/search/semantic requested (query length=%d, limit=%d)", len(query), limit)
            results = engine.search(query, limit=limit)
        except Exception:
            logger.exception("Semantic search engine failed; falling back to metadata search")

    if not results:
        fallback_results = search_metadata_fallback(query, limit)
        if fallback_results:
            results = fallback_results
            source = 'metadata'
    if not results:
        storage_results = search_storage_fallback(query, limit)
        if storage_results:
            results = storage_results
            source = 'storage'

    results = normalize_search_results(results)

    return jsonify({
        'success': True,
        'results': results,
        'source': source
    }), 200


@app.route('/api/download', methods=['GET'])
def download_asset():
    """Download a file from the storage directory."""
    requested_path = (request.args.get('path') or '').strip()
    if not requested_path:
        return jsonify({'success': False, 'error': 'Path parameter is required'}), 400

    storage_root = resolve_storage_root()
    if not storage_root:
        return jsonify({'success': False, 'error': 'Storage directory is not configured'}), 503

    candidates = []
    candidates.append((storage_root / requested_path).resolve())
    if os.path.isabs(requested_path):
        candidates.append(Path(requested_path).resolve())

    target_file = None
    for candidate in candidates:
        if candidate.is_file() and _is_within_root(candidate, storage_root):
            target_file = candidate
            break

    if not target_file:
        logger.warning("Download requested for missing/invalid path: %s", requested_path)
        return jsonify({'success': False, 'error': 'File not found'}), 404

    logger.info("Serving download for %s", target_file)
    return send_file(
        target_file,
        as_attachment=True,
        download_name=target_file.name
    )


def build_file_tree(files: list) -> dict:
    """
    Build a hierarchical file tree structure from MongoDB file documents.
    
    Args:
        files: List of file documents from MongoDB
        
    Returns:
        Dictionary representing the file tree structure
    """
    # Create a nested dictionary structure
    tree = {}
    
    for file_doc in files:
        # Use storage_uri if available, otherwise use original_name
        path_str = file_doc.get('storage_uri') or file_doc.get('original_name', 'unknown')
        
        # Extract path components
        path_parts = path_str.replace('\\', '/').strip('/').split('/')
        
        # Build nested structure
        current = tree
        for i, part in enumerate(path_parts):
            is_file = i == len(path_parts) - 1
            
            if part not in current:
                if is_file:
                    # It's a file
                    size_bytes = file_doc.get('size_bytes', 0)
                    extension = file_doc.get('extension', '')
                    
                    # Determine MIME type
                    import mimetypes
                    mime_type = mimetypes.guess_type(file_doc.get('original_name', ''))[0] or ''
                    
                    current[part] = {
                        'name': part,
                        'type': 'file',
                        'size': format_size(size_bytes),
                        'mimeType': mime_type,
                        'file_id': file_doc.get('_id'),
                        'original_name': file_doc.get('original_name', part),
                        'extension': extension,
                        'collection': file_doc.get('collection_hint', 'unknown')
                    }
                else:
                    # It's a folder
                    current[part] = {
                        'name': part,
                        'type': 'folder',
                        'children': {}
                    }
            
            current = current[part].get('children', {})
    
    # Convert nested dictionaries to the expected format with children arrays
    def convert_to_tree(node_dict):
        """Recursively convert dictionary structure to tree format."""
        result = []
        for key, value in node_dict.items():
            if isinstance(value, dict):
                if value.get('type') == 'folder':
                    node = {
                        'name': value['name'],
                        'type': 'folder',
                        'children': convert_to_tree(value.get('children', {}))
                    }
                else:
                    # It's a file
                    node = value.copy()
                    node.pop('children', None)  # Remove children if it exists
                result.append(node)
        return result
    
    root_children = convert_to_tree(tree)
    
    # If we have files at root level, create a simple structure
    # Otherwise organize by type (Media, Documents, etc.)
    if root_children:
        # Check if files are already organized in folders
        has_folders = any(child.get('type') == 'folder' for child in root_children)
        
        if has_folders:
            # Already organized, return as is
            return {
                'name': 'Root',
                'type': 'folder',
                'children': root_children
            }
        else:
            # Organize by file type
            media_files = []
            document_files = []
            other_files = []
            
            for child in root_children:
                mime_type = child.get('mimeType', '')
                extension = child.get('extension', '').lower()
                
                if mime_type.startswith(('image/', 'video/', 'audio/')) or \
                   extension in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', 
                                '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv',
                                '.mp3', '.wav', '.flac', '.aac', '.ogg']:
                    media_files.append(child)
                elif mime_type.startswith(('text/', 'application/pdf', 'application/json')) or \
                     extension in ['.txt', '.pdf', '.json', '.csv', '.xml']:
                    document_files.append(child)
                else:
                    other_files.append(child)
            
            children = []
            if media_files:
                children.append({
                    'name': 'Media',
                    'type': 'folder',
                    'children': media_files
                })
            if document_files:
                children.append({
                    'name': 'Documents',
                    'type': 'folder',
                    'children': document_files
                })
            if other_files:
                children.append({
                    'name': 'Other',
                    'type': 'folder',
                    'children': other_files
                })
            
            return {
                'name': 'Root',
                'type': 'folder',
                'children': children
            }
    else:
        # No files found
        return {
            'name': 'Root',
            'type': 'folder',
            'children': []
        }


def fetch_file_tree_from_db() -> dict:
    """
    Build the visualization tree from either the on-disk storage directory or,
    if unavailable, from MongoDB metadata.
    """
    storage_tree = build_storage_tree()
    if storage_tree.get('children'):
        return storage_tree

    default_tree = {
        'name': 'Root',
        'type': 'folder',
        'children': []
    }

    if not HAS_MONGODB:
        logger.warning("Visualization requested but MongoDB not available")
        return default_tree

    try:
        nosql_db = get_nosql_db()
        files_collection = nosql_db['files']

        cleaned_files = []
        for doc in files_collection.find({}):
            doc = dict(doc)
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            cleaned_files.append(doc)

        logger.info("/api/visualization: found %d files in database", len(cleaned_files))
        return build_file_tree(cleaned_files)
    except Exception as exc:
        logger.exception("Failed to fetch file tree from MongoDB")
        return {
            **default_tree,
            'error': str(exc)
        }


def resolve_storage_root() -> Path | None:
    """
    Locate the storage root configured via LOCAL_ROOT_REPO or fall back to
    ../storage relative to the backend directory.
    """
    candidates = []
    env_root = os.getenv('LOCAL_ROOT_REPO')

    if env_root:
        env_path = Path(env_root)
        if env_path.is_absolute():
            candidates.append(env_path)
        else:
            candidates.append((backend_path / env_path).resolve())

    candidates.append((backend_path.parent / 'storage').resolve())

    for candidate in candidates:
        try:
            if candidate.exists() and candidate.is_dir():
                return candidate
        except Exception:
            continue

    return None


def _is_within_root(path: Path, root: Path) -> bool:
    """Ensure the provided path is inside the storage root."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def search_metadata_fallback(query: str, limit: int = 10) -> list[dict]:
    """Fallback: search MongoDB metadata for query text."""
    if not HAS_MONGODB or not query:
        return []

    try:
        nosql_db = get_nosql_db()
        files_collection = nosql_db['files']
        safe_query = re.escape(query)
        regex = {'$regex': safe_query, '$options': 'i'}
        cursor = files_collection.find({
            '$or': [
                {'summary_preview': regex},
                {'original_name': regex},
                {'storage_uri': regex},
                {'collection_hint': regex}
            ]
        }).limit(limit)

        results = []
        for doc in cursor:
            doc_id = str(doc.get('_id'))
            storage_uri = doc.get('storage_uri') or doc.get('original_name')
            metadata = {
                'file_id': doc_id,
                'modality': (doc.get('extra') or {}).get('modality') or doc.get('collection_hint'),
                'collection': doc.get('collection_hint'),
                'path': storage_uri,
                'storage_uri': storage_uri,
                'summary': doc.get('summary_preview', ''),
            }
            results.append({
                'id': doc_id,
                'similarity': None,
                'distance': None,
                'text': doc.get('summary_preview', ''),
                'metadata': metadata,
                'modality': metadata.get('modality')
            })
        return results
    except Exception:
        logger.exception("Metadata fallback search failed")
        return []


def search_storage_fallback(query: str, limit: int = 10) -> list[dict]:
    """Fallback: scan on-disk storage directory for matching filenames/paths."""
    root = resolve_storage_root()
    if not root or not query:
        return []

    results = []
    lowered_query = query.lower()

    try:
        for path in root.rglob('*'):
            if len(results) >= limit:
                break
            if path.is_file():
                relative_path = path.relative_to(root).as_posix()
                name = path.name
                searchable = f"{relative_path} {name}".lower()
                if lowered_query in searchable:
                    metadata = {
                        'file_id': relative_path,
                        'modality': 'file',
                        'collection': 'storage',
                        'path': relative_path,
                        'storage_uri': relative_path,
                        'summary': '',
                    }
                    results.append({
                        'id': relative_path,
                        'similarity': None,
                        'distance': None,
                        'text': '',
                        'metadata': metadata,
                        'modality': 'file'
                    })
        return results
    except Exception:
        logger.exception("Storage fallback search failed")
        return []


def normalize_search_results(results: list[dict]) -> list[dict]:
    """Ensure each result has a normalized relative download path when possible."""
    if not results:
        return []

    storage_root = resolve_storage_root()

    normalized: list[dict] = []
    for item in results:
        metadata = dict(item.get('metadata') or {})

        candidate_path = metadata.get('path') or metadata.get('storage_uri') or ''
        normalized_path = _normalize_path(candidate_path, storage_root)
        if normalized_path:
            metadata['download_path'] = normalized_path
            metadata['path'] = normalized_path
        else:
            metadata['download_path'] = candidate_path or None

        normalized.append({
            **item,
            'metadata': metadata
        })

    return normalized


def _normalize_path(path_str: str, root: Path | None) -> str | None:
    if not path_str:
        return None

    path = Path(path_str)
    if path.is_absolute():
        if root and _is_within_root(path, root):
            try:
                return path.relative_to(root).as_posix()
            except Exception:
                return path_str
        return path_str

    return path.as_posix()


def build_storage_tree() -> dict:
    """
    Walk the LOCAL_ROOT_REPO directory and convert it into the visualization
    tree format expected by the frontend.
    """
    root_path = resolve_storage_root()
    default_tree = {
        'name': 'Storage',
        'type': 'folder',
        'children': []
    }

    if not root_path:
        logger.info("Storage root not found; falling back to MongoDB visualization data")
        return default_tree

    try:
        import mimetypes

        def walk_directory(current_path: Path):
            entries = []
            for item in sorted(current_path.iterdir(), key=lambda p: (p.is_file(), p.name.lower())):
                if item.name.startswith('.'):
                    continue

                if item.is_dir():
                    entries.append({
                        'name': item.name,
                        'type': 'folder',
                        'children': walk_directory(item)
                    })
                else:
                    size_bytes = item.stat().st_size
                    mime_type = mimetypes.guess_type(item.name)[0] or ''
                    relative_path = item.relative_to(root_path).as_posix()
                    entries.append({
                        'name': item.name,
                        'type': 'file',
                        'size': format_size(size_bytes),
                        'mimeType': mime_type,
                        'storagePath': relative_path,
                        'extension': item.suffix,
                        'collection': 'storage'
                    })
            return entries

        children = walk_directory(root_path)
        logger.info("Storage visualization tree built from %s (%d entries)", root_path, len(children))
        return {
            'name': root_path.name or 'Storage',
            'type': 'folder',
            'children': children
        }
    except Exception:
        logger.exception("Failed to build storage visualization tree")
        return default_tree


def format_size(size_bytes: int) -> str:
    """Format file size in bytes to human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    logger.debug("/api/health check invoked")
    return jsonify({
        'status': 'healthy',
        'message': 'AURAverse API is running'
    }), 200


if __name__ == '__main__':
    logger.info("=" * 70)
    logger.info("AURAVERSE BACKEND API SERVER")
    logger.info("=" * 70)
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


"""
Flask API Server for Clustro Backend

This server connects the frontend to the classification system.
"""

import os
import logging
import re
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

from classification.main import process_upload_request, ClassificationProcessor

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

        # Refresh file tree snapshot for frontend visualization
        visualization_tree = fetch_file_tree_from_db()

        # Format response
        response = {
            'success': True,
            'message': f'Processed {result["total_files"]} files successfully',
            'processing_result': result,
            'databaseState': database_state,
            'visualizationData': visualization_tree
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
    if not HAS_SEMANTIC_SEARCH:
        return jsonify({
            'success': False,
            'error': 'Semantic search engine is not available on this deployment'
        }), 503

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

    engine = get_semantic_engine()
    if not engine or not engine.available:
        return jsonify({
            'success': False,
            'error': 'Semantic search engine is initialising or unavailable'
        }), 503

    logger.info("/api/search/semantic requested (query length=%d, limit=%d)", len(query), limit)
    results = engine.search(query, limit=limit)
    source = 'semantic'

    if not results:
        fallback_results = search_metadata_fallback(query, limit)
        if fallback_results:
            results = fallback_results
            source = 'metadata'

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

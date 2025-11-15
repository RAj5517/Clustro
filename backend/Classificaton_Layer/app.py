"""
Classification Layer - Main Server Application

This server receives file uploads from the frontend, classifies them,
and routes them to appropriate processing pipelines:
- Media files (images/audio/video) -> CLIP_Model
- Text files -> Other processing (TBD)
"""

import os
import sys
import shutil
import tempfile
import mimetypes
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

# Add CLIP_Model directory to path (sibling directory in backend/)
sys.path.insert(0, str(Path(__file__).parent.parent / "CLIP_Model"))

from file_classifier import FileClassifier

# Try to import CLIP_Model service
try:
    from clip_service import CLIPService, get_clip_service
    CLIP_AVAILABLE = True
except ImportError:
    # Fallback to direct import if service not available
    try:
        from multimodal_pipeline import MultiModalPipeline
        CLIP_AVAILABLE = True
        CLIPService = None
        get_clip_service = None
    except ImportError:
        print("Warning: CLIP_Model not available. Media files will be categorized but not processed.")
        CLIP_AVAILABLE = False
        MultiModalPipeline = None
        CLIPService = None
        get_clip_service = None

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Configuration
UPLOAD_FOLDER = Path(__file__).parent / "uploads"
UPLOAD_FOLDER.mkdir(exist_ok=True)
ALLOWED_EXTENSIONS = set()  # Allow all file types

# Initialize components
classifier = FileClassifier()
clip_service = None

if CLIP_AVAILABLE:
    try:
        # Try to use CLIPService first (preferred)
        if CLIPService is not None:
            clip_service = get_clip_service(enable_audio=True)
            print("CLIP_Model service initialized successfully")
        elif MultiModalPipeline is not None:
            # Fallback to direct pipeline
            clip_service = MultiModalPipeline(enable_audio=True)
            print("CLIP_Model pipeline initialized successfully (direct mode)")
    except Exception as e:
        print(f"Warning: Failed to initialize CLIP_Model: {e}")
        CLIP_AVAILABLE = False
        clip_service = None

# In-memory database state (in production, use a real database)
database_state = {
    "tables": [],
    "collections": [],
    "mediaDirectories": []
}


def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed."""
    return True  # Allow all files


def extract_folder_structure(files: List) -> Dict[str, Any]:
    """
    Extract folder structure from uploaded files.
    Files may have webkitRelativePath or path information.
    
    Returns:
        Dictionary mapping relative paths to file objects
    """
    file_map = {}
    
    for file in files:
        # Flask file objects have 'filename' attribute
        if hasattr(file, 'filename') and file.filename:
            # Try to get relative path from filename (may contain folder structure)
            rel_path = file.filename
        elif hasattr(file, 'name'):
            rel_path = file.name
        else:
            rel_path = str(file)
        
        file_map[rel_path] = file
    
    return file_map


def save_uploaded_files(files: Dict[str, Any], temp_dir: Path) -> List[str]:
    """
    Save uploaded files to temporary directory, preserving folder structure.
    
    Args:
        files: Dictionary mapping relative paths to file objects
        temp_dir: Temporary directory to save files
        
    Returns:
        List of absolute file paths
    """
    saved_paths = []
    
    for rel_path, file_obj in files.items():
        # Create secure path
        path_parts = Path(rel_path).parts
        secure_parts = [secure_filename(part) for part in path_parts]
        file_path = temp_dir / Path(*secure_parts)
        
        # Create parent directories
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save file (Flask file objects have 'save' method)
        if hasattr(file_obj, 'save'):
            file_obj.save(str(file_path))
        elif hasattr(file_obj, 'read'):
            # Read and write file content
            file_obj.seek(0)  # Reset file pointer
            with open(file_path, 'wb') as f:
                f.write(file_obj.read())
        elif isinstance(file_obj, str) and os.path.exists(file_obj):
            # If it's already a path string
            shutil.copy2(file_obj, file_path)
        else:
            # Fallback: try to save as-is
            try:
                with open(file_path, 'wb') as f:
                    if hasattr(file_obj, 'read'):
                        file_obj.seek(0)
                        f.write(file_obj.read())
            except Exception as e:
                print(f"Warning: Could not save file {rel_path}: {e}")
                continue
        
        saved_paths.append(str(file_path))
    
    return saved_paths


def process_media_files(file_paths: List[str], base_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    Process media files through CLIP_Model service.
    
    Args:
        file_paths: List of file paths to process
        base_path: Base path to calculate relative paths from
        
    Returns:
        List of processed file information
    """
    processed_files = []
    
    if not CLIP_AVAILABLE or clip_service is None:
        # If CLIP is not available, just return file metadata
        for file_path in file_paths:
            file_stat = os.stat(file_path)
            path_obj = Path(file_path)
            if base_path:
                try:
                    rel_path = str(path_obj.relative_to(base_path))
                except ValueError:
                    rel_path = path_obj.name
            else:
                rel_path = path_obj.name
            
            processed_files.append({
                'path': file_path,
                'name': path_obj.name,
                'relative_path': rel_path,
                'size': file_stat.st_size,
                'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                'processed': False,
                'error': 'CLIP_Model not available'
            })
        return processed_files
    
    # Use CLIPService if available (preferred method)
    if hasattr(clip_service, 'process_files'):
        # Using CLIPService
        results = clip_service.process_files(file_paths)
        
        for result, file_path in zip(results, file_paths):
            file_stat = os.stat(file_path)
            path_obj = Path(file_path)
            if base_path:
                try:
                    rel_path = str(path_obj.relative_to(base_path))
                except ValueError:
                    rel_path = path_obj.name
            else:
                rel_path = path_obj.name
            
            if result['success']:
                processed_files.append({
                    'path': file_path,
                    'name': path_obj.name,
                    'relative_path': rel_path,
                    'size': file_stat.st_size,
                    'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                    'processed': True,
                    'modality': result.get('modality'),
                    'text': result.get('text'),
                    'embedding': result.get('embedding') is not None,
                    'embedding_dim': result.get('embedding_dim', 0)
                })
            else:
                processed_files.append({
                    'path': file_path,
                    'name': path_obj.name,
                    'relative_path': rel_path,
                    'size': file_stat.st_size,
                    'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                    'processed': False,
                    'error': result.get('error', 'Unknown error')
                })
    else:
        # Fallback to direct MultiModalPipeline usage
        for file_path in file_paths:
            try:
                # Process through CLIP pipeline
                result = clip_service.encode_path(file_path)
                
                file_stat = os.stat(file_path)
                path_obj = Path(file_path)
                if base_path:
                    try:
                        rel_path = str(path_obj.relative_to(base_path))
                    except ValueError:
                        rel_path = path_obj.name
                else:
                    rel_path = path_obj.name
                
                processed_files.append({
                    'path': file_path,
                    'name': path_obj.name,
                    'relative_path': rel_path,
                    'size': file_stat.st_size,
                    'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                    'processed': True,
                    'modality': result.get('modality'),
                    'text': result.get('text'),
                    'embedding': result.get('embedding') is not None
                })
            except Exception as e:
                file_stat = os.stat(file_path)
                path_obj = Path(file_path)
                if base_path:
                    try:
                        rel_path = str(path_obj.relative_to(base_path))
                    except ValueError:
                        rel_path = path_obj.name
                else:
                    rel_path = path_obj.name
                
                processed_files.append({
                    'path': file_path,
                    'name': path_obj.name,
                    'relative_path': rel_path,
                    'size': file_stat.st_size,
                    'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                    'processed': False,
                    'error': str(e)
                })
    
    return processed_files


def process_text_files(file_paths: List[str], base_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    Process text files (placeholder - user will specify destination later).
    
    Args:
        file_paths: List of file paths to process
        base_path: Base path to calculate relative paths from
        
    Returns:
        List of file information
    """
    processed_files = []
    
    for file_path in file_paths:
        try:
            file_stat = os.stat(file_path)
            path_obj = Path(file_path)
            if base_path:
                try:
                    rel_path = str(path_obj.relative_to(base_path))
                except ValueError:
                    rel_path = path_obj.name
            else:
                rel_path = path_obj.name
            
            processed_files.append({
                'path': file_path,
                'name': path_obj.name,
                'relative_path': rel_path,
                'size': file_stat.st_size,
                'type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                'extension': path_obj.suffix.lower(),
                'processed': False,
                'note': 'Text file - destination TBD'
            })
        except Exception as e:
            path_obj = Path(file_path)
            if base_path:
                try:
                    rel_path = str(path_obj.relative_to(base_path))
                except ValueError:
                    rel_path = path_obj.name
            else:
                rel_path = path_obj.name
            
            processed_files.append({
                'path': file_path,
                'name': path_obj.name,
                'relative_path': rel_path,
                'size': 0,
                'type': 'application/octet-stream',
                'processed': False,
                'error': str(e)
            })
    
    return processed_files


def update_database_state(media_files: List[Dict], text_files: List[Dict]):
    """
    Update the in-memory database state with processed files.
    
    Args:
        media_files: List of processed media file information
        text_files: List of processed text file information
    """
    
    # Group media files by type
    images = [f for f in media_files if f.get('type', '').startswith('image/')]
    videos = [f for f in media_files if f.get('type', '').startswith('video/')]
    audio = [f for f in media_files if f.get('type', '').startswith('audio/')]
    
    # Create media directories
    timestamp = int(datetime.now().timestamp() * 1000)
    
    if images:
        database_state['mediaDirectories'].append({
            'name': f'directory_{timestamp}_images',
            'category': 'Photos',
            'files': [{
                'name': f['name'],
                'path': f.get('relative_path', f['name']),
                'size': f['size'],
                'type': f['type']
            } for f in images]
        })
    
    if videos:
        database_state['mediaDirectories'].append({
            'name': f'directory_{timestamp}_videos',
            'category': 'Videos',
            'files': [{
                'name': f['name'],
                'path': f.get('relative_path', f['name']),
                'size': f['size'],
                'type': f['type']
            } for f in videos]
        })
    
    if audio:
        database_state['mediaDirectories'].append({
            'name': f'directory_{timestamp}_audio',
            'category': 'Audio',
            'files': [{
                'name': f['name'],
                'path': f.get('relative_path', f['name']),
                'size': f['size'],
                'type': f['type']
            } for f in audio]
        })
    
    # Group text files into collections
    if text_files:
        # Group by extension
        text_by_ext = {}
        for f in text_files:
            ext = f.get('extension', 'unknown')
            if ext not in text_by_ext:
                text_by_ext[ext] = []
            text_by_ext[ext].append(f)
        
        for ext, files in text_by_ext.items():
            collection_name = f'collection_{ext.replace(".", "")}_{timestamp}'
            database_state['collections'].append({
                'name': collection_name,
                'count': len(files),
                'schema': {
                    'fileName': 'string',
                    'fileType': 'string',
                    'fileSize': 'number',
                    'extension': ext,
                    'note': 'Text file - destination TBD'
                }
            })


@app.route('/api/upload', methods=['POST'])
def upload_files():
    """Handle file uploads from frontend."""
    try:
        if 'files' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No files provided',
                'code': 'NO_FILES'
            }), 400
        
        files = request.files.getlist('files')
        metadata = request.form.get('metadata', '')
        
        if not files or all(f.filename == '' for f in files):
            return jsonify({
                'success': False,
                'error': 'No files selected',
                'code': 'EMPTY_FILES'
            }), 400
        
        # Create temporary directory for this upload
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Extract folder structure
            file_map = extract_folder_structure(files)
            
            # Save files preserving folder structure
            saved_paths = save_uploaded_files(file_map, temp_path)
            
            # Classify files
            classification = classifier.classify_files(saved_paths)
            
            # Process media files through CLIP_Model
            media_processed = process_media_files(classification['media'], base_path=temp_path)
            
            # Process text files (placeholder)
            text_processed = process_text_files(classification['text'], base_path=temp_path)
            
            # Update database state
            update_database_state(media_processed, text_processed)
            
            return jsonify({
                'success': True,
                'message': f'Files uploaded and processed successfully. {len(media_processed)} media files, {len(text_processed)} text files.',
                'databaseState': {
                    'tables': database_state['tables'],
                    'collections': database_state['collections'],
                    'mediaDirectories': database_state['mediaDirectories']
                }
            }), 200
    
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Failed to process files: {str(e)}',
            'code': 'PROCESSING_ERROR'
        }), 500


@app.route('/api/database/state', methods=['GET'])
def get_database_state():
    """Get current database state."""
    return jsonify({
        'tables': database_state['tables'],
        'collections': database_state['collections'],
        'mediaDirectories': database_state['mediaDirectories']
    }), 200


@app.route('/api/database/<type>/<id>', methods=['GET'])
def get_database_entity(type: str, id: str):
    """Get specific database entity (optional endpoint)."""
    # This is a placeholder - implement based on your needs
    return jsonify({
        'success': False,
        'error': 'Not implemented yet',
        'code': 'NOT_IMPLEMENTED'
    }), 501


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'clip_available': CLIP_AVAILABLE
    }), 200


if __name__ == '__main__':
    print("Starting Classification Layer server...")
    print(f"CLIP_Model available: {CLIP_AVAILABLE}")
    app.run(host='0.0.0.0', port=8000, debug=True)


"""
Main Entry Point for Backend Classification System

This is the central processing hub that:
1. Receives files/folders from frontend
2. Separates media files from non-media files
3. Routes non-media files to classifier (SQL/NoSQL)
4. Routes SQL files to schema generator/executor
5. Routes NoSQL files to NoSQL processing (placeholder)
6. Routes media files to multimodal pipeline (placeholder)
"""

import os
import sys
import mimetypes
from pathlib import Path
from typing import Dict, List, Any, Optional
import tempfile
import shutil

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import components
from file_classifier import FileClassifier
from Schema_generator.config import get_db_config
from Schema_generator.schema_generator import generate_schema
from Schema_generator.schema_executor import execute_pending_jobs

# Import multimodal pipeline (will be used for media files)
try:
    from CLIP_Model.multimodal_pipeline import MultiModalPipeline
    HAS_CLIP = True
except ImportError:
    HAS_CLIP = False
    print("[WARNING] CLIP_Model not available - media processing will be skipped")


class ClassificationProcessor:
    """Main processor for file classification and routing."""
    
    # Media file extensions
    MEDIA_EXTENSIONS = {
        # Images
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff', '.tif',
        # Videos
        '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.3gp', '.mpg', '.mpeg',
        # Audio
        '.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus'
    }
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize the processor with database configuration."""
        self.db_config = db_config or get_db_config()
        self.classifier = FileClassifier()
        
        # Initialize multimodal pipeline for media files (placeholder for now)
        self.multimodal_pipeline = None
        if HAS_CLIP:
            try:
                # Initialize with default settings (can be configured later)
                self.multimodal_pipeline = MultiModalPipeline()
                print("[OK] Multimodal pipeline initialized")
            except Exception as e:
                print(f"[WARNING] Could not initialize multimodal pipeline: {e}")
                self.multimodal_pipeline = None
    
    def is_media_file(self, file_path: str) -> bool:
        """Check if a file is a media file based on extension."""
        ext = Path(file_path).suffix.lower()
        return ext in self.MEDIA_EXTENSIONS
    
    def process_upload(self, files: List[Any], metadata: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry point - processes uploaded files/folders from frontend.
        
        Args:
            files: List of file objects (from Flask request.files.getlist('files'))
            metadata: Optional metadata string from frontend
            
        Returns:
            Dictionary with processing results
        """
        # Save uploaded files to temporary directory
        temp_dir = tempfile.mkdtemp(prefix='clustro_upload_')
        
        try:
            # Save all files to temp directory
            saved_files = []
            for file_obj in files:
                if hasattr(file_obj, 'filename') and file_obj.filename:
                    # Flask file object
                    file_path = Path(temp_dir) / file_obj.filename
                    file_obj.save(str(file_path))
                    saved_files.append(file_path)
                elif isinstance(file_obj, (str, Path)):
                    # Already a path
                    saved_files.append(Path(file_obj))
            
            # Process all files
            results = self.process_files(saved_files, metadata)
            
            return results
            
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass
    
    def process_files(self, file_paths: List[Path], metadata: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a list of file paths (handles both files and folder contents).
        
        Args:
            file_paths: List of file paths (Path objects)
            metadata: Optional metadata string
            
        Returns:
            Dictionary with processing results
        """
        # Separate media and non-media files
        media_files = []
        non_media_files = []
        
        for file_path in file_paths:
            if file_path.is_file():
                if self.is_media_file(str(file_path)):
                    media_files.append(file_path)
                else:
                    non_media_files.append(file_path)
            elif file_path.is_dir():
                # If it's a directory, extract all files recursively
                for item in file_path.rglob('*'):
                    if item.is_file():
                        if self.is_media_file(str(item)):
                            media_files.append(item)
                        else:
                            non_media_files.append(item)
        
        results = {
            'total_files': len(media_files) + len(non_media_files),
            'media_files': len(media_files),
            'non_media_files': len(non_media_files),
            'metadata': metadata,
            'non_media_results': [],
            'media_results': [],
            'errors': []
        }
        
        # Process non-media files
        if non_media_files:
            print(f"[INFO] Processing {len(non_media_files)} non-media files...")
            non_media_results = self._process_non_media_files(non_media_files)
            results['non_media_results'] = non_media_results
        
        # Process media files (placeholder for now)
        if media_files:
            print(f"[INFO] Processing {len(media_files)} media files...")
            media_results = self._process_media_files(media_files)
            results['media_results'] = media_results
        
        return results
    
    def _process_non_media_files(self, file_paths: List[Path]) -> List[Dict[str, Any]]:
        """
        Process non-media files through classifier and route to SQL/NoSQL.
        
        Args:
            file_paths: List of non-media file paths
            
        Returns:
            List of processing results for each file
        """
        results = []
        sql_files = []
        
        for file_path in file_paths:
            file_result = {
                'file': str(file_path),
                'file_name': file_path.name,
                'classification': None,
                'sql_result': None,
                'nosql_result': None,
                'error': None
            }
            
            try:
                # Classify file as SQL or NoSQL
                classification_result = self.classifier.classify(str(file_path))
                file_result['classification'] = classification_result['classification']
                file_result['sql_score'] = classification_result.get('sql_score', 0)
                file_result['nosql_score'] = classification_result.get('nosql_score', 0)
                file_result['reasons'] = classification_result.get('reasons', [])
                
                if classification_result['classification'] == 'SQL':
                    # Route to schema generator and executor
                    sql_files.append((file_path, file_result))
                else:
                    # Route to NoSQL processing (placeholder for now)
                    file_result['nosql_result'] = {
                        'status': 'pending',
                        'message': 'NoSQL processing will be implemented here',
                        'note': 'Placeholder for NoSQL document storage (MongoDB, DynamoDB, etc.)'
                    }
                
            except Exception as e:
                file_result['error'] = str(e)
                results.append(file_result)
                continue
            
            results.append(file_result)
        
        # Process SQL files through schema generator and executor
        if sql_files:
            print(f"[INFO] Processing {len(sql_files)} SQL files through schema generator...")
            for file_path, file_result in sql_files:
                try:
                    # Generate schema
                    schema_result = generate_schema(str(file_path), self.db_config)
                    file_result['sql_result'] = {
                        'schema_generation': schema_result,
                        'tables': schema_result.get('tables', []),
                        'jobs_created': schema_result.get('jobs_created', 0)
                    }
                    
                    # Execute pending jobs if any were created
                    if schema_result.get('jobs_created', 0) > 0:
                        executor_result = execute_pending_jobs(self.db_config, stop_on_error=False)
                        file_result['sql_result']['execution'] = executor_result
                        
                except Exception as e:
                    file_result['error'] = str(e) if not file_result.get('error') else file_result['error']
                    file_result['sql_result'] = {
                        'error': str(e),
                        'status': 'failed'
                    }
        
        return results
    
    def _process_media_files(self, file_paths: List[Path]) -> List[Dict[str, Any]]:
        """
        Process media files through multimodal pipeline (placeholder for now).
        
        Args:
            file_paths: List of media file paths
            
        Returns:
            List of processing results for each file
        """
        results = []
        
        for file_path in file_paths:
            file_result = {
                'file': str(file_path),
                'file_name': file_path.name,
                'file_type': self._get_media_type(str(file_path)),
                'status': 'pending',
                'result': None,
                'error': None
            }
            
            if self.multimodal_pipeline:
                try:
                    # Process through multimodal pipeline
                    # TODO: Implement actual processing when ready
                    file_result['status'] = 'pending'
                    file_result['result'] = {
                        'message': 'Media processing through multimodal pipeline will be implemented here',
                        'note': 'Placeholder for CLIP embedding generation and captioning'
                    }
                except Exception as e:
                    file_result['error'] = str(e)
                    file_result['status'] = 'error'
            else:
                file_result['status'] = 'skipped'
                file_result['result'] = {
                    'message': 'Multimodal pipeline not available',
                    'note': 'CLIP_Model not initialized or not available'
                }
            
            results.append(file_result)
        
        return results
    
    def _get_media_type(self, file_path: str) -> str:
        """Determine media type (image, video, audio)."""
        ext = Path(file_path).suffix.lower()
        
        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff', '.tif']:
            return 'image'
        elif ext in ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.3gp', '.mpg', '.mpeg']:
            return 'video'
        elif ext in ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus']:
            return 'audio'
        else:
            return 'unknown'


def process_upload_request(files: List[Any], metadata: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function for Flask/FastAPI integration.
    
    Args:
        files: List of file objects from request
        metadata: Optional metadata string
        
    Returns:
        Processing results dictionary
    """
    processor = ClassificationProcessor()
    return processor.process_upload(files, metadata)


if __name__ == "__main__":
    """Test the classification processor."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Classification Processor - Main Entry Point')
    parser.add_argument('path', help='Path to file or folder to process')
    parser.add_argument('--metadata', type=str, help='Optional metadata string')
    args = parser.parse_args()
    
    processor = ClassificationProcessor()
    
    path = Path(args.path)
    if path.exists():
        if path.is_file():
            files = [path]
        else:
            files = list(path.rglob('*'))
            files = [f for f in files if f.is_file()]
        
        results = processor.process_files(files, args.metadata)
        
        print("\n" + "=" * 70)
        print("PROCESSING RESULTS")
        print("=" * 70)
        print(f"Total files: {results['total_files']}")
        print(f"Media files: {results['media_files']}")
        print(f"Non-media files: {results['non_media_files']}")
        
        if results['non_media_results']:
            print("\nNon-Media Files Results:")
            for result in results['non_media_results']:
                print(f"  - {result['file_name']}: {result['classification']}")
                if result.get('sql_result'):
                    print(f"    Tables created: {len(result['sql_result'].get('tables', []))}")
        
        if results['media_results']:
            print("\nMedia Files Results:")
            for result in results['media_results']:
                print(f"  - {result['file_name']}: {result['file_type']} ({result['status']})")
    else:
        print(f"[ERROR] Path does not exist: {path}")


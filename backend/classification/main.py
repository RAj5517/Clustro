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
import logging

# Configure logging if not already configured
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / 'sql'))

# Import components
from sql.file_classifier import FileClassifier
try:
    from sql.config import get_db_config
except ImportError:
    from sql_pipeline_stub import get_db_config

from sql_pipeline_stub import generate_schema, execute_pending_jobs

# Import NoSQL ingestion pipeline
try:
    from nosql_ingestion_pipeline import NoSQLIngestionPipeline
except ImportError:
    NoSQLIngestionPipeline = None  # type: ignore
    logger.warning("NoSQL ingestion pipeline not available - NoSQL processing will be skipped")

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

        # Initialize NoSQL ingestion pipeline (routes NoSQL assets to Mongo/Chroma)
        self.nosql_pipeline = None
        if NoSQLIngestionPipeline is not None:
            try:
                self.nosql_pipeline = NoSQLIngestionPipeline(multimodal_pipeline=self.multimodal_pipeline)
                logger.info("[OK] NoSQL ingestion pipeline initialized")
            except Exception as e:
                logger.warning(f"Could not initialize NoSQL ingestion pipeline: {e}")
                self.nosql_pipeline = None
        else:
            logger.warning("NoSQL ingestion pipeline not available - NoSQL processing disabled")
    
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
        logger.info("process_upload: received %d files with metadata length %d", len(files), len(metadata or ""))
        
        try:
            # Save all files to temp directory
            saved_files = []
            for file_obj in files:
                if hasattr(file_obj, 'filename') and file_obj.filename:
                    # Flask file object
                    # Preserve folder structure from uploaded folder
                    file_path = Path(temp_dir) / file_obj.filename
                    
                    # Create parent directories recursively if they don't exist
                    # This handles nested folders (folder/subfolder/file.csv)
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Save the file
                    file_obj.save(str(file_path))
                    saved_files.append(file_path)
                elif isinstance(file_obj, (str, Path)):
                    # Already a path
                    saved_files.append(Path(file_obj))
            
            # Process all files
            results = self.process_files(saved_files, metadata)
            logger.info("process_upload: completed processing %d files", len(saved_files))
            
            return results
            
        finally:
            # Clean up temporary directory
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                logger.warning("process_upload: failed to remove temp dir %s", temp_dir, exc_info=True)
    
    def process_files(self, file_paths: List[Path], metadata: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a list of file paths (handles both files and folder contents).
        
        Args:
            file_paths: List of file paths (Path objects)
            metadata: Optional metadata string
            
        Returns:
            Dictionary with processing results
        """
        logger.info("process_files: starting with %d paths", len(file_paths))
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
        
        logger.info(
            "process_files: detected %d media and %d non-media files",
            len(media_files),
            len(non_media_files)
        )
        # Process non-media files
        if non_media_files:
            logger.info("process_files: processing %d non-media files", len(non_media_files))
            non_media_results = self._process_non_media_files(non_media_files, metadata)
            results['non_media_results'] = non_media_results
        
        # Process media files (placeholder for now)
        if media_files:
            logger.info("process_files: processing %d media files", len(media_files))
            media_results = self._process_media_files(media_files, metadata)
            results['media_results'] = media_results
        
        logger.info(
            "process_files: completed with %d total results",
            len(results['non_media_results']) + len(results['media_results'])
        )
        
        return results
    
    def _process_non_media_files(self, file_paths: List[Path], metadata: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Process non-media files through classifier and route to SQL/NoSQL.
        
        Args:
            file_paths: List of non-media file paths
            
        Returns:
            List of processing results for each file
        """
        logger.info("_process_non_media_files: handling %d files", len(file_paths))
        results = []
        
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
                logger.info(
                    "Classified %s as %s (SQL score %.3f, NoSQL score %.3f)",
                    file_path.name,
                    classification_result['classification'],
                    classification_result.get('sql_score', 0),
                    classification_result.get('nosql_score', 0)
                )
                file_result['classification'] = classification_result['classification']
                file_result['sql_score'] = classification_result.get('sql_score', 0)
                file_result['nosql_score'] = classification_result.get('nosql_score', 0)
                file_result['reasons'] = classification_result.get('reasons', [])
                
                if classification_result['classification'] == 'SQL':
                    # SQL processing disabled - capture placeholder response
                    logger.info("Routing %s to SQL placeholder handler", file_path.name)
                    file_result['sql_result'] = self._handle_sql_file_placeholder(file_path)
                else:
                    # Route to NoSQL processing pipeline
                    logger.info("Routing %s to NoSQL pipeline", file_path.name)
                    file_result['nosql_result'] = self._route_to_nosql_pipeline(
                        file_path=file_path,
                        classification_result=classification_result,
                        metadata=metadata,
                        modality_hint='text'
                    )
                
            except Exception as e:
                file_result['error'] = str(e)
                logger.exception("Failed processing non-media file %s", file_path)
                results.append(file_result)
                continue
            
            results.append(file_result)
        
        logger.info("_process_non_media_files: completed %d items", len(results))
        return results
    
    def _process_media_files(self, file_paths: List[Path], metadata: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Process media files through multimodal pipeline (placeholder for now).
        
        Args:
            file_paths: List of media file paths
            
        Returns:
            List of processing results for each file
        """
        logger.info("_process_media_files: handling %d files", len(file_paths))
        results = []
        
        for file_path in file_paths:
            file_type = self._get_media_type(str(file_path))
            logger.info("Detected media file %s of type %s", file_path.name, file_type)
            file_result = {
                'file': str(file_path),
                'file_name': file_path.name,
                'file_type': file_type,
                'status': 'pending',
                'result': None,
                'error': None,
                'nosql_result': None
            }

            classification_stub = {
                'classification': 'NoSQL',
                'reason': 'Media assets are routed to the NoSQL ingestion pipeline',
                'modality': file_type
            }

            nosql_result = self._route_to_nosql_pipeline(
                file_path=file_path,
                classification_result=classification_stub,
                metadata=metadata,
                modality_hint=file_type
            )
            file_result['nosql_result'] = nosql_result

            if not nosql_result or nosql_result.get('status') == 'pending':
                file_result['result'] = {
                    'message': 'Media file queued for NoSQL ingestion',
                    'modality': file_type
                }
            elif nosql_result.get('status') == 'completed':
                file_result['status'] = 'completed'
                file_result['result'] = {
                    'message': 'Media file embedded and stored via NoSQL pipeline',
                    'modality': file_type,
                    'file_id': nosql_result.get('file_id'),
                    'graph_nodes': nosql_result.get('graph_nodes', [])
                }
            elif nosql_result.get('status') == 'skipped':
                file_result['status'] = 'skipped'
                file_result['result'] = {
                    'message': 'NoSQL pipeline skipped media processing',
                    'note': nosql_result.get('error') or 'Pipeline disabled'
                }
            else:
                file_result['status'] = 'error'
                file_result['error'] = nosql_result.get('error', 'Unknown media processing error')
                file_result['result'] = {
                    'message': 'Media file failed to process via NoSQL pipeline',
                    'modality': file_type
                }
            
            results.append(file_result)
        
        logger.info("_process_media_files: completed %d items", len(results))
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

    def _handle_sql_file_placeholder(self, file_path: Path) -> Dict[str, Any]:
        """
        Return a placeholder response for SQL files now that the schema generator
        has been removed.
        """
        logger.info("SQL placeholder invoked for %s", file_path.name)
        schema_result = generate_schema(str(file_path), self.db_config)
        execution_result = execute_pending_jobs(self.db_config)
        return {
            'status': 'disabled',
            'message': 'SQL schema processing has been removed from this deployment.',
            'schema_generation': schema_result,
            'execution': execution_result
        }

    def _route_to_nosql_pipeline(
        self,
        file_path: Path,
        classification_result: Dict[str, Any],
        metadata: Optional[str],
        modality_hint: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Invoke the NoSQL ingestion pipeline for the provided file.
        
        This handles routing of NoSQL-classified files (including PDFs) to the
        NoSQL ingestion pipeline for processing and storage.
        """
        file_ext = file_path.suffix.lower()
        is_pdf = file_ext == '.pdf'
        
        if not self.nosql_pipeline:
            error_msg = 'NoSQL ingestion pipeline is not available'
            logger.error("Cannot route %s to NoSQL pipeline: %s", file_path.name, error_msg)
            return {
                'status': 'skipped',
                'error': error_msg
            }

        ingest_metadata = {
            'tenant_id': classification_result.get('tenant_id'),
            'metadata_text': metadata,
            'source': 'classification_processor'
        }

        try:
            # Detect modality if not provided
            if not modality_hint:
                if is_pdf:
                    modality_hint = 'text'  # PDFs are processed as text
                    logger.debug("Detected PDF file, setting modality to 'text' for %s", file_path.name)
                else:
                    modality_hint = classification_result.get('modality', 'text')
            
            logger.info(
                "Routing %s to NoSQL ingestion (modality=%s, classification=%s)",
                file_path.name,
                modality_hint,
                classification_result.get('classification', 'NoSQL')
            )
            
            if is_pdf:
                logger.info("Processing PDF file: %s", file_path.name)
            
            # Process file through NoSQL pipeline
            result = self.nosql_pipeline.process_file(
                file_path=file_path,
                classification_result=classification_result,
                metadata=ingest_metadata,
                modality_hint=modality_hint
            )
            
            status = result.get('status', 'unknown')
            logger.info(
                "NoSQL ingestion completed for %s: status=%s, file_id=%s, collection=%s, chunks=%d",
                file_path.name,
                status,
                result.get('file_id', 'N/A'),
                result.get('collection', 'N/A'),
                result.get('chunk_count', 0)
            )
            
            if status == 'error':
                error_msg = result.get('error', 'Unknown error')
                logger.error("NoSQL ingestion failed for %s: %s", file_path.name, error_msg)
            elif status == 'completed':
                logger.info("NoSQL ingestion successful for %s", file_path.name)
            
            return result
            
        except FileNotFoundError as fnf_exc:
            error_msg = f"File not found: {file_path}"
            logger.error("File not found when routing to NoSQL pipeline: %s - %s", file_path.name, fnf_exc)
            return {
                'status': 'error',
                'error': error_msg
            }
        except ImportError as import_exc:
            error_msg = f"Required library missing: {str(import_exc)}"
            logger.error("Import error when routing %s to NoSQL pipeline: %s", file_path.name, import_exc)
            if is_pdf:
                logger.error("PDF processing requires PyPDF2. Install with: pip install PyPDF2")
            return {
                'status': 'error',
                'error': error_msg
            }
        except Exception as exc:
            error_msg = f"Unexpected error: {str(exc)}"
            logger.exception("NoSQL pipeline failed for %s: %s", file_path.name, exc)
            return {
                'status': 'error',
                'error': error_msg
            }


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


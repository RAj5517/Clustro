"""
Comprehensive test suite for NoSQL processing.

This script tests:
1. Video file processing
2. Image file processing
3. PDF file processing
4. File storage in storage folder
5. MongoDB storage
6. ChromaDB embeddings
7. File structure generation
"""

import sys
import os
import tempfile
import shutil
from pathlib import Path
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_files():
    """Create test files for different types."""
    test_dir = Path(__file__).parent / "test_files"
    test_dir.mkdir(exist_ok=True)
    
    test_files = {}
    
    # Create a test PDF
    try:
        import PyPDF2
        pdf_path = test_dir / "test_document.pdf"
        pdf_writer = PyPDF2.PdfWriter()
        page = PyPDF2.PageObject.create_blank_page(width=612, height=792)
        pdf_writer.add_page(page)
        with open(pdf_path, 'wb') as f:
            pdf_writer.write(f)
        test_files['pdf'] = pdf_path
        logger.info("Created test PDF: %s", pdf_path)
    except ImportError:
        logger.warning("PyPDF2 not available - skipping PDF test file")
    except Exception as exc:
        logger.warning("Failed to create test PDF: %s", exc)
    
    # Create a test text file
    text_path = test_dir / "test_document.txt"
    text_path.write_text("This is a test document for NoSQL processing. It contains some text content.")
    test_files['text'] = text_path
    logger.info("Created test text file: %s", text_path)
    
    # Create a test image file (minimal valid PNG)
    try:
        image_path = test_dir / "test_image.png"
        # Create a minimal 1x1 PNG
        png_data = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82'
        image_path.write_bytes(png_data)
        test_files['image'] = image_path
        logger.info("Created test image: %s", image_path)
    except Exception as exc:
        logger.warning("Failed to create test image: %s", exc)
    
    # Create a test video file placeholder (empty file with .mp4 extension)
    video_path = test_dir / "test_video.mp4"
    video_path.write_bytes(b'fake video data')
    test_files['video'] = video_path
    logger.info("Created test video placeholder: %s", video_path)
    
    return test_files


def test_file_processing(test_files):
    """Test processing of different file types."""
    logger.info("=" * 70)
    logger.info("TEST: File Processing (PDF, Image, Video, Text)")
    logger.info("=" * 70)
    
    try:
        from classification.main import ClassificationProcessor
        
        processor = ClassificationProcessor()
        logger.info("ClassificationProcessor initialized")
        
        results = {
            'pdf': None,
            'text': None,
            'image': None,
            'video': None
        }
        
        # Test PDF
        if 'pdf' in test_files:
            logger.info("\nTesting PDF processing...")
            try:
                pdf_result = processor.process_files([test_files['pdf']], "Test PDF upload")
                results['pdf'] = pdf_result
                logger.info("PDF processing result: %s", pdf_result.get('non_media_results', [{}])[0].get('nosql_result', {}).get('status', 'unknown'))
            except Exception as exc:
                logger.error("PDF processing failed: %s", exc, exc_info=True)
        
        # Test Text
        if 'text' in test_files:
            logger.info("\nTesting Text file processing...")
            try:
                text_result = processor.process_files([test_files['text']], "Test text upload")
                results['text'] = text_result
                logger.info("Text processing result: %s", text_result.get('non_media_results', [{}])[0].get('nosql_result', {}).get('status', 'unknown'))
            except Exception as exc:
                logger.error("Text processing failed: %s", exc, exc_info=True)
        
        # Test Image
        if 'image' in test_files:
            logger.info("\nTesting Image processing...")
            try:
                image_result = processor.process_files([test_files['image']], "Test image upload")
                results['image'] = image_result
                logger.info("Image processing result: %s", image_result.get('media_results', [{}])[0].get('nosql_result', {}).get('status', 'unknown'))
            except Exception as exc:
                logger.error("Image processing failed: %s", exc, exc_info=True)
        
        # Test Video
        if 'video' in test_files:
            logger.info("\nTesting Video processing...")
            try:
                video_result = processor.process_files([test_files['video']], "Test video upload")
                results['video'] = video_result
                logger.info("Video processing result: %s", video_result.get('media_results', [{}])[0].get('nosql_result', {}).get('status', 'unknown'))
            except Exception as exc:
                logger.error("Video processing failed: %s", exc, exc_info=True)
        
        return results
        
    except Exception as exc:
        logger.error("File processing test failed: %s", exc, exc_info=True)
        return {}


def test_storage_verification():
    """Verify files are stored in storage folder."""
    logger.info("=" * 70)
    logger.info("TEST: Storage Folder Verification")
    logger.info("=" * 70)
    
    try:
        from nosql_ingestion_pipeline.pipeline import NoSQLIngestionPipeline
        from pathlib import Path
        
        pipeline = NoSQLIngestionPipeline()
        storage_root = pipeline._storage_root
        
        if not storage_root:
            logger.error("✗ Storage root not configured")
            return False
        
        logger.info("Storage root: %s", storage_root)
        
        if not storage_root.exists():
            logger.error("✗ Storage root does not exist: %s", storage_root)
            return False
        
        if not storage_root.is_dir():
            logger.error("✗ Storage root is not a directory: %s", storage_root)
            return False
        
        logger.info("✓ Storage root exists and is accessible")
        
        # List files in storage
        files_found = []
        for root, dirs, files in os.walk(storage_root):
            for file in files:
                file_path = Path(root) / file
                rel_path = file_path.relative_to(storage_root)
                files_found.append(rel_path.as_posix())
        
        logger.info("Found %d files in storage:", len(files_found))
        for file_path in files_found[:10]:  # Show first 10
            logger.info("  - %s", file_path)
        
        if len(files_found) > 10:
            logger.info("  ... and %d more files", len(files_found) - 10)
        
        return True
        
    except Exception as exc:
        logger.error("Storage verification failed: %s", exc, exc_info=True)
        return False


def test_mongodb_storage():
    """Verify files are stored in MongoDB."""
    logger.info("=" * 70)
    logger.info("TEST: MongoDB Storage Verification")
    logger.info("=" * 70)
    
    try:
        from nosql_processor.main import get_nosql_db
        
        nosql_db = get_nosql_db()
        if not nosql_db:
            logger.warning("⚠ MongoDB not available - skipping MongoDB verification")
            return False
        
        files_collection = nosql_db['files']
        
        # Count files in MongoDB
        file_count = files_collection.count_documents({})
        logger.info("Found %d files in MongoDB", file_count)
        
        # Get sample files
        sample_files = list(files_collection.find({}).limit(5))
        logger.info("Sample files in MongoDB:")
        for doc in sample_files:
            file_id = str(doc.get('_id', 'N/A'))
            file_name = doc.get('original_name', 'N/A')
            storage_uri = doc.get('storage_uri', 'N/A')
            modality = (doc.get('extra') or {}).get('modality', 'N/A')
            descriptive_text = doc.get('descriptive_text', '')[:50] if doc.get('descriptive_text') else 'N/A'
            logger.info("  - %s (ID: %s, Modality: %s, Storage: %s)", 
                       file_name, file_id[:8], modality, storage_uri)
            logger.info("    Descriptive text: %s", descriptive_text)
        
        return True
        
    except Exception as exc:
        logger.error("MongoDB verification failed: %s", exc, exc_info=True)
        return False


def test_chromadb_storage():
    """Verify embeddings are stored in ChromaDB."""
    logger.info("=" * 70)
    logger.info("TEST: ChromaDB Storage Verification")
    logger.info("=" * 70)
    
    try:
        from nosql_ingestion_pipeline.graph_writer import GraphEmbeddingWriter
        from nosql_ingestion_pipeline.config import load_config
        
        config = load_config()
        graph_writer = GraphEmbeddingWriter(
            persist_path=config.chroma_path,
            collection_name=config.chroma_collection
        )
        
        if not graph_writer.available:
            logger.warning("⚠ ChromaDB not available - skipping ChromaDB verification")
            return False
        
        # Query ChromaDB for sample embeddings
        logger.info("ChromaDB collection available: %s", config.chroma_collection)
        logger.info("ChromaDB path: %s", config.chroma_path)
        
        # Try to query with a dummy embedding to get count
        dummy_embedding = [0.0] * 512  # Typical CLIP embedding size
        results = graph_writer.query_similar(dummy_embedding, limit=10)
        
        logger.info("Found %d embeddings in ChromaDB", len(results))
        for idx, result in enumerate(results[:5], 1):
            node_id = result.get('id', 'N/A')
            text = result.get('text', '')[:50] if result.get('text') else 'N/A'
            modality = result.get('metadata', {}).get('modality', 'N/A')
            logger.info("  %d. ID: %s, Modality: %s, Text: %s", idx, node_id, modality, text)
        
        return True
        
    except Exception as exc:
        logger.error("ChromaDB verification failed: %s", exc, exc_info=True)
        return False


def test_file_structure_generation():
    """Test file structure generation from MongoDB."""
    logger.info("=" * 70)
    logger.info("TEST: File Structure Generation")
    logger.info("=" * 70)
    
    try:
        from app import fetch_file_tree_from_db
        
        file_tree = fetch_file_tree_from_db()
        
        logger.info("File tree structure:")
        logger.info("Root: %s", file_tree.get('name', 'Unknown'))
        logger.info("Type: %s", file_tree.get('type', 'Unknown'))
        
        children = file_tree.get('children', [])
        logger.info("Number of top-level folders: %d", len(children))
        
        def print_tree(node, indent=0):
            prefix = "  " * indent
            if node.get('type') == 'folder':
                logger.info("%s📁 %s/", prefix, node.get('name', 'Unknown'))
                for child in node.get('children', [])[:5]:  # Show first 5 children
                    print_tree(child, indent + 1)
                if len(node.get('children', [])) > 5:
                    logger.info("%s  ... and %d more items", prefix, len(node.get('children', [])) - 5)
            else:
                logger.info("%s📄 %s (ID: %s, Modality: %s)", 
                           prefix, 
                           node.get('name', 'Unknown'),
                           node.get('file_id', 'N/A')[:8] if node.get('file_id') else 'N/A',
                           node.get('modality', 'N/A'))
        
        for child in children[:10]:  # Show first 10 top-level items
            print_tree(child)
        
        if len(children) > 10:
            logger.info("  ... and %d more top-level items", len(children) - 10)
        
        return True
        
    except Exception as exc:
        logger.error("File structure generation test failed: %s", exc, exc_info=True)
        return False


def test_search_functionality():
    """Test search functionality."""
    logger.info("=" * 70)
    logger.info("TEST: Search Functionality")
    logger.info("=" * 70)
    
    try:
        from app import semantic_search
        
        test_queries = [
            "video",
            "document",
            "image",
            "test"
        ]
        
        for query in test_queries:
            logger.info("\nTesting search query: '%s'", query)
            try:
                # Simulate the semantic_search endpoint
                from nosql_ingestion_pipeline.semantic_search import SemanticSearchEngine
                engine = SemanticSearchEngine()
                
                if engine.available:
                    results = engine.search(query, limit=5)
                    logger.info("  Found %d results from ChromaDB", len(results))
                    for idx, result in enumerate(results[:3], 1):
                        logger.info("    %d. ID: %s, Modality: %s, Similarity: %s",
                                   idx,
                                   result.get('id', 'N/A')[:8],
                                   result.get('modality', 'N/A'),
                                   result.get('similarity', 'N/A'))
                else:
                    logger.warning("  ChromaDB search engine not available")
                    
                # Test metadata search fallback
                from app import search_metadata_fallback
                metadata_results = search_metadata_fallback(query, limit=5)
                logger.info("  Found %d results from MongoDB metadata", len(metadata_results))
                
            except Exception as exc:
                logger.warning("  Search query '%s' failed: %s", query, exc)
        
        return True
        
    except Exception as exc:
        logger.error("Search functionality test failed: %s", exc, exc_info=True)
        return False


def run_all_tests():
    """Run all NoSQL processing tests."""
    logger.info("\n" + "=" * 70)
    logger.info("NOSQL PROCESSING COMPREHENSIVE TEST SUITE")
    logger.info("=" * 70 + "\n")
    
    # Create test files
    logger.info("Creating test files...")
    test_files = create_test_files()
    logger.info("Created %d test files\n", len(test_files))
    
    tests = [
        ("File Processing (PDF/Text/Image/Video)", lambda: test_file_processing(test_files)),
        ("Storage Folder Verification", test_storage_verification),
        ("MongoDB Storage Verification", test_mongodb_storage),
        ("ChromaDB Storage Verification", test_chromadb_storage),
        ("File Structure Generation", test_file_structure_generation),
        ("Search Functionality", test_search_functionality),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            logger.info("")
            result = test_func()
            results.append((test_name, result))
        except Exception as exc:
            logger.error("Test '%s' crashed: %s", test_name, exc, exc_info=True)
            results.append((test_name, False))
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("TEST SUMMARY")
    logger.info("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        logger.info("  %s: %s", test_name, status)
    
    logger.info("")
    logger.info("Total: %d/%d tests passed", passed, total)
    logger.info("=" * 70 + "\n")
    
    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)


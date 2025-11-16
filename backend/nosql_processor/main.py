"""
NoSQL processor module for MongoDB operations.

This module provides functions for processing NoSQL documents:
- Text extraction
- Chunking
- Metadata generation
- Collection inference
- MongoDB operations

All functions gracefully degrade if dependencies (keybert, pymongo) are unavailable.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Optional imports with graceful degradation
try:
    import pymongo
    from pymongo import MongoClient
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    logger.warning("pymongo not available - MongoDB operations will be disabled")

try:
    from keybert import KeyBERT
    HAS_KEYBERT = True
except ImportError:
    HAS_KEYBERT = False
    KeyBERT = None  # type: ignore
    logger.warning("keybert not available - keyword extraction will be simplified")


def get_nosql_db(mongo_uri: Optional[str] = None, mongo_db: Optional[str] = None) -> Any:
    """
    Get MongoDB database connection.
    
    Args:
        mongo_uri: MongoDB connection URI
        mongo_db: Database name
        
    Returns:
        MongoDB database instance or None if unavailable
    """
    if not HAS_PYMONGO:
        logger.warning("MongoDB unavailable - get_nosql_db returning None")
        return None
    
    if not mongo_uri or not mongo_db:
        logger.warning("MongoDB URI or database name not provided")
        return None
    
    try:
        client = MongoClient(mongo_uri)
        db = client[mongo_db]
        logger.info("MongoDB connection established to %s/%s", mongo_uri, mongo_db)
        return db
    except Exception as exc:
        logger.error("Failed to connect to MongoDB: %s", exc, exc_info=True)
        return None


def extract_full_text(file_path: str) -> str:
    """
    Extract full text from a file.
    
    Supports: txt, md, csv, json, xml, yaml, pdf, docx
    
    Args:
        file_path: Path to the file
        
    Returns:
        Extracted text content
    """
    path = Path(file_path)
    
    if not path.exists():
        logger.error("File not found for text extraction: %s", file_path)
        return ""
    
    file_ext = path.suffix.lower()
    logger.info("Extracting text from %s (extension: %s)", file_path, file_ext)
    
    try:
        # Simple text extraction for common formats
        if file_ext in {'.txt', '.md', '.csv', '.json', '.xml', '.yaml', '.yml', '.html', '.htm'}:
            logger.debug("Reading text file: %s", file_path)
            text = path.read_text(encoding='utf-8', errors='ignore')
            logger.info("Extracted %d characters from text file %s", len(text), file_path)
            return text
        
        # PDF extraction
        elif file_ext == '.pdf':
            logger.info("Attempting PDF text extraction from %s", file_path)
            try:
                import PyPDF2
                logger.debug("PyPDF2 available, extracting PDF text")
                with open(path, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    logger.debug("PDF has %d pages", len(pdf_reader.pages))
                    text = ""
                    for page_num, page in enumerate(pdf_reader.pages, 1):
                        try:
                            page_text = page.extract_text()
                            text += page_text + "\n"
                            logger.debug("Extracted text from page %d/%d (%d chars)", 
                                       page_num, len(pdf_reader.pages), len(page_text))
                        except Exception as page_exc:
                            logger.warning("Failed to extract text from page %d: %s", page_num, page_exc)
                    logger.info("PDF extraction complete: %d characters extracted from %d pages", 
                              len(text), len(pdf_reader.pages))
                    return text
            except ImportError:
                logger.error("PyPDF2 not available - cannot extract PDF text. Install with: pip install PyPDF2")
                return ""
            except Exception as pdf_exc:
                logger.error("Failed to extract PDF text from %s: %s", file_path, pdf_exc, exc_info=True)
                return ""
        
        # DOCX extraction
        elif file_ext in {'.docx', '.doc'}:
            logger.info("Attempting DOCX text extraction from %s", file_path)
            try:
                from docx import Document
                logger.debug("python-docx available, extracting DOCX text")
                doc = Document(path)
                text = '\n'.join([para.text for para in doc.paragraphs])
                logger.info("DOCX extraction complete: %d characters extracted", len(text))
                return text
            except ImportError:
                logger.error("python-docx not available - cannot extract DOCX text. Install with: pip install python-docx")
                return ""
            except Exception as docx_exc:
                logger.error("Failed to extract DOCX text from %s: %s", file_path, docx_exc, exc_info=True)
                return ""
        
        # Try reading as text anyway (for unknown text-like files)
        else:
            logger.debug("Unknown file type, attempting to read as text: %s", file_path)
            try:
                text = path.read_text(encoding='utf-8', errors='ignore')
                logger.info("Read %d characters as text from %s", len(text), file_path)
                return text
            except UnicodeDecodeError:
                logger.warning("File %s appears to be binary (cannot decode as UTF-8)", file_path)
                return ""
            except Exception as text_exc:
                logger.warning("Failed to read %s as text: %s", file_path, text_exc)
                return ""
                
    except Exception as exc:
        logger.error("Unexpected error extracting text from %s: %s", file_path, exc, exc_info=True)
        return ""


def simple_character_chunker(text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Simple character-based text chunker.
    
    Args:
        text: Text to chunk
        chunk_size: Maximum chunk size in characters
        overlap: Overlap size between chunks
        
    Returns:
        List of text chunks
    """
    if not text:
        return []
    
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)
        
        if end >= len(text):
            break
        
        start = end - overlap
    
    return chunks


def infer_collection(text: str) -> str:
    """
    Infer MongoDB collection name from text content.
    
    Args:
        text: Text content to analyze
        
    Returns:
        Collection name
    """
    if not text:
        return "general"
    
    text_lower = text.lower()
    
    # Simple keyword-based collection inference
    if any(word in text_lower for word in ['product', 'price', 'cost', 'inventory', 'stock']):
        return "products"
    elif any(word in text_lower for word in ['user', 'customer', 'client', 'person', 'employee']):
        return "users"
    elif any(word in text_lower for word in ['order', 'transaction', 'purchase', 'sale']):
        return "orders"
    elif any(word in text_lower for word in ['document', 'report', 'article', 'paper']):
        return "documents"
    elif any(word in text_lower for word in ['image', 'photo', 'picture', 'jpg', 'png']):
        return "media"
    elif any(word in text_lower for word in ['video', 'mp4', 'avi', 'mov']):
        return "media"
    elif any(word in text_lower for word in ['audio', 'sound', 'mp3', 'wav']):
        return "media"
    else:
        return "general"


def meta_generator(
    file_path: str,
    tenant_id: str,
    summary: str,
    collection: str,
    nosql_db: Any,
    storage_uri: str,
    extra: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Generate metadata document for MongoDB.
    
    Args:
        file_path: Original file path
        tenant_id: Tenant identifier
        summary: Text summary
        collection: Collection name
        nosql_db: MongoDB database instance
        storage_uri: Storage URI
        extra: Additional metadata
        
    Returns:
        Metadata document with _id
    """
    path = Path(file_path)
    
    # Extract descriptive text from extra metadata if available (from CLIP model or text summary)
    descriptive_text = ""
    if extra:
        # Priority: explicit descriptive_text > multimodal_extra text > summary
        descriptive_text = extra.get("descriptive_text") or extra.get("multimodal_extra", {}).get("text") or ""
    
    # If no descriptive text from CLIP, use summary as descriptive text
    if not descriptive_text:
        descriptive_text = summary
    
    # Use descriptive text if available, otherwise use summary
    display_text = descriptive_text if descriptive_text else summary
    
    meta_doc = {
        "original_name": path.name,
        "storage_uri": storage_uri,
        "tenant_id": tenant_id,
        "collection_hint": collection,
        "summary_preview": display_text[:500] if display_text else "",
        "descriptive_text": descriptive_text,  # Store full descriptive text for file structure
        "extension": path.suffix,
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "extra": extra or {},
    }
    
    # Check if MongoDB is available (use 'is not None' instead of truth value)
    if nosql_db is not None and hasattr(nosql_db, 'files'):
        try:
            result = nosql_db.files.insert_one(meta_doc)
            meta_doc["_id"] = str(result.inserted_id)
            logger.info("Metadata document inserted with ID: %s", meta_doc["_id"])
        except Exception as exc:
            logger.error("Failed to insert metadata document: %s", exc, exc_info=True)
            # Return document with a placeholder ID if MongoDB fails
            import uuid
            meta_doc["_id"] = str(uuid.uuid4())
    else:
        # Generate a placeholder ID if MongoDB is unavailable
        import uuid
        meta_doc["_id"] = str(uuid.uuid4())
        logger.warning("MongoDB unavailable - using placeholder ID: %s", meta_doc["_id"])
    
    return meta_doc


def chunk_generator(
    file_path: str,
    file_id: str,
    tenant_id: str,
    collection: str,
    nosql_db: Any,
    text_override: Optional[str] = None
) -> int:
    """
    Generate and store text chunks in MongoDB.
    
    Args:
        file_path: File path
        file_id: File document ID
        tenant_id: Tenant identifier
        collection: Collection name
        nosql_db: MongoDB database instance
        text_override: Optional text content (if None, extracts from file_path)
        
    Returns:
        Number of chunks created
    """
    # Extract or use provided text
    text = text_override if text_override is not None else extract_full_text(file_path)
    
    if not text:
        logger.warning("No text to chunk for %s", file_path)
        return 0
    
    # Generate chunks
    chunks = simple_character_chunker(text)
    chunk_count = len(chunks)
    
    if nosql_db is None or not hasattr(nosql_db, collection):
        logger.warning("MongoDB unavailable - chunks not stored for %s", file_path)
        return chunk_count
    
    try:
        chunk_collection = getattr(nosql_db, collection)
        chunk_docs = []
        
        for idx, chunk_text in enumerate(chunks):
            chunk_doc = {
                "file_id": file_id,
                "tenant_id": tenant_id,
                "chunk_index": idx,
                "text": chunk_text,
                "chunk_size": len(chunk_text),
            }
            chunk_docs.append(chunk_doc)
        
        if chunk_docs:
            chunk_collection.insert_many(chunk_docs)
            logger.info("Inserted %d chunks into collection %s", chunk_count, collection)
        
        return chunk_count
    except Exception as exc:
        logger.error("Failed to store chunks: %s", exc, exc_info=True)
        return chunk_count


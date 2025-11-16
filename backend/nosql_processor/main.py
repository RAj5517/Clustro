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
    
    Args:
        file_path: Path to the file
        
    Returns:
        Extracted text content
    """
    path = Path(file_path)
    
    if not path.exists():
        logger.warning("File not found for text extraction: %s", file_path)
        return ""
    
    try:
        # Simple text extraction for common formats
        if path.suffix.lower() in {'.txt', '.md', '.csv', '.json', '.xml', '.yaml', '.yml'}:
            return path.read_text(encoding='utf-8', errors='ignore')
        
        # Try reading as text anyway
        return path.read_text(encoding='utf-8', errors='ignore')
    except Exception as exc:
        logger.warning("Failed to extract text from %s: %s", file_path, exc)
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
    
    meta_doc = {
        "original_name": path.name,
        "storage_uri": storage_uri,
        "tenant_id": tenant_id,
        "collection_hint": collection,
        "summary_preview": summary[:500] if summary else "",
        "extension": path.suffix,
        "size_bytes": path.stat().st_size if path.exists() else 0,
        "extra": extra or {},
    }
    
    if nosql_db and hasattr(nosql_db, 'files'):
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
    
    if not nosql_db or not hasattr(nosql_db, collection):
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


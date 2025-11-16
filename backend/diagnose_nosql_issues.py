"""
Diagnostic script to check NoSQL processing configuration and identify issues.
"""

import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

print("=" * 70)
print("NOSQL PROCESSING DIAGNOSTIC")
print("=" * 70)

# 1. Check MongoDB configuration
print("\n1. MongoDB Configuration:")
print("-" * 70)
mongo_uri = os.getenv("MONGO_URI")
mongo_db = os.getenv("MONGO_DB")
print(f"  MONGO_URI: {'[OK] Set' if mongo_uri else '[MISSING] NOT SET'}")
print(f"  MONGO_DB: {'[OK] Set' if mongo_db else '[MISSING] NOT SET'}")
if mongo_uri:
    print(f"    Value: {mongo_uri}")
if mongo_db:
    print(f"    Value: {mongo_db}")

# Try to connect
try:
    from nosql_processor.main import get_nosql_db
    db = get_nosql_db()
    if db:
        print("  Connection: [OK] SUCCESS")
        # Count files
        files_count = db['files'].count_documents({})
        print(f"  Files in database: {files_count}")
    else:
        print("  Connection: [FAILED] (no credentials or connection error)")
except Exception as e:
    print(f"  Connection: [FAILED] ({e})")

# 2. Check ChromaDB
print("\n2. ChromaDB Configuration:")
print("-" * 70)
try:
    import chromadb
    print("  ChromaDB: [OK] INSTALLED")
    
    from nosql_ingestion_pipeline.config import load_config
    config = load_config()
    print(f"  ChromaDB Path: {config.chroma_path}")
    print(f"  Collection: {config.chroma_collection}")
    
    # Check if collection exists
    from nosql_ingestion_pipeline.graph_writer import GraphEmbeddingWriter
    writer = GraphEmbeddingWriter(
        persist_path=config.chroma_path,
        collection_name=config.chroma_collection
    )
    if writer.available:
        print("  ChromaDB: [OK] AVAILABLE")
    else:
        print(f"  ChromaDB: [FAILED] NOT AVAILABLE ({writer.last_error})")
except ImportError:
    print("  ChromaDB: [FAILED] NOT INSTALLED")
except Exception as e:
    print(f"  ChromaDB: [ERROR] ({e})")

# 3. Check CLIP Model
print("\n3. CLIP Model Configuration:")
print("-" * 70)
try:
    from CLIP_Model.multimodal_pipeline import MultiModalPipeline
    print("  CLIP Model: [OK] AVAILABLE")
    
    # Try to initialize
    try:
        pipeline = MultiModalPipeline(enable_audio=False)
        print("  CLIP Pipeline: [OK] INITIALIZED")
        
        # Check text encoder
        if hasattr(pipeline, 'clip') and hasattr(pipeline.clip, 'encode_text'):
            print("  CLIP Text Encoder: [OK] AVAILABLE")
        else:
            print("  CLIP Text Encoder: [FAILED] NOT AVAILABLE")
    except Exception as e:
        print(f"  CLIP Pipeline: [FAILED] FAILED TO INITIALIZE ({e})")
except ImportError as e:
    print(f"  CLIP Model: [FAILED] NOT AVAILABLE ({e})")
except Exception as e:
    print(f"  CLIP Model: [ERROR] ({e})")

# 4. Check NoSQL Pipeline initialization
print("\n4. NoSQL Pipeline:")
print("-" * 70)
try:
    from nosql_ingestion_pipeline.pipeline import NoSQLIngestionPipeline
    from classification.main import ClassificationProcessor
    
    processor = ClassificationProcessor()
    if processor.nosql_pipeline:
        print("  NoSQL Pipeline: [OK] INITIALIZED")
        
        # Check text encoder
        if processor.nosql_pipeline._text_encoder:
            print("  Text Encoder: [OK] AVAILABLE")
        else:
            print("  Text Encoder: [FAILED] NOT AVAILABLE (PDFs won't generate embeddings)")
        
        # Check MongoDB connection
        if processor.nosql_pipeline.nosql_db is not None:
            print("  MongoDB Connection: [OK] CONNECTED")
        else:
            print("  MongoDB Connection: [FAILED] NOT CONNECTED (PDFs won't be stored)")
        
        # Check ChromaDB
        if processor.nosql_pipeline.graph_writer.available:
            print("  ChromaDB Writer: [OK] AVAILABLE")
        else:
            print(f"  ChromaDB Writer: [FAILED] NOT AVAILABLE ({processor.nosql_pipeline.graph_writer.last_error})")
    else:
        print("  NoSQL Pipeline: [FAILED] NOT INITIALIZED")
except Exception as e:
    print(f"  NoSQL Pipeline: [ERROR] ({e})")

# 5. Check environment variables
print("\n5. Environment Variables:")
print("-" * 70)
env_vars = [
    "MONGO_URI",
    "MONGO_DB",
    "CHROMA_PERSIST_PATH",
    "CHROMA_NOSQL_COLLECTION",
    "LOCAL_ROOT_REPO"
]
for var in env_vars:
    value = os.getenv(var)
    if value:
        print(f"  {var}: [OK] Set")
    else:
        print(f"  {var}: [MISSING] NOT SET")

# 6. Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("\nFor PDFs to work, you need:")
print("  1. [OK] MongoDB configured (MONGO_URI and MONGO_DB)")
print("  2. [OK] ChromaDB installed and working")
print("  3. [OK] CLIP model with text encoder available")
print("\nFor semantic search to work, you need:")
print("  1. [OK] ChromaDB with stored embeddings")
print("  2. [OK] CLIP model for encoding search queries")
print("\n" + "=" * 70)


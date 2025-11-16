# Issues Found: PDF Processing and Semantic Search

## Root Causes Identified

### Issue 1: MongoDB Not Configured
**Problem**: PDFs are not being stored in MongoDB, so they don't appear in the file structure.

**Evidence**:
- Test shows: "MongoDB URI or database name not provided"
- `get_nosql_db()` returns `None` when `MONGO_URI` or `MONGO_DB` are not set
- PDFs go through `_process_text_file()` which requires MongoDB to store metadata

**Fix Required**:
1. Set environment variables:
   ```bash
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=your_database_name
   ```
2. Or add to `.env` file in `backend/` or `backend/sql/`:
   ```
   MONGO_URI=mongodb://localhost:27017
   MONGO_DB=clustro_text
   ```

### Issue 2: CLIP Text Encoder Not Initialized for PDFs
**Problem**: PDFs need CLIP text encoder to generate embeddings, but it might not be initialized.

**Evidence**:
- PDFs use `_embed_text()` which requires `self._text_encoder`
- `_text_encoder` is set in `__init__` via `_resolve_text_encoder(multimodal_pipeline)`
- If `multimodal_pipeline` is `None` or doesn't have `clip.encode_text`, embeddings fail

**Flow**:
- Images/Videos: Use `multimodal_pipeline.encode_path()` → gets embeddings directly
- PDFs: Use `_embed_text()` → needs `self._text_encoder` from CLIP

**Fix Required**:
- Ensure CLIP model is initialized and passed to `NoSQLIngestionPipeline`
- Check that `ClassificationProcessor` initializes CLIP and passes it to the pipeline

### Issue 3: ChromaDB Embeddings Not Being Written
**Problem**: Even if embeddings are generated, they might not be written to ChromaDB.

**Evidence**:
- `_write_embeddings()` checks `if not self.graph_writer.available: return []`
- If ChromaDB is not available, no embeddings are written
- PDFs need embeddings in ChromaDB for semantic search

**Fix Required**:
- Ensure ChromaDB is installed and working
- Check `CHROMA_PERSIST_PATH` environment variable
- Verify ChromaDB collection is created

## Complete Flow Analysis

### For Images/Videos (WORKING):
```
1. File uploaded
2. ClassificationProcessor detects as media
3. Routes to NoSQLIngestionPipeline._process_media_file()
4. CLIP model encodes file → gets embedding + descriptive text
5. Stores in MongoDB with descriptive_text
6. Stores embedding in ChromaDB
7. Appears in file structure (uses descriptive_text for folder)
8. Semantic search works (embeddings in ChromaDB)
```

### For PDFs (NOT WORKING):
```
1. File uploaded
2. ClassificationProcessor detects as non-media
3. Routes to NoSQLIngestionPipeline._process_text_file()
4. Extracts text from PDF
5. Builds summary
6. ❌ MongoDB: Fails if MONGO_URI/MONGO_DB not set
7. ❌ Embeddings: Fails if _text_encoder not initialized
8. ❌ ChromaDB: No embeddings written
9. ❌ File structure: Not in MongoDB, so doesn't appear
10. ❌ Semantic search: No embeddings to search
```

## Required Configuration

### Environment Variables Needed:

```bash
# MongoDB (REQUIRED for PDFs)
MONGO_URI=mongodb://localhost:27017
MONGO_DB=clustro_text

# ChromaDB (REQUIRED for semantic search)
CHROMA_PERSIST_PATH=./chroma_db
CHROMA_NOSQL_COLLECTION=nosql_graph_embeddings

# Storage (Optional - defaults to ../storage)
LOCAL_ROOT_REPO=../storage
```

### Check Current Configuration:

Run this Python script to check:
```python
import os
print("MONGO_URI:", os.getenv("MONGO_URI", "NOT SET"))
print("MONGO_DB:", os.getenv("MONGO_DB", "NOT SET"))
print("CHROMA_PERSIST_PATH:", os.getenv("CHROMA_PERSIST_PATH", "NOT SET"))
```

## Verification Steps

1. **Check MongoDB**:
   ```python
   from nosql_processor.main import get_nosql_db
   db = get_nosql_db()
   print("MongoDB:", "Connected" if db else "Not connected")
   ```

2. **Check ChromaDB**:
   ```python
   from nosql_ingestion_pipeline.graph_writer import GraphEmbeddingWriter
   writer = GraphEmbeddingWriter("./chroma_db", "nosql_graph_embeddings")
   print("ChromaDB:", "Available" if writer.available else f"Not available: {writer.last_error}")
   ```

3. **Check CLIP Text Encoder**:
   ```python
   from classification.main import ClassificationProcessor
   processor = ClassificationProcessor()
   if processor.nosql_pipeline:
       print("Text Encoder:", "Available" if processor.nosql_pipeline._text_encoder else "Not available")
   ```

## Quick Fixes

### Fix 1: Add MongoDB Configuration
Create or update `.env` file in `backend/` or `backend/sql/`:
```
MONGO_URI=mongodb://localhost:27017
MONGO_DB=clustro_text
```

### Fix 2: Verify CLIP is Initialized
Check `backend/classification/main.py` - ensure `multimodal_pipeline` is passed to `NoSQLIngestionPipeline`:
```python
self.nosql_pipeline = NoSQLIngestionPipeline(multimodal_pipeline=self.multimodal_pipeline)
```

### Fix 3: Ensure ChromaDB is Working
ChromaDB should be installed (we did this). Verify:
```bash
python -c "import chromadb; print('OK')"
```

## Expected Behavior After Fixes

1. **PDF Upload**:
   - Text extracted from PDF
   - Summary generated
   - Stored in MongoDB with `descriptive_text` = summary
   - Embeddings generated using CLIP text encoder
   - Embeddings stored in ChromaDB
   - File appears in file structure (semantic folder based on summary)

2. **Semantic Search**:
   - Query encoded using CLIP text encoder
   - Searches ChromaDB embeddings
   - Returns PDFs if content matches
   - Works for both PDFs and media files

## Summary

**Why Images/Videos Work**:
- ✅ CLIP model initialized and working
- ✅ Gets embeddings directly from CLIP
- ✅ Stores in MongoDB (if configured)
- ✅ Stores in ChromaDB (if configured)

**Why PDFs Don't Work**:
- ❌ MongoDB not configured → can't store metadata
- ❌ CLIP text encoder might not be initialized → can't generate embeddings
- ❌ No embeddings → can't search semantically
- ❌ Not in MongoDB → doesn't appear in file structure

**Solution**: Configure MongoDB and ensure CLIP text encoder is initialized.


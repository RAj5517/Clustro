# Classification System - Main Entry Point

This is the central processing hub for the Clustro backend that routes files to appropriate processing pipelines.

## Overview

The classification system:
1. **Receives** files/folders from the frontend
2. **Separates** media files from non-media files
3. **Classifies** non-media files as SQL or NoSQL
4. **Routes** files to appropriate processors:
   - SQL files → (disabled - schema generator removed)
   - NoSQL files → NoSQL processing (placeholder)
   - Media files → Multimodal Pipeline (placeholder)

## File Structure

```
Backend/
├── classification/
│   ├── main.py          # Main entry point (THIS FILE)
│   └── README.md        # This documentation
(SQL pipeline removed - legacy Schema_generator module no longer included)
│   └── file_classifier.py
├── CLIP_Model/          # Media processing
│   └── multimodal_pipeline.py
└── file_classifier.py   # SQL/NoSQL classifier
```

## Usage

### As a Module (Flask/FastAPI)

```python
from classification.main import process_upload_request

# In your Flask/FastAPI route
@app.route('/api/upload', methods=['POST'])
def upload():
    files = request.files.getlist('files')
    metadata = request.form.get('metadata')
    
    result = process_upload_request(files, metadata)
    return jsonify(result)
```

### Command Line

```bash
# Process a single file
python classification/main.py path/to/file.json

# Process a folder
python classification/main.py path/to/folder/

# With metadata
python classification/main.py path/to/file.json --metadata "Test data"
```

## Processing Flow

```
┌─────────────────┐
│  Frontend       │
│  (Files/Folder) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────┐
│  Classification Processor   │
│  (main.py)                  │
└────────┬────────────────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌────────┐ ┌────────┐
│ Media  │ │ Non-   │
│ Files  │ │ Media  │
└───┬────┘ └───┬────┘
    │          │
    │          ▼
    │    ┌──────────────┐
    │    │ File         │
    │    │ Classifier   │
    │    └───┬──────┬───┘
    │        │      │
    │        ▼      ▼
    │    ┌─────┐ ┌──────┐
    │    │ SQL │ │NoSQL │
    │    └──┬──┘ └──┬───┘
    │       │       │
    │       ▼       │
    │  ┌──────────┐│
    │  │ Schema   ││
    │  │ Generator││
    │  └────┬─────┘│
    │       │      │
    │       ▼      │
    │  ┌──────────┐│
    │  │ Schema   ││
    │  │ Executor ││
    │  └──────────┘│
    │              │
    ▼              ▼
┌─────────────┐ ┌──────────┐
│ Multimodal  │ │ NoSQL    │
│ Pipeline    │ │ Process  │
│ (Placeholder)│ │ (Placeholder)│
└─────────────┘ └──────────┘
```

## File Classification

### Media Files
Automatically detected by extension:
- **Images**: `.jpg`, `.jpeg`, `.png`, `.gif`, `.bmp`, `.webp`, `.svg`, etc.
- **Videos**: `.mp4`, `.avi`, `.mov`, `.wmv`, `.flv`, `.webm`, `.mkv`, etc.
- **Audio**: `.mp3`, `.wav`, `.flac`, `.aac`, `.ogg`, `.wma`, etc.

### Non-Media Files
Processed through `FileClassifier` which determines:
- **SQL**: Structured, tabular, consistent data
- **NoSQL**: Nested, flexible, document-like data

## Response Format

```json
{
  "total_files": 10,
  "media_files": 3,
  "non_media_files": 7,
  "metadata": "Optional metadata string",
  "non_media_results": [
    {
      "file": "/path/to/file.json",
      "file_name": "file.json",
      "classification": "SQL",
      "sql_score": 12,
      "nosql_score": 3,
      "reasons": ["Flat structure", "Consistent schema"],
      "sql_result": {
        "schema_generation": {...},
        "tables": ["users", "orders"],
        "jobs_created": 2,
        "execution": {...}
      }
    }
  ],
  "media_results": [
    {
      "file": "/path/to/image.jpg",
      "file_name": "image.jpg",
      "file_type": "image",
      "status": "pending",
      "result": {
        "message": "Media processing placeholder"
      }
    }
  ],
  "errors": []
}
```

## Placeholders

### NoSQL Processing
Currently returns a placeholder message. Future implementation should:
- Store documents in MongoDB/DynamoDB/Couchbase
- Index content for search
- Handle schema-less data structures

### Media Processing
Currently initialized but not fully implemented. Future implementation should:
- Generate CLIP embeddings for images/videos
- Extract audio transcripts
- Create captions for images
- Store embeddings in vector database

## Configuration

SQL schema processing has been removed, so no database configuration is required.

## Dependencies

- `file_classifier.py` - SQL/NoSQL classification
- (Removed) SQL schema generation and execution
- `CLIP_Model/multimodal_pipeline.py` - Media processing (optional)

See `requirements.txt` in the Backend directory for all dependencies.
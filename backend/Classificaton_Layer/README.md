# Classification Layer

This layer receives file uploads from the frontend, classifies them into two categories, and routes them to appropriate processing pipelines.

## Overview

The Classification Layer acts as the main entry point for file uploads. It:

1. **Receives uploads** from the frontend via `/api/upload` endpoint
2. **Handles nested folder structures** (folder -> folder -> folder -> files)
3. **Classifies files** into two categories:
   - **Media files** (images, audio, video) → sent to CLIP_Model for processing
   - **Text files** (all other structured/unstructured text) → placeholder (destination TBD)

## File Classification

### Media Files (→ CLIP_Model)
- **Images**: `.jpg`, `.jpeg`, `.png`, `.bmp`, `.gif`, `.webp`
- **Videos**: `.mp4`, `.mov`, `.avi`, `.mkv`, `.webm`
- **Audio**: `.mp3`, `.wav`, `.flac`, `.ogg`, `.m4a`

### Text Files (→ TBD)
- **Structured**: `.json`, `.csv`, `.xlsx`, `.xls`, `.xml`, `.html`, `.htm`, `.yaml`, `.yml`
- **Documents**: `.pdf`, `.docx`, `.doc`
- **Text**: `.txt`, `.md`, `.log`
- **Config**: `.ini`, `.cfg`, `.conf`
- **Unknown files**: Treated as text files

## API Endpoints

### POST `/api/upload`
Upload files and folders (any file type).

**Request:**
- `multipart/form-data`
- `files`: Array of File objects (required)
- `metadata`: Optional metadata/comments

**Response:**
```json
{
  "success": true,
  "message": "Files uploaded and processed successfully",
  "databaseState": {
    "tables": [],
    "collections": [],
    "mediaDirectories": []
  }
}
```

### GET `/api/database/state`
Get current database state.

**Response:**
```json
{
  "tables": [],
  "collections": [],
  "mediaDirectories": []
}
```

### GET `/health`
Health check endpoint.

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install CLIP_Model dependencies (if not already installed):
```bash
cd ../CLIP_Model
# Follow instructions in CLIP_Model/requirements.txt
```

## Running the Server

```bash
python app.py
```

The server will start on `http://localhost:8000`

## Project Structure

```
Classificaton_Layer/
├── app.py                 # Main Flask server
├── file_classifier.py     # File classification logic
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Integration with CLIP_Model

The Classification Layer automatically imports and uses the CLIP_Model from `../CLIP_Model/` (sibling directory in backend/). If CLIP_Model is not available, media files will still be categorized but not processed.

## Text Files Processing

Currently, text files are categorized and stored in the database state but not processed. The destination for text file processing will be specified later.

## Notes

- Files are temporarily stored during processing
- Folder structure is preserved when uploading nested folders
- The server uses in-memory storage for database state (use a real database in production)
- CORS is enabled to allow frontend requests


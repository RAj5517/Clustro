# Classification Layer ↔ CLIP Model Integration Guide

This document explains how the Classification Layer sends classified media files to the CLIP Model for processing.

## Overview

The Classification Layer classifies uploaded files into two categories:
1. **Media files** (images, audio, video) → Sent to CLIP_Model for processing
2. **Text files** → Placeholder (destination TBD)

## Architecture

```
Frontend → Classification_Layer → CLIP_Model Service → MultiModalPipeline
```

## Files Created/Modified

### New Files in CLIP_Model

1. **`clip_service.py`** - Service wrapper for CLIP processing
   - Provides clean API for batch processing
   - Handles errors gracefully
   - Groups results by modality

2. **`README_SERVICE.md`** - Documentation for CLIP Service

### Modified Files

1. **`backend/Classificaton_Layer/app.py`** - Updated to use CLIPService
   - Imports CLIPService instead of directly using MultiModalPipeline
   - Falls back to direct pipeline if service not available
   - Uses batch processing for better performance
   - Updated import paths to reflect new location in backend/

## How It Works

### 1. File Upload Flow

```
User uploads files → Frontend sends to /api/upload
→ Classification_Layer receives files
→ Files are classified (media vs text)
→ Media files are sent to CLIPService
→ CLIPService processes through MultiModalPipeline
→ Results returned to Classification_Layer
→ Database state updated and returned to Frontend
```

### 2. CLIP Service Integration

The Classification Layer uses the CLIPService in the following way:

```python
# In backend/Classificaton_Layer/app.py
from clip_service import CLIPService, get_clip_service

# Initialize service
clip_service = get_clip_service(enable_audio=True)

# Process media files
results = clip_service.process_files(file_paths)
```

### 3. Processing Results

Each processed file returns:

```python
{
    'success': True,
    'path': 'path/to/file.jpg',
    'modality': 'image',
    'text': 'A caption describing the image',
    'embedding': [0.123, -0.456, ...],  # CLIP embedding vector
    'embedding_dim': 512,
    'extra': {
        'width': 1920,
        'height': 1080
    }
}
```

## Supported File Types

### Media Files (→ CLIP_Model)

**Images:**
- `.jpg`, `.jpeg`, `.png`, `.bmp`, `.gif`, `.webp`

**Videos:**
- `.mp4`, `.mov`, `.avi`, `.mkv`, `.webm`

**Audio:**
- `.mp3`, `.wav`, `.flac`, `.ogg`, `.m4a`

### Text Files (→ TBD)

- `.json`, `.csv`, `.xlsx`, `.xml`, `.html`
- `.txt`, `.md`, `.log`
- `.pdf`, `.docx`, `.doc`
- `.yaml`, `.yml`
- `.ini`, `.cfg`, `.conf`

## Error Handling

The integration handles errors at multiple levels:

1. **Import Errors**: Falls back to direct pipeline if service unavailable
2. **Processing Errors**: Individual file failures don't stop batch processing
3. **File Errors**: Invalid or corrupted files return error status

## Usage Example

### From Classification Layer

```python
# Files are automatically sent to CLIP_Model when classified as media
media_files = ['image1.jpg', 'video1.mp4', 'audio1.mp3']

# Classification Layer processes them
results = process_media_files(media_files)

# Results include:
# - Modality detection
# - Captions/transcripts
# - CLIP embeddings
# - Metadata
```

### Direct CLIP Service Usage

```python
from clip_service import CLIPService

service = CLIPService(enable_audio=True)

# Process single file
result = service.process_file("image.jpg")

# Process multiple files
results = service.process_files([
    "image1.jpg",
    "video1.mp4",
    "audio1.mp3"
])

# Group by modality
grouped = service.process_media_files(file_paths)
print(f"Images: {len(grouped['images'])}")
print(f"Videos: {len(grouped['videos'])}")
print(f"Audio: {len(grouped['audio'])}")
```

## Configuration

### CLIP Service Options

```python
CLIPService(
    enable_audio=True,              # Enable Whisper for audio
    audio_model_name="small",       # Whisper model size
    clip_model_name="ViT-B-32",     # CLIP model
    clip_pretrained="openai",       # Pretrained weights
    max_frames_per_video=None,      # Max frames per video
    frames_per_second_factor=0.3    # Frame sampling rate
)
```

## Performance Considerations

1. **Batch Processing**: More efficient than processing files individually
2. **Vector Spaces**: Embeddings are stored in memory for quick access
3. **Video Processing**: May take longer depending on video length
4. **Model Loading**: Models are loaded once and reused

## Testing

To test the integration:

1. Start Classification Layer server:
   ```bash
   cd backend/Classificaton_Layer
   python app.py
   ```

2. Upload files via frontend or API:
   ```bash
   curl -X POST http://localhost:8000/api/upload \
     -F "files=@image.jpg" \
     -F "files=@video.mp4"
   ```

3. Check results in response or database state:
   ```bash
   curl http://localhost:8000/api/database/state
   ```

## Troubleshooting

### CLIP_Model not available

If you see "CLIP_Model not available":
1. Check that CLIP_Model dependencies are installed
2. Verify the import path is correct
3. Check for any initialization errors

### Processing fails

If file processing fails:
1. Check file format is supported
2. Verify file is not corrupted
3. Check error messages in response
4. Ensure sufficient memory/disk space

## Next Steps

- [ ] Text file processing destination (TBD)
- [ ] Persistent storage for embeddings
- [ ] Database integration for results
- [ ] API endpoints for querying embeddings
- [ ] Search functionality using embeddings


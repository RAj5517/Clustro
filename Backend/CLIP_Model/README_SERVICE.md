# CLIP Model Service

This document describes the CLIP Service interface for receiving and processing files from the Classification Layer.

## Overview

The `clip_service.py` module provides a clean service interface for processing media files through the CLIP pipeline. It's designed to be used by the Classification Layer to send classified media files for processing.

## Service Interface

### CLIPService Class

The main service class that wraps the MultiModalPipeline and provides batch processing capabilities.

#### Initialization

```python
from clip_service import CLIPService

service = CLIPService(
    enable_audio=True,
    audio_model_name="small",
    clip_model_name="ViT-B-32",
    clip_pretrained="openai",
    caption_model_name="Salesforce/blip-image-captioning-base",
    max_frames_per_video=None,
    frames_per_second_factor=0.3,
)
```

#### Methods

##### `process_file(file_path: str) -> Dict[str, Any]`

Process a single file through CLIP pipeline.

**Returns:**
```python
{
    'success': bool,
    'path': str,
    'modality': str,  # 'image', 'video', 'audio', 'text'
    'text': str or None,  # Caption/transcript/summary
    'embedding': list or None,  # CLIP embedding vector
    'embedding_dim': int,
    'extra': dict,  # Additional metadata
    'error': str or None
}
```

##### `process_files(file_paths: List[str]) -> List[Dict[str, Any]]`

Process multiple files in batch.

**Returns:** List of result dictionaries (one per file)

##### `process_media_files(file_paths: List[str]) -> Dict[str, List[Dict]]`

Process files and group results by modality.

**Returns:**
```python
{
    'images': [results...],
    'videos': [results...],
    'audio': [results...],
    'text': [results...],
    'errors': [results...]
}
```

##### `get_vector_spaces_info() -> Dict[str, int]`

Get counts of stored embeddings in each vector space.

##### `get_embeddings(modality: Optional[str] = None) -> Dict[str, List]`

Get stored embeddings, optionally filtered by modality.

### Convenience Functions

#### `get_clip_service(**kwargs) -> CLIPService`

Get or create a global CLIP service instance (singleton pattern).

#### `process_files_with_clip(file_paths: List[str], **kwargs) -> List[Dict]`

Convenience function to process files without manually creating a service.

## Usage Examples

### Basic Usage

```python
from clip_service import CLIPService

# Initialize service
service = CLIPService(enable_audio=True)

# Process a single file
result = service.process_file("path/to/image.jpg")
if result['success']:
    print(f"Modality: {result['modality']}")
    print(f"Caption: {result['text']}")
    print(f"Embedding dim: {result['embedding_dim']}")

# Process multiple files
results = service.process_files([
    "image1.jpg",
    "video1.mp4",
    "audio1.mp3"
])
```

### Using Global Service

```python
from clip_service import get_clip_service

# Get global service instance
service = get_clip_service(enable_audio=True)

# Process files
results = service.process_files(file_paths)
```

### Grouped Processing

```python
from clip_service import CLIPService

service = CLIPService()
grouped = service.process_media_files(file_paths)

print(f"Processed {len(grouped['images'])} images")
print(f"Processed {len(grouped['videos'])} videos")
print(f"Processed {len(grouped['audio'])} audio files")
print(f"Errors: {len(grouped['errors'])}")
```

## Integration with Classification Layer

The Classification Layer automatically uses this service when available. The integration works as follows:

1. Classification Layer classifies files into media and text
2. Media files are sent to CLIPService for processing
3. CLIPService processes files and returns results
4. Results are included in the database state response

## Supported File Types

### Images
- `.jpg`, `.jpeg`, `.png`, `.bmp`, `.gif`, `.webp`

### Videos
- `.mp4`, `.mov`, `.avi`, `.mkv`, `.webm`

### Audio
- `.mp3`, `.wav`, `.flac`, `.ogg`, `.m4a`

### Text
- `.txt`, `.md`, `.log`, `.json`, `.csv`, `.pdf`, `.docx`, etc.

## Error Handling

The service handles errors gracefully:

- If a file cannot be processed, it returns `success: False` with an error message
- Processing continues for other files even if one fails
- Error details are included in the result dictionary

## Performance Notes

- The service maintains vector spaces in memory for processed files
- Batch processing is more efficient than processing files one by one
- Video processing may take longer depending on video length and frame sampling rate

## Dependencies

See `requirements.txt` for full dependencies. Key requirements:
- torch, torchvision
- open_clip_torch
- transformers
- openai-whisper (for audio)
- opencv-python (for video)
- PIL/Pillow (for images)


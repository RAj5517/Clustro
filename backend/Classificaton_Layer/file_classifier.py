"""
File Classifier for Classification Layer

This module classifies files into two categories:
1. Media files (images, audio, video) -> to be sent to CLIP_Model
2. Text files (all other structured/unstructured text) -> to be sent elsewhere (TBD)
"""

import mimetypes
from pathlib import Path
from typing import Dict, List, Tuple, Optional


class FileClassifier:
    """Classifies files into media (CLIP_Model) or text (other processing)."""
    
    def __init__(self):
        # Media file extensions (images, audio, video)
        self.image_exts = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
        self.video_exts = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
        self.audio_exts = {".mp3", ".wav", ".flac", ".ogg", ".m4a"}
        
        # Text-like / structured types → treat as "text"
        self.text_exts = {
            ".json", ".csv", ".xlsx", ".xls",
            ".xml", ".html", ".htm",
            ".txt", ".md", ".log",
            ".yaml", ".yml",
            ".ini", ".cfg", ".conf",
            ".pdf", ".docx", ".doc",
        }
    
    def classify_file(self, file_path: str) -> Dict[str, str]:
        """
        Classify a single file into media or text category.
        
        Args:
            file_path: Path to the file to classify
            
        Returns:
            Dictionary with:
            {
                'category': 'media' or 'text',
                'type': 'image', 'video', 'audio', or 'text',
                'extension': file extension
            }
        """
        ext = Path(file_path).suffix.lower()
        mime, _ = mimetypes.guess_type(file_path)
        
        # Check by MIME type first
        if mime:
            if mime.startswith("image/"):
                return {
                    'category': 'media',
                    'type': 'image',
                    'extension': ext
                }
            if mime.startswith("video/"):
                return {
                    'category': 'media',
                    'type': 'video',
                    'extension': ext
                }
            if mime.startswith("audio/"):
                return {
                    'category': 'media',
                    'type': 'audio',
                    'extension': ext
                }
        
        # Check by extension
        if ext in self.image_exts:
            return {
                'category': 'media',
                'type': 'image',
                'extension': ext
            }
        if ext in self.video_exts:
            return {
                'category': 'media',
                'type': 'video',
                'extension': ext
            }
        if ext in self.audio_exts:
            return {
                'category': 'media',
                'type': 'audio',
                'extension': ext
            }
        if ext in self.text_exts:
            return {
                'category': 'text',
                'type': 'text',
                'extension': ext
            }
        
        # Default: unknown files are treated as text
        return {
            'category': 'text',
            'type': 'text',
            'extension': ext
        }
    
    def classify_files(self, file_paths: List[str]) -> Dict[str, List[Dict]]:
        """
        Classify multiple files and group them by category.
        
        Args:
            file_paths: List of file paths to classify
            
        Returns:
            Dictionary with:
            {
                'media': [{'path': str, 'type': str, 'extension': str}, ...],
                'text': [{'path': str, 'type': str, 'extension': str}, ...]
            }
        """
        media_files = []
        text_files = []
        
        for file_path in file_paths:
            classification = self.classify_file(file_path)
            
            file_info = {
                'path': file_path,
                'type': classification['type'],
                'extension': classification['extension']
            }
            
            if classification['category'] == 'media':
                media_files.append(file_info)
            else:
                text_files.append(file_info)
        
        return {
            'media': media_files,
            'text': text_files
        }


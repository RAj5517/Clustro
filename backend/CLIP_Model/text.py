# text.py
"""
Text Backend for loading and summarizing text files.
"""

import logging
from pathlib import Path
from typing import Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)


class TextBackend:
    """
    Backend for loading and summarizing text files.
    Handles various text file formats and generates summaries.
    """
    
    def __init__(self):
        """Initialize text backend."""
        pass
    
    def load_and_summarise(self, path: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        Load text file and generate summary.
        
        Args:
            path: Path to text file
            
        Returns:
            Tuple of (summary, full_text, metadata_dict)
        """
        file_path = Path(path)
        
        if not file_path.exists():
            logger.warning(f"Text file not found: {path}")
            return f"File: {file_path.name}", "", {"error": "File not found"}
        
        try:
            # Read file content
            text = self._read_file(file_path)
            
            if not text:
                return f"File: {file_path.name}", "", {"error": "Empty file"}
            
            # Generate summary (first few sentences or first 200 chars)
            summary = self._generate_summary(text)
            
            metadata = {
                "file_size": len(text),
                "file_name": file_path.name,
                "file_ext": file_path.suffix,
            }
            
            return summary, text, metadata
            
        except Exception as e:
            logger.error(f"Failed to load text file {path}: {e}")
            return f"File: {file_path.name} (error)", "", {"error": str(e)}
    
    def _read_file(self, file_path: Path) -> str:
        """Read file content based on extension."""
        ext = file_path.suffix.lower()
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for encoding in encodings:
            try:
                return file_path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"Failed to read {file_path} with {encoding}: {e}")
                continue
        
        # If all encodings fail, read as binary and decode with errors='ignore'
        try:
            return file_path.read_bytes().decode('utf-8', errors='ignore')
        except Exception as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return ""
    
    def _generate_summary(self, text: str, max_length: int = 200) -> str:
        """
        Generate summary from text.
        
        Args:
            text: Full text content
            max_length: Maximum summary length
            
        Returns:
            Summary string
        """
        if not text:
            return ""
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # If text is short, return as-is
        if len(text) <= max_length:
            return text
        
        # Try to find sentence boundaries
        sentences = self._split_sentences(text)
        
        if sentences:
            # Take first few sentences that fit in max_length
            summary_parts = []
            current_length = 0
            
            for sentence in sentences:
                if current_length + len(sentence) > max_length:
                    break
                summary_parts.append(sentence)
                current_length += len(sentence) + 1  # +1 for space
            
            if summary_parts:
                return ' '.join(summary_parts)
        
        # Fallback: just take first max_length characters
        return text[:max_length].rstrip() + "..."
    
    def _split_sentences(self, text: str) -> list:
        """Split text into sentences."""
        import re
        # Simple sentence splitting on common punctuation
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        return [s.strip() for s in sentences if s.strip()]

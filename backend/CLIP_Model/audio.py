# audio.py
"""
Audio Backend for transcribing audio files using Whisper or similar models.
"""

import logging
from pathlib import Path
from typing import Tuple, Dict, Any, Optional

logger = logging.getLogger(__name__)

try:
    import whisper
    HAS_WHISPER = True
except ImportError:
    HAS_WHISPER = False
    logger.warning("whisper not available, audio transcription will be disabled")


class AudioBackend:
    """
    Backend for transcribing audio files.
    Uses OpenAI Whisper by default.
    """
    
    def __init__(self, model_name: str = "small"):
        """
        Initialize audio backend.
        
        Args:
            model_name: Whisper model size ("tiny", "base", "small", "medium", "large")
        """
        self.model_name = model_name
        self.model = None
        
        self._load_model()
    
    def _load_model(self):
        """Load Whisper model."""
        if HAS_WHISPER:
            try:
                self.model = whisper.load_model(self.model_name)
                logger.info(f"Loaded Whisper model: {self.model_name}")
            except Exception as e:
                logger.warning(f"Failed to load Whisper model {self.model_name}: {e}")
                self.model = None
        else:
            logger.warning("Whisper not available, audio transcription disabled")
    
    def transcribe(self, audio_path: str) -> Tuple[str, Dict[str, Any]]:
        """
        Transcribe audio file to text.
        
        Args:
            audio_path: Path to audio file
            
        Returns:
            Tuple of (transcription_text, metadata_dict)
        """
        if self.model is None:
            # Fallback: return filename-based description
            path = Path(audio_path)
            return f"Audio file: {path.name}", {"error": "Whisper not available"}
        
        try:
            path = Path(audio_path)
            if not path.exists():
                raise FileNotFoundError(f"Audio file not found: {audio_path}")
            
            # Transcribe audio
            result = self.model.transcribe(str(audio_path))
            
            text = result.get("text", "").strip()
            metadata = {
                "language": result.get("language"),
                "duration": result.get("segments", [{}])[0].get("end", 0) if result.get("segments") else None,
            }
            
            return text, metadata
        except Exception as e:
            logger.error(f"Audio transcription failed for {audio_path}: {e}")
            path = Path(audio_path)
            return f"Audio file: {path.name} (transcription failed)", {"error": str(e)}

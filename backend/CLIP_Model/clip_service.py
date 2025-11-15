"""
CLIP Model Service

This service provides an interface for processing media files through the CLIP pipeline.
It can be used by the Classification_Layer to send files for processing.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
import traceback

from multimodal_pipeline import MultiModalPipeline


class CLIPService:
    """
    Service wrapper for CLIP Model processing.
    Handles batch processing of media files and provides clean API.
    """
    
    def __init__(
        self,
        enable_audio: bool = True,
        audio_model_name: str = "small",
        clip_model_name: str = "ViT-B-32",
        clip_pretrained: str = "openai",
        caption_model_name: str = "Salesforce/blip-image-captioning-base",
        max_frames_per_video: Optional[int] = None,
        frames_per_second_factor: float = 0.3,
    ):
        """
        Initialize CLIP Service.
        
        Args:
            enable_audio: Enable audio processing (Whisper)
            audio_model_name: Whisper model size
            clip_model_name: CLIP model name
            clip_pretrained: CLIP pretrained weights
            caption_model_name: Image captioning model
            max_frames_per_video: Max frames to process per video
            frames_per_second_factor: Frames per second sampling rate
        """
        self.pipeline = MultiModalPipeline(
            enable_audio=enable_audio,
            audio_model_name=audio_model_name,
            clip_model_name=clip_model_name,
            clip_pretrained=clip_pretrained,
            caption_model_name=caption_model_name,
            max_frames_per_video=max_frames_per_video,
            frames_per_second_factor=frames_per_second_factor,
        )
    
    def process_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single file through CLIP pipeline.
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            Dictionary with processing results:
            {
                'success': bool,
                'path': str,
                'modality': str,
                'text': str or None,
                'embedding': list or None,
                'embedding_dim': int,
                'extra': dict,
                'error': str or None
            }
        """
        try:
            result = self.pipeline.encode_path(file_path)
            
            embedding = result.get('embedding')
            embedding_dim = len(embedding) if embedding else 0
            
            return {
                'success': True,
                'path': result['path'],
                'modality': result['modality'],
                'text': result.get('text'),
                'embedding': embedding,
                'embedding_dim': embedding_dim,
                'extra': result.get('extra', {}),
                'error': None
            }
        except Exception as e:
            return {
                'success': False,
                'path': file_path,
                'modality': None,
                'text': None,
                'embedding': None,
                'embedding_dim': 0,
                'extra': {},
                'error': str(e),
                'traceback': traceback.format_exc()
            }
    
    def process_files(self, file_paths: List[str]) -> List[Dict[str, Any]]:
        """
        Process multiple files through CLIP pipeline.
        
        Args:
            file_paths: List of file paths to process
            
        Returns:
            List of processing results (one dict per file)
        """
        results = []
        total = len(file_paths)
        
        for idx, file_path in enumerate(file_paths, 1):
            print(f"Processing file {idx}/{total}: {Path(file_path).name}")
            result = self.process_file(file_path)
            results.append(result)
            
            if result['success']:
                print(f"  ✓ Success: {result['modality']} - {result.get('text', 'N/A')[:50]}")
            else:
                print(f"  ✗ Error: {result.get('error', 'Unknown error')}")
        
        return results
    
    def process_media_files(self, file_paths: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process media files and group results by modality.
        
        Args:
            file_paths: List of file paths to process
            
        Returns:
            Dictionary with results grouped by modality:
            {
                'images': [results...],
                'videos': [results...],
                'audio': [results...],
                'text': [results...],
                'errors': [results...]
            }
        """
        results = self.process_files(file_paths)
        
        grouped = {
            'images': [],
            'videos': [],
            'audio': [],
            'text': [],
            'errors': []
        }
        
        for result in results:
            if not result['success']:
                grouped['errors'].append(result)
            else:
                modality = result.get('modality', 'unknown')
                if modality == 'image':
                    grouped['images'].append(result)
                elif modality == 'video':
                    grouped['videos'].append(result)
                elif modality == 'audio':
                    grouped['audio'].append(result)
                elif modality == 'text':
                    grouped['text'].append(result)
                else:
                    grouped['errors'].append(result)
        
        return grouped
    
    def get_vector_spaces_info(self) -> Dict[str, int]:
        """
        Get information about the vector spaces (how many embeddings are stored).
        
        Returns:
            Dictionary with counts for each vector space
        """
        return {
            'image_space': len(self.pipeline.image_space),
            'video_space': len(self.pipeline.video_space),
            'audio_space': len(self.pipeline.audio_space),
            'text_space': len(self.pipeline.text_space),
        }
    
    def get_embeddings(self, modality: Optional[str] = None) -> Dict[str, List]:
        """
        Get stored embeddings.
        
        Args:
            modality: Optional filter by modality ('image', 'video', 'audio', 'text')
            
        Returns:
            Dictionary mapping file paths to embeddings
        """
        if modality == 'image':
            return {path: emb.tolist() for path, emb in self.pipeline.image_space.items()}
        elif modality == 'video':
            return {path: emb.tolist() for path, emb in self.pipeline.video_space.items()}
        elif modality == 'audio':
            return {path: emb.tolist() for path, emb in self.pipeline.audio_space.items()}
        elif modality == 'text':
            return {path: emb.tolist() for path, emb in self.pipeline.text_space.items()}
        else:
            return {
                'images': {path: emb.tolist() for path, emb in self.pipeline.image_space.items()},
                'videos': {path: emb.tolist() for path, emb in self.pipeline.video_space.items()},
                'audio': {path: emb.tolist() for path, emb in self.pipeline.audio_space.items()},
                'text': {path: emb.tolist() for path, emb in self.pipeline.text_space.items()},
            }


# Global service instance (can be initialized once and reused)
_clip_service: Optional[CLIPService] = None


def get_clip_service(
    enable_audio: bool = True,
    **kwargs
) -> CLIPService:
    """
    Get or create global CLIP service instance.
    
    Args:
        enable_audio: Enable audio processing
        **kwargs: Additional arguments for CLIPService
        
    Returns:
        CLIPService instance
    """
    global _clip_service
    
    if _clip_service is None:
        _clip_service = CLIPService(enable_audio=enable_audio, **kwargs)
    
    return _clip_service


def process_files_with_clip(file_paths: List[str], **service_kwargs) -> List[Dict[str, Any]]:
    """
    Convenience function to process files with CLIP.
    
    Args:
        file_paths: List of file paths to process
        **service_kwargs: Arguments for CLIPService initialization
        
    Returns:
        List of processing results
    """
    service = get_clip_service(**service_kwargs)
    return service.process_files(file_paths)


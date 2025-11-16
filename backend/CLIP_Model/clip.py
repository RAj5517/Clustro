# clip.py
"""
CLIP Backend for encoding images and text using CLIP models.
"""

import torch
import torch.nn.functional as F
from PIL import Image
from typing import Optional
import logging

logger = logging.getLogger(__name__)

try:
    import open_clip
    HAS_OPEN_CLIP = True
except ImportError:
    HAS_OPEN_CLIP = False
    logger.warning("open_clip not available, CLIP functionality will be limited")

try:
    import clip as openai_clip
    HAS_OPENAI_CLIP = True
except ImportError:
    HAS_OPENAI_CLIP = False
    logger.warning("openai clip not available")


class CLIPBackend:
    """
    CLIP backend for encoding images and text.
    Supports both open_clip and openai clip libraries.
    """
    
    def __init__(self, model_name: str = "ViT-B-32", pretrained: str = "openai"):
        """
        Initialize CLIP backend.
        
        Args:
            model_name: Model architecture (e.g., "ViT-B-32", "RN101")
            pretrained: Pretrained weights source (e.g., "openai")
        """
        self.model_name = model_name
        self.pretrained = pretrained
        self.model = None
        self.preprocess_fn = None
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        
        self._load_model()
    
    def _load_model(self):
        """Load CLIP model and preprocessing function."""
        try:
            if HAS_OPEN_CLIP:
                # Try open_clip first (more flexible)
                try:
                    self.model, _, self.preprocess_fn = open_clip.create_model_and_transforms(
                        self.model_name,
                        pretrained=self.pretrained,
                        device=self.device
                    )
                    self.model.eval()
                    logger.info(f"Loaded CLIP model {self.model_name} using open_clip")
                    return
                except Exception as e:
                    logger.warning(f"Failed to load with open_clip: {e}, trying openai clip")
            
            if HAS_OPENAI_CLIP:
                # Fallback to openai clip
                self.model, self.preprocess_fn = openai_clip.load(
                    f"{self.model_name}/{self.pretrained}",
                    device=self.device
                )
                logger.info(f"Loaded CLIP model {self.model_name} using openai clip")
                return
            
            raise ImportError("Neither open_clip nor openai clip available")
            
        except Exception as e:
            logger.error(f"Failed to load CLIP model: {e}")
            raise
    
    def preprocess(self, image: Image.Image):
        """
        Preprocess image for CLIP encoding.
        
        Args:
            image: PIL Image
            
        Returns:
            Preprocessed tensor
        """
        if self.preprocess_fn is None:
            raise RuntimeError("CLIP model not loaded")
        return self.preprocess_fn(image)
    
    @torch.no_grad()
    def encode_image_tensor(self, image_tensor: torch.Tensor) -> torch.Tensor:
        """
        Encode image tensor to embedding.
        
        Args:
            image_tensor: Preprocessed image tensor [batch_size, channels, height, width]
            
        Returns:
            Image embedding tensor [batch_size, embedding_dim]
        """
        if self.model is None:
            raise RuntimeError("CLIP model not loaded")
        
        image_tensor = image_tensor.to(self.device)
        
        # Get image features
        if hasattr(self.model, 'encode_image'):
            # open_clip style
            image_features = self.model.encode_image(image_tensor)
        elif hasattr(self.model, 'visual'):
            # openai clip style
            image_features = self.model.visual(image_tensor)
        else:
            raise RuntimeError("Unknown CLIP model structure")
        
        # Normalize features
        image_features = F.normalize(image_features, dim=-1)
        
        return image_features.squeeze(0)  # Remove batch dimension if batch_size=1
    
    @torch.no_grad()
    def encode_text(self, text: str) -> torch.Tensor:
        """
        Encode text to embedding.
        
        Args:
            text: Text string
            
        Returns:
            Text embedding tensor [embedding_dim]
        """
        if self.model is None:
            raise RuntimeError("CLIP model not loaded")
        
        # Tokenize text
        if hasattr(self.model, 'tokenize'):
            # open_clip style
            text_tokens = self.model.tokenize([text]).to(self.device)
            text_features = self.model.encode_text(text_tokens)
        elif hasattr(openai_clip, 'tokenize'):
            # openai clip style
            text_tokens = openai_clip.tokenize([text]).to(self.device)
            text_features = self.model.encode_text(text_tokens)
        else:
            raise RuntimeError("Unable to tokenize text")
        
        # Normalize features
        text_features = F.normalize(text_features, dim=-1)
        
        return text_features.squeeze(0)  # Remove batch dimension


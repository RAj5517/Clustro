# caption.py
"""
Caption Backend for generating image captions using BLIP or similar models.
"""

import logging
from PIL import Image
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from transformers import BlipProcessor, BlipForConditionalGeneration
    HAS_BLIP = True
except ImportError:
    HAS_BLIP = False
    logger.warning("transformers or BLIP not available, captioning will use fallback")


class CaptionBackend:
    """
    Backend for generating image captions.
    Uses BLIP model by default, falls back to simple description if unavailable.
    """
    
    def __init__(self, model_name: str = "Salesforce/blip-image-captioning-base"):
        """
        Initialize caption backend.
        
        Args:
            model_name: HuggingFace model name for captioning
        """
        self.model_name = model_name
        self.processor = None
        self.model = None
        
        self._load_model()
    
    def _load_model(self):
        """Load captioning model."""
        if HAS_BLIP:
            try:
                self.processor = BlipProcessor.from_pretrained(self.model_name)
                self.model = BlipForConditionalGeneration.from_pretrained(self.model_name)
                logger.info(f"Loaded caption model: {self.model_name}")
            except Exception as e:
                logger.warning(f"Failed to load BLIP model {self.model_name}: {e}")
                self.processor = None
                self.model = None
        else:
            logger.warning("BLIP not available, using fallback captioning")
    
    def caption_image(self, image: Image.Image) -> str:
        """
        Generate caption for image.
        
        Args:
            image: PIL Image (should be RGB)
            
        Returns:
            Caption string
        """
        if self.model is None or self.processor is None:
            # Fallback: return simple description
            return self._fallback_caption(image)
        
        try:
            # Ensure image is RGB
            if image.mode != "RGB":
                image = image.convert("RGB")
            
            # Generate caption
            inputs = self.processor(image, return_tensors="pt")
            out = self.model.generate(**inputs, max_length=50)
            caption = self.processor.decode(out[0], skip_special_tokens=True)
            
            return caption.strip()
        except Exception as e:
            logger.warning(f"Caption generation failed: {e}, using fallback")
            return self._fallback_caption(image)
    
    def _fallback_caption(self, image: Image.Image) -> str:
        """Fallback caption when model is unavailable."""
        width, height = image.size
        return f"An image with dimensions {width}x{height} pixels"

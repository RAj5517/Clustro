# caption_backend.py
from typing import Optional

import torch
from PIL import Image
from transformers import BlipProcessor, BlipForConditionalGeneration

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


class CaptionBackend:
    def __init__(self, model_name: str = "Salesforce/blip-image-captioning-base"):
        self.device = DEVICE
        self.processor = BlipProcessor.from_pretrained(model_name)
        self.model = BlipForConditionalGeneration.from_pretrained(model_name).to(self.device)
        self.model.eval()

    @torch.no_grad()
    def caption_image(self, img: Image.Image, max_length: int = 30) -> str:
        inputs = self.processor(images=img, return_tensors="pt").to(self.device)
        out = self.model.generate(**inputs, max_length=max_length)
        caption = self.processor.decode(out[0], skip_special_tokens=True)
        return caption.strip()

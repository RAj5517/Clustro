# audio.py
from typing import Tuple, Dict, Any
import torch

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


class AudioBackend:
    def __init__(self, model_name: str = "small"):
        self.device = DEVICE
        self.model_name = model_name
        self.model = None  # lazy-loaded

    def _ensure_loaded(self):
        if self.model is None:
            import whisper
            self.model = whisper.load_model(self.model_name, device=self.device)

    @torch.no_grad()
    def transcribe(self, audio_path: str) -> Tuple[str, Dict[str, Any]]:
        self._ensure_loaded()

        result = self.model.transcribe(audio_path, fp16=(self.device == "cuda"))

        # 1) Try the global text field
        text = (result.get("text") or "").strip()

        # 2) If empty, try to build text from segments
        if not text and "segments" in result:
            segments_text = [
                (seg.get("text") or "").strip()
                for seg in result["segments"]
                if seg.get("text")
            ]
            text = " ".join(segments_text).strip()

        # 3) If still empty, fall back to placeholder instead of failing
        if not text:
            text = "(no speech detected)"

        meta = {
            "language": result.get("language"),
            "segments": len(result.get("segments", [])),
        }
        return text, meta

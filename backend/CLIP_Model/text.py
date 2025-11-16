# text.py
"""
Lightweight text backend used by the multimodal pipeline.

The original repository only shipped the compiled artefact for this module,
so we provide a simple, dependency-friendly implementation that can extract
text from a few common formats and generate short summaries for CLIP.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Tuple

try:
    from PyPDF2 import PdfReader  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    PdfReader = None

try:
    from docx import Document  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Document = None

logger = logging.getLogger(__name__)


class TextBackend:
    """
    Minimal text loader that mirrors the API expected by MultiModalPipeline.
    """

    def __init__(self, summary_chars: int = 800, embed_chars: int = 2048):
        self.summary_chars = summary_chars
        self.embed_chars = embed_chars

    def load_and_summarise(self, path: str) -> Tuple[str, str, Dict[str, int]]:
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"TextBackend could not find file: {path}")

        raw_text = self._extract_text(file_path)
        normalized = " ".join(raw_text.split())

        if not normalized:
            summary = ""
            embed_text = ""
        else:
            summary = normalized[: self.summary_chars]
            embed_text = normalized[: self.embed_chars]

        meta = {
            "num_chars": len(normalized),
            "summary_chars": len(summary),
        }

        return summary, embed_text, meta

    def _extract_text(self, path: Path) -> str:
        ext = path.suffix.lower()

        if ext == ".pdf" and PdfReader:
            try:
                reader = PdfReader(str(path))
                return "\n".join(page.extract_text() or "" for page in reader.pages)
            except Exception as exc:  # pragma: no cover - best effort
                logger.warning("Failed to parse PDF %s: %s", path, exc)

        if ext == ".docx" and Document:
            try:
                doc = Document(str(path))
                return "\n".join(p.text for p in doc.paragraphs)
            except Exception as exc:  # pragma: no cover - best effort
                logger.warning("Failed to parse DOCX %s: %s", path, exc)

        if ext in {".json", ".yaml", ".yml"}:
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
                json.loads(text)  # validates json but we keep raw for embeddings
                return text
            except Exception:
                return path.read_text(encoding="utf-8", errors="ignore")

        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            # Final fallback to binary read when encoding is unknown
            data = path.read_bytes()
            return data.decode("utf-8", errors="ignore")


__all__ = ["TextBackend"]

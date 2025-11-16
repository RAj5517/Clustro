from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .config import load_config
from .graph_writer import GraphEmbeddingWriter

try:  # pragma: no cover - optional dependency
    from CLIP_Model.multimodal_pipeline import MultiModalPipeline
except Exception as exc:  # pragma: no cover - optional import
    MultiModalPipeline = None  # type: ignore
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

logger = logging.getLogger(__name__)


class SemanticSearchEngine:
    """
    Lightweight semantic search helper that embeds user text with CLIP and
    queries the graph embedding store for the nearest nodes (files/chunks).
    """

    def __init__(self):
        self.config = load_config()
        self.graph_writer = GraphEmbeddingWriter(
            persist_path=self.config.chroma_path,
            collection_name=self.config.chroma_collection,
        )
        self._pipeline: Optional[Any] = None
        self._ready = False
        self._initialize_pipeline()

    @property
    def available(self) -> bool:
        return self._ready and self.graph_writer.available

    def _initialize_pipeline(self) -> None:
        if MultiModalPipeline is None:
            logger.warning("CLIP multimodal pipeline unavailable: %s", _IMPORT_ERROR)
            self._ready = False
            return

        try:
            # Audio is not required for query embedding
            self._pipeline = MultiModalPipeline(enable_audio=False)
            self._ready = True
            logger.info("Semantic search pipeline initialised")
        except Exception as exc:  # pragma: no cover - GPU/runtime failures
            logger.warning("Failed to initialise semantic search pipeline: %s", exc)
            self._pipeline = None
            self._ready = False

    def _encode_query(self, text: str) -> Optional[List[float]]:
        if not text:
            return None

        if not self._ready or not self._pipeline:
            self._initialize_pipeline()

        if not self._pipeline:
            return None

        try:
            tensor = self._pipeline.clip.encode_text(text)
            if hasattr(tensor, "tolist"):
                return tensor.tolist()
            if isinstance(tensor, list):
                return tensor
            return list(tensor)
        except Exception as exc:  # pragma: no cover - encoding failures
            logger.warning("Failed to encode semantic query: %s", exc)
            return None

    def search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not query or not self.graph_writer.available:
            return []

        embedding = self._encode_query(query)
        if not embedding:
            return []

        results = self.graph_writer.query_similar(embedding, limit=limit)
        enriched_results: List[Dict[str, Any]] = []
        for result in results:
            metadata = result.get("metadata") or {}
            text = result.get("text", "")
            enriched_results.append(
                {
                    "id": result.get("id"),
                    "similarity": result.get("similarity"),
                    "distance": result.get("distance"),
                    "text": text,
                    "metadata": metadata,
                    "modality": metadata.get("modality") or metadata.get("type"),
                    "collection": metadata.get("collection"),
                    "path": metadata.get("path"),
                    "file_id": metadata.get("file_id"),
                }
            )
        return enriched_results

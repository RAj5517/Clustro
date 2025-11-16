from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import chromadb  # type: ignore
except Exception:  # pragma: no cover - dependency optional in tests
    chromadb = None


class GraphEmbeddingWriter:
    """
    Handles the persistence of file + chunk embeddings into ChromaDB.
    """

    def __init__(self, persist_path: str, collection_name: str):
        self.persist_path = persist_path
        self.collection_name = collection_name
        self._collection = None
        self._error: Optional[str] = None

        if chromadb is None:
            self._error = "chromadb is not installed"
            logger.warning("Chromadb not available, graph embeddings disabled.")
            return

        try:
            client = chromadb.PersistentClient(path=persist_path)
            self._collection = client.get_or_create_collection(name=collection_name)
        except Exception as exc:  # pragma: no cover - disk / driver errors
            self._error = str(exc)
            logger.warning("Unable to initialise ChromaDB collection: %s", exc)

    @property
    def available(self) -> bool:
        return self._collection is not None

    @property
    def last_error(self) -> Optional[str]:
        return self._error

    def upsert_nodes(self, nodes: List[Dict[str, Any]]) -> List[str]:
        if not self.available or not nodes:
            return []

        ids: List[str] = []
        embeddings: List[List[float]] = []
        documents: List[str] = []
        metadatas: List[Dict[str, Any]] = []

        for node in nodes:
            embedding = node.get("embedding")
            node_id = node.get("id")
            if embedding is None or node_id is None:
                continue

            ids.append(node_id)
            embeddings.append(embedding)
            documents.append(node.get("text") or "")
            metadatas.append(self._sanitize_metadata(node.get("metadata") or {}))

        if not ids:
            return []

        self._collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=documents,
            metadatas=metadatas,
        )
        return ids

    def _sanitize_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        sanitized: Dict[str, Any] = {}
        for key, value in metadata.items():
            if isinstance(value, (str, int, float, bool)) or value is None:
                sanitized[key] = value
            else:
                try:
                    sanitized[key] = json.dumps(value)
                except Exception:
                    sanitized[key] = str(value)
        return sanitized

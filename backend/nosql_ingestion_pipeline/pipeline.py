from __future__ import annotations

import logging
import mimetypes
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from nosql_processor.main import (
    chunk_generator,
    extract_full_text,
    get_nosql_db,
    infer_collection,
    meta_generator,
    simple_character_chunker,
)

from .config import NoSQLPipelineConfig, load_config
from .graph_writer import GraphEmbeddingWriter
from .path_resolver import LocalPathPlanner, PathPlan

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NoSQLProcessingResult:
    status: str
    file_id: Optional[str] = None
    collection: Optional[str] = None
    chunk_count: int = 0
    modality: str = "text"
    storage_plan: Optional[dict] = None
    graph_nodes: List[str] = field(default_factory=list)
    mongo_collections: Dict[str, str] = field(default_factory=dict)
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "file_id": self.file_id,
            "collection": self.collection,
            "chunk_count": self.chunk_count,
            "modality": self.modality,
            "storage_plan": self.storage_plan,
            "graph_nodes": self.graph_nodes,
            "mongo_collections": self.mongo_collections,
            "error": self.error,
            "metadata": self.metadata,
        }


class NoSQLIngestionPipeline:
    """
    Coordinates the ingestion of NoSQL-friendly files into MongoDB and ChromaDB.
    """

    def __init__(
        self,
        config: Optional[NoSQLPipelineConfig] = None,
        multimodal_pipeline: Any = None,
    ):
        self.config = config or load_config()
        self.nosql_db = get_nosql_db(self.config.mongo_uri, self.config.mongo_db)
        self.graph_writer = GraphEmbeddingWriter(
            persist_path=self.config.chroma_path,
            collection_name=self.config.chroma_collection,
        )
        self.path_planner = LocalPathPlanner(
            enabled=self.config.local_path_enabled,
            move_files=self.config.local_path_move_files,
            root=self.config.local_path_root,
        )
        self.multimodal_pipeline = multimodal_pipeline
        self._text_encoder: Optional[Callable[[str], Any]] = self._resolve_text_encoder(multimodal_pipeline)
        self._storage_root = self._compute_storage_root()

    def process_file(
        self,
        file_path: Path,
        classification_result: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        modality_hint: Optional[str] = None,
    ) -> Dict[str, Any]:
        path = Path(file_path)
        classification_result = classification_result or {}
        metadata = metadata or {}

        if not path.exists():
            raise FileNotFoundError(f"NoSQL pipeline cannot access file: {path}")

        modality = modality_hint or classification_result.get("modality") or self._detect_modality(path)
        tenant_id = metadata.get("tenant_id") or self.config.default_tenant_id

        logger.info(
            "NoSQL pipeline: processing %s (modality=%s, tenant=%s)",
            path,
            modality,
            tenant_id
        )

        extra_payload = {
            "classification": classification_result,
            "upload_metadata": metadata,
        }

        if modality in {"image", "video", "audio"}:
            logger.info("NoSQL pipeline: treating %s as media file", path)
            result = self._process_media_file(path, modality, tenant_id, extra_payload)
        else:
            logger.info("NoSQL pipeline: treating %s as text file", path)
            result = self._process_text_file(path, tenant_id, extra_payload)

        logger.info("NoSQL pipeline: completed %s with status %s", path, result.status)
        return result.to_dict()

    # ------------------------------------------------------------------ helpers

    def _process_text_file(self, path: Path, tenant_id: str, extra_payload: Dict[str, Any]) -> NoSQLProcessingResult:
        logger.info("Text pipeline: extracting text from %s", path)
        result = NoSQLProcessingResult(status="pending", modality="text", metadata=extra_payload)
        raw_text = extract_full_text(str(path))
        summary = self._build_summary(raw_text, path)
        collection = infer_collection(summary or raw_text)
        plan = self._resolve_path_plan(summary, path)
        logger.info(
            "Text pipeline: summary length=%d, collection=%s, plan=%s",
            len(summary or ""),
            collection,
            plan.payload.get("moved_to") if plan else None
        )

        storage_uri = self._resolve_storage_uri(plan, path)

        try:
            meta_doc = meta_generator(
                file_path=str(path),
                tenant_id=tenant_id,
                summary=summary,
                collection=collection,
                nosql_db=self.nosql_db,
                storage_uri=storage_uri,
                extra=extra_payload,
            )

            chunk_texts = simple_character_chunker(raw_text)
            chunk_count = chunk_generator(
                file_path=str(path),
                file_id=meta_doc["_id"],
                tenant_id=tenant_id,
                collection=collection,
                nosql_db=self.nosql_db,
                text_override=raw_text,
            )
            logger.info("Text pipeline: generated %d chunks for %s", chunk_count, path)

            graph_nodes = self._write_embeddings(
                file_id=meta_doc["_id"],
                summary=summary,
                chunk_texts=chunk_texts,
                modality="text",
                collection=collection,
                file_path=storage_uri,
            )

            result.status = "completed"
            result.file_id = meta_doc["_id"]
            result.collection = collection
            result.chunk_count = chunk_count
            result.storage_plan = plan.payload if plan else None
            result.graph_nodes = graph_nodes
            result.mongo_collections = {"files": "files", "chunks": collection}
            logger.info(
                "Text pipeline: stored file_id=%s chunks=%d graph_nodes=%d",
                meta_doc["_id"],
                chunk_count,
                len(graph_nodes)
            )
        except Exception as exc:  # pragma: no cover - db failures
            logger.exception("Failed to process text file %s", path)
            result.status = "error"
            result.error = str(exc)

        return result

    def _process_media_file(
        self,
        path: Path,
        modality: str,
        tenant_id: str,
        extra_payload: Dict[str, Any],
    ) -> NoSQLProcessingResult:
        logger.info("Media pipeline: processing %s (%s)", path, modality)
        result = NoSQLProcessingResult(status="pending", modality=modality, metadata=extra_payload)

        if not self.multimodal_pipeline:
            logger.warning("Media pipeline: CLIP unavailable, storing %s with fallback", path)
            return self._fallback_media_ingest(path, modality, tenant_id, extra_payload)

        try:
            info = self.multimodal_pipeline.encode_path(str(path))
            logger.info("Media pipeline: embedding generated for %s", path)
        except Exception as exc:  # pragma: no cover - model runtime errors
            logger.exception("Failed to encode media file %s", path)
            result.status = "error"
            result.error = str(exc)
            return result

        summary = info.get("text") or path.name
        collection = infer_collection(summary)
        plan = self._resolve_path_plan(summary, path)

        storage_uri = self._resolve_storage_uri(plan, path)

        try:
            meta_doc = meta_generator(
                file_path=str(path),
                tenant_id=tenant_id,
                summary=summary,
                collection=collection,
                nosql_db=self.nosql_db,
                storage_uri=storage_uri,
                extra={**extra_payload, "modality": modality, "multimodal_extra": info.get("extra")},
            )

            chunk_texts = simple_character_chunker(summary)
            chunk_count = chunk_generator(
                file_path=storage_uri,
                file_id=meta_doc["_id"],
                tenant_id=tenant_id,
                collection=collection,
                nosql_db=self.nosql_db,
                text_override=summary,
            )

            file_embedding = info.get("embedding")
            graph_nodes = self._write_embeddings(
                file_id=meta_doc["_id"],
                summary=summary,
                chunk_texts=chunk_texts,
                modality=modality,
                collection=collection,
                file_path=str(path),
                file_embedding=file_embedding,
            )

            result.status = "completed"
            result.file_id = meta_doc["_id"]
            result.collection = collection
            result.chunk_count = chunk_count
            result.storage_plan = plan.payload if plan else None
            result.graph_nodes = graph_nodes
            result.mongo_collections = {"files": "files", "chunks": collection}
            logger.info(
                "Media pipeline: stored file_id=%s chunks=%d graph_nodes=%d",
                meta_doc["_id"],
                chunk_count,
                len(graph_nodes)
            )
        except Exception as exc:  # pragma: no cover - db failures
            logger.exception("Failed to persist media file %s", path)
            result.status = "error"
            result.error = str(exc)

        return result

    def _fallback_media_ingest(
        self,
        path: Path,
        modality: str,
        tenant_id: str,
        extra_payload: Dict[str, Any],
    ) -> NoSQLProcessingResult:
        """Persist media metadata even when the CLIP pipeline is unavailable."""
        result = NoSQLProcessingResult(status="pending", modality=modality, metadata=extra_payload)
        summary = f"{modality.title()} file {path.name}"
        collection = "media_assets"
        plan = self._resolve_path_plan(summary, path)
        storage_uri = self._resolve_storage_uri(plan, path)

        try:
            meta_doc = meta_generator(
                file_path=str(path),
                tenant_id=tenant_id,
                summary=summary,
                collection=collection,
                nosql_db=self.nosql_db,
                storage_uri=storage_uri,
                extra={**extra_payload, "modality": modality, "clip_status": "unavailable"},
            )

            chunk_texts = simple_character_chunker(summary)
            chunk_count = chunk_generator(
                file_path=str(path),
                file_id=meta_doc["_id"],
                tenant_id=tenant_id,
                collection=collection,
                nosql_db=self.nosql_db,
                text_override=summary,
            )

            graph_nodes = self._write_embeddings(
                file_id=meta_doc["_id"],
                summary=summary,
                chunk_texts=chunk_texts,
                modality=modality,
                collection=collection,
                file_path=storage_uri,
            )

            result.status = "completed"
            result.file_id = meta_doc["_id"]
            result.collection = collection
            result.chunk_count = chunk_count
            result.storage_plan = plan.payload if plan else None
            result.graph_nodes = graph_nodes
            result.mongo_collections = {"files": "files", "chunks": collection}
            logger.info("Fallback media ingest stored %s as %s", path, collection)
        except Exception as exc:
            logger.exception("Fallback media ingest failed for %s", path)
            result.status = "error"
            result.error = str(exc)

        return result

    # ------------------------------------------------------------------ utilities

    def _resolve_text_encoder(self, multimodal_pipeline: Any) -> Optional[Callable[[str], Any]]:
        if multimodal_pipeline and hasattr(multimodal_pipeline, "clip"):
            clip_backend = getattr(multimodal_pipeline, "clip")
            if hasattr(clip_backend, "encode_text"):
                return clip_backend.encode_text
        return None

    def _detect_modality(self, path: Path) -> str:
        mime, _ = mimetypes.guess_type(path.name)
        if mime:
            if mime.startswith("image/"):
                return "image"
            if mime.startswith("video/"):
                return "video"
            if mime.startswith("audio/"):
                return "audio"
        return "text"

    def _build_summary(self, text: str, path: Path, max_sentences: int = 5) -> str:
        if not text:
            return path.name
        sentences = re.split(r"(?<=[.!?])\s+", text.strip())
        return " ".join(sentences[:max_sentences])

    def _resolve_path_plan(self, description: str, path: Path) -> Optional[PathPlan]:
        if not self.path_planner or not self.path_planner.enabled:
            logger.debug("Path planner disabled; skipping storage plan for %s", path)
            return None
        plan_description = description or path.name
        logger.info("Requesting storage plan for '%s' (%s)", plan_description, path)
        plan = self.path_planner.plan(plan_description, path, move_file=True)
        if plan:
            logger.info("Storage plan resolved for %s -> %s", path, plan.payload.get("moved_to") or plan.path)
        else:
            logger.warning("Storage plan not generated for %s", path)
        return plan

    def _write_embeddings(
        self,
        *,
        file_id: str,
        summary: str,
        chunk_texts: List[str],
        modality: str,
        collection: str,
        file_path: str,
        file_embedding: Optional[List[float]] = None,
    ) -> List[str]:
        if not self.graph_writer.available:
            return []

        nodes: List[Dict[str, Any]] = []

        if file_embedding is None:
            file_embedding = self._embed_text(summary)
        elif hasattr(file_embedding, "tolist"):
            file_embedding = list(file_embedding)

        if file_embedding is not None:
            nodes.append(
                {
                    "id": f"{file_id}:file",
                    "embedding": file_embedding,
                    "text": summary,
                    "metadata": {
                        "file_id": file_id,
                        "type": "file",
                        "modality": modality,
                        "collection": collection,
                        "path": file_path,
                    },
                }
            )

        for idx, chunk_text in enumerate(chunk_texts):
            emb = self._embed_text(chunk_text)
            if emb is None:
                continue
            nodes.append(
                {
                    "id": f"{file_id}:chunk:{idx}",
                    "embedding": emb,
                    "text": chunk_text,
                    "metadata": {
                        "file_id": file_id,
                        "type": "chunk",
                        "chunk_index": idx,
                        "modality": modality,
                        "collection": collection,
                        "path": file_path,
                    },
                }
            )

        return self.graph_writer.upsert_nodes(nodes)

    def _embed_text(self, text: str) -> Optional[List[float]]:
        if not text or not self._text_encoder:
            return None

        try:
            tensor = self._text_encoder(text)
            if hasattr(tensor, "tolist"):
                return tensor.tolist()
            if isinstance(tensor, list):
                return tensor
            return list(tensor)
        except Exception:  # pragma: no cover - GPU/CLIP failures
            logger.warning("Failed to encode text for embeddings", exc_info=True)
            return None

    def _compute_storage_root(self) -> Optional[Path]:
        root = self.config.local_path_root or os.getenv("LOCAL_ROOT_REPO")
        if not root:
            return None
        try:
            path = Path(root).expanduser().resolve()
            if path.exists():
                return path
        except Exception:
            logger.warning("Unable to resolve storage root %s", root, exc_info=True)
        return None

    def _resolve_storage_uri(self, plan: Optional[PathPlan], original_path: Path) -> str:
        candidate: Optional[str] = None
        if plan:
            payload = getattr(plan, "payload", {}) or {}
            moved_to = payload.get("moved_to")
            if moved_to:
                candidate = str(moved_to)
            elif plan.path:
                candidate = str(plan.path)

        if not candidate:
            candidate = str(original_path)

        candidate_path = Path(candidate)
        try:
            if candidate_path.is_absolute():
                if self._storage_root:
                    try:
                        relative = candidate_path.resolve().relative_to(self._storage_root)
                        return relative.as_posix()
                    except Exception:
                        return candidate_path.as_posix()
                return candidate_path.as_posix()
            return candidate_path.as_posix()
        except Exception:
            logger.warning("Failed to normalise storage uri for %s", candidate, exc_info=True)
            return candidate

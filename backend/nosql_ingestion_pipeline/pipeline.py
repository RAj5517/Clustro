from __future__ import annotations

import logging
import mimetypes
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

try:
    from nosql_processor.main import (
        chunk_generator,
        extract_full_text,
        get_nosql_db,
        infer_collection,
        meta_generator,
        simple_character_chunker,
    )
    HAS_NOSQL_PROCESSOR = True
except ImportError as exc:
    HAS_NOSQL_PROCESSOR = False
    logger.warning("nosql_processor.main not available: %s", exc)
    # Define stub functions for graceful degradation
    def get_nosql_db(*args, **kwargs):
        return None
    def extract_full_text(path: str) -> str:
        try:
            return Path(path).read_text(encoding='utf-8', errors='ignore')
        except Exception:
            return ""
    def simple_character_chunker(text: str, chunk_size: int = 1000, overlap: int = 200):
        if not text:
            return []
        if len(text) <= chunk_size:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            if end >= len(text):
                break
            start = end - overlap
        return chunks
    def infer_collection(text: str) -> str:
        return "general"
    def meta_generator(*args, **kwargs):
        import uuid
        return {"_id": str(uuid.uuid4())}
    def chunk_generator(*args, **kwargs):
        return 0

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
        if not HAS_NOSQL_PROCESSOR:
            logger.warning("NoSQL processor unavailable - MongoDB operations will be disabled")
        self.nosql_db = get_nosql_db(self.config.mongo_uri, self.config.mongo_db) if HAS_NOSQL_PROCESSOR else None
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
        """Process text files including PDFs and documents."""
        logger.info("Text pipeline: starting processing for %s (tenant=%s)", path, tenant_id)
        result = NoSQLProcessingResult(status="pending", modality="text", metadata=extra_payload)
        
        try:
            # Extract text content
            logger.debug("Step 1: Extracting text from %s", path)
            raw_text = extract_full_text(str(path))
            
            # Check if extraction returned any content
            if raw_text is None:
                error_msg = f"Text extraction failed for {path.name}. Extraction returned None."
                logger.error(error_msg)
                result.status = "error"
                result.error = error_msg
                return result
            
            # Allow minimal text (even whitespace) as valid extraction
            # Empty PDFs may extract minimal whitespace characters
            text_stripped = raw_text.strip()
            if not text_stripped:
                logger.warning("Minimal or no text extracted from %s (extracted %d chars including whitespace). "
                             "This may be an empty PDF or image-based PDF.", path.name, len(raw_text))
                # For PDFs, we still proceed but with minimal content
                # Use filename as fallback content
                raw_text = f"PDF document: {path.name}"
                logger.info("Using filename as fallback content for empty PDF")
            
            logger.info("Text extraction successful: %d characters extracted from %s", len(raw_text), path.name)
            
            # Build summary
            logger.debug("Step 2: Building summary for %s", path)
            summary = self._build_summary(raw_text, path)
            logger.debug("Summary built: %d characters", len(summary))
            
            # Infer collection
            logger.debug("Step 3: Inferring collection for %s", path)
            collection = infer_collection(summary or raw_text)
            logger.info("Collection inferred: %s for %s", collection, path.name)
            
            # Resolve storage path
            logger.debug("Step 4: Resolving storage path for %s", path)
            plan = self._resolve_path_plan(summary, path)
            
            # Copy file to storage directory
            logger.debug("Step 4a: Copying file to storage directory")
            stored_path = self._copy_file_to_storage(path, "text", collection)
            if stored_path:
                logger.info("File copied to storage: %s -> %s", path.name, stored_path)
                storage_uri = str(stored_path) if isinstance(stored_path, Path) else stored_path
            else:
                # Fallback to path plan or original path
                storage_uri = self._resolve_storage_uri(plan, path)
                logger.warning("File copy failed, using original path: %s", storage_uri)
            
            logger.info("Storage URI resolved: %s -> %s", path, storage_uri)
            
            # Generate metadata document
            logger.debug("Step 5: Generating metadata document for %s", path)
            try:
                # Store summary as descriptive text for text files (for file structure organization)
                meta_doc = meta_generator(
                    file_path=str(path),
                    tenant_id=tenant_id,
                    summary=summary,
                    collection=collection,
                    nosql_db=self.nosql_db,
                    storage_uri=storage_uri,
                    extra={
                        **extra_payload,
                        "descriptive_text": summary,  # Store summary as descriptive text for file structure
                        "modality": "text"
                    },
                )
                logger.info("Metadata document created: file_id=%s", meta_doc.get("_id"))
            except Exception as meta_exc:
                logger.error("Failed to generate metadata document for %s: %s", path, meta_exc, exc_info=True)
                result.status = "error"
                result.error = f"Metadata generation failed: {str(meta_exc)}"
                return result
            
            # Generate chunks
            logger.debug("Step 6: Generating chunks for %s", path)
            try:
                chunk_texts = simple_character_chunker(raw_text)
                chunk_count = chunk_generator(
                    file_path=str(path),
                    file_id=meta_doc["_id"],
                    tenant_id=tenant_id,
                    collection=collection,
                    nosql_db=self.nosql_db,
                    text_override=raw_text,
                )
                logger.info("Chunks generated: %d chunks (%d expected) for %s", 
                          chunk_count, len(chunk_texts), path.name)
            except Exception as chunk_exc:
                logger.error("Failed to generate chunks for %s: %s", path, chunk_exc, exc_info=True)
                # Continue even if chunking fails - metadata is still created
                chunk_count = 0
                chunk_texts = []
            
            # Write embeddings
            logger.debug("Step 7: Writing embeddings for %s", path)
            try:
                graph_nodes = self._write_embeddings(
                    file_id=meta_doc["_id"],
                    summary=summary,
                    chunk_texts=chunk_texts,
                    modality="text",
                    collection=collection,
                    file_path=storage_uri,
                )
                logger.info("Embeddings written: %d graph nodes for %s", len(graph_nodes), path.name)
            except Exception as embed_exc:
                logger.error("Failed to write embeddings for %s: %s", path, embed_exc, exc_info=True)
                # Continue even if embeddings fail - metadata and chunks are still created
                graph_nodes = []
            
            # Mark as completed
            result.status = "completed"
            result.file_id = meta_doc["_id"]
            result.collection = collection
            result.chunk_count = chunk_count
            result.storage_plan = plan.payload if plan else None
            result.graph_nodes = graph_nodes
            result.mongo_collections = {"files": "files", "chunks": collection}
            
            logger.info(
                "Text pipeline completed successfully: file_id=%s, collection=%s, chunks=%d, graph_nodes=%d, storage=%s",
                meta_doc["_id"],
                collection,
                chunk_count,
                len(graph_nodes),
                storage_uri
            )
            
        except Exception as exc:
            logger.exception("Failed to process text file %s", path)
            result.status = "error"
            result.error = f"Text processing failed: {str(exc)}"
            logger.error("Text pipeline error details for %s: %s", path, exc, exc_info=True)

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

        # Extract descriptive text from CLIP model
        descriptive_text = info.get("text") or path.name
        logger.info("CLIP generated descriptive text for %s: %s", path.name, descriptive_text[:100] if len(descriptive_text) > 100 else descriptive_text)
        
        # Use descriptive text for collection inference and path planning
        summary = descriptive_text
        collection = infer_collection(descriptive_text)
        plan = self._resolve_path_plan(descriptive_text, path)
        
        # Copy file to storage directory
        logger.debug("Copying media file to storage directory")
        stored_path = self._copy_file_to_storage(path, modality, collection)
        if stored_path:
            logger.info("Media file copied to storage: %s -> %s", path.name, stored_path)
            storage_uri = str(stored_path) if isinstance(stored_path, Path) else stored_path
        else:
            # Fallback to path plan or original path
            storage_uri = self._resolve_storage_uri(plan, path)
            logger.warning("Media file copy failed, using original path: %s", storage_uri)

        try:
            # Store descriptive text in metadata for file structure generation
            meta_doc = meta_generator(
                file_path=str(path),
                tenant_id=tenant_id,
                summary=descriptive_text,  # Full descriptive text from CLIP
                collection=collection,
                nosql_db=self.nosql_db,
                storage_uri=storage_uri,
                extra={
                    **extra_payload, 
                    "modality": modality, 
                    "multimodal_extra": info.get("extra"),
                    "descriptive_text": descriptive_text,  # Store descriptive text explicitly
                    "clip_generated": True  # Flag to indicate CLIP-generated description
                },
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
        
        # Copy file to storage directory
        logger.debug("Copying fallback media file to storage directory")
        stored_path = self._copy_file_to_storage(path, modality, collection)
        if stored_path:
            logger.info("Fallback media file copied to storage: %s -> %s", path.name, stored_path)
            storage_uri = str(stored_path) if isinstance(stored_path, Path) else stored_path
        else:
            # Fallback to path plan or original path
            storage_uri = self._resolve_storage_uri(plan, path)
            logger.warning("Fallback media file copy failed, using original path: %s", storage_uri)

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
        """Detect file modality based on extension and MIME type."""
        file_ext = path.suffix.lower()
        mime, _ = mimetypes.guess_type(path.name)
        
        logger.debug("Detecting modality for %s (ext=%s, mime=%s)", path, file_ext, mime)
        
        # Media files
        if mime:
            if mime.startswith("image/"):
                logger.debug("Detected image modality for %s", path)
                return "image"
            if mime.startswith("video/"):
                logger.debug("Detected video modality for %s", path)
                return "video"
            if mime.startswith("audio/"):
                logger.debug("Detected audio modality for %s", path)
                return "audio"
        
        # PDF and document files should be treated as text for processing
        if file_ext in {'.pdf', '.docx', '.doc'}:
            logger.debug("Detected document file as text modality: %s", path)
            return "text"
        
        # Default to text for all other files
        logger.debug("Defaulting to text modality for %s", path)
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
        """Compute storage root directory - defaults to ../storage if not configured."""
        root = self.config.local_path_root or os.getenv("LOCAL_ROOT_REPO")
        
        # If not configured, use default storage folder
        if not root:
            # Default to ../storage relative to backend directory
            # pipeline.py is in backend/nosql_ingestion_pipeline/
            # So we need to go up to backend/ then to root, then to storage/
            backend_dir = Path(__file__).parent.parent  # backend/
            project_root = backend_dir.parent  # project root
            default_storage = project_root / "storage"
            logger.info("No LOCAL_ROOT_REPO configured, using default storage: %s", default_storage)
            
            # Create storage directory if it doesn't exist
            if not default_storage.exists():
                try:
                    default_storage.mkdir(parents=True, exist_ok=True)
                    logger.info("Created storage directory: %s", default_storage)
                except Exception as exc:
                    logger.warning("Failed to create storage directory %s: %s", default_storage, exc)
                    return None
            
            if default_storage.exists() and default_storage.is_dir():
                return default_storage
            return None
        
        try:
            path = Path(root).expanduser().resolve()
            if path.exists():
                return path
            
            # Create storage directory if it doesn't exist
            if not path.exists():
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    logger.info("Created storage directory: %s", path)
                except Exception as exc:
                    logger.warning("Failed to create storage directory %s: %s", path, exc)
                    return None
            
            if path.exists() and path.is_dir():
                return path
        except Exception:
            logger.warning("Unable to resolve storage root %s", root, exc_info=True)
        return None

    def _copy_file_to_storage(self, source_path: Path, modality: str, collection: str) -> Optional[Path]:
        """
        Copy file to storage directory organized by modality/collection.
        
        Args:
            source_path: Source file path
            modality: File modality (text, image, video, audio)
            collection: Collection name
            
        Returns:
            Destination path in storage or None if failed
        """
        if not self._storage_root:
            logger.warning("Storage root not available, skipping file copy")
            return None
        
        try:
            # Create directory structure: storage/{modality}/{collection}/
            storage_dir = self._storage_root / modality / collection
            storage_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy file to storage
            dest_path = storage_dir / source_path.name
            
            # Handle file name conflicts
            counter = 1
            original_dest = dest_path
            while dest_path.exists():
                stem = original_dest.stem
                suffix = original_dest.suffix
                dest_path = storage_dir / f"{stem}_{counter}{suffix}"
                counter += 1
            
            logger.info("Copying file %s to storage: %s", source_path.name, dest_path)
            import shutil
            shutil.copy2(source_path, dest_path)
            logger.info("File copied successfully: %s -> %s", source_path, dest_path)
            
            # Return relative path from storage root
            try:
                relative_path = dest_path.relative_to(self._storage_root)
                return relative_path
            except Exception:
                return dest_path
                
        except Exception as exc:
            logger.error("Failed to copy file %s to storage: %s", source_path, exc, exc_info=True)
            return None
    
    def _resolve_storage_uri(self, plan: Optional[PathPlan], original_path: Path) -> str:
        """Resolve storage URI - prefers moved files, falls back to original path."""
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
                        # Path is outside storage root, return as-is
                        return candidate_path.as_posix()
                return candidate_path.as_posix()
            # Relative path - assume it's relative to storage root
            if self._storage_root:
                return candidate_path.as_posix()
            return candidate_path.as_posix()
        except Exception:
            logger.warning("Failed to normalise storage uri for %s", candidate, exc_info=True)
            return candidate

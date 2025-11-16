from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True)
class NoSQLPipelineConfig:
    mongo_uri: str | None
    mongo_db: str | None
    chroma_path: str
    chroma_collection: str
    default_tenant_id: str
    local_path_enabled: bool
    local_path_move_files: bool
    local_path_root: str | None


def load_config() -> NoSQLPipelineConfig:
    """
    Load configuration from environment variables and fall back to defaults.
    """
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")

    chroma_path = os.getenv("CHROMA_PERSIST_PATH", "./chroma_db")
    chroma_collection = os.getenv("CHROMA_NOSQL_COLLECTION", "nosql_graph_embeddings")

    default_tenant_id = os.getenv("DEFAULT_TENANT_ID", "tenant_default")

    local_path_enabled = os.getenv("ENABLE_LOCAL_PATH_GENERATOR", "true").lower() == "true"
    local_path_move_files = os.getenv("LOCAL_PATH_GENERATOR_MOVE_FILES", "true").lower() == "true"
    local_path_root = os.getenv("LOCAL_ROOT_REPO")

    return NoSQLPipelineConfig(
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        chroma_path=chroma_path,
        chroma_collection=chroma_collection,
        default_tenant_id=default_tenant_id,
        local_path_enabled=local_path_enabled,
        local_path_move_files=local_path_move_files,
        local_path_root=local_path_root,
    )

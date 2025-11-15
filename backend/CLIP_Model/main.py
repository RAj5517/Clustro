import json
import argparse
import sys
from pathlib import Path
import chromadb


# ------------------------------
# Utility class to silence noisy libs
# ------------------------------
class _DevNull:
    def write(self, _):
        return 0

    def flush(self):
        return 0

    def close(self):
        # Some logging handlers call .close() on their stream at exit.
        # Make this a no-op so atexit doesn't crash.
        return 0


# ------------------------------
# Main
# ------------------------------
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        help="Path to file (image/video/audio/text)",
    )
    parser.add_argument(
        "--no-audio",
        action="store_true",
        help="Disable audio transcription support.",
    )
    parser.add_argument(
        "--collection",
        default="clip_embeddings",
        help="ChromaDB collection name (default: clip_embeddings)",
    )
    parser.add_argument(
        "--chroma-path",
        default="./chroma_db",
        help="Directory for persistent ChromaDB storage",
    )
    args = parser.parse_args()

    # Resolve absolute input path
    input_path = str(Path(args.path).expanduser().resolve())

    # ------------------------------
    # Silence model + library noise while loading + encoding
    # ------------------------------
    real_stdout, real_stderr = sys.stdout, sys.stderr
    sys.stdout = _DevNull()
    sys.stderr = _DevNull()

    try:
        # Import inside silenced block
        from multimodal_pipeline import MultiModalPipeline

        pipe = MultiModalPipeline(enable_audio=not args.no_audio)
        info = pipe.encode_path(input_path)

    finally:
        # Restore clean output
        sys.stdout = real_stdout
        sys.stderr = real_stderr

    # Extract embedding
    emb = info.get("embedding")
    emb_dim = len(emb) if emb else 0

    # ------------------------------
    # Connect to ChromaDB
    # ------------------------------
    client = chromadb.PersistentClient(path=args.chroma_path)
    collection = client.get_or_create_collection(name=args.collection)

    chroma_id = input_path  # unique ID per file

    # ------------------------------
    # Sanitize metadata for ChromaDB
    # ------------------------------
    raw_metadata = {
        "path": info.get("path"),
        "modality": info.get("modality"),
        **(info.get("extra") or {}),
    }

    sanitized_metadata = {}
    for key, value in raw_metadata.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            sanitized_metadata[key] = value
        else:
            # Convert unsupported types (lists, dicts, numpy types...) to JSON strings
            try:
                sanitized_metadata[key] = json.dumps(value)
            except Exception:
                sanitized_metadata[key] = str(value)

    # ------------------------------
    # Upsert into ChromaDB
    # ------------------------------
    if emb is not None:
        collection.upsert(
            ids=[chroma_id],
            embeddings=[emb],
            documents=[info.get("text") or ""],
            metadatas=[sanitized_metadata],
        )

    # ------------------------------
    # Clean final output (no embeddings / no vector sizes)
    # ------------------------------
    output = {
        "path": info.get("path"),
        "modality": info.get("modality"),
        "text": info.get("text"),
        "embedding_dim": emb_dim,
        "chroma_collection": collection.name,
        "chroma_id": chroma_id,
        "extra": info.get("extra"),
    }

    print("=== RESULT ===")
    print(json.dumps(output, indent=2))


# ------------------------------
# Entry point
# ------------------------------
if __name__ == "__main__":
    main()
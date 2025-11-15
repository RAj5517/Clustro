#!/usr/bin/env python
# main.py

import argparse
import os
import re
from pathlib import Path
from datetime import datetime
from uuid import uuid4
from collections import defaultdict

from keybert import KeyBERT
from pymongo import MongoClient

# ------------------------------------------------------------
# PLACEHOLDER IMPORTS – adjust to your actual modules
# ------------------------------------------------------------
# Assume you already have these implemented somewhere.
# Replace `your_module` with the real module name.

def to_text_space(x): return "A travel itinerary covering major attractions in Japan for 7 days."

# from your_module import to_text_space, save_local
from local_path_generator.main import main as save_local

# ------------------------------------------------------------


# ------------------------------------------------------------
# Hugging Face / KeyBERT model (lightweight)
# ------------------------------------------------------------
kw_model = KeyBERT(model="sentence-transformers/all-MiniLM-L6-v2")


# ------------------------------------------------------------
# Collection vocab: semantic labels for different collections
# ------------------------------------------------------------
COLLECTION_VOCAB = {
    "meeting_notes": [
        "meeting", "minutes", "agenda", "standup", "retro", "retrospective",
        "attendees", "discussion", "action item", "follow up"
    ],
    "incident_reports": [
        "incident", "outage", "downtime", "postmortem", "root cause",
        "mitigation", "severity", "sev", "impact"
    ],
    "technical_specs": [
        "technical specification", "spec", "design doc", "architecture",
        "api", "interface", "requirements", "implementation", "workflow"
    ],
    "legal_documents": [
        "contract", "agreement", "terms and conditions", "nda",
        "confidentiality", "liability", "jurisdiction"
    ],
    "knowledge_articles": [
        "guide", "how to", "tutorial", "manual", "documentation",
        "faq", "steps", "instructions"
    ],
    "email_threads": [
        "email", "thread", "inbox", "subject", "reply", "forward",
        "cc", "bcc", "recipient", "sender"
    ],
}


# ------------------------------------------------------------
# Simple tokenizer / normalizer helpers
# ------------------------------------------------------------
def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


# ------------------------------------------------------------
# infer_collection(): uses HF model (via KeyBERT) for keyword scoring
# ------------------------------------------------------------
def infer_collection(text_content: str, top_n: int = 20) -> str:
    """
    Uses a lightweight HF model (via KeyBERT + all-MiniLM-L6-v2) to extract
    keyphrases from the text and chooses the collection whose vocabulary
    best matches those keyphrases.

    Returns the chosen collection name, or 'generic_documents' as fallback.
    """
    text_content = (text_content or "").strip()
    if not text_content:
        return "generic_documents"

    keywords = kw_model.extract_keywords(
        text_content,
        top_n=top_n,
        keyphrase_ngram_range=(1, 3),
        stop_words="english",
    )
    # keywords: list[(phrase, relevance_score)]
    if not keywords:
        return "generic_documents"

    normalized_vocab = {
        col: [_normalize(kw) for kw in vocab]
        for col, vocab in COLLECTION_VOCAB.items()
    }

    scores = defaultdict(float)

    for phrase, model_score in keywords:
        norm_phrase = _normalize(phrase)
        for collection, vocab in normalized_vocab.items():
            for v in vocab:
                if not v:
                    continue
                if v in norm_phrase or norm_phrase in v:
                    scores[collection] += float(model_score)

    if not scores:
        return "generic_documents"

    best_collection = max(scores.items(), key=lambda x: x[1])[0]
    return best_collection


# ------------------------------------------------------------
# Mongo / NoSQL helper
# ------------------------------------------------------------
def get_nosql_db(uri: str | None = None, db_name: str | None = None):
    """
    Returns a MongoDB database handle.

    Controlled via env vars by default:
      MONGO_URI  (default: mongodb://localhost:27017)
      MONGO_DB   (default: clustro_text)
    """
    uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = db_name or os.getenv("MONGO_DB", "clustro_text")

    client = MongoClient(uri)
    return client[db_name]


# ------------------------------------------------------------
# Metadata generator – stores one "file" doc in nosql_db["files"]
# ------------------------------------------------------------
def meta_generator(
    file_path: str,
    tenant_id: str,
    summary: str,
    collection: str,
    nosql_db,
    storage_uri: str | None = None,
    extra: dict | None = None,
) -> dict:
    """
    Generates and persists a 'file' metadata document and returns it.

    The metadata document is saved in the 'files' collection of the
    given NoSQL database (nosql_db["files"]).
    """
    path = Path(file_path)
    file_id = f"file_{uuid4().hex}"
    now = datetime.utcnow().isoformat() + "Z"

    meta_doc = {
        "_id": file_id,
        "tenant_id": tenant_id,
        "original_name": path.name,
        "extension": path.suffix.lower(),
        "size_bytes": path.stat().st_size,
        "storage_uri": storage_uri,
        "summary_preview": summary[:500],
        "collection_hint": collection,
        "created_at": now,
        "ingested_at": now,
        "extra": extra or {},
    }

    files_coll = nosql_db["files"]
    files_coll.insert_one(meta_doc)

    return meta_doc


# ------------------------------------------------------------
# Very basic full-text extractor – you should extend this
# ------------------------------------------------------------
def extract_full_text(file_path: str) -> str:
    """
    Placeholder full-text extractor.

    For now:
    - .txt / .md: read as plain text.
    - others: fall back to a simple read (may look messy for PDF/DOCX).

    You can later:
    - Plug in PyPDF2 / pdfminer for PDFs.
    - Plug in python-docx for DOCX.
    - Use your existing extraction utilities if you already have them.
    """
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext in {".txt", ".md"}:
        return path.read_text(encoding="utf-8", errors="ignore")

    # Fallback: read bytes & decode best-effort
    data = path.read_bytes()
    try:
        return data.decode("utf-8", errors="ignore")
    except Exception:
        return ""


# ------------------------------------------------------------
# Chunker + chunk_generator – stores "file_chunk" docs
# ------------------------------------------------------------
def simple_character_chunker(text: str, max_chars: int = 2000, overlap: int = 200):
    """
    Splits text into overlapping chunks for LLM/search use.
    """
    chunks = []
    text = text or ""
    length = len(text)
    if length == 0:
        return chunks

    start = 0
    while start < length:
        end = min(length, start + max_chars)
        chunk_text = text[start:end]
        chunks.append(chunk_text)
        if end == length:
            break
        start = end - overlap

    return chunks


def chunk_generator(
    file_path: str,
    file_id: str,
    tenant_id: str,
    collection: str,
    nosql_db,
    text_override: str | None = None,
) -> int:
    """
    Generates 'file_chunk' documents for search/LLM use and saves them efficiently.

    The chunk documents are stored in the collection chosen by infer_collection(),
    i.e. nosql_db[collection].

    Returns the number of chunks created.
    """
    if text_override is not None:
        full_text = text_override
    else:
        full_text = extract_full_text(file_path)

    chunk_texts = simple_character_chunker(full_text)

    if not chunk_texts:
        return 0

    chunks_coll = nosql_db[collection]
    now = datetime.utcnow().isoformat() + "Z"

    docs = []
    for idx, chunk_text in enumerate(chunk_texts):
        docs.append({
            "file_id": file_id,
            "tenant_id": tenant_id,
            "chunk_index": idx,
            "text": chunk_text,
            "created_at": now,
        })

    chunks_coll.insert_many(docs)
    return len(docs)


# ------------------------------------------------------------
# The main text file pipeline
# ------------------------------------------------------------
def text_file_pipeline(
    file_path: str,
    tenant_id: str,
    nosql_db,
    storage_uri: str | None = None,
    extra: dict | None = None,
) -> dict:
    """
    End-to-end pipeline for a text-heavy file.

    1. Passes it to to_text_space() to get a textual summary.
    2. Runs infer_collection() on that summary to choose the collection.
    3. Calls meta_generator() to create and save the 'file' document.
    4. Calls chunk_generator() to create and save 'file_chunk' documents.
    5. Passes the summary to save_local() to persist it locally.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Step 1: summary using your existing model/logic
    summary = to_text_space(file_path)

    # Step 2: choose collection using Hugging Face model via KeyBERT
    collection = infer_collection(summary)

    # Step 3: save metadata ("file" doc) into nosql_db["files"]
    meta_doc = meta_generator(
        file_path=file_path,
        tenant_id=tenant_id,
        summary=summary,
        collection=collection,
        nosql_db=nosql_db,
        storage_uri=storage_uri,
        extra=extra,
    )
    file_id = meta_doc["_id"]

    # Step 4: save chunks ("file_chunk" docs) into nosql_db[collection]
    num_chunks = chunk_generator(
        file_path=file_path,
        file_id=file_id,
        tenant_id=tenant_id,
        collection=collection,
        nosql_db=nosql_db,
    )

    # Step 5: persist summary + local folder using your existing function
    save_local(summary, file_path)

    return {
        "file_id": file_id,
        "tenant_id": tenant_id,
        "collection": collection,
        "num_chunks": num_chunks,
        "storage_uri": storage_uri,
    }


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Text-heavy file ingestion pipeline (NoSQL + HF-based routing)"
    )
    parser.add_argument("file_path", help="Path to the text-heavy file to process")
    parser.add_argument(
        "--tenant-id",
        required=True,
        help="Tenant / organization identifier",
    )
    parser.add_argument(
        "--mongo-uri",
        default=None,
        help="MongoDB URI (optional, overrides MONGO_URI env var)",
    )
    parser.add_argument(
        "--mongo-db",
        default=None,
        help="MongoDB database name (optional, overrides MONGO_DB env var)",
    )
    parser.add_argument(
        "--storage-uri",
        default=None,
        help="Optional storage URI for the raw file (e.g. s3://bucket/key)",
    )

    args = parser.parse_args()

    nosql_db = get_nosql_db(uri=args.mongo_uri, db_name=args.mongo_db)

    result = text_file_pipeline(
        file_path=args.file_path,
        tenant_id=args.tenant_id,
        nosql_db=nosql_db,
        storage_uri=args.storage_uri,
        extra={},
    )

    print("Ingestion complete:")
    print(f"  File ID     : {result['file_id']}")
    print(f"  Tenant ID   : {result['tenant_id']}")
    print(f"  Collection  : {result['collection']}")
    print(f"  Num chunks  : {result['num_chunks']}")
    print(f"  Storage URI : {result['storage_uri']}")


if __name__ == "__main__":
    main()

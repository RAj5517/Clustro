# clip.py
"""
Clip utilities: produce image and text embeddings using available models.
Falls back to a deterministic random vector if a heavy model isn't available.
"""
import hashlib
import os
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    S_TEXT = SentenceTransformer("all-MiniLM-L6-v2")
except Exception:
    S_TEXT = None

# Desired default embedding dim
DEFAULT_DIM = 512

def _deterministic_vector(seed: str, dim: int = DEFAULT_DIM):
    # produce a deterministic pseudo-random vector from a seed string
    h = hashlib.sha256(seed.encode("utf-8")).digest()
    rng = np.random.RandomState(int.from_bytes(h[:8], "big"))
    v = rng.normal(size=(dim,)).astype(np.float32)
    # normalize
    v /= np.linalg.norm(v) + 1e-10
    return v.tolist()

def embed_text(text: str, dim: int = DEFAULT_DIM):
    """Return a text embedding (list of floats). Uses sentence-transformers if available,
    otherwise a deterministic fallback.
    """
    if not text:
        return _deterministic_vector("empty-text", dim)

    if S_TEXT is not None:
        try:
            v = S_TEXT.encode(text, convert_to_numpy=True)
            # if model returns different dim, pad or truncate
            if v.shape[0] != dim:
                # if smaller, pad with zeros; if larger, truncate
                out = np.zeros((dim,), dtype=np.float32)
                out[: min(dim, v.shape[0])] = v[:dim]
                v = out
            # normalize
            v = v / (np.linalg.norm(v) + 1e-10)
            return v.astype(np.float32).tolist()
        except Exception:
            pass
    # fallback
    return _deterministic_vector(text, dim)

def embed_image_placeholder(path: str, dim: int = DEFAULT_DIM):
    """If no image encoder is available, create a deterministic vector from file bytes.
    This makes results reproducible and still useful for indexing.
    """
    if not os.path.exists(path):
        return _deterministic_vector("missing-image-" + path, dim)
    with open(path, "rb") as f:
        b = f.read()
    # use hash as seed
    h = hashlib.sha256(b).hexdigest()
    return _deterministic_vector(h, dim)

# Public API

def get_image_embedding(path: str, dim: int = DEFAULT_DIM):
    """Attempt to compute an image embedding. Currently uses deterministic fallback.
    You can extend this to call a real CLIP model (open_clip or transformers) if available.
    """
    return embed_image_placeholder(path, dim)

def get_text_embedding(text: str, dim: int = DEFAULT_DIM):
    return embed_text(text, dim)

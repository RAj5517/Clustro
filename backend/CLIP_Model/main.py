# main.py
import json
import argparse

from multimodal_pipeline import MultiModalPipeline


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        help="Path to file (image / video / audio / text)",
    )
    parser.add_argument(
        "--no-audio",
        action="store_true",
        help="Disable Whisper/audio (avoids SSL/download issues if you only test images/videos/text)",
    )
    args = parser.parse_args()

    pipe = MultiModalPipeline(enable_audio=not args.no_audio)

    info = pipe.encode_path(args.path)
    emb = info["embedding"]

    if emb is not None:
        emb_dim = len(emb)

        # IMPORTANT:
        # Print embedding using Python repr, NOT JSON, so truncation happens automatically.
        embedding_preview = repr(emb)   # << this is the key line
    else:
        emb_dim = 0
        embedding_preview = None

    output = {
        "path": info["path"],
        "modality": info["modality"],
        "text": info["text"],
        "embedding_dim": emb_dim,
        "embedding_preview": embedding_preview,
        "extra": info["extra"],
    }

    # Print JSON WITHOUT embedding (to keep it clean)
    clean_output = {k: v for k, v in output.items() if k != "embedding_preview"}
    print("=== RESULT ===")
    print(json.dumps(clean_output, indent=2))

    # Print embedding separately in raw Python form (auto-truncates)
    print("\n=== EMBEDDING (auto-truncated by Python) ===")
    print(embedding_preview)

    print("\n=== VECTOR SPACES SIZES ===")
    print(f"image_space: {len(pipe.image_space)}")
    print(f"video_space: {len(pipe.video_space)}")
    print(f"audio_space: {len(pipe.audio_space)}")
    print(f"text_space:  {len(pipe.text_space)}")

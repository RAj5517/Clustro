# multimodal_pipeline.py
import mimetypes
from pathlib import Path
from typing import Dict, Any, Tuple, Optional
import sys  # for printing to real stdout

import numpy as np
import torch
import cv2
from PIL import Image

from clip import CLIPBackend
from caption import CaptionBackend
from audio import AudioBackend
from text import TextBackend


class MultiModalPipeline:
    """
    - Detect modality: image / video / audio / text
    - For each:
        * produce text (caption / transcript / summary)
        * produce CLIP embedding (default 512-dim with ViT-B-32)
    - Maintain four vector spaces:
        * image_space[path] -> embedding (np.ndarray)
        * video_space[path] -> embedding
        * audio_space[path] -> embedding
        * text_space[path]  -> embedding
    """

    def __init__(
        self,
        enable_audio: bool = True,
        audio_model_name: str = "small",
        # 512-dim CLIP models: "ViT-B-32" (openai) or "RN101"
        clip_model_name: str = "ViT-B-32",
        clip_pretrained: str = "openai",
        caption_model_name: str = "Salesforce/blip-image-captioning-base",
        max_frames_per_video: Optional[int] = None,   # optional hard cap
        frames_per_second_factor: float = 0.3,        # ~0.3 frames per second
    ):
        self.clip = CLIPBackend(model_name=clip_model_name, pretrained=clip_pretrained)
        self.captioner = CaptionBackend(model_name=caption_model_name)
        self.text_backend = TextBackend()

        self.enable_audio = enable_audio
        self.audio_backend = AudioBackend(model_name=audio_model_name) if enable_audio else None

        self.max_frames_per_video = max_frames_per_video
        self.frames_per_second_factor = frames_per_second_factor

        # vector spaces
        self.image_space: Dict[str, np.ndarray] = {}
        self.video_space: Dict[str, np.ndarray] = {}
        self.audio_space: Dict[str, np.ndarray] = {}
        self.text_space: Dict[str, np.ndarray] = {}

    # -------------- modality detection --------------

    def detect_modality(self, path: str) -> str:
        ext = Path(path).suffix.lower()
        mime, _ = mimetypes.guess_type(path)

        # image / video / audio by mime
        if mime:
            if mime.startswith("image/"):
                return "image"
            if mime.startswith("video/"):
                return "video"
            if mime.startswith("audio/"):
                return "audio"

        # text-like / structured types → treat as "text"
        text_exts = {
            ".json", ".csv", ".xlsx", ".xls",
            ".xml", ".html", ".htm",
            ".txt", ".md", ".log",
            ".yaml", ".yml",
            ".ini", ".cfg", ".conf",
            ".pdf", ".docx", ".doc",
        }

        if ext in {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}:
            return "image"
        if ext in {".mp4", ".mov", ".avi", ".mkv", ".webm"}:
            return "video"
        if ext in {".mp3", ".wav", ".flac", ".ogg", ".m4a"}:
            return "audio"
        if ext in text_exts:
            return "text"

        return "other"

    # -------------- public API --------------

    @torch.no_grad()
    def encode_path(self, path: str) -> Dict[str, Any]:
        modality = self.detect_modality(path)

        if modality == "image":
            text, emb, extra = self._encode_image(path)
            self.image_space[path] = emb.cpu().numpy()

        elif modality == "video":
            text, emb, extra = self._encode_video(path)
            self.video_space[path] = emb.cpu().numpy()

        elif modality == "audio":
            if not self.enable_audio:
                raise RuntimeError("Audio support disabled. Init with enable_audio=True.")
            text, emb, extra = self._encode_audio(path)
            self.audio_space[path] = emb.cpu().numpy()

        elif modality == "text":
            text, emb, extra = self._encode_text_file(path)
            self.text_space[path] = emb.cpu().numpy()

        else:
            text = None
            emb = None
            extra = {}

        return {
            "path": path,
            "modality": modality,
            "text": text,
            "embedding": emb.tolist() if emb is not None else None,
            "extra": extra,  # will always be {} with the changes below
        }

    # -------------- image --------------

    @torch.no_grad()
    def _encode_image(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        img = Image.open(path).convert("RGB")
        caption = self.captioner.caption_image(img)

        img_tensor = self.clip.preprocess(img).unsqueeze(0)
        emb = self.clip.encode_image_tensor(img_tensor)  # 512-dim

        extra: Dict[str, Any] = {}  # stripped: no width/height

        return caption, emb, extra

    # -------------- video (duration * factor + smooth progress bar) --------------

    @torch.no_grad()
    def _encode_video(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            raise RuntimeError(f"Failed to open video: {path}")

        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = float(cap.get(cv2.CAP_PROP_FPS) or 0.0)

        if frame_count <= 0:
            cap.release()
            raise RuntimeError(f"Video has no frames: {path}")

        # ---- compute duration ----
        if fps > 0:
            duration_seconds = frame_count / fps
        else:
            duration_seconds = frame_count / 30.0  # assume 30fps fallback

        # ---- calculate number of frames to sample ----
        base_num_frames = max(1, int(duration_seconds * self.frames_per_second_factor))

        if self.max_frames_per_video is not None:
            num_frames = min(base_num_frames, self.max_frames_per_video)
        else:
            num_frames = base_num_frames

        num_frames = min(num_frames, frame_count)

        indices = np.linspace(0, frame_count - 1, num_frames, dtype=int)

        frame_embs = []
        frame_captions = []

        # ---- progress bar settings ----
        total_frames = len(indices)
        bar_len = 30

        # write directly to the real stdout to bypass main.py's _DevNull
        real_out = getattr(sys, "__stdout__", sys.stdout)

        def render_progress(processed: int) -> None:
            if total_frames == 0:
                return
            ratio = processed / total_frames
            filled = int(bar_len * ratio)
            bar = "█" * filled + "-" * (bar_len - filled)
            percent = ratio * 100
            real_out.write(
                f"\r[{bar}] {processed}/{total_frames} frames ({percent:.1f}%)"
            )
            real_out.flush()

        processed = 0

        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(idx))
            ret, frame = cap.read()

            if not ret:
                processed += 1
                render_progress(processed)
                continue

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            pil_img = Image.fromarray(frame_rgb)

            # caption each frame
            cap_text = self.captioner.caption_image(pil_img)
            frame_captions.append(cap_text)

            # embed frame
            img_tensor = self.clip.preprocess(pil_img).unsqueeze(0)
            feat = self.clip.encode_image_tensor(img_tensor)
            frame_embs.append(feat.unsqueeze(0))

            # ---- update progress bar ----
            processed += 1
            render_progress(processed)

        cap.release()
        real_out.write("\n")  # newline after progress bar ends
        real_out.flush()

        # ---- finalize ----
        if not frame_embs:
            raise RuntimeError(f"No frames could be read from video: {path}")

        feats = torch.cat(frame_embs, dim=0)
        video_emb = feats.mean(dim=0)
        video_emb = torch.nn.functional.normalize(video_emb.unsqueeze(0), dim=-1)[0].cpu()

        unique_caps = []
        for c in frame_captions:
            if c not in unique_caps:
                unique_caps.append(c)

        summary_text = " | ".join(unique_caps)

        extra: Dict[str, Any] = {}  # stripped: no frame_count, fps, etc.

        return summary_text, video_emb, extra

    # -------------- audio --------------

    @torch.no_grad()
    def _encode_audio(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        text, _meta = self.audio_backend.transcribe(path)
        emb = self.clip.encode_text(text)
        extra: Dict[str, Any] = {}  # stripped
        return text, emb, extra

    # -------------- text files (structured) --------------

    @torch.no_grad()
    def _encode_text_file(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        summary, embed_text, _meta = self.text_backend.load_and_summarise(path)
        emb = self.clip.encode_text(embed_text)  # 512-dim
        extra: Dict[str, Any] = {}  # stripped
        return summary, emb, extra
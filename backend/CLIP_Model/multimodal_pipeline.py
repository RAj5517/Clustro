# multimodal_pipeline.py
import mimetypes
from pathlib import Path
from typing import Dict, Any, Tuple, Optional  # ✅ Optional added

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
        max_frames_per_video: Optional[int] = None,   # ✅ no cap by default
    ):
        self.clip = CLIPBackend(model_name=clip_model_name, pretrained=clip_pretrained)
        self.captioner = CaptionBackend(model_name=caption_model_name)
        self.text_backend = TextBackend()

        self.enable_audio = enable_audio
        self.audio_backend = AudioBackend(model_name=audio_model_name) if enable_audio else None

        self.max_frames_per_video = max_frames_per_video  # may be None

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
            extra = {"info": "Unsupported / unknown modality."}

        return {
            "path": path,
            "modality": modality,
            "text": text,
            "embedding": emb.tolist() if emb is not None else None,
            "extra": extra,
        }

    # -------------- image --------------

    @torch.no_grad()
    def _encode_image(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        img = Image.open(path).convert("RGB")
        caption = self.captioner.caption_image(img)

        img_tensor = self.clip.preprocess(img).unsqueeze(0)
        emb = self.clip.encode_image_tensor(img_tensor)  # 512-dim

        extra = {
            "width": img.width,
            "height": img.height,
        }

        return caption, emb, extra

    # -------------- video (1 frame per second, optional cap, with progress) --------------

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

        # 1 frame per second
        if fps > 0:
            duration_seconds = frame_count / fps
            num_frames = max(1, int(duration_seconds))  # 1 fps

            # apply cap only if explicitly set
            if self.max_frames_per_video is not None:
                num_frames = min(num_frames, self.max_frames_per_video)

            indices = (np.arange(num_frames) * int(round(fps))).astype(int)
            indices = np.clip(indices, 0, frame_count - 1)
            indices = np.unique(indices)
        else:
            # fallback: N evenly spaced frames
            fallback_num = self.max_frames_per_video if self.max_frames_per_video is not None else 8
            indices = np.linspace(0, frame_count - 1, fallback_num, dtype=int)

        frame_embs = []
        frame_captions = []

        total_frames_to_process = len(indices)
        processed = 0

        for idx in indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, int(idx))
            ret, frame = cap.read()
            if not ret:
                processed += 1
                continue

            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            pil_img = Image.fromarray(frame_rgb)

            cap_text = self.captioner.caption_image(pil_img)
            frame_captions.append(cap_text)

            img_tensor = self.clip.preprocess(pil_img).unsqueeze(0)
            feat = self.clip.encode_image_tensor(img_tensor)  # 512-dim
            frame_embs.append(feat.unsqueeze(0))

            processed += 1
            progress = (processed / total_frames_to_process) * 100.0
            print(f"\rProcessing video frames: {processed}/{total_frames_to_process} ({progress:.1f}%)", end="")

        cap.release()
        print()  # move to new line after progress

        if not frame_embs:
            raise RuntimeError(f"No frames could be read from video: {path}")

        feats = torch.cat(frame_embs, dim=0)  # (N, D)
        video_emb = feats.mean(dim=0)        # (D,)
        video_emb = torch.nn.functional.normalize(video_emb.unsqueeze(0), dim=-1)[0].cpu()

        unique_caps = []
        for c in frame_captions:
            if c not in unique_caps:
                unique_caps.append(c)
        summary_text = " | ".join(unique_caps)

        extra = {
            "frame_count": frame_count,
            "fps": fps,
            "used_frames": len(frame_embs),
            "frame_indices": indices.tolist(),
            "frame_captions": frame_captions,
        }

        return summary_text, video_emb, extra

    # -------------- audio --------------

    @torch.no_grad()
    def _encode_audio(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        text, meta = self.audio_backend.transcribe(path)
        emb = self.clip.encode_text(text)
        return text, emb, meta

    # -------------- text files (structured) --------------

    @torch.no_grad()
    def _encode_text_file(self, path: str) -> Tuple[str, torch.Tensor, Dict[str, Any]]:
        summary, embed_text, meta = self.text_backend.load_and_summarise(path)
        emb = self.clip.encode_text(embed_text)  # 512-dim
        return summary, emb, meta

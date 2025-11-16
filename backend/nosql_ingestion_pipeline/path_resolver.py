from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PathPlan:
    path: str
    persona: Optional[str]
    payload: dict
    prompt: str


class LocalPathPlanner:
    """
    Thin wrapper around the Gemini-powered local path generator.
    Gracefully degrades when the module or required keys are missing.
    """

    def __init__(self, enabled: bool, move_files: bool, root: Optional[str]):
        self.enabled = enabled and bool(root)
        self.move_files = move_files
        self.root = root
        self._module = None
        self._last_error: Optional[str] = None

        if self.enabled:
            self._load_module()

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def _load_module(self) -> None:
        try:
            self._module = importlib.import_module("local_path_generator.main")
        except Exception as exc:  # pragma: no cover - optional
            self.enabled = False
            self._last_error = str(exc)
            logger.warning("Local path generator disabled: %s", exc)

    def plan(self, description: str, file_path: Path, move_file: Optional[bool] = None) -> Optional[PathPlan]:
        if not self.enabled or not self._module:
            logger.debug("Local path planner inactive; skipping for %s", file_path)
            return None

        try:
            root = self._module.LOCAL_ROOT_REPO
            structure = self._module.get_directory_structure(root)
            prompt = self._module._build_prompt(description, structure, file_path.name)
            response = self._module.model.generate_content(prompt)
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.replace("```json", "").replace("```", "").strip()

            parsed = json.loads(raw)
            resolved_path = parsed.get("path", "")
            do_move = self.move_files if move_file is None else move_file
            moved_to = None

            if do_move and resolved_path:
                logger.info("Moving file %s to resolved path %s", file_path, resolved_path)
                moved_to = str(self._module.move_file_to_destination(str(file_path), resolved_path))
                logger.info("File moved to %s", moved_to)
            else:
                logger.info(
                    "Resolved path %s for %s (move=%s); no move performed",
                    resolved_path,
                    file_path,
                    do_move,
                )

            payload = {
                "plan": parsed,
                "moved_to": moved_to,
            }

            return PathPlan(
                path=resolved_path,
                persona=parsed.get("persona"),
                payload=payload,
                prompt=prompt,
            )
        except Exception as exc:  # pragma: no cover - external service
            self._last_error = str(exc)
            logger.warning("Local path planning failed: %s", exc)
            return None

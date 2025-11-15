from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import torch
from pydantic import BaseModel, ConfigDict, Field, field_validator
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline


# Changed to a standard Mistral 7B (often called 8B) instruction-tuned model.
MODEL_ID = "mistralai/Mistral-7B-Instruct-v0.2"

# Exhaustive persona list requested by the user. It covers the major contexts a user
# would realistically route files into so the model always picks a canonical bucket.
PERSONA_OPTIONS: List[str] = [
    "Work",
    "Personal",
    "Learning",
    "Family",
    "Health",
    "Finance",
    "Creative",
    "Community",
    "Research",
    "Hobby",
    "Travel",
    "Social",
    "Technical",
    "Administrative",
    "Entrepreneurial",
    "Academic",
    "Wellness",
    "Productivity",
    "Civic",
]


def _load_pipeline(model_id: str):
    """
    Lazily construct a text-generation pipeline for the requested model.
    """
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
    has_cuda = torch.cuda.is_available()
    # Mistral models perform well with float16 on compatible hardware
    dtype = torch.float16 if has_cuda else torch.float32
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        dtype=dtype,
        device_map="auto" if has_cuda else None,
        low_cpu_mem_usage=True,
        trust_remote_code=True,
    )
    generator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
        do_sample=False,
        pad_token_id=tokenizer.eos_token_id,
    )
    return generator


def _build_prompt(description: str) -> str:
    """
    Builds the guarded, few-shot prompt for the PathGuard service.
    """
    persona_clause = ", ".join(PERSONA_OPTIONS)
    
    # Example 1: Work/Project
    example1_desc = "Draft proposal for the Q4 Enterprise API integration project with external vendor specifications."
    example1_json = """
{
  "persona": "Work",
  "domain": "Projects",
  "category": "API_Integration",
  "topic": "Q4_Enterprise_Proposal",
  "filename": "Draft_Proposal_Vendor_Specs_v1.docx",
  "path": "Work/Projects/API_Integration/Q4_Enterprise_Proposal/Draft_Proposal_Vendor_Specs_v1.docx"
}
"""
    
    # Example 2: Personal/Hobby/Learning
    example2_desc = "Notes taken from the YouTube tutorial on advanced shaders for Blender 3D, watched on 2025-10-15."
    example2_json = """
{
  "persona": "Learning",
  "domain": "3D_Modeling",
  "category": "Blender",
  "topic": "Advanced_Shaders_Tutorial_2025",
  "filename": "Blender_Shaders_Notes_20251015.md",
  "path": "Learning/3D_Modeling/Blender/Advanced_Shaders_Tutorial_2025/Blender_Shaders_Notes_20251015.md"
}
"""

    return f"""
You are **PathGuard 5000**, an expert, reliable, and strictly hierarchical file path allocation service. Your sole function is to analyze the file description and generate a permanent, fully structured saving path.

**Your output MUST be a single, raw JSON object. Do not include any preceding or surrounding text, prose, comments, or markdown fences (```).**

### Schema and Hierarchy (Persona/Domain/Category/Topic/Filename):
The output must strictly conform to the following JSON schema:
{{
  "persona": <one mandatory high-level context>,
  "domain": <major field or initiative>,
  "category": <functional grouping or asset type>,
  "topic": <specific project or subject matter>,
  "filename": <best-fit file name with an extension>,
  "path": <persona/domain/category/topic/filename>
}}

### Strict Guardrails:
1.  **Persona Level 1 (L1):** MUST be exactly one value from this exhaustive list: {persona_clause}.
2.  **No Junk Folders:** NEVER use generic, non-descriptive, or catch-all terms like "Misc", "Temp", "Other", "General", "To_Sort", or "Archive" for **any** level (L1-L4). Every segment must be specific and descriptive.
3.  **Specificity:** The Domain (L2), Category (L3), and Topic (L4) must be generated dynamically based on the input description. They must be specific and represent a logical, permanent filing structure.
4.  **Filename:** Always include a descriptive filename with an inferred or best-guess extension (e.g., .pdf, .docx, .xlsx, .md). Use ".md" if the type is completely ambiguous.
5.  **Path Assembly:** The "path" value must concatenate the preceding four directory levels and the filename using forward slashes (/).

### Examples:
---
**Example 1 Description:** "{example1_desc}"
**Example 1 JSON:**
{example1_json.strip()}
---
**Example 2 Description:** "{example2_desc}"
**Example 2 JSON:**
{example2_json.strip()}
---

### Target Input:
Description: \"\"\"{description.strip()}\"\"\"
JSON:
""".strip()


def _extract_first_json_blob(text: str) -> Dict[str, Any]:
    """
    Extract the first JSON object from the generated text.
    """
    # Use a non-greedy search to find the JSON object between the first { and last }
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError("Model response did not contain JSON.")
    snippet = match.group(0)
    return json.loads(snippet)


def _sanitize_segment(segment: str) -> str:
    """
    Sanitize folder/file segments for use in a file path.
    """
    normalized = segment.strip().replace("\\", "/")
    # Replace illegal path characters with a hyphen
    normalized = re.sub(r"[\\/:*?\"<>|]", "-", normalized)
    # Collapse multiple spaces or hyphens into single space/hyphen (optional, for cleanup)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


class PathPlan(BaseModel):
    model_config = ConfigDict(validate_by_name=True)
    persona: str
    domain: str
    category: str
    topic: str
    # Use Field alias to accept 'filename' or 'file_name' from LLM output
    filename: str = Field(alias="file_name") 
    path: str

    @field_validator("persona")
    def validate_persona(cls, value: str) -> str:
        if value not in PERSONA_OPTIONS:
            raise ValueError(f"Persona '{value}' not in exhaustive list.")
        return value

    def normalized_path(self) -> Path:
        """Constructs a final, sanitized Path object."""
        segments = [
            _sanitize_segment(self.persona),
            _sanitize_segment(self.domain),
            _sanitize_segment(self.category),
            _sanitize_segment(self.topic),
            _sanitize_segment(self.filename),
        ]
        return Path("/".join(seg for seg in segments if seg))

    def json_dict(self) -> Dict[str, str]:
        """Returns the dictionary representing the final, normalized plan."""
        return {
            "persona": self.persona,
            "domain": self.domain,
            "category": self.category,
            "topic": self.topic,
            "filename": self.filename,
            "path": str(self.normalized_path()),
        }


@dataclass
class PathPlannerService:
    model_id: str = MODEL_ID
    _generator: Optional[Any] = None

    @property
    def generator(self):
        if self._generator is None:
            self._generator = _load_pipeline(self.model_id)
        return self._generator

    def plan(self, description: str, max_new_tokens: int = 256) -> PathPlan:
        prompt = _build_prompt(description)
        outputs = self.generator(
            prompt,
            max_new_tokens=max_new_tokens,
            eos_token_id=self.generator.tokenizer.eos_token_id,
            return_full_text=False,
        )
        generated = outputs[0]["generated_text"]
        payload = _extract_first_json_blob(generated)
        
        # Extract fields, prioritizing explicit keys but handling potential LLM variations
        persona = payload.get("persona", "").strip()
        domain = payload.get("domain", "").strip()
        category = payload.get("category", "").strip()
        topic = payload.get("topic", "").strip()
        # Handle both 'filename' and the schema's 'file_name' if present
        filename = payload.get("filename") or payload.get("file_name", "") 
        path_value = payload.get("path", "")
        
        # Fallback to reconstructing the path if the LLM failed to include the "path" key
        if not path_value:
            path_value = "/".join(
                [
                    persona,
                    domain,
                    category,
                    topic,
                    filename,
                ]
            )
            
        return PathPlan(
            persona=persona,
            domain=domain,
            category=category,
            topic=topic,
            file_name=filename, # Mapped to Pydantic's 'filename' field via alias
            path=path_value,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local path planning service using Mistral 7B.")
    parser.add_argument(
        "--description",
        type=str,
        required=True,
        help="Plain-text description of the file that needs to be routed.",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default=MODEL_ID,
        help="Hugging Face model ID to load.",
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=256,
        help="Maximum number of tokens to sample from the model.",
    )
    args = parser.parse_args()

    service = PathPlannerService(model_id=args.model_id)
    plan = service.plan(args.description, max_new_tokens=args.max_new_tokens)
    print(json.dumps(plan.json_dict(), indent=2))


if __name__ == "__main__":
    main()
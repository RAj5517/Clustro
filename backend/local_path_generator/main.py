from typing import List
import google.generativeai as genai
import argparse
import json
from dotenv import load_dotenv
import os
from pathlib import Path
import shutil

# Load .env file
load_dotenv()

# Read key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
LOCAL_ROOT_REPO = os.getenv("LOCAL_ROOT_REPO")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file")

if not LOCAL_ROOT_REPO:
    raise ValueError("LOCAL_ROOT_REPO not found in .env file")

genai.configure(api_key=GEMINI_API_KEY)

model = genai.GenerativeModel("gemini-2.5-flash")

PERSONA_OPTIONS: List[str] = [
    "Work", "Personal", "Learning", "Family", "Health", "Finance", "Creative",
    "Community", "Research", "Hobby", "Travel", "Social", "Technical",
    "Administrative", "Entrepreneurial", "Academic", "Wellness",
    "Productivity", "Civic"
]

# Recursively scan the target folder and return a tree-like structure
def get_directory_structure(root_path: str) -> str:
    """
    Build a human-readable directory tree of LOCAL_ROOT_REPO.
    This is sent to the LLM so it can choose existing folders when helpful.
    """

    root_path = Path(root_path)

    output_lines = []

    for dirpath, dirnames, filenames in os.walk(root_path):
        depth = len(Path(dirpath).relative_to(root_path).parts)
        indent = "  " * depth
        output_lines.append(f"{indent}- {Path(dirpath).name}/")

        for f in filenames:
            output_lines.append(f"{indent}  - {f}")

    return "\n".join(output_lines)


def _build_prompt(desc: str, folder_structure: str, original_filename: str):
    persona_clause = ", ".join(PERSONA_OPTIONS)

    ext = Path(original_filename).suffix  # keeps .pdf / .docx / .txt / etc.

    return f"""
    You are PathGuard 5000, a deterministic and extremely strict file-path router.
    Return only one JSON object. No comments. No markdown.

    STRICT RULES
    1. Output MUST be exactly one JSON object.
    2. Each field ≤ 6 words.
    3. No repetition.
    4. No nested folders unless it already exists.
    5. All values must be plain strings.
    6. No filler words.
    7. The filename MUST end with the ORIGINAL file extension: "{ext}"
    8. The filename MUST have a short but meaningful descriptive base name.

    ALLOWED PERSONAS
    {persona_clause}

    ADDITIONAL CONTEXT
    Below is the current folder structure of the LOCAL_ROOT_REPO.
    If it is possible, clean, and logically correct, use an existing folder
    instead of creating new ones. If not sensible, create a new clean path.

    CURRENT DIRECTORY STRUCTURE
    {folder_structure}

    MANDATORY JSON SCHEMA
    {{
      "persona": "<Persona>",
      "domain": "<2-3 word domain>",
      "category": "<2-4 word category>",
      "topic": "<3-6 word topic>",
      "filename": "<short descriptive name>{ext}",
      "path": "persona/domain/category/topic/filename"
    }}

    INPUT
    Description: \"\"\"{desc.strip()}\"\"\" 

    RESPOND WITH ONLY JSON
    """


def move_file_to_destination(original_file: str, resolved_path: str):
    """
    Move the input file into LOCAL_ROOT_REPO/resolved_path (creating dirs).
    """
    root = Path(LOCAL_ROOT_REPO)

    destination = root / resolved_path
    destination.parent.mkdir(parents=True, exist_ok=True)

    shutil.move(original_file, destination)

    return destination


def main(
    description: str,
    file_path: str,
    model_name: str = "gemini-2.5-flash"
) -> None:
    print()

    # Build structure tree
    structure = get_directory_structure(LOCAL_ROOT_REPO)

    # Build prompt
    prompt = _build_prompt(description, structure, file_path)

    # Gemini request
    response = model.generate_content(prompt)
    raw = response.text.strip()

    # Pretty print JSON if valid
    try:
        if ('```json' in raw): raw = raw.replace('```json', '').replace('```', '')
        parsed = json.loads(raw)
        print("Generated Routing JSON:")
        print(json.dumps(parsed, indent=2))

        # Move the file according to LLM output
        final_path = parsed["path"]
        moved_to = move_file_to_destination(file_path, final_path)

        print("\nFile successfully moved to:")
        print(moved_to)

    except json.JSONDecodeError:
        print("Model did not return valid JSON. Raw output:")
        print(raw)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PathGuard 5000 using Gemini API")
    parser.add_argument("--description", type=str, required=True)
    parser.add_argument("--file", type=str, required=True, help="Path of file to move")
    parser.add_argument("--model", type=str, default="gemini-2.5-flash")
    args = parser.parse_args()

    main(
        args.description,
        args.file,
        args.model,
    )

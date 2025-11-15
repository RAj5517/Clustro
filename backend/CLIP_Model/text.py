# text_backend.py
from pathlib import Path
from typing import Tuple, Dict

import json
import configparser

import pandas as pd
import yaml
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from docx import Document
import xml.etree.ElementTree as ET


class TextBackend:
    """
    Handles loading and simple summarisation of many text/structured file types.

    Supported:
      - .txt, .md, .log
      - .json
      - .csv
      - .xlsx, .xls
      - .xml
      - .html, .htm
      - .yaml, .yml
      - .ini, .cfg, .conf
      - .pdf
      - .docx (.doc is tricky; we treat it as docx if possible)
    """

    def __init__(
        self,
        max_summary_chars: int = 300,
        max_embed_chars: int = 4000,
    ):
        self.max_summary_chars = max_summary_chars
        self.max_embed_chars = max_embed_chars

    def load_and_summarise(self, path: str) -> Tuple[str, str, Dict[str, int]]:
        ext = Path(path).suffix.lower()

        if ext in {".txt", ".md", ".log"}:
            content = self._read_plain_text(path)

        elif ext == ".json":
            content = self._read_json(path)

        elif ext in {".csv"}:
            content = self._read_csv(path)

        elif ext in {".xlsx", ".xls"}:
            content = self._read_excel(path)

        elif ext in {".xml"}:
            content = self._read_xml(path)

        elif ext in {".html", ".htm"}:
            content = self._read_html(path)

        elif ext in {".yaml", ".yml"}:
            content = self._read_yaml(path)

        elif ext in {".ini", ".cfg", ".conf"}:
            content = self._read_ini_cfg(path)

        elif ext == ".pdf":
            content = self._read_pdf(path)

        elif ext == ".docx":
            content = self._read_docx(path)

        elif ext == ".doc":
            # crude fallback – many .doc won't parse cleanly with python-docx
            try:
                content = self._read_docx(path)
            except Exception:
                content = self._read_plain_binary(path)

        else:
            # Fallback: raw text read
            content = self._read_plain_text(path)

        content = content.strip()
        if not content:
            raise RuntimeError(f"Text file is empty or has no extractable text: {path}")

        # Simple summary: first N characters
        summary = content[: self.max_summary_chars]
        if len(content) > self.max_summary_chars:
            summary += "..."

        # Text for embedding: first M characters
        embed_text = content[: self.max_embed_chars]

        meta = {
            "char_len": len(content),
            "summary_len": len(summary),
        }

        return summary, embed_text, meta

    # ---------- individual readers ----------

    def _read_plain_text(self, path: str) -> str:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    def _read_plain_binary(self, path: str) -> str:
        with open(path, "rb") as f:
            data = f.read()
        return f"[binary file {Path(path).name} of {len(data)} bytes]"

    def _read_json(self, path: str) -> str:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            obj = json.load(f)
        # convert to pretty text for CLIP
        return json.dumps(obj, indent=2)

    def _read_csv(self, path: str) -> str:
        df = pd.read_csv(path)
        # Describe columns + few rows
        head = df.head(5).to_markdown(index=False)
        return f"CSV file with columns: {list(df.columns)}\n\nSample rows:\n{head}"

    def _read_excel(self, path: str) -> str:
        # read first sheet
        df = pd.read_excel(path)
        head = df.head(5).to_markdown(index=False)
        return f"Excel file with columns: {list(df.columns)}\n\nSample rows:\n{head}"

    def _read_xml(self, path: str) -> str:
        tree = ET.parse(path)
        root = tree.getroot()

        texts = []

        def recurse(node):
            if node.text and node.text.strip():
                texts.append(node.text.strip())
            for child in node:
                recurse(child)

        recurse(root)
        return "\n".join(texts)

    def _read_html(self, path: str) -> str:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(separator="\n")
        return text

    def _read_yaml(self, path: str) -> str:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            obj = yaml.safe_load(f)
        return yaml.dump(obj)

    def _read_ini_cfg(self, path: str) -> str:
        parser = configparser.ConfigParser()
        parser.read(path, encoding="utf-8")
        lines = []
        for section in parser.sections():
            lines.append(f"[{section}]")
            for key, value in parser.items(section):
                lines.append(f"{key} = {value}")
        return "\n".join(lines)

    def _read_pdf(self, path: str) -> str:
        reader = PdfReader(path)
        pages_text = []
        for page in reader.pages:
            t = page.extract_text() or ""
            pages_text.append(t)
        return "\n".join(pages_text)

    def _read_docx(self, path: str) -> str:
        doc = Document(path)
        paras = [p.text for p in doc.paragraphs if p.text.strip()]
        return "\n".join(paras)
